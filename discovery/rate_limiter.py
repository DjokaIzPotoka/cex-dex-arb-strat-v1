"""Generic token-bucket rate limiter, per docs/mev-discovery-plan.md
section 5.4 point 1: "Svaki servis (5 CEX + 4 DEX = 9 izvora, plus RPC
za Tier 2) dobija sopstveni token-bucket rate limiter, podesen na ~90%
objavljenog/orijentacionog limita kao sigurnosnu marginu."

Deliberately venue-agnostic: it knows nothing about "weight",
"requests", or any specific exchange's accounting scheme -- it just
tracks an abstract token count that refills continuously over a
period. Each CEX provider decides what a "token" costs for its own
endpoints (Binance: request-weight, a per-call integer cost from the
exchange's own docs/response; a future Bybit/OKX/etc. provider might
just spend 1 token per request instead). That mapping belongs in each
provider, not here -- this module only ever needs to change if the
bucket algorithm itself needs to change, not every time a new exchange
with a different limit shape gets added.
"""
from __future__ import annotations

import threading
import time


class TokenBucket:
    """A classic token bucket: `capacity` tokens available up front,
    refilling continuously at `capacity / refill_period_s` tokens per
    second, capped at `capacity`. `consume()` blocks (sleeping, not
    busy-waiting) until enough tokens are available, then spends them.

    Thread-safe (a single lock around refill+consume) even though
    nothing in this project is multi-threaded yet -- cheap to get
    right now, expensive to retrofit once several provider instances
    might share one bucket per exchange.
    """

    def __init__(self, capacity: float, refill_period_s: float):
        if capacity <= 0 or refill_period_s <= 0:
            raise ValueError("capacity and refill_period_s must both be positive")
        self._capacity = capacity
        self._refill_rate = capacity / refill_period_s  # tokens/second
        self._tokens = capacity
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill_locked(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._refill_rate)
            self._last_refill = now

    def consume(self, amount: float = 1.0) -> None:
        """Blocks until `amount` tokens are available, then consumes
        them. Raises ValueError if `amount` exceeds the bucket's total
        capacity -- that request could never succeed no matter how
        long it waits, which is a caller bug (a single call costing
        more than the whole budget), not something to silently spin on.
        """
        if amount > self._capacity:
            raise ValueError(
                f"requested {amount} tokens exceeds bucket capacity {self._capacity} -- can never be satisfied"
            )
        with self._lock:
            while True:
                self._refill_locked()
                if self._tokens >= amount:
                    self._tokens -= amount
                    return
                deficit = amount - self._tokens
                wait_s = deficit / self._refill_rate
                time.sleep(wait_s)
