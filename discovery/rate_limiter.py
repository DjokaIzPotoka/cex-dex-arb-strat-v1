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

    @property
    def rate_per_s(self) -> float:
        """Tokens/second this bucket refills at -- for telemetry only
        (Phase 2a's `rate_limit_rps` field), a static property of the
        bucket's construction, not a live measurement.
        """
        return self._refill_rate

    def fill_pct(self) -> float:
        """Best-effort snapshot of current fill level as a percentage of
        capacity, for telemetry only (Phase 2a's `bucket_fill_pct`
        field). Deliberately does NOT take `self._lock`: `consume()`
        holds that lock for the full duration of any sleep it does
        while waiting for tokens (see `consume()`'s own docstring), so
        a caller blocking on the lock here could stall behind an
        in-progress wait instead of getting an instant read -- exactly
        the "without taking any lock the poll loop needs" requirement
        this exists to satisfy. Reads `self._tokens`/`self._last_refill`
        without synchronization and projects the refill that would have
        happened by now; this is racy (a concurrent `consume()` could
        be mid-refill at the exact same instant) but telemetry only
        needs an approximate gauge, not a linearizable one.
        """
        now = time.monotonic()
        elapsed = max(0.0, now - self._last_refill)
        projected = min(self._capacity, self._tokens + elapsed * self._refill_rate)
        return round(100.0 * projected / self._capacity, 1)

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
