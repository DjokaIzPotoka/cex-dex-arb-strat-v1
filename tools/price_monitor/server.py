#!/usr/bin/env python3
"""Live CEX-vs-DEX price monitor for ZORA.

Reads the same POSIX shared-memory segments the CEX/DEX ingestors write
and the spread detector reads (see shared/include/zora_ipc.h,
ingestor-dex-solana/src/types.rs, spread-detector/src/types.rs) and
serves a small local web UI: one button per pair, each opening a live
line chart of CEX bid vs. DEX price, plus a live table of all pairs.

Stdlib only -- no pip installs, no GUI toolkit. Run the ZORA processes
first (`make start`), then:

    python3 tools/price_monitor/server.py
    # open http://127.0.0.1:8765/

Read-only: never opens any segment for writing, never touches the
trading processes.

This file is pure Python -- all HTML/CSS/JS lives under templates/ and
static/ next to it, and is served fresh off disk on every request (no
caching, no build step). To restyle or rewire the UI, edit those files
and refresh the browser; no server restart needed. See
static/theme.css for the design tokens (colors, fonts) and
static/index.js / static/chart.js for the page logic.
"""
import argparse
import json
import mmap
import os
import struct
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")

# Mirrors ZoraCexPrice from shared/include/zora_ipc.h byte-for-byte:
# double bid, ask, mid; uint64_t timestamp_us; uint32_t sequence;
# char symbol[16]; then 4 trailing pad bytes so the struct's total size
# (56) is a multiple of the 8-byte alignment its doubles require --
# confirmed against spread-detector/src/types.rs's own
# cex_struct_matches_verified_cpp_layout test (size 56, align 8).
CEX_FMT = "<dddQI16s4x"
CEX_SIZE = struct.calcsize(CEX_FMT)
CEX_SEQ_OFFSET = 8 + 8 + 8 + 8  # after bid, ask, mid, timestamp_us
assert CEX_SIZE == 56

# Mirrors ZoraDexPrice from ingestor-dex-solana/src/types.rs: u64
# reserve_sol, reserve_usdc; f64 price; u64 timestamp_us; u32 sequence;
# then 4 pad bytes to re-align u64 slot to an 8-byte boundary --
# confirmed against dex_struct_matches_existing_ingestor_layout (size
# 48, align 8).
DEX_FMT = "<QQdQI4xQ"
DEX_SIZE = struct.calcsize(DEX_FMT)
DEX_SEQ_OFFSET = 8 + 8 + 8 + 8  # after reserve_sol, reserve_usdc, price, timestamp_us
assert DEX_SIZE == 48

# (display label, CEX shm name, DEX shm name, base-token decimals,
# base-token symbol, DEX pool fee pct) -- shm names from
# shared/include/zora_ipc.h and ingestor-dex-solana/src/types.rs;
# decimals and per-pair DEX fees from CLAUDE.md's "Adding a New Trading
# Pair" section, matching spread-detector/src/detector.rs's
# SOL_CLMM_FEE_PCT/RAYDIUM_FEE_PCT/BONK_CLMM_FEE_PCT/
# ZEC_WHIRLPOOL_FEE_PCT/TRX_CLMM_FEE_PCT/KMNO_WHIRLPOOL_FEE_PCT
# constants -- decimals needed to turn a pool's raw reserve_base into a
# human-readable quantity/TVL (same normalization pool.rs's
# calculate_price does for price itself); fee feeds into THRESHOLD
# below, the same way it feeds detector.rs's total_costs_pct.
PAIRS = {
    "SOL_USDC": ("SOL/USDC", "/zora_cex_price", "/zora_dex_price", 9, "SOL", 0.0004),
    "RAY_USDC": ("RAY/USDC", "/zora_cex_ray", "/zora_dex_ray", 6, "RAY", 0.0025),
    "BONK_USDC": ("BONK/USDC", "/zora_cex_bonk", "/zora_dex_bonk", 5, "BONK", 0.0015),
    "ZEC_USDC": ("ZEC/USDC", "/zora_cex_zec", "/zora_dex_zec", 8, "ZEC", 0.0016),
    "TRX_USDC": ("TRX/USDC", "/zora_cex_trx", "/zora_dex_trx", 6, "TRX", 0.0005),
    "KMNO_USDC": ("KMNO/USDC", "/zora_cex_kmno", "/zora_dex_kmno", 6, "KMNO", 0.0001),
}

# USDC's own decimals (matches ingestor-dex-solana/src/pool.rs's
# USDC_DECIMALS constant) -- used the same way here, to normalize a
# pool's raw reserve_usdc into a human-readable quantity.
USDC_DECIMALS = 6

# Mirrors spread-detector/src/detector.rs's JITO_TIP_PCT and
# BINANCE_TAKER_FEE_PCT -- the round-trip cost components that don't
# depend on which DEX pool a pair trades against. Combined with each
# pair's own DEX fee (in PAIRS above) via total_costs_pct below, this
# is exactly the threshold gross spread has to clear for the spread
# detector to ever classify a reading as a genuinely profitable
# Opportunity rather than just Interesting.
JITO_TIP_PCT = 0.0010
BINANCE_TAKER_FEE_PCT = 0.00075


def total_costs_pct(dex_fee_pct: float) -> float:
    return dex_fee_pct + JITO_TIP_PCT + BINANCE_TAKER_FEE_PCT


# Same default thresholds as spread-detector's CEX_STALE_MS/DEX_STALE_MS
# (spread-detector/src/main.rs) -- used here only to color the age
# readout, not to hide data: unlike the detector, this tool still plots
# the last known price past this age so you can *see* a feed go stale
# instead of the line just vanishing.
CEX_STALE_MS = 500
DEX_STALE_MS = 1000


def now_micros() -> int:
    return time.time_ns() // 1000


def shm_path(name: str) -> str:
    # POSIX shm_open("/foo") on Linux surfaces as /dev/shm/foo.
    return f"/dev/shm/{name.lstrip('/')}"


def read_segment(path: str, fmt: str, size: int, seq_offset: int):
    """Sequence-lock read of one segment. Returns the unpacked tuple,
    or None if the segment doesn't exist (yet) or a consistent snapshot
    couldn't be observed. Opens and closes the mapping fresh every call
    -- deliberately not cached like the Rust/C++ readers -- so an
    ingestor restart (segment unlinked and recreated under the same
    name) is picked up automatically on the next poll with no separate
    reopen logic needed. This tool polls at most a few times a second,
    so the extra open/mmap/close cost is negligible.
    """
    try:
        fd = os.open(path, os.O_RDONLY)
    except FileNotFoundError:
        return None
    try:
        try:
            mm = mmap.mmap(fd, size, mmap.MAP_SHARED, mmap.PROT_READ)
        except (ValueError, OSError):
            # e.g. segment exists but hasn't been sized to `size` yet.
            return None
        try:
            # Same pattern as ShmReader::read_cex/read_dex in
            # spread-detector/src/shm.rs: read the sequence before and
            # after snapshotting the payload, retry if they differ.
            for _ in range(200):
                before = struct.unpack_from("<I", mm, seq_offset)[0]
                data = mm[:size]
                after = struct.unpack_from("<I", mm, seq_offset)[0]
                if before == after:
                    return struct.unpack(fmt, data)
            return None
        finally:
            mm.close()
    finally:
        os.close(fd)


def build_snapshot(pair_key: str) -> dict:
    label, cex_name, dex_name, base_decimals, base_symbol, dex_fee_pct = PAIRS[pair_key]
    now_us = now_micros()
    # Percent, e.g. 0.425 for RAY/USDC -- the gross spread this pair
    # needs to clear before it's a genuine, cost-covered opportunity,
    # not just noise. Static per pair (fees don't change live), so
    # computed fresh each call rather than cached.
    threshold_pct = total_costs_pct(dex_fee_pct) * 100

    cex = None
    cex_raw = read_segment(shm_path(cex_name), CEX_FMT, CEX_SIZE, CEX_SEQ_OFFSET)
    if cex_raw is not None:
        bid, ask, mid, ts_us, seq, _symbol = cex_raw
        # ts_us == 0 means "mapped but never written" (writers zero the
        # segment on mmap before their first real write) -- not data.
        if ts_us != 0:
            cex = {
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "seq": seq,
                "age_ms": (now_us - ts_us) / 1000.0,
                "stale": (now_us - ts_us) / 1000.0 > CEX_STALE_MS,
            }

    dex = None
    dex_raw = read_segment(shm_path(dex_name), DEX_FMT, DEX_SIZE, DEX_SEQ_OFFSET)
    if dex_raw is not None:
        reserve_base_raw, reserve_usdc_raw, price, ts_us, seq, slot = dex_raw
        if ts_us != 0:
            # Same normalization as calculate_price in
            # ingestor-dex-solana/src/pool.rs: raw reserves are in the
            # token's smallest unit, not human-readable quantities.
            reserve_base = reserve_base_raw / 10**base_decimals
            reserve_usdc = reserve_usdc_raw / 10**USDC_DECIMALS
            dex = {
                "price": price,
                "seq": seq,
                "slot": slot,
                "age_ms": (now_us - ts_us) / 1000.0,
                "stale": (now_us - ts_us) / 1000.0 > DEX_STALE_MS,
                "reserve_base": reserve_base,
                "reserve_usdc": reserve_usdc,
                "base_symbol": base_symbol,
                # Pool TVL, in USD: the quote side is already USDC (~$1),
                # the base side is converted at this pool's own price --
                # not an external quote, so it stays internally
                # consistent with the price shown next to it.
                "tvl_usd": reserve_usdc + reserve_base * price,
            }

    return {
        "pair": label,
        "pair_key": pair_key,
        "ts_us": now_us,
        "cex": cex,
        "dex": dex,
        "threshold_pct": threshold_pct,
    }


MIME_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # quiet -- this is a read-only viewer, not a service worth logging

    def _send(self, status: int, content_type: str, body: bytes):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_file(self, directory: str, rel_path: str, content_type: str):
        # Read fresh off disk on every request (no caching) so editing
        # templates/static files takes effect on the next browser
        # refresh, no server restart. normpath + startswith guards
        # against `rel_path` (which comes from the URL) escaping
        # `directory` via "..".
        full_path = os.path.normpath(os.path.join(directory, rel_path))
        if not full_path.startswith(os.path.normpath(directory) + os.sep):
            self._send(403, "text/plain", b"forbidden")
            return
        try:
            with open(full_path, "rb") as f:
                body = f.read()
        except (FileNotFoundError, IsADirectoryError):
            self._send(404, "text/plain", b"not found")
            return
        self._send(200, content_type, body)

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/":
            self._serve_file(TEMPLATES_DIR, "index.html", MIME_TYPES[".html"])
            return

        if parsed.path == "/chart":
            self._serve_file(TEMPLATES_DIR, "chart.html", MIME_TYPES[".html"])
            return

        if parsed.path.startswith("/static/"):
            rel_path = parsed.path[len("/static/"):]
            ext = os.path.splitext(rel_path)[1]
            content_type = MIME_TYPES.get(ext, "application/octet-stream")
            self._serve_file(STATIC_DIR, rel_path, content_type)
            return

        if parsed.path == "/api/pairs":
            pairs = [{"key": key, "label": entry[0]} for key, entry in PAIRS.items()]
            self._send(200, "application/json", json.dumps(pairs).encode())
            return

        if parsed.path == "/api/prices":
            qs = parse_qs(parsed.query)
            pair_key = qs.get("pair", [""])[0]
            if pair_key not in PAIRS:
                self._send(404, "application/json", b'{"error":"unknown pair"}')
                return
            snapshot = build_snapshot(pair_key)
            self._send(200, "application/json", json.dumps(snapshot).encode())
            return

        if parsed.path == "/api/all":
            snapshots = [build_snapshot(pair_key) for pair_key in PAIRS]
            self._send(200, "application/json", json.dumps({"pairs": snapshots}).encode())
            return

        self._send(404, "text/plain", b"not found")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1", help="bind address (default: 127.0.0.1, local-only)")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"ZORA price monitor: http://{args.host}:{args.port}/  (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
