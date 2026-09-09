#!/usr/bin/env python3
"""Phase 2b: read-only ops dashboard for a discovery run.

Serves discovery/dashboard/static/index.html and three read-only JSON
endpoints, backed by the Phase 2a heartbeat file (discovery/src/
telemetry.py), the `cycle_telemetry` ClickHouse table it writes, and
the scheduler's own log file. This module produces no data itself --
it only reads what the scheduler (discovery/src/scheduler.py) and its
RunTelemetry instance already write.

Hard constraints, all deliberate -- see the Phase 2b task spec this
module was built against:

- Separate process, separate lifecycle. Not started by
  `make discovery-start` (see scripts/zora_discovery_dashboard.sh /
  `make discovery-dashboard` instead) -- if this process crashes, or is
  never started at all, the scheduler must not notice or be affected
  in any way.
- Read-only, all the way down. No writes to ClickHouse (the query
  client below is opened with `settings={"readonly": 1}`, which makes
  ClickHouse itself reject any non-SELECT statement server-side, as a
  belt-and-suspenders on top of this module simply never issuing one),
  no control endpoints, no start/stop/promote/demote actions. Nothing
  in this service can mutate scheduler state.
- No imports from the scheduler's runtime modules beyond pure
  constants -- and even those are inlined as literals below (see
  SCHEMA_VERSION's comment) rather than imported, since importing
  discovery/src/telemetry.py would transitively import
  discovery/src/providers/solana_rpc_state.py (a provider module) just
  to reach a couple of small helper functions, which is exactly the
  "accidentally construct a client/TokenBucket via a provider import"
  failure mode this constraint exists to avoid. discovery/src/logger.py
  is the one exception: it's a stateless formatting helper (no client,
  no bucket, no network), reused here purely for log-line consistency
  with the rest of the subsystem.
- Its own ClickHouse client, constructed fresh in this module, never
  the scheduler's own `client` object (which this process has no
  access to anyway -- it's a separate OS process). No read-only
  ClickHouse user exists in discovery/docker-compose.yml today (only
  the single `default` user) -- noted here rather than silently
  assumed away; CLICKHOUSE_USER/CLICKHOUSE_PASSWORD env vars are
  provided below so a future read-only user can be wired in without a
  code change, but until then this connects as the same `default` user
  discovery/src/db.py uses, relying on the `readonly` query setting
  above as the actual enforcement.
- Binds 127.0.0.1 by default -- there is no authentication anywhere in
  this module, so binding 0.0.0.0 by default would expose an
  unauthenticated read surface (including ClickHouse row contents and
  raw scheduler log lines) to the whole network. DISCOVERY_DASHBOARD_BIND
  overrides this for an operator who has their own reason to (e.g. a
  reverse proxy on the same host); it is on them to add auth in front
  if they do.
- Standard-library `http.server.ThreadingHTTPServer` only -- this is a
  single-operator tool polled every 2-30s per endpoint (see
  static/index.html's fetch intervals), nowhere near where FastAPI/
  uvicorn/Flask would earn their weight, and Phase 2b's spec calls this
  out explicitly (the previous FastAPI ops-dashboard attempt was
  removed for being a poor fit for this subsystem's dependency posture
  -- see CLAUDE.md's "Ops dashboard" section).

Out of scope, deliberately, per the Phase 2b spec -- do not add these
in a later session without an explicit new decision to do so: auth,
TLS, multi-run comparison, alerting, and any control action (start/
stop/promote/demote/replay/anything else that would write to
ClickHouse or signal the scheduler process).

Usage:
    python3 discovery/dashboard/server.py
"""
from __future__ import annotations

import datetime
import json
import mimetypes
import os
import pathlib
import re
import sys
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from discovery.src.logger import Level, log  # noqa: E402 -- stateless formatting helper only, see module docstring

DASH_SRC = "DSH"

# This file lives at discovery/dashboard/server.py -- two parents up is
# the repo root (dashboard -> discovery -> repo root), same technique
# discovery/src/telemetry.py and discovery/src/scheduler.py already use
# for their own path-relative-to-repo-root constants.
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
STATIC_DIR = pathlib.Path(__file__).resolve().parent / "static"

# Mirrors discovery/src/telemetry.py's own DEFAULT_STATUS_FILE and
# SCHEMA_VERSION exactly (same env var name, same default path, same
# version literal) -- kept as independent literals here rather than
# imported, per this module's "no scheduler-runtime imports" docstring
# note above. If telemetry.py's schema or default path ever changes,
# this constant must be updated to match by hand.
DEFAULT_STATUS_FILE = _REPO_ROOT / "discovery" / "run" / "status.json"
STATUS_FILE = pathlib.Path(os.environ.get("DISCOVERY_STATUS_FILE") or str(DEFAULT_STATUS_FILE))
SCHEMA_VERSION = 1

# scripts/zora_discovery.sh's own SCHED_LOG constant -- confirmed by
# reading that script rather than assumed, per the task spec's explicit
# "check that, do not guess" instruction.
DEFAULT_LOG_FILE = _REPO_ROOT / "logs" / "zora_discovery_sched.log"
LOG_FILE = pathlib.Path(os.environ.get("DISCOVERY_LOG_FILE") or str(DEFAULT_LOG_FILE))

BIND = os.environ.get("DISCOVERY_DASHBOARD_BIND", "127.0.0.1")
PORT = int(os.environ.get("DISCOVERY_DASHBOARD_PORT", "8787"))

HEARTBEAT_STALE_S = 30.0

HISTORY_LIMIT_DEFAULT = 40
HISTORY_LIMIT_MAX = 200

LOGS_LIMIT_DEFAULT = 220
LOGS_LIMIT_MAX = 2000
# How much of the log file's tail to read looking for LOGS_LIMIT_MAX
# lines, doubling up to this cap if the first read comes up short (a
# very verbose recent burst could need more than one chunk to reach
# `limit` lines). Never reads the whole file -- these logs get long
# (see the task spec) -- so a file smaller than this cap is read in
# full, a larger one only ever from its tail.
_LOG_TAIL_CHUNK_S = 65536
_LOG_TAIL_CHUNK_MAX = 8 * 1024 * 1024

# `HH:MM:SS.mmm  LEVEL  SRC  key=value key=value...` -- discovery/src/
# logger.py's exact format (also spread-detector/src/logger.rs's, per
# CLAUDE.md's "Conventions" section). `\s+` rather than a fixed count
# of spaces since Level's own enum values ("INFO ", "WARN ", "ERROR")
# aren't uniform width before the f-string's own "  " separators are
# added on top -- see logger.py.
_LOG_LINE_RE = re.compile(r"^(\d{2}:\d{2}:\d{2}\.\d{3})\s+(\S+)\s+(\S+)\s+(.*)$")
_KV_RE = re.compile(r"(\S+)=(\S*)")


# --- ClickHouse -------------------------------------------------------


def _get_clickhouse_client():
    """A fresh, read-only-by-setting client, independent of the
    scheduler's own (discovery/src/db.py) -- see module docstring.
    Same connection env vars as db.py so an operator's existing
    ClickHouse configuration just works, plus CLICKHOUSE_USER/
    CLICKHOUSE_PASSWORD (db.py doesn't need these today since it only
    ever connects as the compose file's single `default` user) for a
    future read-only user without a code change.
    """
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        database=os.environ.get("CLICKHOUSE_DB", "zora_discovery"),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        settings={"readonly": 1},
    )


# --- /api/status --------------------------------------------------------


def _read_status_payload() -> dict:
    """Never raises. A missing file is a normal state (scheduler not
    running), not an error -- see this endpoint's spec. A corrupt file
    (shouldn't happen -- telemetry.py writes via tmp-file + os.replace
    -- but this process doesn't control that file, so it's handled
    defensively anyway) degrades to the same "not_running"-shaped
    response rather than a 500.
    """
    try:
        raw = STATUS_FILE.read_bytes()
    except FileNotFoundError:
        return {"schema_version": SCHEMA_VERSION, "scheduler": "not_running"}
    except OSError as exc:
        return {"schema_version": SCHEMA_VERSION, "scheduler": "not_running", "read_error": str(exc)}

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        return {"schema_version": SCHEMA_VERSION, "scheduler": "not_running", "read_error": f"corrupt status file: {exc}"}


def build_status_response() -> dict:
    payload = _read_status_payload()
    if payload.get("scheduler") == "not_running":
        return payload

    heartbeat_at = payload.get("heartbeat_at")
    age_s: Optional[float] = None
    if heartbeat_at:
        try:
            dt = datetime.datetime.fromisoformat(heartbeat_at)
            age_s = (datetime.datetime.now(datetime.timezone.utc) - dt).total_seconds()
        except ValueError:
            age_s = None

    payload["heartbeat_age_s"] = round(age_s, 1) if age_s is not None else None
    payload["heartbeat_stale"] = bool(age_s is not None and age_s > HEARTBEAT_STALE_S)

    if payload.get("schema_version") != SCHEMA_VERSION:
        payload["schema_mismatch"] = True

    return payload


# --- /api/history --------------------------------------------------------


def build_history_response(limit: int) -> dict:
    """One buffered read against cycle_telemetry: the latest run_id,
    that run's most recent `limit` cycle_n's, then every segment row
    for those cycles -- three parameterized queries (no string-
    formatted SQL, per the task spec), not N+1. Never raises past this
    function: any ClickHouse error (down, unreachable, table missing)
    comes back as {"error": ...} with an empty cycle list so the
    caller can render a 200 either way -- see this endpoint's
    "history fails, status stays fine" degraded-state requirement.
    """
    limit = max(1, min(limit, HISTORY_LIMIT_MAX))
    try:
        client = _get_clickhouse_client()
        try:
            run_row = client.query("SELECT run_id FROM cycle_telemetry ORDER BY cycle_started_at DESC LIMIT 1")
            if not run_row.result_rows:
                return {"run_id": None, "cycles": []}
            run_id = run_row.result_rows[0][0]

            cycle_rows = client.query(
                "SELECT cycle_n, min(cycle_started_at) AS started_at "
                "FROM cycle_telemetry WHERE run_id = {run_id:String} "
                "GROUP BY cycle_n ORDER BY started_at DESC LIMIT {limit:UInt32}",
                parameters={"run_id": run_id, "limit": limit},
            )
            cycle_ns = [row[0] for row in cycle_rows.result_rows]
            if not cycle_ns:
                return {"run_id": run_id, "cycles": []}

            seg_rows = client.query(
                "SELECT cycle_n, cycle_started_at, segment_idx, segment_name, segment_kind, "
                "duration_s, rows_written, failed, error_class, extra "
                "FROM cycle_telemetry "
                "WHERE run_id = {run_id:String} AND cycle_n IN {cycle_ns:Array(UInt32)} "
                "ORDER BY cycle_n ASC, segment_idx ASC",
                parameters={"run_id": run_id, "cycle_ns": cycle_ns},
            ).result_rows
        finally:
            client.close()
    except Exception as exc:  # noqa: BLE001 -- ClickHouse down/unreachable/schema drift, all degrade the same way
        return {"error": str(exc), "cycles": []}

    cycles: dict[int, dict] = {}
    for cycle_n, started_at, seg_idx, name, kind, dur_s, rows_written, failed, error_class, extra in seg_rows:
        cyc = cycles.setdefault(
            cycle_n,
            {"n": cycle_n, "started_at": _iso(started_at), "total_s": 0.0, "rows_total": 0, "failed": False, "segments": []},
        )
        extra_parsed = None
        if extra:
            try:
                extra_parsed = json.loads(extra)
            except json.JSONDecodeError:
                extra_parsed = extra
        cyc["segments"].append(
            {
                "idx": seg_idx,
                "name": name,
                "kind": kind,
                "duration_s": round(float(dur_s), 1),
                "rows": int(rows_written),
                "failed": bool(failed),
                "error_class": error_class or None,
                "extra": extra_parsed,
            }
        )
        cyc["total_s"] += float(dur_s)
        cyc["rows_total"] += int(rows_written)
        cyc["failed"] = cyc["failed"] or bool(failed)

    ordered = [cycles[n] for n in sorted(cycles)]
    for cyc in ordered:
        cyc["total_s"] = round(cyc["total_s"], 1)

    return {"run_id": run_id, "cycles": ordered}


def _iso(dt) -> Optional[str]:
    if dt is None:
        return None
    if isinstance(dt, datetime.datetime):
        return dt.isoformat()
    return str(dt)


# --- /api/logs --------------------------------------------------------


def _tail_text(path: pathlib.Path, min_lines: int) -> list[str]:
    """Reads only the tail of `path`, growing the read window (up to
    _LOG_TAIL_CHUNK_MAX) until at least `min_lines` whole lines are
    captured or the start of the file is reached -- never reads the
    whole file up front for a log that could be very large. The first
    line of a partial read is dropped (it's very likely truncated
    mid-line by the seek point).
    """
    try:
        size = path.stat().st_size
    except OSError:
        return []
    if size == 0:
        return []

    read_size = min(size, _LOG_TAIL_CHUNK_S)
    with open(path, "rb") as f:
        while True:
            partial = read_size < size
            f.seek(-read_size if partial else 0, os.SEEK_END if partial else os.SEEK_SET)
            data = f.read()
            lines = data.split(b"\n")
            if partial and lines:
                lines = lines[1:]  # likely-truncated leading partial line
            if len(lines) >= min_lines or not partial or read_size >= _LOG_TAIL_CHUNK_MAX:
                break
            read_size = min(read_size * 4, _LOG_TAIL_CHUNK_MAX, size)

    # rstrip("\r") only -- scripts/zora_discovery.sh's tmux pipe-pane
    # writes CRLF line endings into this file (its `sed -u` strips ANSI
    # codes but not the carriage return); that's a display artifact of
    # how the file got written, not meaningful log content, so it's
    # normalized away here rather than surfacing as a literal \r in
    # every line's `raw` field.
    return [ln.decode("utf-8", errors="replace").rstrip("\r") for ln in lines if ln.strip()]


def _parse_log_line(line: str) -> dict:
    m = _LOG_LINE_RE.match(line)
    if not m:
        # Malformed line, not dropped -- see this endpoint's spec: "a
        # malformed line during an incident is exactly the line worth
        # seeing".
        return {"ts": None, "level": "RAW", "src": None, "fields": {}, "raw": line}
    ts, level, src, rest = m.groups()
    fields = dict(_KV_RE.findall(rest))
    return {"ts": ts, "level": level, "src": src, "fields": fields, "raw": line}


def build_logs_response(level: Optional[str], limit: int) -> dict:
    limit = max(1, min(limit, LOGS_LIMIT_MAX))
    if not LOG_FILE.exists():
        return {"lines": [], "log_file": str(LOG_FILE), "log_file_missing": True}

    try:
        raw_lines = _tail_text(LOG_FILE, min_lines=limit * 3 if level else limit)
    except OSError as exc:
        return {"lines": [], "log_file": str(LOG_FILE), "log_file_missing": False, "error": str(exc)}

    parsed = [_parse_log_line(ln) for ln in raw_lines]
    if level:
        level_upper = level.upper()
        parsed = [p for p in parsed if (p["level"] or "").upper() == level_upper]

    tail = parsed[-limit:]
    tail.reverse()  # newest first, matching the run-log panel's display order
    return {"lines": tail, "log_file": str(LOG_FILE), "log_file_missing": False}


# --- HTTP handler --------------------------------------------------------


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "ZoraDiscoveryDashboard/1.0"

    def log_message(self, fmt, *args):  # noqa: A003 -- BaseHTTPRequestHandler's own name
        log(Level.INFO, [("event", "http_request"), ("line", fmt % args)], src=DASH_SRC)

    def do_GET(self) -> None:  # noqa: N802 -- BaseHTTPRequestHandler's own naming convention
        try:
            parts = urlsplit(self.path)
            path = parts.path
            query = parse_qs(parts.query)

            if path == "/":
                self._serve_static("index.html")
            elif path == "/api/status":
                self._send_json(200, build_status_response())
            elif path == "/api/history":
                limit = _int_param(query, "limit", HISTORY_LIMIT_DEFAULT)
                self._send_json(200, build_history_response(limit))
            elif path == "/api/logs":
                level = (query.get("level") or [None])[0]
                limit = _int_param(query, "limit", LOGS_LIMIT_DEFAULT)
                self._send_json(200, build_logs_response(level, limit))
            elif path.startswith("/"):
                self._serve_static(path[1:])
            else:
                self._send_json(404, {"error": "not found"})
        except Exception as exc:  # noqa: BLE001 -- one request's bug must not take down the server/other requests
            log(Level.ERROR, [("event", "request_FAILED"), ("path", self.path), ("err", str(exc))], src=DASH_SRC)
            traceback.print_exc()
            try:
                self._send_json(500, {"error": "internal error"})
            except Exception:  # noqa: BLE001 -- connection may already be broken
                pass

    def _serve_static(self, rel_path: str) -> None:
        candidate = _safe_static_path(rel_path)
        if candidate is None:
            self._send_json(404, {"error": "not found"})
            return
        content_type, _ = mimetypes.guess_type(str(candidate))
        data = candidate.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_json(self, status: int, obj: dict) -> None:
        data = json.dumps(_json_safe(obj), default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def _json_safe(obj):
    """Recursively replaces non-finite floats (NaN/Infinity/-Infinity)
    with None. Confirmed live: a Tier 2 hot pool's tvl_growth_24h can
    be a literal `float('inf')` (division against a ~0 prior value in
    tier2.py's growth math) by the time it reaches status.json.
    `json.dumps` happily emits the bare token `Infinity` for that
    (Python's json module isn't strict-JSON by default) and
    telemetry.py's own `_write_status_file()` doesn't pass
    allow_nan=False either -- so the heartbeat file on disk can
    already contain it. That token is NOT valid JSON per spec, and
    every browser's JSON.parse (unlike Python's lenient json.loads)
    throws a hard SyntaxError on it -- which would otherwise take out
    this endpoint's entire response client-side over one field deep in
    a 300-pool list. Applied centrally in _send_json so every response
    body is covered, not just /api/status.
    """
    if isinstance(obj, float):
        if obj != obj or obj in (float("inf"), float("-inf")):  # obj != obj is NaN
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    return obj


def _int_param(query: dict, name: str, default: int) -> int:
    try:
        return int(query[name][0])
    except (KeyError, IndexError, ValueError):
        return default


def _safe_static_path(rel_path: str) -> Optional[pathlib.Path]:
    """Serves only files inside STATIC_DIR -- rejects any path that
    resolves outside it (`..` traversal, absolute-path smuggling via
    a leading `//`, symlinks pointing out, etc).
    """
    rel_path = rel_path.lstrip("/") or "index.html"
    static_root = STATIC_DIR.resolve()
    candidate = (STATIC_DIR / rel_path).resolve()
    try:
        candidate.relative_to(static_root)
    except ValueError:
        return None
    if not candidate.is_file():
        return None
    return candidate


def main() -> None:
    server = ThreadingHTTPServer((BIND, PORT), DashboardHandler)
    url = f"http://{BIND}:{PORT}"
    log(
        Level.INFO,
        [
            ("event", "dashboard_start"),
            ("bind", BIND),
            ("port", str(PORT)),
            ("status_file", str(STATUS_FILE)),
            ("log_file", str(LOG_FILE)),
        ],
        src=DASH_SRC,
    )
    print(f"ZORA discovery dashboard listening on {url}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        log(Level.INFO, [("event", "dashboard_stopped")], src=DASH_SRC)


if __name__ == "__main__":
    main()
