"""Phase 2a: run telemetry for the discovery scheduler.

Not part of docs/mev-discovery-plan.md (that document predates this
work and doesn't mention telemetry/dashboarding at all -- there is no
disagreement to resolve against it, just a gap it never covered).
This module adds NO dashboard and NO UI, per this session's explicit
scope -- it only produces data for a separate, later dashboard process
(Phase 2b) to render: a live JSON heartbeat file (volatile, in-process
state -- which segment is executing, Tier2State's registries,
MeteoraCursor's page range, TokenBucket fill, consecutive failure
counts) and a durable ClickHouse table (per-cycle segment history --
durations, rows, failures, independently useful as the measurement
layer for the 300s-cycle-target work, not just dashboard fuel).

Design constraints this module exists to satisfy, all deliberate:

- Zero new third-party dependencies -- standard library plus the
  ClickHouse client the scheduler already holds.
- Fully no-op-able: telemetry must never raise into the scheduler's
  poll loop and must never abort a cycle, no matter what breaks on its
  own side (an unwritable status file path, a ClickHouse insert
  failure, or a bug in this module itself). Every public method wraps
  its own body and swallows what it catches; `segment()` is the one
  exception to "swallow everything" -- it must re-raise whatever the
  wrapped block raised (per-source failure isolation in scheduler.py
  is untouched by this module and must stay exactly where it already
  is), it just also records that the block failed before doing so.
- Edge-triggered WARN logging for the two expected failure modes (an
  unwritable status file, a down ClickHouse) -- one WARN when the
  failure starts, silence for as long as it persists, one more line
  when it recovers. `cycle_end()`'s ClickHouse insert and
  `checkpoint()`'s file write are each called every few seconds to
  every few minutes for as long as the scheduler runs; logging a WARN
  on every single call during a real outage (which could last hours)
  would drown the log in a message that says nothing new after the
  first line. A genuinely unexpected bug inside this module itself
  (not one of those two expected failure shapes) still gets logged
  every time it happens, deliberately not deduplicated -- that
  indicates an actual defect in this module worth seeing promptly, not
  a routine external outage to ride out quietly.
- `checkpoint()` is called at every one of scheduler.py's 9 per-cycle
  checkpoints (same sites tier2_poller.maybe_poll() is already checked
  at), so it must be cheap -- no ClickHouse query, no filesystem walk,
  just serializing whatever this instance already holds in memory. The
  Tier 2 "pools" list embedded in the status file is exactly this
  module's job to keep cheap, not tier2_poller.py's: it's a passive
  recorder of whatever record_tier2_snapshot() was last handed (itself
  built from a poll pass's own already-in-memory rows, not a fresh
  query), with a defensive cap (MAX_TIER2_POOLS_IN_STATUS) on top in
  case a caller ever hands it something larger than expected.
"""
from __future__ import annotations

import datetime
import json
import os
import pathlib
import time
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Optional

from discovery.src.logger import Level, log
from discovery.src.providers.solana_rpc_state import (
    QUICKNODE_BATCH_LIMIT,
    get_http_413_total,
    get_last_call_ok,
    get_last_latency_ms,
)

SCHEMA_VERSION = 1
TELEMETRY_SRC = "TEL"

# This file lives at discovery/src/telemetry.py -- two parents up is
# the repo root, same technique scheduler.py/run_migrations.py already
# use. Needed so the default status file path doesn't depend on the
# scheduler's CWD at launch: scripts/zora_discovery.sh's tmux pane
# doesn't guarantee CWD == repo root, only that PYTHON_BIN/
# SCHEDULER_SCRIPT are absolute paths.
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
DEFAULT_STATUS_FILE = _REPO_ROOT / "discovery" / "run" / "status.json"

# Defensive cap on how many Tier 2 pool entries get embedded in
# status.json -- see record_tier2_snapshot()'s docstring. In steady
# state this is never reached: a poll pass's own per-venue batch is
# already capped at tier2_poller.py's MAX_POOLS_PER_VENUE_PER_POLL
# (100) across at most 4 DEX venues, well under this.
MAX_TIER2_POOLS_IN_STATUS = 300

# Source health-state thresholds (Phase 2a's own scheme -- the design
# doc doesn't define one, nothing to disagree with). ok = succeeded
# last cycle; stale = failed last cycle but succeeded within the last
# BAD_AFTER_CONSECUTIVE_FAILURES cycles; bad = that many or more
# consecutive failures.
BAD_AFTER_CONSECUTIVE_FAILURES = 3


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _iso(dt: Optional[datetime.datetime]) -> Optional[str]:
    return dt.isoformat() if dt is not None else None


@dataclass
class _Segment:
    """Mutable handle yielded by RunTelemetry.segment()'s `with` block.
    Callers set `rows`/`failed`/`error_class`/`extra` during the block;
    `duration_s` is filled in by segment() itself on exit.
    """

    name: str
    kind: str
    started_at: datetime.datetime
    mono_start: float
    duration_s: float = 0.0
    rows: int = 0
    failed: bool = False
    error_class: str = ""
    extra: dict = field(default_factory=dict)


@dataclass
class _SourceState:
    category: str  # "dex" | "cex"
    state: str = "ok"
    consecutive_failures: int = 0
    last_duration_s: float = 0.0
    last_rows: int = 0
    last_success_at: Optional[datetime.datetime] = None
    last_error: Optional[str] = None
    bucket_fill_pct: Optional[float] = None
    rate_limit_rps: Optional[float] = None
    partial_coverage: bool = False


class RunTelemetry:
    """Constructed once at scheduler startup, same lifetime as
    Tier2State/MeteoraCursor (see scheduler.py's main()). See module
    docstring for the no-op-able/cheap-checkpoint contracts every
    method below honors.
    """

    def __init__(self, client, target_cycle_s: float = 300.0, status_file_path: Optional[str] = None):
        self._client = client
        self._target_cycle_s = target_cycle_s
        self._status_file_path = pathlib.Path(
            status_file_path or os.environ.get("DISCOVERY_STATUS_FILE") or str(DEFAULT_STATUS_FILE)
        )

        now = _utcnow()
        self.run_id = f"disc_{now.strftime('%Y%m%d_%H%M')}"
        self._started_at = now

        self._cycle_n = 0
        self._cycle_started_at: Optional[datetime.datetime] = None
        self._cycle_t0: Optional[float] = None
        self._current_segment: Optional[_Segment] = None
        self._completed_segments: list[_Segment] = []  # this cycle only, for status.json
        self._cycle_rows_buffer: list[list] = []  # this cycle's cycle_telemetry insert rows

        self._sources: dict[str, _SourceState] = {}
        self._tier2: dict = {}
        self._tier2_polls_this_cycle = 0
        self._meteora_cursor: dict = {}

        self._counters = {
            "rows_total": 0,
            "cycles_completed": 0,
            "new_entities_total": 0,
            "errors_total": 0,
        }

        self._ch_ok = True
        self._ch_last_error: Optional[str] = None
        self._ch_last_insert_ms: Optional[float] = None
        self._ch_warned = False
        self._file_warned = False

        log(
            Level.INFO,
            [("event", "telemetry_start"), ("run_id", self.run_id), ("status_file", str(self._status_file_path))],
            src=TELEMETRY_SRC,
        )

    # --- Public API -------------------------------------------------

    def cycle_start(self, n: int) -> None:
        try:
            self._cycle_n = n
            self._cycle_started_at = _utcnow()
            self._cycle_t0 = time.monotonic()
            self._completed_segments = []
            self._cycle_rows_buffer = []
            self._tier2_polls_this_cycle = 0
        except Exception as exc:  # noqa: BLE001 -- must never break the poll loop
            self._log_internal_bug("cycle_start", exc)

    @contextmanager
    def segment(
        self,
        name: str,
        kind: str,
        *,
        category: Optional[str] = None,
        bucket=None,
        partial_coverage: bool = False,
    ):
        """`kind` is one of "source", "tier2_poll", "detection".
        `category` ("dex"/"cex") and `bucket` (a TokenBucket, read via
        its lock-free fill_pct()/rate_per_s) only matter for
        kind="source". Never swallows an exception raised by the
        wrapped block -- per-source failure isolation stays exactly
        where scheduler.py already puts it; this only records that the
        block failed before re-raising.
        """
        seg = _Segment(name=name, kind=kind, started_at=_utcnow(), mono_start=time.monotonic())
        self._current_segment = seg
        try:
            yield seg
        except Exception as exc:
            seg.failed = True
            seg.error_class = type(exc).__name__
            raise
        finally:
            seg.duration_s = time.monotonic() - seg.mono_start
            try:
                self._finish_segment(seg, category=category, bucket=bucket, partial_coverage=partial_coverage)
            except Exception as exc:  # noqa: BLE001
                self._log_internal_bug("segment_finish", exc)
            self._current_segment = None

    def checkpoint(self) -> None:
        """Rewrites the heartbeat file. Cheap by construction -- see
        module docstring -- safe to call often.
        """
        try:
            self._write_status_file()
        except Exception as exc:  # noqa: BLE001 -- truly unexpected (the file-write path itself
            # already handles OSError internally); a bug here must not
            # break the caller.
            self._log_internal_bug("checkpoint", exc)

    def cycle_end(self) -> None:
        """One buffered INSERT of the whole cycle's segments into
        cycle_telemetry, then a final checkpoint() for this cycle.
        """
        try:
            self._flush_cycle_telemetry()
            self._counters["cycles_completed"] += 1
        except Exception as exc:  # noqa: BLE001
            self._log_internal_bug("cycle_end", exc)
        self.checkpoint()

    def record_tier2_snapshot(self, snapshot: dict) -> None:
        """Merges a pre-built dict matching status.json's "tier2"
        section (see scheduler.py/tier2_poller.py's call sites for what
        they build) into this instance's held state. Purely a passive
        recorder -- never queries anything itself. `snapshot["pools"]`
        is capped at MAX_TIER2_POOLS_IN_STATUS defensively; the caller
        is expected to already be handing over a poll pass's own
        bounded batch (see tier2_poller.py's MAX_POOLS_PER_VENUE_PER_POLL),
        not a fresh query over the full hot set.
        """
        try:
            self._tier2_polls_this_cycle += 1
            merged = dict(snapshot)
            pools = merged.get("pools") or []
            cap_hit = len(pools) > MAX_TIER2_POOLS_IN_STATUS
            merged["pools"] = pools[:MAX_TIER2_POOLS_IN_STATUS]
            merged["pools_cap_hit"] = cap_hit or bool(snapshot.get("pools_cap_hit"))
            merged["polls_this_cycle"] = self._tier2_polls_this_cycle
            self._tier2 = merged
        except Exception as exc:  # noqa: BLE001
            self._log_internal_bug("record_tier2_snapshot", exc)

    def record_meteora_cursor(self, snapshot: dict) -> None:
        try:
            self._meteora_cursor = dict(snapshot)
        except Exception as exc:  # noqa: BLE001
            self._log_internal_bug("record_meteora_cursor", exc)

    # --- Internals ----------------------------------------------------

    def _log_internal_bug(self, where: str, exc: Exception) -> None:
        # Deliberately NOT edge-triggered, unlike the file-write/
        # ClickHouse-insert WARNs below -- see module docstring. A bug
        # here is a defect in this module, not a routine external
        # outage to go quiet about.
        log(Level.WARN, [("event", "telemetry_internal_error"), ("where", where), ("err", str(exc))], src=TELEMETRY_SRC)

    def _finish_segment(self, seg: _Segment, *, category, bucket, partial_coverage) -> None:
        self._completed_segments.append(seg)

        segment_idx = len(self._cycle_rows_buffer)
        self._cycle_rows_buffer.append(
            [
                self.run_id,
                self._cycle_n,
                self._cycle_started_at,
                segment_idx,
                seg.name,
                seg.kind,
                round(seg.duration_s, 3),
                int(seg.rows or 0),
                int(seg.failed),
                seg.error_class,
                json.dumps(seg.extra) if seg.extra else "",
            ]
        )

        if seg.kind in ("source", "tier2_poll"):
            self._counters["rows_total"] += seg.rows or 0
        if seg.failed:
            self._counters["errors_total"] += 1
        if seg.kind == "detection":
            self._counters["new_entities_total"] += int(seg.extra.get("new_count", 0))

        if seg.kind == "source":
            self._update_source_state(seg, category=category, bucket=bucket, partial_coverage=partial_coverage)

    def _update_source_state(self, seg: _Segment, *, category, bucket, partial_coverage) -> None:
        st = self._sources.setdefault(seg.name, _SourceState(category=category or "dex"))
        st.category = category or st.category
        st.partial_coverage = partial_coverage
        st.last_duration_s = round(seg.duration_s, 1)
        st.last_rows = seg.rows

        if bucket is not None:
            try:
                st.bucket_fill_pct = bucket.fill_pct()
                st.rate_limit_rps = round(bucket.rate_per_s, 2)
            except Exception:  # noqa: BLE001 -- a bucket-read glitch must not break source bookkeeping
                pass

        if seg.failed:
            st.consecutive_failures += 1
            st.last_error = seg.error_class or "error"
        else:
            st.consecutive_failures = 0
            st.last_error = None
            st.last_success_at = _utcnow()

        if st.consecutive_failures >= BAD_AFTER_CONSECUTIVE_FAILURES:
            st.state = "bad"
        elif st.consecutive_failures > 0:
            st.state = "stale"
        else:
            st.state = "ok"

    def _flush_cycle_telemetry(self) -> None:
        if not self._cycle_rows_buffer:
            return
        t0 = time.monotonic()
        try:
            self._client.insert(
                "cycle_telemetry",
                self._cycle_rows_buffer,
                column_names=[
                    "run_id",
                    "cycle_n",
                    "cycle_started_at",
                    "segment_idx",
                    "segment_name",
                    "segment_kind",
                    "duration_s",
                    "rows_written",
                    "failed",
                    "error_class",
                    "extra",
                ],
            )
        except Exception as exc:  # noqa: BLE001 -- clickhouse-connect can raise several exception types
            self._ch_ok = False
            self._ch_last_error = str(exc)
            self._ch_last_insert_ms = None
            if not self._ch_warned:
                log(Level.WARN, [("event", "cycle_telemetry_insert_FAILED"), ("err", str(exc))], src=TELEMETRY_SRC)
                self._ch_warned = True
            return  # never raise -- cycle_end() must proceed to checkpoint() regardless

        self._ch_last_insert_ms = (time.monotonic() - t0) * 1000.0
        if not self._ch_ok:
            log(Level.INFO, [("event", "cycle_telemetry_insert_recovered")], src=TELEMETRY_SRC)
        self._ch_ok = True
        self._ch_last_error = None
        self._ch_warned = False

    def _write_status_file(self) -> None:
        payload = self._build_payload()
        data = json.dumps(payload, default=str).encode("utf-8")

        try:
            self._status_file_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self._status_file_path.with_name(self._status_file_path.name + ".tmp")
            with open(tmp_path, "wb") as f:
                f.write(data)
            os.replace(tmp_path, self._status_file_path)
        except OSError as exc:
            if not self._file_warned:
                log(Level.WARN, [("event", "status_file_write_FAILED"), ("err", str(exc))], src=TELEMETRY_SRC)
                self._file_warned = True
            return  # never raise -- checkpoint() must not propagate this

        if self._file_warned:
            log(Level.INFO, [("event", "status_file_write_recovered")], src=TELEMETRY_SRC)
        self._file_warned = False

    def _build_payload(self) -> dict:
        now = _utcnow()

        current_segment = None
        seg = self._current_segment
        if seg is not None:
            current_segment = {
                "name": seg.name,
                "kind": seg.kind,
                "started_at": _iso(seg.started_at),
                "elapsed_s": round(time.monotonic() - seg.mono_start, 1),
            }

        completed_segments = [
            {
                "name": s.name,
                "kind": s.kind,
                "duration_s": round(s.duration_s, 1),
                "rows": s.rows,
                "failed": s.failed,
            }
            for s in self._completed_segments
        ]

        cycle_elapsed = round(time.monotonic() - self._cycle_t0, 1) if self._cycle_t0 is not None else 0.0
        cycle = {
            "n": self._cycle_n,
            "started_at": _iso(self._cycle_started_at),
            "elapsed_s": cycle_elapsed,
            "target_s": self._target_cycle_s,
            "current_segment": current_segment,
            "completed_segments": completed_segments,
        }

        sources = []
        for name, st in self._sources.items():
            sources.append(
                {
                    "id": name,
                    "kind": st.category,
                    "state": st.state,
                    "consecutive_failures": st.consecutive_failures,
                    "last_duration_s": st.last_duration_s,
                    "last_rows": st.last_rows,
                    "last_success_at": _iso(st.last_success_at),
                    "last_error": st.last_error,
                    "bucket_fill_pct": st.bucket_fill_pct,
                    "rate_limit_rps": st.rate_limit_rps,
                    "partial_coverage": st.partial_coverage,
                }
            )

        http_413_total = get_http_413_total()
        last_latency = get_last_latency_ms()
        infra = {
            "clickhouse": {
                "state": "ok" if self._ch_ok else "bad",
                "last_insert_ms": round(self._ch_last_insert_ms, 1) if self._ch_last_insert_ms is not None else None,
                "last_error": self._ch_last_error,
            },
            "solana_rpc": {
                "state": "ok" if get_last_call_ok() else "bad",
                "batch_limit": QUICKNODE_BATCH_LIMIT,
                "http_413_total": http_413_total,
                "last_latency_ms": round(last_latency, 1) if last_latency is not None else None,
            },
        }

        tier2 = self._tier2 or {
            "hot_total": 0,
            "age_based": {"count": 0, "window_h": None},
            "growth_based": {"count": 0, "hysteresis_cycles": None},
            "polls_this_cycle": 0,
            "last_poll_at": None,
            "last_poll_duration_s": None,
            "max_pools_per_venue_per_poll": None,
            "venue_capped": {},
            "pools": [],
            "pools_cap_hit": False,
        }

        meteora_cursor = self._meteora_cursor or {
            "page_from": 0,
            "page_to": 0,
            "total_pages": 0,
            "sweep_count": 0,
            "last_wrap_at": None,
        }

        counters = dict(self._counters)
        counters["http_413_total"] = http_413_total

        return {
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "started_at": _iso(self._started_at),
            "heartbeat_at": _iso(now),
            "cycle": cycle,
            "sources": sources,
            "infra": infra,
            "tier2": tier2,
            "meteora_cursor": meteora_cursor,
            "counters": counters,
        }


def segment_or_noop(tel: Optional[RunTelemetry], name: str, kind: str, **kwargs):
    """`tel.segment(...)`, or a harmless stand-in when `tel` is None
    (RunTelemetry is optional on Tier2Poller for callers that construct
    it standalone) -- same shape (a `with ... as seg:` handle exposing
    rows/failed/error_class/extra), just discarding whatever's written
    to it. Never itself a source of exceptions.
    """
    if tel is None:
        return nullcontext(SimpleNamespace(rows=0, failed=False, error_class="", extra={}))
    return tel.segment(name, kind, **kwargs)
