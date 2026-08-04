"""Structured logging for the MEV discovery subsystem, matching the
shared ZORA log format documented in CLAUDE.md:
`HH:MM:SS.mmm  LEVEL  SRC  key=value key=value...` -- same convention
as spread-detector/src/logger.rs, tools/price_monitor uses print()
directly since it's a passive viewer, but this is an active ingestion
component so it gets the real thing. No color handling here (unlike
the Rust logger) -- this runs as a one-shot script piped through `make
discovery-test`, not attached to an interactive tmux pane, so ANSI
codes would just be noise in captured output.

`flush=True` on the print (added for the Phase 1e scheduler): every
one-shot run_*.py script's stdout goes straight to an interactive
terminal or a captured pipe, where Python's line-buffering already
flushes each line promptly. The scheduler is different -- it runs as
a long-lived tmux pane whose output is mirrored to a log file via
`tmux pipe-pane`, and separately can be launched with stdout
redirected straight to a file, both of which make Python switch to
full (block) buffering by default. Without an explicit flush, log
lines -- including a WARN/ERROR that should be visible to whoever's
watching the pane -- could sit in an in-process buffer for a long
time instead of appearing when they're logged.
"""
from __future__ import annotations

import datetime
import enum

SOURCE = "DSC"


class Level(enum.Enum):
    INFO = "INFO "
    WARN = "WARN "
    ERROR = "ERROR"


def log(level: Level, fields: list[tuple[str, str]], src: str = SOURCE) -> None:
    """`src` defaults to the module-wide "DSC" every existing one-shot
    script/provider call site already relies on (fully backward
    compatible -- no existing call needs to change). The scheduler
    (discovery/src/scheduler.py) is the one caller that overrides it
    per source (e.g. "ORC", "BNC"), since it's a single long-lived
    process orchestrating all 9 providers' calls -- see that module's
    docstring for the per-source tag table and why only the
    scheduler's own wrapper lines use this, not every provider-internal
    log call.
    """
    now = datetime.datetime.now()
    ts = f"{now.strftime('%H:%M:%S')}.{now.microsecond // 1000:03d}"
    message = " ".join(f"{key}={value}" for key, value in fields)
    print(f"{ts}  {level.value}  {src}  {message}", flush=True)
