"""Structured logging for the MEV discovery subsystem, matching the
shared ZORA log format documented in CLAUDE.md:
`HH:MM:SS.mmm  LEVEL  SRC  key=value key=value...` -- same convention
as spread-detector/src/logger.rs, tools/price_monitor uses print()
directly since it's a passive viewer, but this is an active ingestion
component so it gets the real thing. No color handling here (unlike
the Rust logger) -- this runs as a one-shot script piped through `make
discovery-test`, not attached to an interactive tmux pane, so ANSI
codes would just be noise in captured output.
"""
from __future__ import annotations

import datetime
import enum

SOURCE = "DSC"


class Level(enum.Enum):
    INFO = "INFO "
    WARN = "WARN "
    ERROR = "ERROR"


def log(level: Level, fields: list[tuple[str, str]]) -> None:
    now = datetime.datetime.now()
    ts = f"{now.strftime('%H:%M:%S')}.{now.microsecond // 1000:03d}"
    message = " ".join(f"{key}={value}" for key, value in fields)
    print(f"{ts}  {level.value}  {SOURCE}  {message}")
