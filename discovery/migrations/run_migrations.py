#!/usr/bin/env python3
"""Applies discovery/migrations/*.sql, in filename order, against
ClickHouse. The schema statements are plain CREATE TABLE (no IF NOT
EXISTS -- kept verbatim from the design doc), so this is meant to be
run once against a fresh database; re-running after tables already
exist will fail loudly rather than silently no-op, which is preferable
to masking an accidental double-apply.
"""
from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from discovery.src.db import get_client  # noqa: E402
from discovery.src.logger import Level, log  # noqa: E402

MIGRATIONS_DIR = pathlib.Path(__file__).resolve().parent


def _split_statements(sql: str) -> list[str]:
    # Strip "-- ..." line comments before splitting on ";" -- several
    # of the schema's inline comments (see 001_create_tables.sql)
    # themselves contain semicolons, which would otherwise chop a
    # single CREATE TABLE into malformed fragments.
    without_comments = "\n".join(
        line.split("--", 1)[0] for line in sql.splitlines()
    )
    return [s.strip() for s in without_comments.split(";") if s.strip()]


def main() -> None:
    client = get_client()
    for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
        statements = _split_statements(path.read_text())
        for statement in statements:
            client.command(statement)
        log(Level.INFO, [("event", "migration_applied"), ("file", path.name)])


if __name__ == "__main__":
    main()
