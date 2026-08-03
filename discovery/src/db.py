"""ClickHouse connection helper for the discovery subsystem. HTTP
interface (port 8123), matching discovery/docker-compose.yml.
"""
from __future__ import annotations

import os

import clickhouse_connect

DEFAULT_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
DEFAULT_PORT = int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"))
DEFAULT_DATABASE = os.environ.get("CLICKHOUSE_DB", "zora_discovery")


def get_client():
    return clickhouse_connect.get_client(
        host=DEFAULT_HOST, port=DEFAULT_PORT, database=DEFAULT_DATABASE
    )
