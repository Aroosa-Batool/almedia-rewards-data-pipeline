"""Postgres connection + a minimal, versioned migration runner.

All objects live in one Postgres schema (DB_SCHEMA, default "rewards"), so several
services can share a database without colliding.
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

MIGRATIONS = Path(__file__).resolve().parent.parent / "migrations"
SCHEMA_NAME = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


def database_url() -> str:
    # The direct (unpooled) endpoint: we use session state (search_path) and advisory locks.
    url = os.environ.get("DATABASE_URL_UNPOOLED") or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    return url


def _schema(schema: str | None) -> str:
    schema = schema or os.environ.get("DB_SCHEMA", "rewards")
    if not SCHEMA_NAME.match(schema):
        raise ValueError(f"invalid schema name {schema!r}")
    return schema


def _configure(schema: str):
    def configure(conn: psycopg.Connection) -> None:
        conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema)))
    return configure


def connect(schema: str | None = None, url: str | None = None) -> psycopg.Connection:
    """A single connection (CLI, tests)."""
    conn = psycopg.connect(url or database_url(), row_factory=dict_row, connect_timeout=10, autocommit=True)
    _configure(_schema(schema))(conn)
    migrate(conn)
    return conn


def make_pool(schema: str | None = None, url: str | None = None, max_size: int = 4) -> ConnectionPool:
    """A small pool for the API: each request thread gets its own connection, so
    transactions from concurrent requests never interleave on one socket."""
    pool = ConnectionPool(
        url or database_url(), min_size=1, max_size=max_size, open=True, check=ConnectionPool.check_connection,
        kwargs={"row_factory": dict_row, "autocommit": True, "connect_timeout": 10},
        configure=_configure(_schema(schema)),
    )
    with pool.connection() as conn:
        migrate(conn)
    return pool


def migrate(conn: psycopg.Connection) -> list[str]:
    """Apply migrations/NNN_*.sql in order, once each. A lock stops two cold starts racing."""
    applied: list[str] = []
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(hashtext(current_schema() || ':migrate'))")
        conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations "
                     "(version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())")
        done = {r["version"] for r in conn.execute("SELECT version FROM schema_migrations")}
        for f in sorted(MIGRATIONS.glob("*.sql")):
            if f.stem not in done:
                conn.execute(f.read_text())
                conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (f.stem,))
                applied.append(f.stem)
    return applied


def record_run(conn: psycopg.Connection, stage: str, started_at: datetime, status: str,
               rows: int = 0, detail: dict | None = None) -> None:
    conn.execute(
        "INSERT INTO pipeline_runs (stage, started_at, status, rows, detail) VALUES (%s,%s,%s,%s,%s)",
        (stage, started_at, status, rows, json.dumps(detail or {}, default=str)),
    )


def now(conn: psycopg.Connection) -> datetime:
    return conn.execute("SELECT clock_timestamp() AS t").fetchone()["t"]
