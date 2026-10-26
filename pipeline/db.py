"""Database connection helpers.

One SQLite file holds both the operational tables and the warehouse marts.
In production these would be separate systems (e.g. Postgres + BigQuery/Snowflake).
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

SCHEMA = Path(__file__).with_name("schema.sql")


def default_path() -> str:
    # Vercel's filesystem is read-only except /tmp.
    return os.environ.get("PIPELINE_DB", "/tmp/rewards.db" if os.environ.get("VERCEL") else "rewards.db")


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def connect(path: str | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(path or default_path(), isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # WAL lets readers (the API) keep reading while the writer (the pipeline) writes.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.executescript(SCHEMA.read_text())
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Explicit BEGIN/COMMIT so a batch is all-or-nothing."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def record_run(conn: sqlite3.Connection, stage: str, started_at: str, status: str, rows: int = 0, detail: str = "") -> None:
    conn.execute(
        "INSERT INTO pipeline_runs (stage, started_at, finished_at, status, rows, detail) VALUES (?,?,?,?,?,?)",
        (stage, started_at, utcnow(), status, rows, detail),
    )
