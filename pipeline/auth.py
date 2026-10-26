"""API keys for producers. Keys are shown once; only their SHA-256 hash is stored."""
from __future__ import annotations

import hashlib
import secrets

import psycopg

PREFIX = "rwd_"


def _hash(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def create_producer(conn: psycopg.Connection, name: str) -> str:
    key = PREFIX + secrets.token_urlsafe(32)
    conn.execute("INSERT INTO producers (name, key_hash) VALUES (%s, %s)", (name, _hash(key)))
    return key


def authenticate(conn: psycopg.Connection, key: str | None) -> int | None:
    if not key or not key.startswith(PREFIX):
        return None
    row = conn.execute("SELECT id FROM producers WHERE key_hash = %s AND revoked_at IS NULL", (_hash(key),)).fetchone()
    return row["id"] if row else None


def revoke(conn: psycopg.Connection, name: str) -> bool:
    return conn.execute("UPDATE producers SET revoked_at = now() WHERE name = %s AND revoked_at IS NULL",
                        (name,)).rowcount == 1
