"""A tiny Kafka-shaped log, so the near-real-time path can be learned without a broker.

  producer  ->  event_log (append-only, partitioned)  ->  consumer group  ->  raw_events
                                                           commits offsets

The semantics mirror Kafka on purpose:
  * a message is keyed by user_id, so one user's events stay ordered within a partition;
  * the consumer reads from its last *committed* offset;
  * writes to raw_events and the offset commit happen in ONE transaction, which gives
    effectively-once processing (at-least-once delivery + idempotent writes).
"""
from __future__ import annotations

import json
import sqlite3

from pydantic import ValidationError

from .db import record_run, transaction, utcnow
from .events import RewardEvent, hash_key

PARTITIONS = 4
GROUP = "raw-loader"


def produce(conn: sqlite3.Connection, payloads: list[dict]) -> int:
    rows = [
        (hash_key(str(p.get("user_id", ""))) % PARTITIONS, json.dumps(p, default=str), utcnow())
        for p in payloads
    ]
    with transaction(conn):
        conn.executemany("INSERT INTO event_log (partition_key, payload, produced_at) VALUES (?,?,?)", rows)
    return len(rows)


def lag(conn: sqlite3.Connection, group: str = GROUP) -> dict[int, int]:
    """Consumer lag per partition = newest offset - committed offset. The #1 streaming metric."""
    out = {}
    for p in range(PARTITIONS):
        committed = conn.execute(
            "SELECT committed_offset FROM consumer_offsets WHERE consumer_group=? AND partition_key=?", (group, p)
        ).fetchone()
        c = committed[0] if committed else 0
        out[p] = conn.execute(
            "SELECT COUNT(*) FROM event_log WHERE partition_key=? AND offset_id>?", (p, c)
        ).fetchone()[0]
    return out


def consume(conn: sqlite3.Connection, max_batch: int = 500, group: str = GROUP) -> dict:
    """Read one micro-batch per partition and load it into raw_events."""
    started = utcnow()
    stats = {"read": 0, "inserted": 0, "duplicates": 0, "dead_lettered": 0}
    for p in range(PARTITIONS):
        row = conn.execute(
            "SELECT committed_offset FROM consumer_offsets WHERE consumer_group=? AND partition_key=?", (group, p)
        ).fetchone()
        committed = row[0] if row else 0
        batch = conn.execute(
            "SELECT offset_id, payload FROM event_log WHERE partition_key=? AND offset_id>? ORDER BY offset_id LIMIT ?",
            (p, committed, max_batch),
        ).fetchall()
        if not batch:
            continue
        with transaction(conn):
            for msg in batch:
                stats["read"] += 1
                result = load_one(conn, json.loads(msg["payload"]), partition=p)
                stats[result] += 1
            conn.execute(
                "INSERT INTO consumer_offsets (consumer_group, partition_key, committed_offset) VALUES (?,?,?) "
                "ON CONFLICT(consumer_group, partition_key) DO UPDATE SET committed_offset=excluded.committed_offset",
                (group, p, batch[-1]["offset_id"]),
            )
    record_run(conn, "stream_consume", started, "success", stats["inserted"], json.dumps(stats))
    return stats


def load_one(conn: sqlite3.Connection, payload: dict, partition: int | None = None) -> str:
    """Validate + idempotent insert. Returns which counter to bump."""
    try:
        ev = RewardEvent.model_validate(payload)
    except ValidationError as e:
        conn.execute(
            "INSERT INTO dead_letter_events (payload, error, received_at) VALUES (?,?,?)",
            (json.dumps(payload, default=str), e.errors(include_url=False).__repr__()[:500], utcnow()),
        )
        return "dead_lettered"
    cur = conn.execute(
        """INSERT INTO raw_events (event_id, event_type, user_id, offer_id, advertiser, country,
                                   reward_cents, occurred_at, ingested_at, partition_key)
           VALUES (?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(event_id) DO NOTHING""",
        (
            ev.event_id, ev.event_type, ev.user_id, ev.offer_id, ev.advertiser, ev.country,
            ev.reward_cents, ev.occurred_at.isoformat(), utcnow(),
            partition if partition is not None else ev.partition(PARTITIONS),
        ),
    )
    return "inserted" if cur.rowcount == 1 else "duplicates"
