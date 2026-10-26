"""Kafka-shaped log on Postgres: keyed partitions, consumer groups, committed offsets, lag.

  producer -> event_log (append-only, partitioned) -> consumer group -> raw_events
                                                     commits offsets

* Events are keyed by user_id, so one user's events stay ordered within a partition.
* A consumer reads from its last committed offset.
* Inserting into raw_events and committing the offset happen in ONE transaction:
  at-least-once delivery + idempotent writes = effectively-once processing.
* Each partition is guarded by an advisory lock, so two serverless instances never
  consume the same partition at once (Kafka's "one consumer per partition per group").
"""
from __future__ import annotations

import json

import psycopg
from pydantic import ValidationError

from .db import now, record_run
from .events import RewardEvent, hash_key

PARTITIONS = 4
GROUP = "raw-loader"


def partition_for(payload: dict) -> int:
    return hash_key(str(payload.get("user_id", ""))) % PARTITIONS


def produce(conn: psycopg.Connection, payloads: list[dict], producer_id: int | None = None) -> int:
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO event_log (partition_key, producer_id, payload) VALUES (%s,%s,%s)",
            [(partition_for(p), producer_id, json.dumps(p, default=str)) for p in payloads],
        )
    return len(payloads)


def lag(conn: psycopg.Connection, group: str = GROUP) -> dict[int, int]:
    """Messages produced but not yet committed, per partition. The #1 streaming metric."""
    rows = conn.execute(
        """SELECT p.partition_key,
                  (SELECT COUNT(*) FROM event_log l
                    WHERE l.partition_key = p.partition_key
                      AND l.offset_id > COALESCE(o.committed_offset, 0)) AS lag
           FROM generate_series(0, %s - 1) AS p(partition_key)
           LEFT JOIN consumer_offsets o
             ON o.partition_key = p.partition_key AND o.consumer_group = %s
           ORDER BY 1""",
        (PARTITIONS, group),
    ).fetchall()
    return {r["partition_key"]: r["lag"] for r in rows}


def consume(conn: psycopg.Connection, max_batch: int = 1000, group: str = GROUP) -> dict:
    """One micro-batch per partition. Safe to call from many processes concurrently."""
    started = now(conn)
    stats = {"read": 0, "inserted": 0, "duplicates": 0, "dead_lettered": 0, "skipped_locked": 0}
    for p in range(PARTITIONS):
        with conn.transaction():
            got = conn.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s), %s) AS ok", (group, p)).fetchone()["ok"]
            if not got:
                stats["skipped_locked"] += 1   # another instance owns this partition right now
                continue
            row = conn.execute(
                "SELECT committed_offset FROM consumer_offsets WHERE consumer_group=%s AND partition_key=%s",
                (group, p),
            ).fetchone()
            committed = row["committed_offset"] if row else 0
            batch = conn.execute(
                "SELECT offset_id, payload FROM event_log WHERE partition_key=%s AND offset_id>%s "
                "ORDER BY offset_id LIMIT %s",
                (p, committed, max_batch),
            ).fetchall()
            if not batch:
                continue
            for k, v in load_batch(conn, batch, p).items():
                stats[k] += v
            conn.execute(
                """INSERT INTO consumer_offsets (consumer_group, partition_key, committed_offset)
                   VALUES (%s,%s,%s)
                   ON CONFLICT (consumer_group, partition_key)
                   DO UPDATE SET committed_offset = EXCLUDED.committed_offset, updated_at = now()""",
                (group, p, batch[-1]["offset_id"]),
            )
    if stats["read"]:
        record_run(conn, "stream_consume", started, "success", stats["inserted"], stats)
    return stats


def drain(conn: psycopg.Connection, max_rounds: int = 50) -> dict:
    total = {"read": 0, "inserted": 0, "duplicates": 0, "dead_lettered": 0, "skipped_locked": 0}
    for _ in range(max_rounds):
        s = consume(conn)
        for k, v in s.items():
            total[k] += v
        if s["read"] == 0:
            break
    return total


def load_batch(conn: psycopg.Connection, batch: list[dict], partition: int) -> dict:
    """Validate in Python, then load the whole batch in two set-based statements.

    One INSERT ... SELECT FROM jsonb_to_recordset() replaces N round trips; with a remote
    database that is the difference between seconds and milliseconds.
    """
    good, bad = [], []
    for msg in batch:
        try:
            ev = RewardEvent.model_validate(msg["payload"])
            good.append({**ev.model_dump(mode="json"), "partition_key": partition, "source_offset": msg["offset_id"]})
        except ValidationError as e:
            errors = [{"loc": ".".join(map(str, x["loc"])), "msg": x["msg"]} for x in e.errors(include_url=False)]
            bad.append((msg["offset_id"], json.dumps(msg["payload"], default=str), json.dumps(errors)))
    inserted = 0
    if good:
        inserted = len(conn.execute(
            """INSERT INTO raw_events (event_id, event_type, user_id, offer_id, advertiser, country,
                                       reward_cents, occurred_at, partition_key, source_offset)
               SELECT event_id, event_type, user_id, offer_id, advertiser, country,
                      reward_cents, occurred_at, partition_key, source_offset
               FROM jsonb_to_recordset(%s::jsonb) AS r(
                    event_id uuid, event_type text, user_id text, offer_id text, advertiser text,
                    country char(2), reward_cents int, occurred_at timestamptz,
                    partition_key smallint, source_offset bigint)
               ON CONFLICT (event_id) DO NOTHING
               RETURNING 1""",
            (json.dumps(good),),
        ).fetchall())
    if bad:
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO dead_letter_events (source_offset, payload, error) VALUES (%s,%s,%s)", bad)
    return {"read": len(batch), "inserted": inserted, "duplicates": len(good) - inserted, "dead_lettered": len(bad)}
