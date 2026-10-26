"""HTTP API + dashboard.

Write path:  POST /v1/events (API key) -> event_log -> micro-batch consume -> raw_events
Read path:   marts are refreshed lazily when older than REFRESH_SECONDS, plus a scheduled run.

Local:  uvicorn api.index:app --reload
"""
from __future__ import annotations

import os
import sys
import threading
from pathlib import Path
from typing import Annotated, Iterator

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg  # noqa: E402
from fastapi import Depends, FastAPI, Header, HTTPException, Query  # noqa: E402
from fastapi.responses import HTMLResponse  # noqa: E402

from pipeline import auth, quality, stream, transform  # noqa: E402
from pipeline.db import make_pool  # noqa: E402
from pipeline.events import RewardEvent, generate  # noqa: E402

REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "120"))
DEMO_COOLDOWN_SECONDS = 30
MAX_BATCH = 1000

app = FastAPI(
    title="RewardStream",
    version="1.1.0",
    description="Event ingestion and analytics for rewarded advertising: offer clicks, completions and payouts.",
)

_pool = None
_lock = threading.Lock()


def db() -> Iterator[psycopg.Connection]:
    """Created lazily so importing the app never needs the network; reused while the instance is warm."""
    global _pool
    with _lock:
        if _pool is None:
            _pool = make_pool()
    with _pool.connection() as conn:
        yield conn


Conn = Annotated[psycopg.Connection, Depends(db)]


def rows(conn: psycopg.Connection, q: str, params: tuple = ()) -> list[dict]:
    return conn.execute(q, params).fetchall()


def ensure_fresh(conn: psycopg.Connection) -> None:
    age = transform.seconds_since_last_run(conn)
    if age is None or age > REFRESH_SECONDS:
        stream.drain(conn)
        if transform.run_all(conn) is not None:
            quality.run_checks(conn)


def require_producer(conn: Conn, x_api_key: Annotated[str | None, Header()] = None) -> int:
    pid = auth.authenticate(conn, x_api_key)
    if pid is None:
        raise HTTPException(401, "missing or invalid X-API-Key")
    return pid


@app.get("/health")
def health(conn: Conn) -> dict:
    r = conn.execute("SELECT COUNT(*) AS n, MAX(ingested_at) AS newest FROM raw_events").fetchone()
    return {"status": "ok", "raw_events": r["n"], "newest_event_ingested_at": r["newest"],
            "lag": sum(stream.lag(conn).values())}


@app.post("/v1/events", status_code=202)
def ingest(events: list[dict], conn: Conn, producer_id: Annotated[int, Depends(require_producer)]) -> dict:
    """Append a batch to the log, then load a micro-batch.

    Invalid events are NOT rejected here: they go to the dead-letter table during consume,
    so one bad record never fails a producer's whole batch.
    """
    if not 1 <= len(events) <= MAX_BATCH:
        raise HTTPException(422, f"send 1..{MAX_BATCH} events per request")
    accepted = stream.produce(conn, events, producer_id)
    consumed = stream.drain(conn, max_rounds=3)
    return {"accepted": accepted, "consumed": consumed, "lag": stream.lag(conn)}


@app.post("/v1/events/validate")
def validate(event: RewardEvent) -> dict:
    """Synchronous contract check for producers' CI pipelines."""
    return {"valid": True, "partition": stream.partition_for(event.model_dump())}


@app.post("/v1/demo/traffic")
def demo_traffic(conn: Conn) -> dict:
    """Public, rate-limited: generates 300 synthetic events so visitors can watch the pipeline move."""
    last = conn.execute("SELECT EXTRACT(EPOCH FROM now() - MAX(finished_at)) AS s FROM pipeline_runs "
                        "WHERE stage = 'demo_traffic'").fetchone()["s"]
    if last is not None and float(last) < DEMO_COOLDOWN_SECONDS:
        raise HTTPException(429, f"demo traffic is limited to once every {DEMO_COOLDOWN_SECONDS}s")
    # Claim the slot first so concurrent clicks can't both pass the check.
    conn.execute("INSERT INTO pipeline_runs (stage, started_at, status, rows) VALUES ('demo_traffic', now(), 'success', 300)")
    stream.produce(conn, generate(300))
    consumed = stream.drain(conn)
    models = transform.run_all(conn)
    checks = quality.run_checks(conn)
    return {"consumed": consumed, "models": models, "quality_passed": checks["passed"]}


@app.get("/api/cron/pipeline")
def cron(conn: Conn, authorization: Annotated[str | None, Header()] = None) -> dict:
    """Scheduled full run (Vercel Cron sends `Authorization: Bearer $CRON_SECRET`)."""
    secret = os.environ.get("CRON_SECRET")
    if not secret or authorization != f"Bearer {secret}":
        raise HTTPException(401)
    consumed = stream.drain(conn)
    models = transform.run_all(conn)
    return {"consumed": consumed, "models": models, "quality": quality.run_checks(conn)["passed"]}


@app.get("/v1/stream/lag")
def lag(conn: Conn) -> dict:
    return stream.lag(conn)


@app.get("/v1/marts/advertisers")
def advertisers(conn: Conn, days: int = Query(7, ge=1, le=90)) -> list[dict]:
    ensure_fresh(conn)
    return rows(conn, """SELECT advertiser, SUM(rewards_paid)::int AS rewards_paid, SUM(payout_cents)::bigint AS payout_cents,
                                SUM(paying_users)::int AS paying_users
                         FROM mart_advertiser_daily WHERE event_date >= current_date - %s
                         GROUP BY advertiser ORDER BY payout_cents DESC""", (days,))


@app.get("/v1/marts/daily")
def daily(conn: Conn, days: int = Query(14, ge=1, le=90)) -> list[dict]:
    ensure_fresh(conn)
    return rows(conn, """SELECT event_date, SUM(payout_cents)::bigint AS payout_cents, SUM(rewards_paid)::int AS rewards_paid
                         FROM mart_advertiser_daily WHERE event_date >= current_date - %s
                         GROUP BY event_date ORDER BY event_date""", (days,))


@app.get("/v1/marts/funnel")
def funnel(conn: Conn, limit: int = Query(10, ge=1, le=100)) -> list[dict]:
    ensure_fresh(conn)
    return rows(conn, "SELECT * FROM fct_offer_funnel ORDER BY clickers DESC, offer_id LIMIT %s", (limit,))


@app.get("/v1/users/top")
def top_users(conn: Conn, limit: int = Query(10, ge=1, le=100)) -> list[dict]:
    ensure_fresh(conn)
    return rows(conn, "SELECT * FROM dim_user_ltv ORDER BY lifetime_cents DESC LIMIT %s", (limit,))


@app.get("/v1/users/{user_id}")
def user(user_id: str, conn: Conn) -> dict:
    ensure_fresh(conn)
    ltv = rows(conn, "SELECT * FROM dim_user_ltv WHERE user_id = %s", (user_id,))
    if not ltv:
        raise HTTPException(404, "user has no paid rewards")
    events = rows(conn, "SELECT event_type, offer_id, reward_cents, occurred_at FROM raw_events "
                        "WHERE user_id = %s ORDER BY occurred_at DESC LIMIT 20", (user_id,))
    return {"ltv": ltv[0], "recent_events": events}


@app.get("/v1/quality")
def checks(conn: Conn) -> dict:
    ensure_fresh(conn)
    return quality.run_checks(conn, record=False)


@app.get("/v1/runs")
def runs(conn: Conn, limit: int = Query(20, ge=1, le=200)) -> list[dict]:
    return rows(conn, "SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT %s", (limit,))


@app.get("/v1/dead-letters")
def dead_letters(conn: Conn, limit: int = Query(20, ge=1, le=200)) -> list[dict]:
    return rows(conn, "SELECT * FROM dead_letter_events ORDER BY id DESC LIMIT %s", (limit,))


@app.get("/v1/lineage")
def lineage() -> dict:
    models = transform.load_models()
    return {"order": transform.run_order(models),
            "edges": [[d, m.name] for m in models.values() for d in sorted(m.deps)],
            "materializations": {m.name: m.materialized for m in models.values()}}


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    return Path(__file__).with_name("dashboard.html").read_text()
