"""HTTP layer: ingestion API + read API over the marts + a live dashboard.

Run locally:  uvicorn api.index:app --reload
On Vercel this file is the serverless function; vercel.json routes every path here.
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import FastAPI, HTTPException, Query  # noqa: E402
from fastapi.responses import HTMLResponse  # noqa: E402

from pipeline import cli, quality, stream, transform  # noqa: E402
from pipeline.db import connect  # noqa: E402
from pipeline.events import RewardEvent  # noqa: E402

app = FastAPI(title="Rewards Data Pipeline", version="1.0.0",
              description="Ingestion -> stream -> raw -> dbt-style marts -> data-quality checks")

_conn = connect()
_lock = threading.Lock()  # SQLite has one writer; serialize writes inside this process

# Serverless instances start empty: seed demo data on cold start so the dashboard has content.
if _conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0] == 0:
    with _lock:
        cli.run(_conn, events=3000, seed=42)


def rows(sql: str, params: tuple = ()) -> list[dict]:
    return [dict(r) for r in _conn.execute(sql, params).fetchall()]


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "raw_events": _conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]}


@app.post("/events", status_code=202)
def ingest(events: list[dict]) -> dict:
    """Accept a batch, append to the log (fast), return 202. Loading happens in the consumer.

    Validation errors are NOT rejected here: they go to the dead-letter table during consume,
    so one bad record never fails a whole batch from a producer.
    """
    if not events or len(events) > 1000:
        raise HTTPException(422, "send 1..1000 events per request")
    with _lock:
        n = stream.produce(_conn, events)
    return {"accepted": n, "lag": stream.lag(_conn)}


@app.post("/events/validate")
def validate(event: RewardEvent) -> dict:
    """Synchronous contract check, handy for producers' CI."""
    return {"valid": True, "partition": event.partition(stream.PARTITIONS)}


@app.post("/pipeline/run")
def run_pipeline(simulate: int = Query(500, ge=0, le=5000), full_refresh: bool = False) -> dict:
    """Simulate N new events, drain the stream, rebuild marts, run checks."""
    with _lock:
        return cli.run(_conn, events=simulate, full_refresh=full_refresh)


@app.get("/stream/lag")
def lag() -> dict:
    return stream.lag(_conn)


@app.get("/marts/advertisers")
def advertisers(days: int = Query(7, ge=1, le=90)) -> list[dict]:
    return rows(
        """SELECT advertiser, SUM(rewards_paid) AS rewards_paid, SUM(payout_cents) AS payout_cents,
                  SUM(paying_users) AS paying_users
           FROM mart_advertiser_daily WHERE event_date >= DATE('now', ?)
           GROUP BY advertiser ORDER BY payout_cents DESC""",
        (f"-{days} days",),
    )


@app.get("/marts/daily")
def daily() -> list[dict]:
    return rows("SELECT event_date, SUM(payout_cents) AS payout_cents, SUM(rewards_paid) AS rewards_paid "
                "FROM mart_advertiser_daily GROUP BY event_date ORDER BY event_date")


@app.get("/marts/funnel")
def funnel(limit: int = Query(10, ge=1, le=100)) -> list[dict]:
    return rows("SELECT * FROM fct_offer_funnel ORDER BY clickers DESC LIMIT ?", (limit,))


@app.get("/users/{user_id}")
def user(user_id: str) -> dict:
    ltv = rows("SELECT * FROM dim_user_ltv WHERE user_id=?", (user_id,))
    if not ltv:
        raise HTTPException(404, "user has no paid rewards")
    # This lookup uses ix_raw_events_user_time (see EXPLAIN in /debug/explain).
    events = rows("SELECT event_type, offer_id, reward_cents, occurred_at FROM raw_events "
                  "WHERE user_id=? ORDER BY occurred_at DESC LIMIT 20", (user_id,))
    return {"ltv": ltv[0], "recent_events": events}


@app.get("/quality")
def checks() -> dict:
    with _lock:
        return quality.run_checks(_conn)


@app.get("/runs")
def runs(limit: int = 20) -> list[dict]:
    return rows("SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT ?", (limit,))


@app.get("/dead-letters")
def dead_letters(limit: int = 20) -> list[dict]:
    return rows("SELECT * FROM dead_letter_events ORDER BY id DESC LIMIT ?", (limit,))


@app.get("/lineage")
def lineage() -> dict:
    models = transform.load_models()
    return {"order": transform.run_order(models),
            "edges": [[d, m.name] for m in models.values() for d in sorted(m.deps)],
            "materializations": {m.name: m.materialized for m in models.values()}}


@app.get("/debug/explain")
def explain() -> dict:
    q = "SELECT * FROM raw_events WHERE user_id='u0001' ORDER BY occurred_at DESC LIMIT 20"
    return {"query": q, "plan": [r["detail"] for r in rows("EXPLAIN QUERY PLAN " + q)]}


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    return (Path(__file__).with_name("dashboard.html")).read_text()
