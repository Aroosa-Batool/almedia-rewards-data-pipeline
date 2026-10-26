# RewardStream

Event ingestion and payout analytics for rewarded advertising. Game and app backends send
offer events (click → complete → reward paid); RewardStream streams them into Postgres,
deduplicates retries, quarantines bad records, builds analytics marts and checks data quality
continuously.

**Live:** https://almedia-rewards-pipeline.vercel.app · **API docs:** https://almedia-rewards-pipeline.vercel.app/docs

## Architecture

```
producers ──POST /v1/events (X-API-Key)──▶ event_log (append-only, 4 partitions keyed by user_id)
                                              │
                         consumer group "raw-loader" (advisory lock per partition,
                         offsets committed in the same transaction as the load)
                                              │
                      ┌───────────────────────┴───────────────────────┐
                 raw_events (UUID idempotency key)          dead_letter_events
                      │
         SQL models (ref() DAG, one transaction)
         stg_events (view) → fct_rewards_paid (incremental) → mart_advertiser_daily, dim_user_ltv
                           → fct_offer_funnel (table)
                      │
         data-quality checks (uniqueness, reconciliation, freshness, lag, dead-letter rate, orphans)
                      │
         read API + dashboard (marts refreshed when older than 2 min, plus a daily scheduled run)
```

| Concern | How |
|---|---|
| Duplicates from producer retries | `event_id UUID PRIMARY KEY` + `ON CONFLICT DO NOTHING` |
| Exactly-once effect | at-least-once log + idempotent insert + offset commit in the same transaction |
| Concurrent serverless consumers | `pg_try_advisory_xact_lock` per partition |
| Bad records | validated against a Pydantic contract; failures go to `dead_letter_events`, batch continues |
| Incremental models | watermark on `ingested_at` with a 10-min lookback + upsert on unique key |
| Consistent marts | the whole DAG runs in one transaction (Postgres DDL is transactional) |
| Fast scans / lookups | BRIN on `ingested_at`, B-tree `(user_id, occurred_at DESC)`, partial index for completions |
| Schema changes | versioned SQL migrations, applied once under an advisory lock |
| Auth | per-producer API keys, SHA-256 hashed, revocable |

## Stack
Python 3.12 · FastAPI · Pydantic v2 · PostgreSQL (Neon) · psycopg 3 + psycopg-pool · Vercel (serverless + cron) · GitHub Actions · pytest

## Run locally
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
export DATABASE_URL=postgresql://user:pass@localhost:5432/rewards
python -m pipeline.cli migrate
python -m pipeline.cli create-producer my-service      # prints an API key once
python -m pipeline.cli run --simulate 5000              # produce, consume, transform, check
uvicorn api.index:app --reload                           # http://127.0.0.1:8000
pytest -q                                                # each test runs in its own schema
```

## Send events
```bash
curl -X POST https://almedia-rewards-pipeline.vercel.app/v1/events \
  -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '[{"event_id":"7b1e…","event_type":"reward_paid","user_id":"u0042","offer_id":"o007",
        "advertiser":"RaidQuest","country":"DE","reward_cents":300,"occurred_at":"2026-10-26T18:00:00Z"}]'
```
or `python -m pipeline.cli send --url https://almedia-rewards-pipeline.vercel.app --key $KEY --events 500`.

## API
| Method | Path | |
|---|---|---|
| POST | `/v1/events` | ingest 1–1000 events (API key) |
| POST | `/v1/events/validate` | contract check for producer CI |
| GET | `/v1/marts/advertisers`, `/v1/marts/daily`, `/v1/marts/funnel` | analytics |
| GET | `/v1/users/top`, `/v1/users/{id}` | user LTV + recent events |
| GET | `/v1/quality`, `/v1/runs`, `/v1/dead-letters`, `/v1/stream/lag`, `/v1/lineage` | operations |
| GET | `/health` | liveness + lag |
