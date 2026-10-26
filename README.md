# Rewards Data Pipeline

A production-shaped data pipeline for a rewarded-advertising platform (offer clicks → completions → reward payouts), built as interview prep for **Almedia – Junior Software Engineer, Data**.

- **Ingestion API** (FastAPI) → **partitioned append-only log** (Kafka semantics: keys, offsets, consumer groups, lag)
- **Consumer** with at-least-once delivery + idempotent writes = effectively-once
- **dbt-style transformation layer** written from scratch: `ref()`, DAG, view / table / incremental materializations, watermarks, full refresh
- **Data-quality checks**: uniqueness, reconciliation, freshness, dead-letter rate, orphan detection
- **Monitoring**: run history, consumer lag, dead-letter queue, `EXPLAIN QUERY PLAN` for index checks
- **Tests** (pytest) + **CI** (GitHub Actions)

Live demo: see `GUIDE.md` (top). Full learning guide, trade-offs and interview questions: [`GUIDE.md`](GUIDE.md).

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
python -m pipeline.cli --events 5000 --seed 1
uvicorn api.index:app --reload   # http://127.0.0.1:8000
```
