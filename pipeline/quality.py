"""Data-quality checks. Each check is SQL returning the number of BAD rows (0 = pass).

severity "error" blocks (the run is marked failed, the dashboard turns red);
severity "warn" alerts only.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

import psycopg

from .db import now, record_run


@dataclass
class Check:
    name: str
    sql: str
    severity: str = "error"
    threshold: int = 0


CHECKS = [
    Check("fct_rewards_paid.event_id unique",
          "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM fct_rewards_paid"),
    Check("fct_rewards_paid.reward_cents > 0",
          "SELECT COUNT(*) FROM fct_rewards_paid WHERE reward_cents <= 0"),
    # Only raw rows up to the fact's watermark: newer rows are simply not transformed yet.
    Check("fct_rewards_paid complete vs raw (up to watermark)",
          "SELECT COUNT(*) FROM raw_events r WHERE r.event_type = 'reward_paid'"
          " AND r.ingested_at <= (SELECT high_watermark FROM model_state WHERE model_name = 'fct_rewards_paid')"
          " AND NOT EXISTS (SELECT 1 FROM fct_rewards_paid f WHERE f.event_id = r.event_id)"),
    Check("reconciliation: mart payout = fact payout",
          "SELECT ABS((SELECT COALESCE(SUM(payout_cents),0) FROM mart_advertiser_daily)"
          " - (SELECT COALESCE(SUM(reward_cents),0) FROM fct_rewards_paid))"),
    Check("funnel: completers <= clickers",
          "SELECT COUNT(*) FROM fct_offer_funnel WHERE completers > clickers", severity="warn"),
    Check("freshness: newest event < 90 min old",
          "SELECT (COALESCE(MAX(ingested_at), 'epoch') < now() - interval '90 minutes')::int FROM raw_events",
          severity="warn"),
    Check("consumer lag < 5,000 messages",
          "SELECT GREATEST(0, (SELECT COUNT(*) FROM event_log l LEFT JOIN consumer_offsets o"
          " ON o.partition_key = l.partition_key AND o.consumer_group = 'raw-loader'"
          " WHERE l.offset_id > COALESCE(o.committed_offset, 0)) - 5000)", severity="warn"),
    Check("dead-letter rate < 10% (24 h)",
          "SELECT (COALESCE((SELECT COUNT(*) FROM dead_letter_events WHERE received_at > now() - interval '1 day')::numeric"
          " / NULLIF((SELECT COUNT(*) FROM raw_events WHERE ingested_at > now() - interval '1 day'), 0), 0) > 0.10)::int",
          severity="warn"),
    Check("orphan payouts (paid without completion)",
          "SELECT COUNT(*) FROM fct_rewards_paid p WHERE NOT EXISTS (SELECT 1 FROM raw_events c"
          " WHERE c.event_type = 'offer_complete' AND c.user_id = p.user_id AND c.offer_id = p.offer_id)",
          severity="warn", threshold=25),
]


def run_checks(conn: psycopg.Connection, record: bool = True) -> dict:
    started = now(conn)
    results = []
    for c in CHECKS:
        try:
            with conn.transaction():
                bad = list(conn.execute(c.sql).fetchone().values())[0] or 0
            status = "pass" if bad <= c.threshold else ("fail" if c.severity == "error" else "warn")
        except psycopg.errors.UndefinedTable:
            bad, status = None, "fail"
        results.append({**asdict(c), "bad_rows": bad, "status": status})
    blocking = [r["name"] for r in results if r["status"] == "fail"]
    if record:
        record_run(conn, "quality", started, "failed" if blocking else "success", len(results),
                   {"failed": blocking, "warned": [r["name"] for r in results if r["status"] == "warn"]})
    return {"passed": not blocking, "checks": results}
