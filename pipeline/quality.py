"""Data-quality checks: catch the problem before a stakeholder does.

Each check is a SQL query that returns the number of *bad* rows (0 = pass), the same
idea as dbt tests / Great Expectations. Severity decides whether a failure blocks.
"""
from __future__ import annotations

import sqlite3
from dataclasses import asdict, dataclass

from .db import record_run, utcnow


@dataclass
class Check:
    name: str
    sql: str
    severity: str = "error"   # error = block downstream; warn = alert only
    threshold: int = 0        # allowed bad rows


CHECKS = [
    Check("raw_events.event_id unique",
          "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM raw_events"),
    Check("fct_rewards_paid.reward_cents > 0",
          "SELECT COUNT(*) FROM fct_rewards_paid WHERE reward_cents <= 0"),
    Check("fct_offer_funnel: completers <= clickers",
          "SELECT COUNT(*) FROM fct_offer_funnel WHERE completers > clickers", severity="warn"),
    Check("reconciliation: mart payout == fact payout",
          "SELECT ABS((SELECT COALESCE(SUM(payout_cents),0) FROM mart_advertiser_daily)"
          " - (SELECT COALESCE(SUM(reward_cents),0) FROM fct_rewards_paid))"),
    Check("freshness: newest event < 60 min old",
          "SELECT CASE WHEN (JULIANDAY('now') - JULIANDAY(MAX(ingested_at))) * 1440 > 60 THEN 1 ELSE 0 END FROM raw_events",
          severity="warn"),
    Check("dead-letter rate < 10% of last hour",
          "SELECT CASE WHEN 1.0 * (SELECT COUNT(*) FROM dead_letter_events WHERE received_at > DATETIME('now','-1 hour'))"
          " / NULLIF((SELECT COUNT(*) FROM raw_events WHERE ingested_at > DATETIME('now','-1 hour')),0) > 0.10"
          " THEN 1 ELSE 0 END", severity="warn"),
    Check("orphan payments: reward_paid without offer_complete",
          "SELECT COUNT(*) FROM fct_rewards_paid p WHERE NOT EXISTS (SELECT 1 FROM raw_events c"
          " WHERE c.user_id=p.user_id AND c.offer_id=p.offer_id AND c.event_type='offer_complete')",
          severity="warn", threshold=5),
]


def run_checks(conn: sqlite3.Connection) -> dict:
    started = utcnow()
    results = []
    for c in CHECKS:
        try:
            bad = conn.execute(c.sql).fetchone()[0] or 0
            status = "pass" if bad <= c.threshold else ("fail" if c.severity == "error" else "warn")
        except sqlite3.OperationalError as e:  # table missing -> pipeline has not run yet
            bad, status = None, f"error: {e}"
        results.append({**asdict(c), "bad_rows": bad, "status": status})
    blocking = [r for r in results if r["status"] == "fail" or r["status"].startswith("error")]
    record_run(conn, "quality", started, "failed" if blocking else "success", len(results),
               ", ".join(r["name"] for r in blocking))
    return {"passed": not blocking, "checks": results}
