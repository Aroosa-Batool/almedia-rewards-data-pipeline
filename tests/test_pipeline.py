from datetime import datetime, timedelta, timezone

import pytest

from pipeline import cli, quality, stream, transform
from pipeline.events import RewardEvent, generate


def ev(**kw):
    base = dict(event_id="e" * 10, event_type="reward_paid", user_id="u1", offer_id="o1",
                advertiser="A", country="de", reward_cents=100,
                occurred_at=datetime.now(timezone.utc).isoformat())
    return {**base, **kw}


def test_contract_normalizes_and_rejects():
    assert RewardEvent.model_validate(ev()).country == "DE"
    with pytest.raises(ValueError):
        RewardEvent.model_validate(ev(reward_cents=0))            # paid without reward
    with pytest.raises(ValueError):
        RewardEvent.model_validate(ev(occurred_at=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()))


def test_duplicates_are_idempotent(conn):
    stream.produce(conn, [ev(), ev(), ev()])                   # producer retried twice
    stats = stream.consume(conn)
    assert stats == {"read": 3, "inserted": 1, "duplicates": 2, "dead_lettered": 0}
    assert conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0] == 1


def test_bad_record_goes_to_dead_letter_not_crash(conn):
    stream.produce(conn, [ev(event_id="good-event-1"), ev(event_id="bad-event-1", country="")])
    stats = stream.consume(conn)
    assert stats["inserted"] == 1 and stats["dead_lettered"] == 1


def test_offsets_commit_so_replay_does_not_reprocess(conn):
    stream.produce(conn, generate(200, seed=1))
    first = stream.consume(conn)
    again = stream.consume(conn)
    assert first["read"] == 200 and again["read"] == 0
    assert sum(stream.lag(conn).values()) == 0


def test_same_user_same_partition():
    a, b = RewardEvent.model_validate(ev(event_id="x" * 9)), RewardEvent.model_validate(ev(event_id="y" * 9))
    assert a.partition() == b.partition()


def test_dag_order_and_cycle_detection():
    models = transform.load_models()
    order = transform.run_order(models)
    assert order.index("stg_events") < order.index("fct_rewards_paid") < order.index("mart_advertiser_daily")
    models["stg_events"].deps.add("dim_user_ltv")              # introduce a cycle
    with pytest.raises(ValueError, match="cycle"):
        transform.run_order(models)


def test_incremental_only_processes_new_rows(conn):
    cli.run(conn, events=500, seed=3)
    first = conn.execute("SELECT rows_written FROM model_state WHERE model_name='fct_rewards_paid'").fetchone()[0]
    assert first > 0
    transform.run_all(conn)                                    # no new data
    assert conn.execute("SELECT rows_written FROM model_state WHERE model_name='fct_rewards_paid'").fetchone()[0] == 0


def test_full_refresh_equals_incremental(conn):
    cli.run(conn, events=800, seed=4)
    cli.run(conn, events=800, seed=5)
    inc = conn.execute("SELECT COUNT(*), SUM(reward_cents) FROM fct_rewards_paid").fetchone()
    transform.run_all(conn, full_refresh=True)
    full = conn.execute("SELECT COUNT(*), SUM(reward_cents) FROM fct_rewards_paid").fetchone()
    assert tuple(inc) == tuple(full)


def test_quality_checks_pass_on_clean_run(conn):
    cli.run(conn, events=1000, seed=6)
    res = quality.run_checks(conn)
    assert res["passed"], [c for c in res["checks"] if c["status"] != "pass"]


def test_quality_catches_reconciliation_break(conn):
    cli.run(conn, events=1000, seed=7)
    conn.execute("UPDATE mart_advertiser_daily SET payout_cents = payout_cents + 1 WHERE rowid = 1")
    res = quality.run_checks(conn)
    assert not res["passed"]


def test_user_lookup_uses_index(conn):
    plan = " ".join(r["detail"] for r in conn.execute(
        "EXPLAIN QUERY PLAN SELECT * FROM raw_events WHERE user_id='u1' ORDER BY occurred_at DESC"))
    assert "ix_raw_events_user_time" in plan
