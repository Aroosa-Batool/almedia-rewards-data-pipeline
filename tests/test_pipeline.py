import uuid
from datetime import datetime, timedelta, timezone

import pytest

from pipeline import auth, cli, quality, stream, transform
from pipeline.db import migrate
from pipeline.events import RewardEvent, generate


def ev(**kw):
    base = dict(event_id=str(uuid.uuid4()), event_type="reward_paid", user_id="u1", offer_id="o1",
                advertiser="A", country="de", reward_cents=100,
                occurred_at=datetime.now(timezone.utc).isoformat())
    return {**base, **kw}


def test_contract_normalizes_and_rejects():
    assert RewardEvent.model_validate(ev()).country == "DE"
    for bad in (dict(reward_cents=0), dict(event_id="not-a-uuid"), dict(country="DEU"),
                dict(occurred_at=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat())):
        with pytest.raises(ValueError):
            RewardEvent.model_validate(ev(**bad))


def test_migrations_are_idempotent(conn):
    assert migrate(conn) == []


def test_duplicates_are_idempotent(conn):
    e = ev()
    stream.produce(conn, [e, e, e])                         # producer retried twice
    stats = stream.consume(conn)
    assert (stats["read"], stats["inserted"], stats["duplicates"]) == (3, 1, 2)


def test_bad_record_goes_to_dead_letter(conn):
    stream.produce(conn, [ev(), ev(country="")])
    stats = stream.consume(conn)
    assert stats["inserted"] == 1 and stats["dead_lettered"] == 1
    assert conn.execute("SELECT error FROM dead_letter_events").fetchone()["error"]


def test_offsets_commit_and_lag_drops_to_zero(conn):
    stream.produce(conn, generate(300, seed=1))
    assert sum(stream.lag(conn).values()) == 300
    first = stream.drain(conn)
    assert first["read"] == 300 and stream.consume(conn)["read"] == 0
    assert sum(stream.lag(conn).values()) == 0


def test_partition_is_locked_while_another_consumer_holds_it(conn, schema):
    from pipeline.db import connect
    stream.produce(conn, generate(50, seed=2))
    other = connect(schema)
    with other.transaction():
        other.execute("SELECT pg_advisory_xact_lock(hashtext(%s), %s)", (stream.GROUP, 0))
        stats = stream.consume(conn)
    other.close()
    assert stats["skipped_locked"] == 1


def test_same_user_same_partition():
    assert stream.partition_for(ev()) == stream.partition_for(ev())


def test_dag_order_and_cycle_detection():
    models = transform.load_models()
    order = transform.run_order(models)
    assert order.index("stg_events") < order.index("fct_rewards_paid") < order.index("mart_advertiser_daily")
    models["stg_events"].deps.add("dim_user_ltv")
    with pytest.raises(ValueError, match="cycle"):
        transform.run_order(models)


def test_incremental_equals_full_refresh(conn):
    cli.run(conn, simulate=600)
    cli.run(conn, simulate=600)
    inc = conn.execute("SELECT COUNT(*) AS n, SUM(reward_cents) AS s FROM fct_rewards_paid").fetchone()
    transform.run_all(conn, full_refresh=True)
    full = conn.execute("SELECT COUNT(*) AS n, SUM(reward_cents) AS s FROM fct_rewards_paid").fetchone()
    assert inc == full and inc["n"] > 0


def test_quality_passes_then_catches_reconciliation_break(conn):
    assert cli.run(conn, simulate=800)["quality_passed"]
    conn.execute("UPDATE mart_advertiser_daily SET payout_cents = payout_cents + 1 "
                 "WHERE ctid = (SELECT ctid FROM mart_advertiser_daily LIMIT 1)")
    assert not quality.run_checks(conn)["passed"]


def test_api_keys(conn):
    key = auth.create_producer(conn, "game-backend")
    assert auth.authenticate(conn, key) and not auth.authenticate(conn, key + "x")
    assert conn.execute("SELECT key_hash FROM producers").fetchone()["key_hash"] != key
    auth.revoke(conn, "game-backend")
    assert auth.authenticate(conn, key) is None
