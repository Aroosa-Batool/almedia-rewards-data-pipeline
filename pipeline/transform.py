"""SQL model runner: ref(), a dependency DAG, and view / table / incremental materializations.

  1. parse each model's ref() calls      -> dependency graph
  2. topological sort                    -> run order (and cycle detection)
  3. render placeholders                 -> plain SQL
  4. materialize, the whole DAG in ONE transaction (Postgres DDL is transactional),
     so readers never see marts that disagree with each other.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import timedelta
from graphlib import CycleError, TopologicalSorter
from pathlib import Path

import psycopg
from psycopg import sql

from .db import now, record_run

MODELS_DIR = Path(__file__).resolve().parent.parent / "models"
REF = re.compile(r"\{\{\s*ref\('([a-z0-9_]+)'\)\s*\}\}")
INC = re.compile(r"\{\{\s*incremental_filter\('([a-z0-9_]+)'\)\s*\}\}")
CONFIG = re.compile(r"^--\s*(materialized|unique_key):\s*(\S+)", re.M)
# Rows can commit slightly out of ingested_at order (concurrent consumers). Re-reading a
# small window behind the watermark and upserting makes late commits impossible to miss.
LOOKBACK = timedelta(minutes=10)


@dataclass
class Model:
    name: str
    sql: str
    materialized: str = "table"
    unique_key: str | None = None
    deps: set[str] = field(default_factory=set)


def load_models(models_dir: Path = MODELS_DIR) -> dict[str, Model]:
    models = {}
    for f in sorted(models_dir.glob("*.sql")):
        text = f.read_text()
        cfg = dict(CONFIG.findall(text))
        models[f.stem] = Model(f.stem, text, cfg.get("materialized", "table"), cfg.get("unique_key"),
                               set(REF.findall(text)))
    for m in models.values():
        if missing := m.deps - models.keys():
            raise ValueError(f"{m.name} refs unknown model(s): {sorted(missing)}")
    return models


def run_order(models: dict[str, Model]) -> list[str]:
    try:
        return list(TopologicalSorter({n: m.deps for n, m in models.items()}).static_order())
    except CycleError as e:
        raise ValueError(f"dependency cycle: {e.args[1]}") from e


def _exists(conn: psycopg.Connection, name: str) -> bool:
    return conn.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (name,)).fetchone()["ok"]


def render(conn: psycopg.Connection, m: Model, full_refresh: bool) -> str:
    text = REF.sub(lambda r: r.group(1), m.sql)

    def inc(match: re.Match) -> str:
        if m.materialized != "incremental" or full_refresh or not _exists(conn, m.name):
            return "TRUE"
        row = conn.execute("SELECT high_watermark FROM model_state WHERE model_name=%s", (m.name,)).fetchone()
        if not row or row["high_watermark"] is None:
            return "TRUE"
        since = sql.Literal(row["high_watermark"] - LOOKBACK).as_string(conn)
        return f"{match.group(1)} > {since}::timestamptz"

    return INC.sub(inc, text)


def materialize(conn: psycopg.Connection, m: Model, full_refresh: bool = False) -> int:
    body = render(conn, m, full_refresh)
    name = sql.Identifier(m.name)
    if m.materialized == "view":
        conn.execute(sql.SQL("DROP VIEW IF EXISTS {}").format(name))
        conn.execute(sql.SQL("CREATE VIEW {} AS ").format(name) + sql.SQL(body))
        return 0
    if m.materialized == "table":
        tmp = sql.Identifier(f"{m.name}__new")
        conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(tmp))
        conn.execute(sql.SQL("CREATE TABLE {} AS ").format(tmp) + sql.SQL(body))
        conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(name))
        conn.execute(sql.SQL("ALTER TABLE {} RENAME TO {}").format(tmp, name))
        return conn.execute(sql.SQL("SELECT COUNT(*) AS n FROM {}").format(name)).fetchone()["n"]
    if m.materialized == "incremental":
        if not m.unique_key:
            raise ValueError(f"incremental model {m.name} needs unique_key")
        key = sql.Identifier(m.unique_key)
        if full_refresh or not _exists(conn, m.name):
            conn.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(name))
            conn.execute(sql.SQL("CREATE TABLE {} AS SELECT * FROM (").format(name) + sql.SQL(body)
                         + sql.SQL(") s WITH NO DATA"))
            conn.execute(sql.SQL("ALTER TABLE {} ADD PRIMARY KEY ({})").format(name, key))
        cols = [d.name for d in conn.execute(sql.SQL("SELECT * FROM {} LIMIT 0").format(name)).description]
        updates = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in cols if c != m.unique_key)
        cur = conn.execute(sql.SQL("INSERT INTO {} ").format(name) + sql.SQL(body)
                           + sql.SQL(" ON CONFLICT ({}) DO UPDATE SET ").format(key) + updates)
        written = cur.rowcount
        wm = conn.execute(sql.SQL("SELECT MAX(ingested_at) AS wm FROM {}").format(name)).fetchone()["wm"]
        conn.execute(
            """INSERT INTO model_state (model_name, high_watermark, last_run_at, rows_written)
               VALUES (%s,%s,now(),%s)
               ON CONFLICT (model_name) DO UPDATE SET high_watermark=EXCLUDED.high_watermark,
                    last_run_at=EXCLUDED.last_run_at, rows_written=EXCLUDED.rows_written""",
            (m.name, wm, written),
        )
        return written
    raise ValueError(f"unknown materialization {m.materialized!r}")


def run_all(conn: psycopg.Connection, full_refresh: bool = False) -> dict[str, int] | None:
    """Run the DAG. Returns None if another process is already running it."""
    models = load_models()
    started = now(conn)
    with conn.transaction():
        if not conn.execute("SELECT pg_try_advisory_xact_lock(hashtext(current_schema() || ':transform')) AS ok"
                            ).fetchone()["ok"]:
            return None
        results = {n: materialize(conn, models[n], full_refresh) for n in run_order(models)}
        record_run(conn, "transform", started, "success", sum(results.values()), results)
    return results


def seconds_since_last_run(conn: psycopg.Connection) -> float | None:
    row = conn.execute("SELECT EXTRACT(EPOCH FROM now() - MAX(finished_at)) AS s FROM pipeline_runs "
                       "WHERE stage='transform' AND status='success'").fetchone()
    return float(row["s"]) if row["s"] is not None else None
