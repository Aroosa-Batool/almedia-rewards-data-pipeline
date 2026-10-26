"""A ~100-line dbt: SQL models with ref(), a DAG, and three materializations.

Why build it instead of importing dbt? So every step dbt hides is visible:
  1. parse each model's ref() calls      -> dependency graph
  2. topological sort                    -> run order (and cycle detection)
  3. render Jinja-like placeholders      -> plain SQL
  4. materialize as view / table / incremental
The real dbt does the same, plus tests, docs, adapters and Jinja macros.
"""
from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass, field
from graphlib import CycleError, TopologicalSorter
from pathlib import Path

from .db import record_run, transaction, utcnow

MODELS_DIR = Path(__file__).resolve().parent.parent / "models"
REF = re.compile(r"\{\{\s*ref\('([a-z0-9_]+)'\)\s*\}\}")
INC = re.compile(r"\{\{\s*incremental_filter\('([a-z0-9_]+)'\)\s*\}\}")
CONFIG = re.compile(r"^--\s*(materialized|unique_key):\s*(\S+)", re.M)


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
        sql = f.read_text()
        cfg = dict(CONFIG.findall(sql))
        models[f.stem] = Model(
            name=f.stem,
            sql=sql,
            materialized=cfg.get("materialized", "table"),
            unique_key=cfg.get("unique_key"),
            deps=set(REF.findall(sql)),
        )
    for m in models.values():
        missing = m.deps - models.keys()
        if missing:
            raise ValueError(f"{m.name} refs unknown model(s): {sorted(missing)}")
    return models


def run_order(models: dict[str, Model]) -> list[str]:
    try:
        return list(TopologicalSorter({n: m.deps for n, m in models.items()}).static_order())
    except CycleError as e:
        raise ValueError(f"dependency cycle: {e.args[1]}") from e


def _exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE name=?", (name,)).fetchone() is not None


def render(conn: sqlite3.Connection, m: Model, full_refresh: bool) -> str:
    sql = REF.sub(lambda r: r.group(1), m.sql)

    def inc(match: re.Match) -> str:
        if m.materialized != "incremental" or full_refresh or not _exists(conn, m.name):
            return "1=1"
        row = conn.execute("SELECT high_watermark FROM model_state WHERE model_name=?", (m.name,)).fetchone()
        return f"{match.group(1)} > '{row[0]}'" if row and row[0] else "1=1"

    return INC.sub(inc, sql)


def materialize(conn: sqlite3.Connection, m: Model, full_refresh: bool = False) -> int:
    sql = render(conn, m, full_refresh)
    if m.materialized == "view":
        conn.execute(f"DROP VIEW IF EXISTS {m.name}")
        conn.execute(f"CREATE VIEW {m.name} AS {sql}")
        return 0
    if m.materialized == "table":
        # Build into a temp name, then swap: readers never see a half-built table.
        tmp = f"{m.name}__tmp"
        conn.execute(f"DROP TABLE IF EXISTS {tmp}")
        conn.execute(f"CREATE TABLE {tmp} AS {sql}")
        conn.execute(f"DROP TABLE IF EXISTS {m.name}")
        conn.execute(f"ALTER TABLE {tmp} RENAME TO {m.name}")
        return conn.execute(f"SELECT COUNT(*) FROM {m.name}").fetchone()[0]
    if m.materialized == "incremental":
        if not m.unique_key:
            raise ValueError(f"incremental model {m.name} needs unique_key")
        if full_refresh or not _exists(conn, m.name):
            conn.execute(f"DROP TABLE IF EXISTS {m.name}")
            conn.execute(f"CREATE TABLE {m.name} AS SELECT * FROM ({sql}) WHERE 0")
            conn.execute(f"CREATE UNIQUE INDEX ux_{m.name}_{m.unique_key} ON {m.name} ({m.unique_key})")
        cur = conn.execute(f"INSERT OR REPLACE INTO {m.name} {sql}")
        wm = conn.execute(f"SELECT MAX(ingested_at) FROM {m.name}").fetchone()[0]
        conn.execute(
            "INSERT INTO model_state (model_name, high_watermark, last_run_at, rows_written) VALUES (?,?,?,?) "
            "ON CONFLICT(model_name) DO UPDATE SET high_watermark=excluded.high_watermark, "
            "last_run_at=excluded.last_run_at, rows_written=excluded.rows_written",
            (m.name, wm, utcnow(), cur.rowcount),
        )
        return cur.rowcount
    raise ValueError(f"unknown materialization {m.materialized!r}")


def run_all(conn: sqlite3.Connection, full_refresh: bool = False) -> dict[str, int]:
    models = load_models()
    results = {}
    started = utcnow()
    with transaction(conn):  # whole DAG is atomic: marts never disagree with each other
        for name in run_order(models):
            results[name] = materialize(conn, models[name], full_refresh)
    record_run(conn, "transform", started, "success", sum(results.values()), str(results))
    return results
