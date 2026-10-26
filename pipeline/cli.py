"""End-to-end run: python -m pipeline.cli --events 5000"""
from __future__ import annotations

import argparse
import json

from . import quality, stream, transform
from .db import connect
from .events import generate


def run(conn, events: int = 2000, seed: int | None = None, full_refresh: bool = False) -> dict:
    produced = stream.produce(conn, generate(events, seed=seed)) if events else 0
    consumed = {"read": 0, "inserted": 0, "duplicates": 0, "dead_lettered": 0}
    while any(stream.lag(conn).values()):          # drain the log in micro-batches
        for k, v in stream.consume(conn).items():
            consumed[k] += v
    models = transform.run_all(conn, full_refresh=full_refresh)
    checks = quality.run_checks(conn)
    return {"produced": produced, "consumed": consumed, "models": models, "quality_passed": checks["passed"]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=2000)
    ap.add_argument("--seed", type=int)
    ap.add_argument("--db")
    ap.add_argument("--full-refresh", action="store_true")
    a = ap.parse_args()
    print(json.dumps(run(connect(a.db), a.events, a.seed, a.full_refresh), indent=2))


if __name__ == "__main__":
    main()
