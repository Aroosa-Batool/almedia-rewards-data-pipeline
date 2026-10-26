"""Operations CLI.

  python -m pipeline.cli migrate
  python -m pipeline.cli create-producer <name>      # prints the API key once
  python -m pipeline.cli revoke-producer <name>
  python -m pipeline.cli run [--simulate N] [--full-refresh]
  python -m pipeline.cli send --url URL --key KEY [--events N]   # HTTP producer
"""
from __future__ import annotations

import argparse
import json
import urllib.request

from . import auth, quality, stream, transform
from .db import connect, migrate
from .events import generate


def run(conn, simulate: int = 0, full_refresh: bool = False) -> dict:
    produced = stream.produce(conn, generate(simulate)) if simulate else 0
    consumed = stream.drain(conn)
    models = transform.run_all(conn, full_refresh=full_refresh)
    checks = quality.run_checks(conn)
    return {"produced": produced, "consumed": consumed, "models": models, "quality_passed": checks["passed"]}


def send(url: str, key: str, events: int, batch: int = 500) -> list[dict]:
    """A real producer: POST batches over HTTP like any client service would."""
    out, payload = [], generate(events)
    for i in range(0, len(payload), batch):
        req = urllib.request.Request(
            url.rstrip("/") + "/v1/events", data=json.dumps(payload[i:i + batch]).encode(),
            headers={"Content-Type": "application/json", "X-API-Key": key}, method="POST")
        with urllib.request.urlopen(req, timeout=60) as r:
            out.append(json.loads(r.read()))
    return out


def main() -> None:
    ap = argparse.ArgumentParser(prog="pipeline")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate")
    sub.add_parser("create-producer").add_argument("name")
    sub.add_parser("revoke-producer").add_argument("name")
    r = sub.add_parser("run")
    r.add_argument("--simulate", type=int, default=0)
    r.add_argument("--full-refresh", action="store_true")
    s = sub.add_parser("send")
    s.add_argument("--url", required=True)
    s.add_argument("--key", required=True)
    s.add_argument("--events", type=int, default=300)
    a = ap.parse_args()

    if a.cmd == "send":
        print(json.dumps(send(a.url, a.key, a.events), indent=2))
        return
    conn = connect()
    if a.cmd == "migrate":
        print("applied:", migrate(conn) or "nothing (up to date)")
    elif a.cmd == "create-producer":
        print(auth.create_producer(conn, a.name))
    elif a.cmd == "revoke-producer":
        print("revoked" if auth.revoke(conn, a.name) else "not found")
    elif a.cmd == "run":
        print(json.dumps(run(conn, a.simulate, a.full_refresh), indent=2, default=str))


if __name__ == "__main__":
    main()
