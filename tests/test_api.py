import os

from fastapi.testclient import TestClient

from pipeline import auth
from pipeline.events import generate


def test_api_end_to_end(conn, schema):
    os.environ["DB_SCHEMA"] = schema
    import api.index as m
    m._pool = None
    c = TestClient(m.app)
    key = auth.create_producer(conn, "test")
    assert c.post("/v1/events", json=generate(5)).status_code == 401
    assert c.post("/v1/events", json=[], headers={"X-API-Key": key}).status_code == 422
    r = c.post("/v1/events", json=generate(400), headers={"X-API-Key": key})
    assert r.status_code == 202 and r.json()["consumed"]["read"] == 400
    assert c.get("/v1/marts/advertisers").status_code == 200
    assert c.get("/v1/quality").json()["passed"]
    assert c.post("/v1/demo/traffic").status_code == 200
    assert c.post("/v1/demo/traffic").status_code == 429   # cooldown
    assert c.get("/health").json()["lag"] == 0
    assert "RewardStream" in c.get("/").text
    m._pool.close()
