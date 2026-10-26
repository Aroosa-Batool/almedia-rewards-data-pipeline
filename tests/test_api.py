import os

from fastapi.testclient import TestClient


def test_api_end_to_end(tmp_path):
    os.environ["PIPELINE_DB"] = str(tmp_path / "api.db")
    from api.index import app
    c = TestClient(app)
    assert c.get("/health").json()["raw_events"] > 0
    assert c.post("/events", json=[]).status_code == 422
    r = c.post("/pipeline/run?simulate=100").json()
    assert r["quality_passed"] is True
    assert c.get("/marts/advertisers").status_code == 200
    assert c.get("/lineage").json()["order"][0] in {"stg_events"}
    assert "Rewards Data Pipeline" in c.get("/").text
