from __future__ import annotations

import io
from contextlib import contextmanager

import app as app_module


def test_healthz_and_readyz() -> None:
    client = app_module.app.test_client()

    health = client.get("/healthz")
    assert health.status_code == 200
    health_json = health.get_json()
    assert health_json["status"] == "ok"

    ready = client.get("/readyz")
    assert ready.status_code == 200
    ready_json = ready.get_json()
    assert ready_json["status"] == "ready"
    assert ready_json["max_concurrent_parse_jobs"] >= 1


def test_parse_inventory_returns_429_when_busy(monkeypatch) -> None:
    @contextmanager
    def always_busy_slot():
        yield False

    monkeypatch.setattr(app_module, "parse_job_slot", always_busy_slot)

    client = app_module.app.test_client()
    response = client.post(
        "/parse-inventory-api",
        data={"inventory_file": (io.BytesIO(b"col1,col2\n1,2\n"), "input.csv")},
        content_type="multipart/form-data",
    )

    assert response.status_code == 429
    payload = response.get_json()
    assert "busy" in payload["error"].lower()
