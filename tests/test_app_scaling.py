from __future__ import annotations

import io
from contextlib import contextmanager

import backend.app as app_module
from backend.bulk_job_runner import BulkJobTimeoutError


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


def test_parse_inventory_returns_504_when_bulk_worker_times_out(monkeypatch) -> None:
    def timeout_runner(*_args, **_kwargs):
        raise BulkJobTimeoutError("Bulk parsing exceeded 120 seconds.")

    monkeypatch.setattr(app_module, "run_legacy_inventory_job", timeout_runner)

    client = app_module.app.test_client()
    response = client.post(
        "/parse-inventory-api",
        data={"inventory_file": (io.BytesIO(b"Product Name\nPixel 8 Ear Speaker\n"), "input.csv")},
        content_type="multipart/form-data",
    )

    assert response.status_code == 504
    payload = response.get_json()
    assert "exceeded" in payload["error"].lower()


def test_analyze_title_api_returns_live_analysis_payload() -> None:
    client = app_module.app.test_client()
    response = client.post(
        "/analyze-title",
        json={"title": "Pixel 8 ear speaker"},
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["parse_status"] in {"parsed", "partial", "not_understandable"}
    assert payload["sku"] == "PIXEL 8 ES"
    assert payload["part"] == "ES"
    assert payload["rule_confidence"] >= 0.80
    assert payload["final_confidence"] >= payload["rule_confidence"]
    assert payload["parse_stage"] == "rule_only"
    assert payload["ai_used"] is False
    assert "validation_failed_reason" in payload


def test_analyze_title_api_returns_partial_for_model_only_input() -> None:
    client = app_module.app.test_client()
    response = client.post(
        "/analyze-title",
        json={"title": "Pixel 8 pro"},
    )
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["parse_status"] == "partial"
    assert payload["model"] == "PIXEL 8 PRO"
    assert payload["part"] == ""
    assert payload["sku"] == ""
    assert payload["parse_stage"] == "rule_normalized"
    assert payload["ai_used"] is False
