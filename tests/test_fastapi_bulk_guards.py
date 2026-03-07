from __future__ import annotations

from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

import backend.fastapi_app as fastapi_module
from backend.bulk_job_runner import BulkJobTimeoutError


def test_fastapi_parse_inventory_queues_and_polls_completed_job(monkeypatch, tmp_path: Path) -> None:
    output_dir = tmp_path / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "job-123_products_sku_processed.xlsx").write_bytes(b"placeholder")

    class FakeQueue:
        def submit_legacy_job(self, *, input_file, output_file):
            return {
                "job_id": "job-123",
                "status": "queued",
                "queue_position": 0,
                "poll_interval_ms": 1000,
            }

        def get_job(self, job_id: str):
            assert job_id == "job-123"
            return {
                "job_id": "job-123",
                "status": "completed",
                "queue_position": 0,
                "poll_interval_ms": 1000,
                "download_file": "job-123_products_sku_processed.xlsx",
            }

    monkeypatch.setattr(fastapi_module, "BULK_JOB_QUEUE", FakeQueue())
    monkeypatch.setattr(fastapi_module, "OUTPUT_DIR", output_dir)
    monkeypatch.setattr(
        fastapi_module,
        "load_processed_inventory_preview",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {
                    "Product Name": "Pixel 8 Ear Speaker",
                    "Product SKU": "",
                    "Product Web SKU": "",
                    "Product New SKU": "PIXEL 8 ES",
                    "SKU Duplicate": "",
                    "Title Duplicate": "",
                }
            ]
        ),
    )

    client = TestClient(fastapi_module.app)

    submit = client.post(
        "/parse-inventory-api",
        files={"inventory_file": ("input.csv", b"Product Name\nPixel 8 Ear Speaker\n", "text/csv")},
    )
    assert submit.status_code == 202
    assert submit.json()["job_id"] == "job-123"

    poll = client.get("/parse-inventory-api/job-123")
    assert poll.status_code == 200
    payload = poll.json()
    assert payload["status"] == "completed"
    assert payload["stats"]["parsed_rows"] == 1
    assert payload["rows"][0]["Product New SKU"] == "PIXEL 8 ES"


def test_fastapi_parse_inventory_returns_failed_job_payload(monkeypatch) -> None:
    class FakeQueue:
        def get_job(self, job_id: str):
            assert job_id == "job-404"
            return {
                "job_id": "job-404",
                "status": "failed",
                "error": "Bulk parsing exceeded 120 seconds.",
            }

    monkeypatch.setattr(fastapi_module, "BULK_JOB_QUEUE", FakeQueue())

    client = TestClient(fastapi_module.app)
    response = client.get("/parse-inventory-api/job-404")

    assert response.status_code == 200
    assert response.json()["status"] == "failed"
    assert "exceeded" in response.json()["error"].lower()


def test_fastapi_process_inventory_excel_returns_504_when_worker_times_out(
    monkeypatch,
    tmp_path: Path,
) -> None:
    def timeout_runner(*_args, **_kwargs):
        raise BulkJobTimeoutError("Bulk parsing exceeded 120 seconds.")

    monkeypatch.setattr(fastapi_module, "run_structured_inventory_job", timeout_runner)

    input_path = tmp_path / "input.xlsx"
    input_path.write_bytes(b"placeholder")
    output_path = tmp_path / "output.xlsx"

    client = TestClient(fastapi_module.app)
    response = client.post(
        "/process-inventory-excel",
        json={
            "input_file": str(input_path),
            "output_file": str(output_path),
            "title_column": "Product Name",
        },
    )

    assert response.status_code == 504
    assert "exceeded" in response.json()["detail"].lower()


def test_fastapi_cache_status_exposes_ai_and_learning_meta() -> None:
    client = TestClient(fastapi_module.app)
    response = client.get("/cache/status")

    assert response.status_code == 200
    payload = response.json()
    for key in (
        "single_ai_model",
        "batch_ai_model",
        "ai_single_threshold",
        "rule_accept_threshold",
        "approved_pattern_count",
        "candidate_pattern_count",
        "approved_spelling_count",
        "candidate_spelling_count",
    ):
        assert key in payload


def test_fastapi_candidate_learning_review_endpoints(monkeypatch) -> None:
    class FakeTrainingService:
        def list_candidate_learning(self):
            return {
                "patterns": [
                    {
                        "candidate_type": "pattern",
                        "normalized_source": "TOP RECEIVER SPEAKER",
                        "mapped_value": "ES",
                        "review_status": "PENDING",
                        "review_note": "",
                    }
                ],
                "spellings": [],
            }

        def review_candidate_learning(self, **kwargs):
            return {
                "candidate_type": kwargs["candidate_type"],
                "normalized_source": kwargs["normalized_source"].upper(),
                "mapped_value": kwargs["mapped_value"].upper(),
                "review_status": kwargs["review_status"].upper(),
                "review_note": kwargs["review_note"],
            }

    monkeypatch.setattr(fastapi_module, "TRAINING_SERVICE", FakeTrainingService())

    client = TestClient(fastapi_module.app)
    listing = client.get("/admin/training/candidates")
    assert listing.status_code == 200
    assert listing.json()["patterns"][0]["review_status"] == "PENDING"

    review = client.post(
        "/admin/training/review-candidate",
        json={
            "candidate_type": "pattern",
            "normalized_source": "top receiver speaker",
            "mapped_value": "ES",
            "review_status": "approved",
            "review_note": "Looks valid",
        },
    )
    assert review.status_code == 200
    assert review.json()["review_status"] == "APPROVED"
