from __future__ import annotations

import json
from pathlib import Path

from backend.sku_validation_framework import run_validation_suite


def test_validation_framework_generates_structured_report(tmp_path: Path) -> None:
    output_json = tmp_path / "validation_report.json"
    output_md = tmp_path / "validation_report.md"

    report = run_validation_suite(
        output_json=output_json,
        output_markdown=output_md,
        include_performance=False,
        workspace_dir=tmp_path / "workspace",
        strict=False,
    )

    assert output_json.exists()
    assert output_md.exists()

    loaded = json.loads(output_json.read_text(encoding="utf-8"))
    assert loaded["summary"]["total_tests_run"] >= 20
    assert loaded["summary"]["tests_passed"] >= 1
    assert "average_confidence_score" in loaded["summary"]
    assert "new_patterns_learned" in loaded["summary"]
    assert "new_spelling_variations_learned" in loaded["summary"]

    categories = {row["category"] for row in loaded["categories"]}
    assert "CATEGORY 1 — CLEAN TITLES" in categories
    assert "CATEGORY 9 — CONFIDENCE SCORING" in categories

    assert report["summary"]["total_tests_run"] == loaded["summary"]["total_tests_run"]
