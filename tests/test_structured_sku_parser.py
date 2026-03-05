from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from structured_sku_parser import (
    ParsedSKUResult,
    StructuredParseError,
    StructuredSKUParserService,
    process_inventory_excel,
)


def make_service(tmp_path: Path) -> StructuredSKUParserService:
    return StructuredSKUParserService(
        enable_ai=False,
        db_path=tmp_path / "structured_logs.db",
        cache_size=100,
        ai_threshold=0.85,
        review_threshold=0.75,
    )


def test_rule_based_parse_returns_structured_result(tmp_path: Path) -> None:
    service = make_service(tmp_path)

    execution = service.analyze_title(
        title="Samsung Galaxy A52 A525 Charging Port",
    )

    assert execution.source == "rule"
    assert execution.parsed.primary_part == "CP"
    assert execution.parsed.sku == "GALAXY A52 A525 CP"
    assert execution.parsed.confidence == 0.98
    assert execution.parse_status == "parsed"


def test_ai_fallback_used_when_rule_confidence_low(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    service = make_service(tmp_path)

    low_rule = ParsedSKUResult(
        brand="SAMSUNG",
        model="GALAXY A52",
        model_code="A525",
        primary_part="CP",
        secondary_part=None,
        sku="GALAXY A52 A525 CP",
        confidence=0.70,
        corrections=["charng->charging"],
    )

    ai_result = ParsedSKUResult(
        brand="SAMSUNG",
        model="GALAXY A52",
        model_code="A525",
        primary_part="CP",
        secondary_part="HJ",
        sku="GALAXY A52 A525 CP HJ",
        confidence=0.80,
        corrections=[],
    )

    monkeypatch.setattr(
        service,
        "_run_rule_parser",
        lambda **_: (low_rule, "layer3_fuzzy_phrase", [{"from": "charng", "to": "charging"}]),
    )
    monkeypatch.setattr(service, "_parse_with_ai_retry", lambda **_: ai_result)

    execution = service.analyze_title(title="Samsung Galaxy A52 Charng Port With Headphone Jack")

    assert execution.source == "ai"
    assert execution.parsed.sku == "GALAXY A52 A525 CP HJ"
    assert execution.parsed.confidence == 0.80
    assert execution.parser_reason == "ai_structured_inference"


def test_ai_fallback_failure_raises_structured_parse_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    service = make_service(tmp_path)

    low_rule = ParsedSKUResult(
        brand="SAMSUNG",
        model="GALAXY A52",
        model_code="A525",
        primary_part="CP",
        secondary_part=None,
        sku="GALAXY A52 A525 CP",
        confidence=0.70,
        corrections=["charng->charging"],
    )

    monkeypatch.setattr(
        service,
        "_run_rule_parser",
        lambda **_: (low_rule, "layer3_fuzzy_phrase", [{"from": "charng", "to": "charging"}]),
    )
    monkeypatch.setattr(service, "_parse_with_ai_retry", lambda **_: None)

    with pytest.raises(StructuredParseError):
        service.analyze_title(title="Samsung Galaxy A52 Charng Port")


def test_cache_avoids_repeated_rule_parse(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    service = make_service(tmp_path)

    call_count = {"count": 0}

    original_run_rule = service._run_rule_parser

    def wrapped(**kwargs):
        call_count["count"] += 1
        return original_run_rule(**kwargs)

    monkeypatch.setattr(service, "_run_rule_parser", wrapped)

    first = service.analyze_title(title="Samsung Galaxy A52 A525 Battery")
    second = service.analyze_title(title="Samsung Galaxy A52 A525 Battery")

    assert first.source == "rule"
    assert second.source == "cache"
    assert call_count["count"] == 1


def test_process_inventory_excel_appends_structured_columns(tmp_path: Path) -> None:
    service = make_service(tmp_path)

    input_path = tmp_path / "input.xlsx"
    output_path = tmp_path / "output.xlsx"

    pd.DataFrame(
        {
            "Product Name": [
                "Samsung Galaxy A52 A525 Charging Port",
                "Samsung Galaxy A52 A525 Battery",
            ],
            "Product SKU": ["", ""],
            "Product Web SKU": ["", ""],
        }
    ).to_excel(input_path, index=False)

    result_df = service.process_inventory_excel(
        input_file=input_path,
        output_file=output_path,
    )

    assert output_path.exists()
    assert "Generated SKU" in result_df.columns
    assert "Confidence" in result_df.columns
    assert "Review Required" in result_df.columns
    assert result_df.loc[0, "Generated SKU"] == "GALAXY A52 A525 CP"
    assert result_df.loc[1, "Generated SKU"] == "GALAXY A52 A525 BATT"


def test_process_inventory_excel_helper_function(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output.xlsx"

    pd.DataFrame(
        {
            "Product Name": ["Samsung Galaxy A71 A716 Charging Port With Headphone Jack"],
            "Product SKU": [""],
            "Product Web SKU": [""],
        }
    ).to_csv(input_path, index=False)

    result_df = process_inventory_excel(
        input_file=input_path,
        output_file=output_path,
        service=service,
    )

    assert output_path.exists()
    assert result_df.loc[0, "Generated SKU"] == "GALAXY A71 A716 CP HJ"


def test_analyze_title_accepts_product_description_hint(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    execution = service.analyze_title(
        title="",
        product_description="Samsung Galaxy A52 A525 Battery replacement part",
    )

    assert execution.parsed.sku == "GALAXY A52 A525 BATT"
    assert execution.parse_status == "parsed"
