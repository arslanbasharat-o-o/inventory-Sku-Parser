from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from backend.structured_sku_parser import (
    AIInterpretationResult,
    ParsedSKUResult,
    StructuredSKUParserService,
    process_inventory_excel,
)


def make_service(tmp_path: Path) -> StructuredSKUParserService:
    return StructuredSKUParserService(
        enable_ai=False,
        db_path=tmp_path / "structured_logs.db",
        cache_size=100,
        ai_threshold=0.40,
        review_threshold=0.75,
        rule_accept_threshold=0.80,
    )


def make_ai_service(tmp_path: Path) -> StructuredSKUParserService:
    service = StructuredSKUParserService(
        enable_ai=True,
        db_path=tmp_path / "structured_logs.db",
        cache_size=100,
        ai_threshold=0.40,
        review_threshold=0.75,
        rule_accept_threshold=0.80,
    )
    service._ai_enabled = True
    service._ai_provider = "gemini"
    return service


def test_rule_based_parse_returns_structured_result(tmp_path: Path) -> None:
    service = make_service(tmp_path)

    execution = service.analyze_title(
        title="Samsung Galaxy A52 A525 Charging Port",
    )

    assert execution.source == "rule"
    assert execution.parsed.primary_part == "CP"
    assert execution.parsed.sku == "GALAXY A52 A525 CP"
    assert execution.parsed.rule_confidence == 1.0
    assert execution.parsed.final_confidence == 1.0
    assert execution.parsed.confidence == 1.0
    assert execution.parse_status == "parsed"
    assert execution.parse_stage == "rule_only"
    assert execution.ai_used is False


def test_ai_fallback_used_when_rule_confidence_low(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    service = make_ai_service(tmp_path)

    low_rule = ParsedSKUResult(
        brand="SAMSUNG",
        model="GALAXY A52",
        model_code="A525",
        primary_part="CP",
        secondary_part=None,
        sku="GALAXY A52 A525 CP",
        confidence=0.20,
        rule_confidence=0.20,
        final_confidence=0.20,
        corrections=["charng->charging"],
    )

    ai_result = ParsedSKUResult(
        brand="SAMSUNG",
        model="GALAXY A52",
        model_code="A525",
        primary_part="CP",
        secondary_part="HJ",
        sku="GALAXY A52 A525 CP HJ",
        confidence=0.35,
        rule_confidence=0.20,
        final_confidence=0.35,
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
    assert execution.parsed.rule_confidence == 0.20
    assert execution.parsed.final_confidence == 0.35
    assert execution.parsed.confidence == 0.35
    assert execution.parser_reason == "ai_structured_inference"
    assert execution.parse_stage == "ai_assisted"
    assert execution.ai_used is True


def test_ai_prompt_includes_rule_normalization_contract(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    prompt = service._ai_prompt(
        title="HOME BUTTON FLEX FOR SAMSUNG NOTE 20",
        rule_result=ParsedSKUResult(
            brand="SAMSUNG",
            model="GALAXY NOTE 20",
            model_code="",
            primary_part="HB-FC",
            secondary_part=None,
            sku="GALAXY NOTE 20 HB-FC",
            confidence=0.98,
            corrections=[],
        ),
    )

    assert "HOME BUTTON FLEX -> HB-FC" in prompt
    assert "never invent abbreviations" in prompt.lower()
    assert "<= 31 characters including spaces" in prompt


def test_ai_validation_repairs_duplicate_flex_attributes(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    validated = service._validate_ai_result(
        AIInterpretationResult(
            brand="SAMSUNG",
            model="GALAXY NOTE 20",
            model_code="",
            primary_part="HB-FC",
            secondary_part="MB-FC",
            corrections=[],
        ),
        title="HOME BUTTON FLEX FOR SAMSUNG NOTE 20",
        rule_result=ParsedSKUResult(
            brand="SAMSUNG",
            model="GALAXY NOTE 20",
            model_code="",
            primary_part="HB-FC",
            secondary_part=None,
            sku="GALAXY NOTE 20 HB-FC",
            confidence=0.20,
            rule_confidence=0.20,
            final_confidence=0.20,
            corrections=[],
        ),
    )

    assert validated is not None
    assert validated.primary_part == "HB-FC"
    assert validated.secondary_part is None
    assert validated.sku == "GALAXY NOTE 20 HB-FC"


def test_ai_fallback_failure_preserves_rule_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    service = make_ai_service(tmp_path)

    low_rule = ParsedSKUResult(
        brand="SAMSUNG",
        model="GALAXY A52",
        model_code="A525",
        primary_part="CP",
        secondary_part=None,
        sku="GALAXY A52 A525 CP",
        confidence=0.20,
        rule_confidence=0.20,
        final_confidence=0.20,
        corrections=["charng->charging"],
    )

    monkeypatch.setattr(
        service,
        "_run_rule_parser",
        lambda **_: (low_rule, "layer3_fuzzy_phrase", [{"from": "charng", "to": "charging"}]),
    )
    monkeypatch.setattr(service, "_parse_with_ai_retry", lambda **_: None)

    execution = service.analyze_title(title="Samsung Galaxy A52 Charng Port")

    assert execution.source == "rule"
    assert execution.parsed.sku == "GALAXY A52 A525 CP"
    assert execution.parser_reason.endswith("+ai_fallback_missed")
    assert execution.parsed.validation_failed_reason == "ai_validation_failed"
    assert execution.parse_stage == "rule_normalized"
    assert execution.ai_used is False


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


def test_process_inventory_excel_does_not_use_ai_by_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    service = StructuredSKUParserService(
        enable_ai=True,
        db_path=tmp_path / "structured_logs.db",
        cache_size=100,
        ai_threshold=0.40,
        review_threshold=0.75,
        rule_accept_threshold=0.80,
    )

    low_rule = ParsedSKUResult(
        brand="SAMSUNG",
        model="GALAXY A52",
        model_code="A525",
        primary_part="CP",
        secondary_part=None,
        sku="GALAXY A52 A525 CP",
        confidence=0.20,
        rule_confidence=0.20,
        final_confidence=0.20,
        corrections=[],
    )

    monkeypatch.setattr(
        service,
        "_run_rule_parser",
        lambda **_: (low_rule, "layer3_fuzzy_phrase", []),
    )

    called = {"count": 0}

    def should_not_run(**_kwargs):
        called["count"] += 1
        return None

    monkeypatch.setattr(service, "_parse_with_ai_retry", should_not_run)

    input_path = tmp_path / "input.xlsx"
    output_path = tmp_path / "output.xlsx"
    pd.DataFrame({"Product Name": ["Samsung Galaxy A52 Charng Port"]}).to_excel(input_path, index=False)

    result_df = service.process_inventory_excel(
        input_file=input_path,
        output_file=output_path,
    )

    assert output_path.exists()
    assert result_df.loc[0, "Generated SKU"] == "GALAXY A52 A525 CP"
    assert called["count"] == 0


def test_analyze_title_accepts_product_description_hint(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    execution = service.analyze_title(
        title="",
        product_description="Samsung Galaxy A52 A525 Battery replacement part",
    )

    assert execution.parsed.sku == "GALAXY A52 A525 BATT"
    assert execution.parse_status == "parsed"


def test_model_only_title_returns_partial_status(tmp_path: Path) -> None:
    service = make_service(tmp_path)

    execution = service.analyze_title(title="Pixel 8 pro")

    assert execution.parsed.brand == "GOOGLE"
    assert execution.parsed.model == "PIXEL 8 PRO"
    assert execution.parsed.primary_part == ""
    assert execution.parsed.sku == "NOT UNDERSTANDABLE TITLE"
    assert execution.parsed.rule_confidence == 0.0
    assert execution.parsed.final_confidence == 0.0
    assert execution.parse_status == "partial"
    assert execution.parse_stage == "rule_normalized"


def test_model_only_partial_result_does_not_call_ai(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    service = StructuredSKUParserService(
        enable_ai=True,
        db_path=tmp_path / "structured_logs.db",
        cache_size=100,
        ai_threshold=0.40,
        review_threshold=0.75,
        rule_accept_threshold=0.80,
    )

    low_rule = ParsedSKUResult(
        brand="GOOGLE",
        model="PIXEL 8 PRO",
        model_code="",
        primary_part="",
        secondary_part=None,
        sku="NOT UNDERSTANDABLE TITLE",
        confidence=0.0,
        rule_confidence=0.0,
        final_confidence=0.0,
        corrections=[],
    )

    monkeypatch.setattr(
        service,
        "_run_rule_parser",
        lambda **_: (low_rule, "model_only_detected", []),
    )

    called = {"count": 0}

    def should_not_run(**_kwargs):
        called["count"] += 1
        return None

    monkeypatch.setattr(service, "_parse_with_ai_retry", should_not_run)

    execution = service.analyze_title(title="Pixel 8 pro")

    assert execution.source == "rule"
    assert execution.parse_status == "partial"
    assert called["count"] == 0


def test_same_title_repeats_deterministically_when_datasets_unchanged(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    results = []

    for _ in range(20):
        service.clear_cache()
        execution = service.analyze_title(
            title="Samsung Galaxy A52 A525 Charging Port With Headphone Jack",
        )
        results.append(
            (
                execution.parsed.brand,
                execution.parsed.model,
                execution.parsed.model_code,
                execution.parsed.primary_part,
                execution.parsed.secondary_part,
                execution.parsed.variant,
                execution.parsed.color,
                execution.parsed.sku,
                execution.parsed.rule_confidence,
                execution.parsed.final_confidence,
                execution.parse_stage,
                execution.ai_used,
            )
        )

    assert len(set(results)) == 1
