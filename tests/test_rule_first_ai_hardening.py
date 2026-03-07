from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

import benchmark_sku_engine as benchmark_module
from backend.sku_intelligence_engine import (
    EngineConfig,
    PARTS_DICTIONARY_FILE,
    PARTS_ONTOLOGY_FILE,
    PART_CODE_RULES_FILE,
    SKUIntelligenceEngine,
    SPELLING_CORRECTIONS_FILE,
)
from backend.training_dashboard_service import TrainingDashboardService


def test_candidate_learned_runtime_data_is_quarantined_by_default(tmp_path: Path) -> None:
    learned_patterns_file = tmp_path / "learned_patterns.json"
    approved_learned_patterns_file = tmp_path / "approved_learned_patterns.json"
    learned_spelling_file = tmp_path / "learned_spelling_variations.json"
    approved_spelling_file = tmp_path / "approved_learned_spelling_variations.json"

    learned_patterns_file.write_text(
        json.dumps(
            {
                "transparent clear hybrid matching": "BC",
            },
            indent=2,
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )
    approved_learned_patterns_file.write_text(
        json.dumps(
            {
                "custom charging tray phrase": "ST",
            },
            indent=2,
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )
    learned_spelling_file.write_text(
        json.dumps(
            {
                "slim": "sim",
            },
            indent=2,
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )
    approved_spelling_file.write_text(
        json.dumps(
            {
                "samxung": "samsung",
            },
            indent=2,
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )

    engine = SKUIntelligenceEngine(
        EngineConfig(
            ontology_file=PARTS_ONTOLOGY_FILE,
            dictionary_file=PARTS_DICTIONARY_FILE,
            part_rules_file=PART_CODE_RULES_FILE,
            spelling_corrections_file=SPELLING_CORRECTIONS_FILE,
            learned_patterns_file=learned_patterns_file,
            approved_learned_patterns_file=approved_learned_patterns_file,
            learned_spelling_variations_file=learned_spelling_file,
            approved_learned_spelling_variations_file=approved_spelling_file,
            legacy_learned_title_patterns_file=tmp_path / "legacy_title_patterns.json",
            legacy_learned_parts_file=tmp_path / "legacy_parts.json",
            unknown_log_file=tmp_path / "unknown_parts.json",
            training_patterns_file=tmp_path / "training_patterns.json",
            enable_candidate_learned_patterns=False,
            enable_candidate_spelling_variations=False,
            enable_vector_layer=False,
        )
    )

    assert "transparent clear hybrid matching" not in engine.learned_patterns
    assert engine.learned_patterns["custom charging tray phrase"] == "ST"
    assert "slim" not in engine.spelling_corrections
    assert engine.spelling_corrections["samxung"] == "samsung"


def test_promote_candidate_learning_moves_only_safe_entries(tmp_path: Path, monkeypatch) -> None:
    service = TrainingDashboardService(base_dir=tmp_path, structured_log_db_path=tmp_path / "structured.db")
    service.learned_patterns_file = tmp_path / "candidate_patterns.json"
    service.approved_learned_patterns_file = tmp_path / "approved_patterns.json"
    service.learned_spelling_file = tmp_path / "candidate_spelling.json"
    service.approved_learned_spelling_file = tmp_path / "approved_spelling.json"

    for path, seed in (
        (service.learned_patterns_file, {
            "top receiver speaker": "ES",
            "microsoft surface laptop refurbished": "LCD",
        }),
        (service.approved_learned_patterns_file, {}),
        (service.learned_spelling_file, {
            "samsng": "samsung",
            "cloud": "loud",
        }),
        (service.approved_learned_spelling_file, {}),
    ):
        path.write_text(json.dumps(seed, indent=2, ensure_ascii=True), encoding="utf-8")

    monkeypatch.setattr(service, "_reload_parser", lambda: None)

    pending = service.list_candidate_learning()
    assert pending["patterns"][0]["review_status"] == "PENDING"
    assert pending["spellings"][0]["review_status"] == "PENDING"

    service.review_candidate_learning(
        candidate_type="pattern",
        normalized_source="top receiver speaker",
        mapped_value="ES",
        review_status="approved",
    )
    service.review_candidate_learning(
        candidate_type="pattern",
        normalized_source="microsoft surface laptop refurbished",
        mapped_value="LCD",
        review_status="rejected",
    )
    service.review_candidate_learning(
        candidate_type="spelling",
        normalized_source="samsng",
        mapped_value="samsung",
        review_status="approved",
    )
    service.review_candidate_learning(
        candidate_type="spelling",
        normalized_source="cloud",
        mapped_value="loud",
        review_status="rejected",
    )

    result = service.promote_candidate_learning()

    approved_patterns = json.loads(service.approved_learned_patterns_file.read_text(encoding="utf-8"))
    candidate_patterns = json.loads(service.learned_patterns_file.read_text(encoding="utf-8"))
    approved_spelling = json.loads(service.approved_learned_spelling_file.read_text(encoding="utf-8"))
    candidate_spelling = json.loads(service.learned_spelling_file.read_text(encoding="utf-8"))

    assert result == {
        "promoted_patterns": 1,
        "rejected_patterns": 1,
        "pending_patterns": 0,
        "promoted_spelling": 1,
        "rejected_spelling": 1,
        "pending_spelling": 0,
    }
    assert approved_patterns == {"top receiver speaker": "ES"}
    assert candidate_patterns == {}
    assert approved_spelling == {"samsng": "samsung"}
    assert candidate_spelling == {}


def test_unreviewed_candidates_stay_quarantined_during_promotion(tmp_path: Path, monkeypatch) -> None:
    service = TrainingDashboardService(base_dir=tmp_path, structured_log_db_path=tmp_path / "structured.db")
    service.learned_patterns_file.write_text(
        json.dumps({"charging socket board": "CP"}, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    service.approved_learned_patterns_file.write_text("{}", encoding="utf-8")
    service.learned_spelling_file.write_text(
        json.dumps({"samsng": "samsung"}, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    service.approved_learned_spelling_file.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(service, "_reload_parser", lambda: None)

    result = service.promote_candidate_learning()

    assert result == {
        "promoted_patterns": 0,
        "rejected_patterns": 0,
        "pending_patterns": 1,
        "promoted_spelling": 0,
        "rejected_spelling": 0,
        "pending_spelling": 1,
    }
    assert json.loads(service.approved_learned_patterns_file.read_text(encoding="utf-8")) == {}
    assert json.loads(service.approved_learned_spelling_file.read_text(encoding="utf-8")) == {}
    assert json.loads(service.learned_patterns_file.read_text(encoding="utf-8")) == {"charging socket board": "CP"}
    assert json.loads(service.learned_spelling_file.read_text(encoding="utf-8")) == {"samsng": "samsung"}


def test_build_txparts_benchmark_keeps_only_rule_only_rows(tmp_path: Path, monkeypatch) -> None:
    catalog_path = tmp_path / "catalog.xlsx"
    benchmark_path = tmp_path / "txparts_benchmark.csv"
    pd.DataFrame(
        {
            "Product Name": [
                "Samsung Galaxy A52 A525 Charging Port",
                "Samsung Galaxy A52 Charng Port",
            ]
        }
    ).to_excel(catalog_path, index=False)

    class FakeService:
        def __init__(self, *args, **kwargs):
            pass

        def analyze_title(self, *, title: str, allow_ai: bool, mode: str):
            if "Charng" in title:
                return type(
                    "Exec",
                    (),
                    {
                        "parsed": type(
                            "Parsed",
                            (),
                            {
                                "sku": "GALAXY A52 A525 CP",
                                "final_confidence": 0.96,
                                "confidence": 0.96,
                                "primary_part": "CP",
                                "brand": "SAMSUNG",
                                "model": "GALAXY A52",
                            },
                        )(),
                        "parse_status": "parsed",
                        "parser_reason": "layer3_fuzzy_phrase",
                        "parse_stage": "rule_normalized",
                    },
                )()
            return type(
                "Exec",
                (),
                {
                    "parsed": type(
                        "Parsed",
                        (),
                        {
                            "sku": "GALAXY A52 A525 CP",
                            "final_confidence": 1.0,
                            "confidence": 1.0,
                            "primary_part": "CP",
                            "brand": "SAMSUNG",
                            "model": "GALAXY A52",
                        },
                    )(),
                    "parse_status": "parsed",
                    "parser_reason": "rule_exact",
                    "parse_stage": "rule_only",
                },
            )()

    monkeypatch.setattr(benchmark_module, "StructuredSKUParserService", FakeService)

    summary = benchmark_module.build_txparts_benchmark(catalog_path, benchmark_path)
    result = pd.read_csv(benchmark_path)

    assert summary["benchmark_rows"] == 1
    assert result.to_dict(orient="records") == [
        {
            "title": "Samsung Galaxy A52 A525 Charging Port",
            "expected_sku": "GALAXY A52 A525 CP",
            "expected_brand": "SAMSUNG",
            "expected_model": "GALAXY A52",
            "expected_part": "CP",
        }
    ]


def test_run_benchmark_reports_accuracy_metrics(tmp_path: Path) -> None:
    benchmark_path = tmp_path / "benchmark.csv"
    pd.DataFrame(
        [
            {
                "title": "Samsung Galaxy A52 A525 Charging Port",
                "expected_sku": "GALAXY A52 A525 CP",
                "expected_brand": "SAMSUNG",
                "expected_model": "GALAXY A52",
                "expected_part": "CP",
            }
        ]
    ).to_csv(benchmark_path, index=False)

    report = benchmark_module.run_benchmark(benchmark_path, enable_ai=False)

    assert report["rows_tested"] == 1
    assert report["sku_accuracy"] == 1.0
    assert report["brand_accuracy"] == 1.0
    assert report["model_accuracy"] == 1.0
    assert report["part_accuracy"] == 1.0
    assert report["ai_usage_rate"] == 0.0


def test_build_txparts_ambiguous_benchmark_uses_existing_product_code_truth(tmp_path: Path, monkeypatch) -> None:
    catalog_path = tmp_path / "catalog.xlsx"
    benchmark_path = tmp_path / "txparts_benchmark_ambiguous.csv"
    pd.DataFrame(
        {
            "Product Name": [
                "Samsung Galaxy A52 Charng Port",
                "Samsung Galaxy A52 A525 Charging Port",
            ],
            "Product Code": [
                "GALAXY A52 A525 CP",
                "GALAXY A52 A525 CP",
            ],
        }
    ).to_excel(catalog_path, index=False)

    class FakeService:
        def __init__(self, *args, **kwargs):
            pass

        def analyze_title(self, *, title: str, allow_ai: bool, mode: str, product_sku: str = ""):
            if title == "Samsung Galaxy A52 Charng Port" and not product_sku:
                return type(
                    "Exec",
                    (),
                    {
                        "parsed": type(
                            "Parsed",
                            (),
                            {
                                "sku": "GALAXY A52 A525 CP",
                                "brand": "SAMSUNG",
                                "model": "GALAXY A52",
                                "primary_part": "CP",
                                "rule_confidence": 0.30,
                            },
                        )(),
                        "parse_status": "parsed",
                        "parse_stage": "rule_normalized",
                    },
                )()
            if title == "Samsung Galaxy A52 Charng Port" and product_sku:
                return type(
                    "Exec",
                    (),
                    {
                        "parsed": type(
                            "Parsed",
                            (),
                            {
                                "sku": "GALAXY A52 A525 CP",
                                "brand": "SAMSUNG",
                                "model": "GALAXY A52",
                                "primary_part": "CP",
                                "rule_confidence": 1.0,
                            },
                        )(),
                        "parse_status": "parsed",
                        "parse_stage": "rule_only",
                    },
                )()
            return type(
                "Exec",
                (),
                {
                    "parsed": type(
                        "Parsed",
                        (),
                        {
                            "sku": "GALAXY A52 A525 CP",
                            "brand": "SAMSUNG",
                            "model": "GALAXY A52",
                            "primary_part": "CP",
                            "rule_confidence": 1.0,
                        },
                    )(),
                    "parse_status": "parsed",
                    "parse_stage": "rule_only",
                },
            )()

    monkeypatch.setattr(benchmark_module, "StructuredSKUParserService", FakeService)

    summary = benchmark_module.build_txparts_ambiguous_benchmark(catalog_path, benchmark_path)
    result = pd.read_csv(benchmark_path)

    assert summary["benchmark_rows"] == 1
    assert result.to_dict(orient="records") == [
        {
            "title": "Samsung Galaxy A52 Charng Port",
            "expected_sku": "GALAXY A52 A525 CP",
            "expected_brand": "SAMSUNG",
            "expected_model": "GALAXY A52",
            "expected_part": "CP",
        }
    ]
