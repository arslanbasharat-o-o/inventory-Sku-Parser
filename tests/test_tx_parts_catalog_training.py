from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from backend.sku_intelligence_engine import (
    EngineConfig,
    PARTS_DICTIONARY_FILE,
    PARTS_ONTOLOGY_FILE,
    PART_CODE_RULES_FILE,
    SKUIntelligenceEngine,
    SPELLING_CORRECTIONS_FILE,
)
from backend.train_tx_parts_catalog import (
    build_catalog_overlay_from_dataframe,
    build_title_memory_from_dataframe,
    train_tx_parts_catalog,
)


def test_build_catalog_overlay_filters_non_phone_categories() -> None:
    df = pd.DataFrame(
        {
            "Product Code": [
                "LPixel 6 Pro W/F",
                "LGalaxy Zfold 3 5g Outer",
                "Liwatch S5/SE 40mm",
            ],
            "Product Name": [
                "OLED Assembly for Google Pixel 6 Pro with Frame (With Fingerprint Sensor) (Refurbished)",
                "OLED Without Frame for Galaxy Z Fold 3 5G Outer (Refurbished)",
                "LCD Assembly For iWatch Series 5/SE (40MM) (Refurbished)",
            ],
            "Category": [
                "Google,6 Series,Pixel 6 pro",
                "Samsung,Fold Series,Galaxy Z Fold 3 5G",
                "Apple,IWatch,Watch Series SE (40MM) | Apple,IWatch,Watch Series 5 (40MM)",
            ],
        }
    )

    overlay, summary = build_catalog_overlay_from_dataframe(df)

    assert summary["rows_total"] == 3
    assert summary["rows_used_for_overlay"] == 2
    assert summary["rows_skipped_non_phone_or_unknown_brand"] == 1
    assert {row["brand"] for row in overlay["models"]} == {"Google", "Samsung"}


def test_engine_loads_tx_parts_catalog_overlay(tmp_path: Path) -> None:
    overlay_path = tmp_path / "tx_parts_catalog_overlay.json"
    title_memory_path = tmp_path / "tx_parts_title_memory.json"
    overlay_path.write_text(
        json.dumps(
            {
                "brand_aliases": {"google pixel": "Google"},
                "models": [
                    {
                        "brand": "Google",
                        "model": "Pixel 10 Pro XL",
                        "model_codes": ["GC3VE"],
                        "aliases": ["pixel 10 pro xl", "google pixel 10 pro xl"],
                    }
                ],
            },
            indent=2,
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )
    title_memory_path.write_text(
        json.dumps(
            {
                "titles": {
                    "front camera for google pixel 10 pro xl": {
                        "brand": "Google",
                        "model": "Pixel 10 Pro XL",
                        "model_codes": ["GC3VE"],
                        "canonical_title": "Front Camera for Google Pixel 10 Pro XL",
                    }
                }
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
            catalog_training_file=overlay_path,
            title_training_file=title_memory_path,
            enable_vector_layer=False,
        )
    )

    brand, model, model_code = engine._detect_brand_model_from_title_memory(
        engine.normalize_text("Front Camera for Google Pixel 10 Pro XL")
    )
    assert brand == "PIXEL"
    assert model == "10 PRO XL"
    assert model_code == "GC3VE"


def test_build_title_memory_keeps_only_unambiguous_phone_titles() -> None:
    df = pd.DataFrame(
        {
            "Product Code": ["A", "B", "C"],
            "Product Name": [
                "Front Camera for Google Pixel 10 Pro XL",
                "OLED Without Frame for Galaxy Z Fold 3 5G Outer",
                "LCD Assembly For iWatch Series 5/SE (40MM) (Refurbished)",
            ],
            "Category": [
                "Google,10 Series,Pixel 10 Pro XL",
                "Samsung,Fold Series,Galaxy Z Fold 3 5G",
                "Apple,IWatch,Watch Series SE (40MM) | Apple,IWatch,Watch Series 5 (40MM)",
            ],
        }
    )

    title_memory, stats = build_title_memory_from_dataframe(df)

    assert stats["title_memory_entries"] == 2
    assert "front camera for google pixel 10 pro xl" in title_memory["titles"]
    assert "lcd assembly for iwatch series 5 se 40mm refurbished" not in title_memory["titles"]
    assert (
        title_memory["titles"]["front camera for google pixel 10 pro xl"]["canonical_title"]
        == "Front Camera for Google Pixel 10 Pro XL"
    )


def test_engine_fuzzy_title_memory_corrects_employee_typos(tmp_path: Path) -> None:
    title_memory_path = tmp_path / "tx_parts_title_memory.json"
    title_memory_path.write_text(
        json.dumps(
            {
                "titles": {
                    "front camera for google pixel 10 pro xl": {
                        "brand": "Google",
                        "model": "Pixel 10 Pro XL",
                        "model_codes": ["GC3VE"],
                        "canonical_title": "Front Camera for Google Pixel 10 Pro XL",
                    }
                }
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
            title_training_file=title_memory_path,
            enable_vector_layer=False,
        )
    )

    payload = engine._resolve_trained_title_memory(
        engine.normalize_text("Frnt Camra for Gogle Pixel 10 Pro XL")
    )
    assert payload[0] == "Front Camera for Google Pixel 10 Pro XL"
    assert payload[1] == "PIXEL"
    assert payload[2] == "10 PRO XL"
    assert payload[3] == "GC3VE"
    assert payload[4] == "catalog_title_fuzzy"


def test_train_tx_parts_catalog_writes_overlay_and_audit(tmp_path: Path) -> None:
    input_path = tmp_path / "input.xlsx"
    overlay_path = tmp_path / "overlay.json"
    title_memory_path = tmp_path / "title_memory.json"
    audit_path = tmp_path / "audit.json"
    summary_path = tmp_path / "summary.json"

    pd.DataFrame(
        {
            "Product Code": [
                "LPixel 6 Pro W/F",
                "LPixel 6 Pro With Frame",
            ],
            "Product Name": [
                "OLED Assembly for Google Pixel 6 Pro with Frame (With Fingerprint Sensor) (Refurbished)",
                "OLED Assembly for Google Pixel 6 Pro with Frame (Refurbished)",
            ],
            "Category": [
                "Google,6 Series,Pixel 6 pro",
                "Google,6 Series,Pixel 6 pro",
            ],
        }
    ).to_excel(input_path, index=False)

    summary = train_tx_parts_catalog(
        input_file=input_path,
        overlay_file=overlay_path,
        title_memory_file=title_memory_path,
        audit_file=audit_path,
        summary_file=summary_path,
    )

    assert overlay_path.exists()
    assert title_memory_path.exists()
    assert audit_path.exists()
    assert summary_path.exists()
    assert summary["models"] >= 1
    assert summary["title_memory_entries"] >= 1
    assert summary["patterns_total"] >= 1
