from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

import sku_parser
from sku_intelligence_engine import EngineConfig, SKUIntelligenceEngine
from sku_parser import (
    NOT_UNDERSTANDABLE,
    LEARNED_TITLE_PATTERNS_FILE,
    PART_CODE_RULES_FILE,
    UNKNOWN_LOG_FILE,
    generate_sku,
    process_inventory,
    semantic_part_detection,
)


def test_part_code_rules_file_exists_and_contains_required_entries() -> None:
    assert PART_CODE_RULES_FILE.exists()
    data = json.loads(PART_CODE_RULES_FILE.read_text(encoding="utf-8"))
    assert data.get("Power Volume Flex") == "PV-F"
    assert data.get("SIM Reader") == "SC-R"
    assert data.get("Charging Port With Headphone Jack") == "CP HJ"


def test_learned_title_patterns_file_exists_and_is_json_dict() -> None:
    assert LEARNED_TITLE_PATTERNS_FILE.exists()
    data = json.loads(LEARNED_TITLE_PATTERNS_FILE.read_text(encoding="utf-8"))
    assert isinstance(data, dict)


def test_strict_rules_dataset_outputs_standardized_codes() -> None:
    assert generate_sku("Galaxy A52 A525 Power Volume Flex") == "GALAXY A52 A525 PV-F"
    assert generate_sku("Galaxy A52 A525 Power Button Flex") == "GALAXY A52 A525 PB-F"
    assert generate_sku("Galaxy A52 A525 Volume Button Flex") == "GALAXY A52 A525 VOL-F"
    assert generate_sku("Galaxy A52 A525 Power Flex") == "GALAXY A52 A525 P-F"
    assert generate_sku("Galaxy A52 A525 Vibrator Flex") == "GALAXY A52 A525 VB-F"
    assert generate_sku("Galaxy A52 A525 Loudspeaker Flex") == "GALAXY A52 A525 L-FLEX"
    assert generate_sku("Galaxy A52 A525 Camera Flex") == "GALAXY A52 A525 CAM-F"
    assert generate_sku("Galaxy A52 A525 Microphone Flex") == "GALAXY A52 A525 MIC-FC"
    assert generate_sku("Galaxy A52 A525 WiFi Antenna") == "GALAXY A52 A525 WIF-ANNT"
    assert generate_sku("Galaxy A52 A525 Antenna Connector") == "GALAXY A52 A525 ANNT-CONN"
    assert generate_sku("Galaxy A52 A525 SIM Reader") == "GALAXY A52 A525 SC-R"
    assert generate_sku("Galaxy A52 A525 Mainboard Flex Cable") == "GALAXY A52 A525 MFC"
    assert generate_sku("Galaxy A52 A525 NFC Flex") == "GALAXY A52 A525 NFC"
    assert generate_sku("Galaxy A52 A525 Ear Speaker Proximity Sensor") == "GALAXY A52 A525 ES-PS"
    assert generate_sku("Galaxy A52 A525 Vibration Ear Speaker") == "GALAXY A52 A525 V/ES"
    assert generate_sku("Galaxy A52 A525 Lift Motor") == "GALAXY A52 A525 LIFT-MOT"


def test_charging_component_rules() -> None:
    assert generate_sku("Galaxy A52 A525 Charging Port") == "GALAXY A52 A525 CP"
    assert generate_sku("Galaxy A52 A525 Charging Port Flex") == "GALAXY A52 A525 CF"
    assert generate_sku("Galaxy A52 A525 Charging Board") == "GALAXY A52 A525 CP"
    assert (
        generate_sku("Galaxy A52 A525 Charging Port With Headphone Jack")
        == "GALAXY A52 A525 CP HJ"
    )


def test_pcb_sim_reader_suffix_rule() -> None:
    assert generate_sku("Galaxy A52 A525 SIM Reader PCB") == "GALAXY A52 A525 SC-R-PCB-SR"


def test_slash_usage_only_for_combined_components_example() -> None:
    assert generate_sku("Galaxy A52 A525 Power + Volume Flex") == "GALAXY A52 A525 P/V-F"


def test_col_legacy_replacement_to_conn() -> None:
    assert generate_sku("Galaxy A52 A525 Antenna Connector Cable") == "GALAXY A52 A525 ANNT-CONN"


def test_model_prefix_rule_and_length_limit() -> None:
    sku = generate_sku("Samsung Galaxy A52 A525 Power Button Flex Black")
    assert sku.startswith("GALAXY A52 A525")
    assert len(sku) <= 31


def test_display_assembly_filtering_kept() -> None:
    assert generate_sku("Galaxy A52 OLED Assembly") == NOT_UNDERSTANDABLE
    assert generate_sku("Galaxy A52 Screen Assembly") == NOT_UNDERSTANDABLE


def test_display_filter_exceptions_keep_small_parts() -> None:
    assert generate_sku("Galaxy A52 LCD FPC Connector") == "GALAXY A52 FPC"
    assert generate_sku("Galaxy A52 Display Connector Flex") == "GALAXY A52 FPC"
    assert generate_sku("Galaxy A52 Touch Connector Flex") == "GALAXY A52 FPC"


def test_semantic_detection_uses_standardized_codes() -> None:
    assert semantic_part_detection("Galaxy A52 SIM reader PCB") == "SC-R-PCB-SR"
    assert semantic_part_detection("Galaxy A52 Power Button Flex") == "PB-F"
    assert semantic_part_detection("Galaxy A52 WiFi Antenna") == "WIF-ANNT"


def test_battery_aliases_always_map_to_batt() -> None:
    assert generate_sku("Samsung Galaxy A52 A525 Battery") == "GALAXY A52 A525 BATT"
    assert generate_sku("Samsung Galaxy A52 A525 Batt") == "GALAXY A52 A525 BATT"
    assert generate_sku("Samsung Galaxy A52 A525 Bat") == "GALAXY A52 A525 BATT"


def test_battery_typos_resolve_to_batt() -> None:
    assert generate_sku("Samsung Galaxy A52 A525 Battry") == "GALAXY A52 A525 BATT"
    assert generate_sku("Samsung Galaxy A52 A525 Batery") == "GALAXY A52 A525 BATT"
    assert generate_sku("Samsung Galaxy A52 A525 Battary") == "GALAXY A52 A525 BATT"


def test_part_priority_prefers_higher_priority_primary_component() -> None:
    assert (
        generate_sku("Galaxy A71 A716 Charging Port With Headphone Jack")
        == "GALAXY A71 A716 CP HJ"
    )


def test_part_priority_battery_wins_when_multiple_components_present() -> None:
    assert (
        generate_sku("Galaxy A71 A716 Battery Charging Port")
        == "GALAXY A71 A716 BATT CP"
    )


def test_combination_rules_map_to_special_codes() -> None:
    assert generate_sku("Galaxy A71 A716 Power And Volume Flex") == "GALAXY A71 A716 PV-F"
    assert generate_sku("Galaxy A71 A716 Ear Speaker Proximity Sensor") == "GALAXY A71 A716 ES-PS"
    assert (
        generate_sku("Galaxy A71 A716 Wireless NFC Charging Flex")
        == "GALAXY A71 A716 NFC CF"
    )


def test_unknown_log_written_for_generic_fallback(tmp_path: Path) -> None:
    original_unknown_file = sku_parser.UNKNOWN_LOG_FILE
    original_learned_file = sku_parser.LEARNED_PARTS_FILE
    original_dictionary = dict(sku_parser.MOBILE_PARTS_DICTIONARY)
    sku_parser.UNKNOWN_LOG_FILE = tmp_path / "unknown_parts_log.json"
    sku_parser.LEARNED_PARTS_FILE = tmp_path / "learned_parts.json"
    sku_parser._UNKNOWN_PATTERN_COUNTER.clear()

    try:
        for _ in range(3):
            generate_sku("Galaxy A52 custom component item")

        assert sku_parser.UNKNOWN_LOG_FILE.exists()
        logged = json.loads(sku_parser.UNKNOWN_LOG_FILE.read_text(encoding="utf-8"))
        assert any(item.get("suggested_code") == "GEN" for item in logged)
    finally:
        sku_parser.UNKNOWN_LOG_FILE = original_unknown_file
        sku_parser.LEARNED_PARTS_FILE = original_learned_file
        sku_parser.MOBILE_PARTS_DICTIONARY.clear()
        sku_parser.MOBILE_PARTS_DICTIONARY.update(original_dictionary)
        sku_parser._refresh_parts_lookup()


def test_process_inventory_duplicate_smoke(tmp_path: Path) -> None:
    input_path = tmp_path / "input.xlsx"
    output_path = tmp_path / "output.xlsx"

    df = pd.DataFrame(
        {
            "Product Name": [
                "Galaxy A52 A525 Power Button Flex",
                "Galaxy A52 A525 Power Button Flex",
                "Galaxy A52 A525 Charging Port",
            ],
            "Product SKU": ["", "", ""],
            "Product Web SKU": ["", "", ""],
        }
    )
    df.to_excel(input_path, index=False)

    result = process_inventory(input_path, output_path)

    assert output_path.exists()
    assert list(result.columns) == [
        "Product Name",
        "Product SKU",
        "Product Web SKU",
        "Product New SKU",
        "SKU Duplicate",
        "Title Duplicate",
    ]
    assert result.loc[0, "Product New SKU"] == "GALAXY A52 A525 PB-F"
    assert result.loc[2, "Product New SKU"] == "GALAXY A52 A525 CP"
    assert result.loc[0, "SKU Duplicate"] == "DUPLICATED"
    assert result.loc[1, "SKU Duplicate"] == "DUPLICATED"


def test_pattern_generator_learns_frequent_ngrams(tmp_path: Path) -> None:
    input_path = tmp_path / "input.xlsx"
    output_path = tmp_path / "output.xlsx"
    review_path = tmp_path / "review_queue.xlsx"
    learned_patterns_path = tmp_path / "learned_patterns.json"
    training_patterns_path = tmp_path / "training_patterns.json"

    rows = []
    for _ in range(5):
        rows.append(
            {
                "Product Name": "Galaxy A52 A525 Charging Socket Board",
                "Product SKU": "",
                "Product Web SKU": "",
            }
        )
    for _ in range(5):
        rows.append(
            {
                "Product Name": "Galaxy A52 A525 Receiver Speaker",
                "Product SKU": "",
                "Product Web SKU": "",
            }
        )

    pd.DataFrame(rows).to_excel(input_path, index=False)

    engine = SKUIntelligenceEngine(
        EngineConfig(
            ontology_file=Path("mobile_parts_ontology.json"),
            dictionary_file=Path("mobile_parts_dictionary.json"),
            part_rules_file=Path("part_code_rules.json"),
            learned_patterns_file=learned_patterns_path,
            legacy_learned_title_patterns_file=tmp_path / "learned_title_patterns.json",
            legacy_learned_parts_file=tmp_path / "learned_parts.json",
            unknown_log_file=tmp_path / "unknown_parts_log.json",
            training_patterns_file=training_patterns_path,
            spelling_corrections_file=Path("spelling_corrections.json"),
            learned_spelling_variations_file=tmp_path / "learned_spelling_variations.json",
            enable_vector_layer=False,
            pattern_min_frequency=5,
        )
    )
    engine.process_inventory(input_path, output_path, review_path)

    assert learned_patterns_path.exists()
    learned = json.loads(learned_patterns_path.read_text(encoding="utf-8"))
    assert learned.get("charging socket") == "CP"
    assert learned.get("receiver speaker") == "ES"

    assert training_patterns_path.exists()
    training = json.loads(training_patterns_path.read_text(encoding="utf-8"))
    assert int(training.get("ngram_frequency_threshold", 0)) == 5
