from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from backend import sku_parser
from backend.sku_intelligence_engine import (
    EngineConfig,
    PARTS_DICTIONARY_FILE,
    PARTS_ONTOLOGY_FILE,
    PART_CODE_RULES_FILE,
    SPELLING_CORRECTIONS_FILE,
    SKUIntelligenceEngine,
)
from backend.sku_parser import (
    NOT_UNDERSTANDABLE,
    LEARNED_TITLE_PATTERNS_FILE,
    PART_CODE_RULES_FILE,
    UNKNOWN_LOG_FILE,
    analyze_title,
    generate_sku,
    generate_sku_with_confidence,
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
    assert generate_sku("Galaxy A52 A525 Power Flex") == "GALAXY A52 A525 PB-F"
    assert generate_sku("Galaxy A52 A525 Vibrator Flex") == "GALAXY A52 A525 VB-F"
    assert generate_sku("Galaxy A52 A525 Loudspeaker Flex") == "GALAXY A52 A525 LCD-F"
    assert generate_sku("Galaxy A52 A525 Camera Flex") == "GALAXY A52 A525 CAM-F"
    assert generate_sku("Galaxy A52 A525 Microphone Flex") == "GALAXY A52 A525 MIC-FC"
    assert generate_sku("Galaxy A52 A525 WiFi Antenna") == "GALAXY A52 A525 WIF-ANNT"
    assert generate_sku("Galaxy A52 A525 Antenna Connector") == "GALAXY A52 A525 ANNT-CONN"
    assert generate_sku("Galaxy A52 A525 SIM Reader") == "GALAXY A52 A525 SR"
    assert generate_sku("Galaxy A52 A525 Mainboard Flex Cable") in {
        "GALAXY A52 A525 MFC",
        "GALAXY A52 A525 MB-FC",
    }
    assert generate_sku("Galaxy A52 A525 NFC Flex") == "GALAXY A52 A525 NFC"
    assert generate_sku("Galaxy A52 A525 Ear Speaker Proximity Sensor") == "GALAXY A52 A525 ES-PS"
    assert generate_sku("Galaxy A52 A525 Vibration Ear Speaker") == "GALAXY A52 A525 V/ES"
    assert generate_sku("Galaxy A52 A525 Lift Motor") == "GALAXY A52 A525 LIFT-MOT"


def test_charging_component_rules() -> None:
    assert generate_sku("Galaxy A52 A525 Charging Port") == "GALAXY A52 A525 CP"
    charging_port_flex = generate_sku("Galaxy A52 A525 Charging Port Flex")
    assert charging_port_flex.startswith("GALAXY A52 A525 ")
    assert "CF" in charging_port_flex.split()
    assert generate_sku("Galaxy A52 A525 Charging Board") in {
        "GALAXY A52 A525 CP",
        "GALAXY A52 A525 CPB",
    }
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


def test_model_detection_prefers_longest_specific_match() -> None:
    assert analyze_title("pixel 8 pro ear speaker")["model"] == "PIXEL 8 PRO"
    assert analyze_title("pixel 8 charging port")["model"] == "PIXEL 8"
    assert analyze_title("galaxy s23 ultra charging port")["model"] == "GALAXY S23 ULTRA"
    assert analyze_title("iphone 13 pro max battery")["model"] == "IPHONE 13 PRO MAX"
    assert analyze_title("redmi note 12 pro display")["model"] == "REDMI NOTE 12 PRO"


def test_model_only_titles_return_partial_without_injecting_catalog_part() -> None:
    payload = analyze_title("Pixel 8 pro")

    assert payload["brand"] == "GOOGLE"
    assert payload["model"] == "PIXEL 8 PRO"
    assert payload["part"] == ""
    assert payload["sku"] == NOT_UNDERSTANDABLE
    assert payload["parse_status"] == "partial"


def test_display_assembly_filtering_kept() -> None:
    assert generate_sku("Galaxy A52 OLED Assembly") == NOT_UNDERSTANDABLE
    assert generate_sku("Galaxy A52 Screen Assembly") == NOT_UNDERSTANDABLE


def test_display_filter_exceptions_keep_small_parts() -> None:
    for title in (
        "Galaxy A52 LCD FPC Connector",
        "Galaxy A52 Display Connector Flex",
        "Galaxy A52 Touch Connector Flex",
    ):
        sku = generate_sku(title)
        assert sku != NOT_UNDERSTANDABLE
        assert "FPC" in sku.split()


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
        == "GALAXY A71 A716 NFC-CF"
    )


def test_backdoor_attributes_include_bcl_and_color_when_detected() -> None:
    assert (
        generate_sku("Galaxy A80 A805 Back Door with Camera Lens White")
        == "GALAXY A80 A805 BDR BCL WHT"
    )
    assert (
        generate_sku("Galaxy A80 A805 Back Door lens cover Gold")
        == "GALAXY A80 A805 BDR BCL GLD"
    )


def test_nfc_flex_bracket_variant_suffix() -> None:
    assert (
        generate_sku("Galaxy A71 5G A716 Wireless NFC Charging Flex")
        == "GALAXY A71 5G A716 NFC-CF"
    )
    assert (
        generate_sku("Galaxy A71 5G A716 Wireless NFC Charging Flex with Bracket")
        == "GALAXY A71 5G A716 NFC-CF BRKT"
    )
    assert (
        generate_sku("Galaxy A71 5G A716 Wireless NFC Charging Flex holder")
        == "GALAXY A71 5G A716 NFC-CF BRKT"
    )


def test_sim_tray_color_suffix_is_preserved() -> None:
    assert (
        generate_sku("Galaxy A71 5G A716 Single SIM Tray Prism Cube Black")
        == "GALAXY A71 5G A716 ST BLK"
    )
    assert (
        generate_sku("Galaxy A71 5G A716 Single SIM Tray Prism Cube Silver")
        == "GALAXY A71 5G A716 ST SLV"
    )
    assert (
        generate_sku("Galaxy A71 5G A716 Single SIM Tray Prism Cube Blue")
        == "GALAXY A71 5G A716 ST BLU"
    )


def test_long_marketing_colors_are_simplified_for_color_bearing_parts() -> None:
    assert (
        generate_sku("Single Sim Card Tray for Galaxy A72 A725 Awesome Violet")
        == "GALAXY A72 A725 ST VIOLET"
    )
    assert (
        generate_sku("Fingerprint Sensor for Galaxy A05S A057 Awesome Violet")
        == "GALAXY A05S A057 FS VIOLET"
    )
    assert (
        generate_sku("Galaxy A71 5G A716 Back Door Awesome Violet")
        == "GALAXY A71 5G A716 BDR VIOLET"
    )


def test_home_button_flex_uses_single_standardized_code_with_color() -> None:
    assert generate_sku("Galaxy Note 5 Home Button Blue") == "GALAXY NOTE 5 HB-FC BLU"
    assert generate_sku("Galaxy Note 5 Home Button White") == "GALAXY NOTE 5 HB-FC WHT"
    assert generate_sku("Galaxy Note 5 Home Button Gold") == "GALAXY NOTE 5 HB-FC GLD"
    assert generate_sku("Galaxy Note 4 Home Button Black") == "GALAXY NOTE 4 HB-FC BLK"
    assert generate_sku("Galaxy Note 4 Home Button White") == "GALAXY NOTE 4 HB-FC WHT"


def test_home_button_titles_do_not_bleed_into_model_detection() -> None:
    parsed = analyze_title("Galaxy Note 5 Home Button Blue")
    assert parsed["model"] == "GALAXY NOTE 5"
    assert parsed["part"] == "HB-FC"
    assert parsed["sku"] == "GALAXY NOTE 5 HB-FC BLU"


def test_home_button_flex_synonyms_collapse_to_single_rule_code() -> None:
    assert generate_sku("HOME BUTTON FLEX FOR SAMSUNG NOTE 20") == "GALAXY NOTE 20 HB-FC"
    assert generate_sku("HOME BUTTON FPC FOR SAMSUNG NOTE 20") == "GALAXY NOTE 20 HB-FC"
    assert generate_sku("HOME BUTTON RIBBON CABLE FOR SAMSUNG NOTE 20") == "GALAXY NOTE 20 HB-FC"
    assert generate_sku("HOME BUTTON FLEX CABLE FOR SAMSUNG NOTE 20") == "GALAXY NOTE 20 HB-FC"
    assert generate_sku("HOME BUTTON WITH FLEX FOR SAMSUNG NOTE 20") == "GALAXY NOTE 20 HB-FC"


def test_home_button_analysis_does_not_duplicate_flex_terms() -> None:
    parsed = analyze_title("HOME BUTTON FLEX CABLE FOR SAMSUNG NOTE 20")
    assert parsed["interpreted_title"] == "home button flex for samsung note 20"
    assert parsed["part"] == "HB-FC"
    assert parsed["sku"] == "GALAXY NOTE 20 HB-FC"


def test_backdoor_without_space_and_head_phone_jack_normalize_correctly() -> None:
    assert generate_sku("BackDoor for Samsung Galaxy Note 4 Charcoal Black") == "GALAXY NOTE 4 BDR BCL BLK"
    assert generate_sku("Head Phone Jack Black for Samsung Galaxy Note 9") == "GALAXY NOTE 9 HJ"


def test_workbook_error_patterns_use_deterministic_rules() -> None:
    assert generate_sku("Charging Port with Board for Samsung Galaxy Note 10") == "GALAXY NOTE 10 CP-B"
    assert generate_sku("S-Pen Sensor Flex for Samsung Galaxy Note 9") == "GALAXY NOTE 9 CF"
    assert (
        generate_sku("Back Camera Wide & Telephoto & Ultra Wide for Samsung Galaxy Note 20 Ultra")
        == "GALAXY NOTE 20 ULTRA BC-W-T-UW"
    )


def test_samsung_note_lite_compatibility_group_beats_tab_alias() -> None:
    assert (
        generate_sku("Vibrator for Samsung Galaxy Note 10 Lite / S10 Lite")
        == "GALAXY NOTE 10 LITE S10 VIB"
    )


def test_lcd_frame_adhesive_does_not_duplicate_lcd_code_from_hint() -> None:
    parsed = analyze_title(
        "LCD Frame Adhesive for Samsung Galaxy Note 20 5G",
        product_sku_hint="N20 LCD Frame Tape",
    )
    assert parsed["sku"] == "GALAXY NOTE 20 5G N20 LCD-F"
    assert parsed["part"] == "LCD-F"
    assert parsed["secondary_part"] == ""


def test_backdoor_rule_includes_bcl_and_color() -> None:
    assert (
        generate_sku("Galaxy A35 5G A356 Back Door White")
        == "GALAXY A35 5G A356 BDR BCL WHT"
    )
    assert (
        generate_sku("Galaxy A35 5G A356 Back Door Black")
        == "GALAXY A35 5G A356 BDR BCL BLK"
    )
    assert (
        generate_sku("Galaxy A36 5G A366 Back Door with steel plate and camera lens Black")
        == "GALAXY A36 5G A366 BDR BCL BLK"
    )


def test_fingerprint_sensor_color_suffix() -> None:
    assert (
        generate_sku("Fingerprint Sensor for Galaxy A05S A057 Blue")
        == "GALAXY A05S A057 FS BLU"
    )


def test_dual_single_sim_tray_color_mapping() -> None:
    assert (
        generate_sku("Dual Sim Tray Galaxy A06 A065 Gold")
        == "GALAXY A06 A065 STD GLD"
    )
    assert (
        generate_sku("Single SIM Tray Galaxy A06 A065 BLK")
        == "GALAXY A06 A065 ST BLK"
    )


def test_wireless_nfc_flex_and_bracket_rule() -> None:
    assert (
        generate_sku("Wireless NFC Charging Flex")
        == "NFC-CF"
    )
    assert (
        generate_sku("Wireless NFC Charging Flex with Bracket")
        == "NFC-CF BRKT"
    )
    assert (
        generate_sku("Wireless Charging Flex mount")
        == "NFC-CF BRKT"
    )


def test_inventory_correction_rules_for_new_part_codes() -> None:
    assert (
        generate_sku("Galaxy A90 5G A908 Battery FPC Connector (8 Pin)")
        == "GALAXY A90 5G A908 BAT FPC"
    )
    assert (
        generate_sku("Galaxy A90 5G A908 Battery Connector")
        == "GALAXY A90 5G A908 BAT FPC"
    )
    assert (
        generate_sku("Galaxy A90 5G A908 Antenna Connecting Cable")
        == "GALAXY A90 5G A908 ANNT-CONN"
    )
    assert (
        generate_sku("Galaxy A90 5G A908 Antenna Flex")
        == "GALAXY A90 5G A908 ANNT-CONN"
    )
    assert (
        generate_sku("Galaxy A90 5G A908 Sim Card Reader")
        == "GALAXY A90 5G A908 SR"
    )
    assert (
        generate_sku("Galaxy A80 A805 Vibrator & Earpiece Speaker")
        == "GALAXY A80 A805 V/ES"
    )
    assert (
        generate_sku("Galaxy A80 A805 Pop-Up Camera Motor")
        == "GALAXY A80 A805 LIFT-MOT"
    )
    assert (
        generate_sku("Galaxy A90 5G A908 Motherboard Flex")
        == "GALAXY A90 5G A908 MB-FC"
    )
    assert (
        generate_sku("Galaxy A90 5G A908 Camera Flex Cable")
        == "GALAXY A90 5G A908 CAM-F"
    )
    assert (
        generate_sku("Galaxy A90 5G A908 Mic Flex")
        == "GALAXY A90 5G A908 MIC-FC"
    )
    assert (
        generate_sku("Galaxy A90 5G A908 LCD Flex")
        == "GALAXY A90 5G A908 LCD-F"
    )


def test_international_version_variant_suffix() -> None:
    assert (
        generate_sku("Charging Port Board Galaxy A16 5G A166 (International Version)")
        == "GALAXY A16 5G A166 CP INT"
    )
    assert (
        generate_sku("Charging Port Board Galaxy A16 5G A166 (INTL Version)")
        == "GALAXY A16 5G A166 CP INT"
    )


def test_earpiece_synonyms_map_to_es() -> None:
    assert generate_sku("Galaxy A52 A525 Earpiece") == "GALAXY A52 A525 ES"
    assert generate_sku("Galaxy A52 A525 Ear Speaker") == "GALAXY A52 A525 ES"
    assert generate_sku("Galaxy A52 A525 Receiver Speaker") == "GALAXY A52 A525 ES"


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


def test_process_inventory_duplicate_resolution_uses_attributes(tmp_path: Path) -> None:
    input_path = tmp_path / "input.xlsx"
    output_path = tmp_path / "output.xlsx"

    df = pd.DataFrame(
        {
            "Product Name": [
                "Galaxy A35 5G A356 Back Door White",
                "Galaxy A35 5G A356 Back Door Black",
                "Galaxy A16 5G A166 Charging Port Board",
                "Galaxy A16 5G A166 Charging Port Board International Version",
            ],
            "Product SKU": ["", "", "", ""],
            "Product Web SKU": ["", "", "", ""],
        }
    )
    df.to_excel(input_path, index=False)

    result = process_inventory(input_path, output_path)
    assert result.loc[0, "Product New SKU"] == "GALAXY A35 5G A356 BDR BCL WHT"
    assert result.loc[1, "Product New SKU"] == "GALAXY A35 5G A356 BDR BCL BLK"
    assert result.loc[2, "Product New SKU"] == "GALAXY A16 5G A166 CP"
    assert result.loc[3, "Product New SKU"] == "GALAXY A16 5G A166 CP INT"
    assert result["SKU Duplicate"].eq("DUPLICATED").sum() == 0


def test_process_inventory_respects_hard_length_limit_for_long_backdoors(tmp_path: Path) -> None:
    input_path = tmp_path / "input.xlsx"
    output_path = tmp_path / "output.xlsx"

    pd.DataFrame(
        {
            "Product Name": [
                "Samsung Galaxy S23 Ultra S918 Back Door Green",
                "Samsung Galaxy S23 Ultra S918 Back Door White",
            ],
            "Product SKU": ["", ""],
            "Product Web SKU": ["", ""],
        }
    ).to_excel(input_path, index=False)

    result = process_inventory(input_path, output_path)

    assert result.loc[0, "Product New SKU"] == "GALAXY S23 ULTRA S918 BDR GRN"
    assert result.loc[1, "Product New SKU"] == "GALAXY S23 ULTRA S918 BDR WHT"
    assert result["Product New SKU"].map(len).max() <= 31
    assert result["SKU Duplicate"].eq("DUPLICATED").sum() == 0


def test_single_title_builder_enforces_hard_limit_on_slash_models() -> None:
    engine = SKUIntelligenceEngine(
        EngineConfig(
            ontology_file=PARTS_ONTOLOGY_FILE,
            dictionary_file=PARTS_DICTIONARY_FILE,
            part_rules_file=PART_CODE_RULES_FILE,
            enable_vector_layer=False,
        )
    )

    vib_sku = engine._build_sku("PIXEL", "6/6A/6PRO/7/7PRO/8/8PRO", "", "VIB", "", "")
    backdoor_sku = engine._build_sku(
        "PIXEL",
        "6/6A/6PRO/7/7PRO/8/8PRO",
        "",
        "BACKDOOR",
        "",
        "BLACK",
    )

    assert vib_sku == "PIXEL 6/6A/6PRO/7/7PRO/8 VIB"
    assert len(vib_sku) <= 31
    assert vib_sku.endswith(" VIB")

    assert backdoor_sku == "PIXEL 6/6A/6PRO BACKDOOR BLACK"
    assert len(backdoor_sku) <= 31
    assert "BACKDOOR" in backdoor_sku.split()


def test_backdoor_attribute_helper_does_not_auto_inject_bcl() -> None:
    engine = SKUIntelligenceEngine(
        EngineConfig(
            ontology_file=PARTS_ONTOLOGY_FILE,
            dictionary_file=PARTS_DICTIONARY_FILE,
            part_rules_file=PART_CODE_RULES_FILE,
            enable_vector_layer=False,
        )
    )

    assert engine._apply_backdoor_attributes("BDR", "back door black") == "BDR"
    assert (
        engine._apply_backdoor_attributes("BDR BCL", "back door with camera lens black")
        == "BDR BCL"
    )


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
            ontology_file=PARTS_ONTOLOGY_FILE,
            dictionary_file=PARTS_DICTIONARY_FILE,
            part_rules_file=PART_CODE_RULES_FILE,
            learned_patterns_file=learned_patterns_path,
            legacy_learned_title_patterns_file=tmp_path / "learned_title_patterns.json",
            legacy_learned_parts_file=tmp_path / "learned_parts.json",
            unknown_log_file=tmp_path / "unknown_parts_log.json",
            training_patterns_file=training_patterns_path,
            spelling_corrections_file=SPELLING_CORRECTIONS_FILE,
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


def test_max_tolerance_title_interpreter_examples() -> None:
    assert generate_sku("Googel Pixle 9A Battry") == "PIXEL 9A BATT"
    assert generate_sku("Samsng A52 Charng Port") == "GALAXY A52 CP"
    assert generate_sku("Galaxi A71 Ear Speker") == "GALAXY A71 ES"
    assert generate_sku("Samung A50 Vib Motor") == "GALAXY A50 VIB"
    assert generate_sku("Pixl 7 Pro Sim Try") == "PIXEL 7 PRO ST"
    assert generate_sku("Smasung A30 Powr Volum Flex") == "GALAXY A30 PV-F"


def test_typo_correction_confidence_scoring() -> None:
    exact = generate_sku_with_confidence("Samsung Galaxy A52 A525 Charging Port")
    typo = generate_sku_with_confidence("Samsng Galaxi A52 Charng Port")
    assert exact[1] > 0.90
    assert typo[1] >= 0.70


def test_analyze_title_payload_contains_details_and_corrections() -> None:
    payload = analyze_title("Samsng A52 Charng Port")
    assert payload["brand"] == "SAMSUNG"
    assert payload["model"] == "GALAXY A52"
    assert payload["part"] == "CP"
    assert payload["sku"] == "GALAXY A52 CP"
    assert payload["parse_status"] == "parsed"
    assert any(item["from"] == "samsng" and item["to"] == "samsung" for item in payload["corrections"])


def test_product_description_hint_can_drive_parsing_when_title_is_sparse() -> None:
    sku = generate_sku(
        "",
        "",
        "",
        "Samsung Galaxy A52 A525 Battery replacement part",
    )
    assert sku == "GALAXY A52 A525 BATT"


def test_pixel_longest_model_detection_stays_specific() -> None:
    assert analyze_title("pixel 8 pro ear speaker")["model"] == "PIXEL 8 PRO"
    assert analyze_title("pixel 8 ear speaker")["model"] == "PIXEL 8"


def test_pixel_multi_model_compatibility_group_is_length_limited() -> None:
    sku = generate_sku(
        "Vibrator for Google Pixel 6 / 6A / 6 Pro / 7 / 7 Pro / 8 / 8 Pro"
    )
    assert sku == "PIXEL 6/6A/6PRO/7/7PRO/8 VIB"
    assert len(sku) <= 31


def test_single_title_backdoor_builder_enforces_hard_length_limit() -> None:
    sku = generate_sku(
        "Back Door for Google Pixel 6 / 6A / 6 Pro / 7 / 7 Pro / 8 / 8 Pro (Black)"
    )
    assert sku == "PIXEL 6/6A/6PRO BACKDOOR BLACK"
    assert len(sku) <= 31


def test_pixel_title_normalization_removes_supplier_noise() -> None:
    payload = analyze_title("Front Camera for Google Pixel 6 Pro replacement repair part")
    assert payload["interpreted_title"] == "front camera pixel 6 pro"
    assert payload["sku"] == "PIXEL 6 PRO FC"


def test_pixel_color_extraction_keeps_full_color_words() -> None:
    assert (
        generate_sku("SIM Tray for Google Pixel 6 Pro (Stormy Black)")
        == "PIXEL 6 PRO ST BLACK"
    )


def test_pixel_backdoor_uses_backdoor_literal_and_color() -> None:
    assert (
        generate_sku("Back Door for Google Pixel 6 Pro (Black)")
        == "PIXEL 6 PRO BACKDOOR BLACK"
    )


def test_engine_applies_learned_sku_overrides(tmp_path: Path) -> None:
    sku_overrides_path = tmp_path / "learned_sku_corrections.json"
    sku_overrides_path.write_text(
        json.dumps(
            {
                "sku_overrides": {
                    "GALAXY A52 A525 ES": "GALAXY A52 A525 ES BLK",
                },
                "title_overrides": {
                    "pixel 8 pro ear speaker": "PIXEL 8 PRO ES BLACK",
                },
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
            learned_patterns_file=tmp_path / "learned_patterns.json",
            legacy_learned_title_patterns_file=tmp_path / "learned_title_patterns.json",
            legacy_learned_parts_file=tmp_path / "learned_parts.json",
            unknown_log_file=tmp_path / "unknown_parts_log.json",
            training_patterns_file=tmp_path / "training_patterns.json",
            spelling_corrections_file=SPELLING_CORRECTIONS_FILE,
            learned_spelling_variations_file=tmp_path / "learned_spelling_variations.json",
            learned_sku_corrections_file=sku_overrides_path,
            enable_vector_layer=False,
        )
    )

    assert engine.parse_title("Galaxy A52 A525 Ear Speaker").suggested_sku == "GALAXY A52 A525 ES BLK"
    assert engine.parse_title("Pixel 8 Pro Ear Speaker").suggested_sku == "PIXEL 8 PRO ES BLACK"
