#!/usr/bin/env python3
"""Compatibility wrapper around the SKU Intelligence Engine."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from sku_intelligence_engine import (
    DEFAULT_MOBILE_PARTS_DICTIONARY,
    DEFAULT_MOBILE_PARTS_ONTOLOGY,
    DEFAULT_PART_CODE_RULES,
    EngineConfig,
    LEARNED_PARTS_FILE,
    LEARNED_PATTERNS_FILE,
    LEARNED_TITLE_PATTERNS_FILE,
    MAX_SKU_LENGTH,
    NOT_UNDERSTANDABLE,
    PARTS_DICTIONARY_FILE,
    PARTS_ONTOLOGY_FILE,
    PART_CODE_RULES_FILE,
    SEMANTIC_TOKEN_HINTS,
    TRAINING_PATTERNS_FILE,
    UNKNOWN_LOG_FILE,
    SKUIntelligenceEngine,
)

# Public alias required by existing app/UI code.
SEMANTIC_SUGGESTION_FILE = UNKNOWN_LOG_FILE

# Legacy global for integrations that import this name.
part_code_rules = DEFAULT_PART_CODE_RULES

_ENGINE = SKUIntelligenceEngine(
    EngineConfig(
        ontology_file=PARTS_ONTOLOGY_FILE,
        dictionary_file=PARTS_DICTIONARY_FILE,
        part_rules_file=PART_CODE_RULES_FILE,
        learned_patterns_file=LEARNED_PATTERNS_FILE,
        legacy_learned_title_patterns_file=LEARNED_TITLE_PATTERNS_FILE,
        legacy_learned_parts_file=LEARNED_PARTS_FILE,
        unknown_log_file=UNKNOWN_LOG_FILE,
        training_patterns_file=TRAINING_PATTERNS_FILE,
    )
)

# Backward-compatible mutable globals used by existing tests/integrations.
MOBILE_PARTS_DICTIONARY = _ENGINE.part_dictionary
_UNKNOWN_PATTERN_COUNTER = _ENGINE._unknown_pattern_counter


def _sync_engine_paths_from_globals() -> None:
    _ENGINE.config.unknown_log_file = Path(UNKNOWN_LOG_FILE)
    _ENGINE.config.legacy_learned_parts_file = Path(LEARNED_PARTS_FILE)
    _ENGINE.config.legacy_learned_title_patterns_file = Path(LEARNED_TITLE_PATTERNS_FILE)
    _ENGINE.config.learned_patterns_file = Path(LEARNED_PATTERNS_FILE)
    _ENGINE.config.training_patterns_file = Path(TRAINING_PATTERNS_FILE)


def _refresh_parts_lookup() -> None:
    _ENGINE.part_dictionary = dict(MOBILE_PARTS_DICTIONARY)
    _ENGINE.ontology_items = _ENGINE._build_phrase_items(_ENGINE.ontology)
    _ENGINE.learned_pattern_items = _ENGINE._build_phrase_items(_ENGINE.learned_patterns)
    _ENGINE.part_items = _ENGINE._build_phrase_items(_ENGINE.part_dictionary)
    _ENGINE.part_phrase_list = [phrase for phrase, _code in _ENGINE.part_items]
    _ENGINE.known_codes = sorted(
        {code for _phrase, code in _ENGINE.part_items},
        key=len,
        reverse=True,
    )
    _ENGINE._component_vocab = _ENGINE._build_component_vocabulary()
    _ENGINE._component_vocab_list = tuple(sorted(_ENGINE._component_vocab))
    _ENGINE._rebuild_phonetic_indexes()
    _ENGINE._correct_token_cached.cache_clear()
    _ENGINE._rebuild_vector_index()
    _ENGINE._parse_cached.cache_clear()


def semantic_part_detection(text: str) -> str:
    """Backward-compatible semantic part detector."""
    _sync_engine_paths_from_globals()
    return _ENGINE.semantic_part_detection(text)


def detect_part(
    title: object,
    product_sku_hint: object = "",
    product_web_sku_hint: object = "",
    product_description_hint: object = "",
) -> str:
    _sync_engine_paths_from_globals()
    return _ENGINE.parse_title(
        title,
        product_sku_hint,
        product_web_sku_hint,
        product_description_hint,
    ).part_code


def interpret_title_semantically(
    title: object,
    product_sku_hint: object = "",
    product_web_sku_hint: object = "",
    product_description_hint: object = "",
) -> dict[str, object]:
    _sync_engine_paths_from_globals()
    parsed = _ENGINE.parse_title(
        title,
        product_sku_hint,
        product_web_sku_hint,
        product_description_hint,
    )
    return {
        "title_text": str(title),
        "hint_text": f"{product_sku_hint} {product_web_sku_hint} {product_description_hint}".strip(),
        "model_component": " ".join(token for token in (parsed.brand, parsed.model) if token).strip(),
        "model_code": parsed.model_code,
        "part": parsed.part_code,
        "subpart": "",
        "structure": "",
        "variants": parsed.variant.split() if parsed.variant else [],
        "color": parsed.color,
        "stage": parsed.parser_reason,
        "confidence": parsed.confidence_score,
        "decision": parsed.decision,
    }


def generate_sku(
    title: object,
    product_sku_hint: object = "",
    product_web_sku_hint: object = "",
    product_description_hint: object = "",
) -> str:
    _sync_engine_paths_from_globals()
    return _ENGINE.parse_title(
        title,
        product_sku_hint,
        product_web_sku_hint,
        product_description_hint,
    ).suggested_sku


def generate_sku_with_confidence(
    title: object,
    product_sku_hint: object = "",
    product_web_sku_hint: object = "",
    product_description_hint: object = "",
) -> tuple[str, float, str, str]:
    _sync_engine_paths_from_globals()
    parsed = _ENGINE.parse_title(
        title,
        product_sku_hint,
        product_web_sku_hint,
        product_description_hint,
    )
    return (
        parsed.suggested_sku,
        parsed.confidence_score,
        parsed.parser_reason,
        parsed.decision,
    )


def analyze_title(
    title: object,
    product_sku_hint: object = "",
    product_web_sku_hint: object = "",
    product_description_hint: object = "",
) -> dict[str, object]:
    _sync_engine_paths_from_globals()
    return _ENGINE.analyze_title(
        title,
        product_sku_hint,
        product_web_sku_hint,
        product_description_hint,
    )


def process_inventory(input_file: str | Path, output_file: str | Path) -> pd.DataFrame:
    _sync_engine_paths_from_globals()
    return _ENGINE.process_inventory(input_file, output_file)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate SKUs for inventory records.")
    parser.add_argument("input_file", help="Input inventory file path (.xlsx/.xls/.csv)")
    parser.add_argument(
        "-o",
        "--output",
        default="products_sku_processed.xlsx",
        help="Output Excel file path.",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()
    process_inventory(args.input_file, args.output)


__all__ = [
    "DEFAULT_MOBILE_PARTS_DICTIONARY",
    "DEFAULT_MOBILE_PARTS_ONTOLOGY",
    "DEFAULT_PART_CODE_RULES",
    "MAX_SKU_LENGTH",
    "NOT_UNDERSTANDABLE",
    "PARTS_DICTIONARY_FILE",
    "PARTS_ONTOLOGY_FILE",
    "PART_CODE_RULES_FILE",
    "LEARNED_TITLE_PATTERNS_FILE",
    "LEARNED_PARTS_FILE",
    "LEARNED_PATTERNS_FILE",
    "TRAINING_PATTERNS_FILE",
    "UNKNOWN_LOG_FILE",
    "SEMANTIC_SUGGESTION_FILE",
    "part_code_rules",
    "MOBILE_PARTS_DICTIONARY",
    "_UNKNOWN_PATTERN_COUNTER",
    "_refresh_parts_lookup",
    "SEMANTIC_TOKEN_HINTS",
    "semantic_part_detection",
    "detect_part",
    "interpret_title_semantically",
    "generate_sku",
    "generate_sku_with_confidence",
    "analyze_title",
    "process_inventory",
]


if __name__ == "__main__":
    main()
