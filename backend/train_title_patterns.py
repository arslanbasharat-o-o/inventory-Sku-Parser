#!/usr/bin/env python3
"""Train learned title-to-part mappings from inventory datasets."""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

from .sku_parser import (
    DEFAULT_MOBILE_PARTS_DICTIONARY,
    DEFAULT_PART_CODE_RULES,
    PARTS_DICTIONARY_FILE,
    PARTS_ONTOLOGY_FILE,
    PART_CODE_RULES_FILE,
)


DATA_ROOT_DIR = Path(
    os.getenv("SKU_DATA_DIR", str(Path(__file__).resolve().parent.parent / "data"))
).resolve()
LEARNED_TITLE_PATTERNS_FILE = DATA_ROOT_DIR / "runtime" / "learned_title_patterns.json"
RE_SEPARATORS = re.compile(r"[_\-/]+")
RE_NON_ALNUM = re.compile(r"[^A-Za-z0-9\s]")
RE_MULTI_SPACE = re.compile(r"\s+")
RE_TOKEN = re.compile(r"[a-z0-9]+")
RE_MODEL_CODE = re.compile(r"\b[A-Z][0-9]{1,4}[A-Z]?\b")
RE_SKU_NON_ALNUM = re.compile(r"[^A-Z0-9/\-\s]")
RE_MODEL_TOKEN_FILTER = re.compile(r"^[a-z]?\d{2,4}[a-z]?$")

BRAND_TOKENS = {
    "samsung",
    "galaxy",
    "apple",
    "iphone",
    "google",
    "pixel",
    "xiaomi",
    "redmi",
    "oppo",
    "vivo",
    "realme",
    "oneplus",
    "infinix",
    "tecno",
    "huawei",
    "honor",
    "motorola",
    "nokia",
    "sony",
    "asus",
    "lenovo",
    "zte",
    "poco",
}

NOISE_TOKENS = {
    "for",
    "with",
    "without",
    "and",
    "or",
    "the",
    "a",
    "an",
    "of",
    "new",
    "original",
    "premium",
    "replacement",
    "refurbished",
    "service",
    "pack",
    "all",
    "color",
    "colors",
    "mobile",
    "phone",
}

DISALLOWED_PHRASE_TOKENS = {
    "black",
    "white",
    "blue",
    "green",
    "gold",
    "silver",
    "purple",
    "pink",
    "gray",
    "grey",
    "refurbished",
    "service",
    "pack",
    "premium",
    "left",
    "right",
    "corner",
}

PART_HINT_TOKENS = {
    "battery",
    "batt",
    "charging",
    "charger",
    "port",
    "connector",
    "board",
    "speaker",
    "earpiece",
    "ear",
    "loudspeaker",
    "camera",
    "lens",
    "sim",
    "tray",
    "reader",
    "flex",
    "cable",
    "antenna",
    "sensor",
    "proximity",
    "mic",
    "microphone",
    "vibration",
    "vibrator",
    "nfc",
    "power",
    "volume",
}

CODE_ANCHOR_TOKENS: dict[str, set[str]] = {
    "BATT": {"battery", "batt"},
    "BATT FPC": {"battery", "batt", "fpc", "connector"},
    "CP": {"charging", "port", "connector", "board", "charger"},
    "CP HJ": {"charging", "port", "jack", "headphone", "audio", "earphone"},
    "CF": {"charging", "flex", "wireless"},
    "ES": {"earpiece", "ear", "speaker", "receiver"},
    "ES-PS": {"ear", "speaker", "proximity", "sensor"},
    "LS": {"loudspeaker", "loud", "speaker"},
    "FC": {"front", "camera", "selfie"},
    "BC": {"back", "rear", "camera", "main"},
    "BCL": {"camera", "lens"},
    "ST": {"sim", "tray", "holder", "card"},
    "STD": {"dual", "sim", "tray"},
    "SC-R": {"sim", "reader", "card", "micro", "type"},
    "MFC": {"mainboard", "flex", "cable"},
    "MFC FPC": {"mainboard", "flex", "fpc", "connector"},
    "FR": {"fingerprint", "reader"},
    "FS": {"fingerprint", "sensor"},
    "TEMP": {"tempered", "glass", "temperature", "sensor"},
    "VIB": {"vibrator", "vibration", "motor"},
    "ANT CON": {"antenna", "cable", "connector", "flex"},
    "ANNT-CONN": {"antenna", "connector"},
    "WIF-ANNT": {"wifi", "antenna"},
    "NFC": {"nfc", "wireless", "charging", "flex"},
    "P-F": {"power", "flex"},
    "PB-F": {"power", "button", "flex"},
    "VOL-F": {"volume", "button", "flex"},
    "PV-F": {"power", "volume", "flex"},
    "P/V-F": {"power", "volume", "flex"},
}

FLEX_RULES = {
    "power volume flex": "PV-F",
    "power button flex": "PB-F",
    "volume flex": "VOL-F",
    "power flex": "P-F",
    "camera flex": "CAM-F",
    "mic flex": "MIC-FC",
    "microphone flex": "MIC-FC",
    "wifi antenna": "WIF-ANNT",
    "antenna connector": "ANNT-CONN",
}

COMPONENT_SEED_MAP = {
    "battery": "BATT",
    "batt": "BATT",
    "charging port": "CP",
    "charging connector": "CP",
    "charge port": "CP",
    "earpiece speaker": "ES",
    "earpiece": "ES",
    "loudspeaker": "LS",
    "loud speaker": "LS",
    "sim tray": "ST",
    "sim card tray": "ST",
    "camera lens": "BCL",
}


def normalize_phrase(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).lower()
    text = RE_SEPARATORS.sub(" ", text)
    text = RE_NON_ALNUM.sub(" ", text)
    text = RE_MULTI_SPACE.sub(" ", text)
    return text.strip()


def normalize_code(code: object) -> str:
    if pd.isna(code):
        return ""
    text = str(code).upper()
    text = RE_SKU_NON_ALNUM.sub(" ", text)
    text = RE_MULTI_SPACE.sub(" ", text)
    return text.strip()


def tokenize(text: str) -> list[str]:
    return RE_TOKEN.findall(text.lower())


def detect_brands(tokens: list[str]) -> set[str]:
    return {token for token in tokens if token in BRAND_TOKENS}


def detect_model_codes(raw_title: str) -> set[str]:
    upper = str(raw_title).upper()
    return {match.group(0) for match in RE_MODEL_CODE.finditer(upper)}


def build_seed_mapping() -> tuple[tuple[str, str], ...]:
    file_sources = (
        _load_json_dictionary(PARTS_ONTOLOGY_FILE),
        _load_json_dictionary(PARTS_DICTIONARY_FILE),
        _load_json_dictionary(PART_CODE_RULES_FILE),
    )
    merged: dict[str, str] = {}
    for source in (
        DEFAULT_MOBILE_PARTS_DICTIONARY,
        DEFAULT_PART_CODE_RULES,
        *file_sources,
        COMPONENT_SEED_MAP,
        FLEX_RULES,
    ):
        for phrase, code in source.items():
            key = normalize_phrase(phrase)
            value = normalize_code(code)
            if key and value:
                merged[key] = value
    items = sorted(merged.items(), key=lambda item: len(item[0]), reverse=True)
    return tuple(items)


def _load_json_dictionary(file_path: Path) -> dict[str, str]:
    try:
        with file_path.open("r", encoding="utf-8") as file_obj:
            data = json.load(file_obj)
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, str] = {}
    for key, value in data.items():
        k = normalize_phrase(key)
        v = normalize_code(value)
        if k and v:
            out[k] = v
    return out


def infer_part_code(
    title: str,
    product_sku: str,
    product_web_sku: str,
    normalized_title: str,
    seed_items: tuple[tuple[str, str], ...],
) -> str:
    padded = f" {normalized_title} "
    for phrase, code in seed_items:
        if f" {phrase} " in padded:
            return code

    hint_inferred = infer_code_from_hints(
        product_sku=product_sku,
        product_web_sku=product_web_sku,
        known_codes={code for _phrase, code in seed_items},
    )
    if hint_inferred:
        return hint_inferred

    return ""


def _contains_compact_code(compact_hint: str, code_token: str) -> bool:
    token = re.sub(r"[^A-Z0-9]", "", code_token.upper())
    if not token:
        return False
    if len(token) <= 3 and token.isalpha():
        return (
            re.search(
                rf"(?:^|[0-9]){re.escape(token)}(?=$|[0-9]|INTL|INT|GLOBAL|USA|US)",
                compact_hint,
            )
            is not None
        )
    return token in compact_hint


def infer_code_from_hints(
    product_sku: str,
    product_web_sku: str,
    known_codes: set[str],
) -> str:
    compact = re.sub(r"[^A-Z0-9]", "", f"{product_sku} {product_web_sku}".upper())
    if not compact:
        return ""

    sorted_codes = sorted(known_codes, key=len, reverse=True)
    for code in sorted_codes:
        code_tokens = re.findall(r"[A-Z0-9]+", code.upper())
        if not code_tokens:
            continue
        if all(_contains_compact_code(compact, token) for token in code_tokens):
            return normalize_code(code)
    return ""


def _valid_phrase_tokens(
    phrase_tokens: list[str],
    brand_tokens: set[str],
    model_tokens: set[str],
) -> bool:
    if not phrase_tokens:
        return False
    if len(phrase_tokens) > 4:
        return False
    if all(token in NOISE_TOKENS for token in phrase_tokens):
        return False
    if any(token in DISALLOWED_PHRASE_TOKENS for token in phrase_tokens):
        return False
    if not any(token in PART_HINT_TOKENS for token in phrase_tokens):
        return False
    if any(token in brand_tokens for token in phrase_tokens):
        return False
    if any(token in model_tokens or RE_MODEL_TOKEN_FILTER.fullmatch(token) for token in phrase_tokens):
        return False
    if "sim" in phrase_tokens and not any(
        token in {"tray", "reader", "card", "holder", "slot", "micro", "type", "connector", "flex"}
        for token in phrase_tokens
    ):
        return False
    if "cable" in phrase_tokens and len(set(phrase_tokens)) == 1:
        return False
    if "board" in phrase_tokens and not any(token in {"charging", "port", "antenna", "pcb"} for token in phrase_tokens):
        return False
    return True


def candidate_phrases_from_tokens(
    tokens: list[str],
    brand_tokens: set[str],
    model_tokens: set[str],
    code: str,
) -> set[str]:
    out: set[str] = set()
    anchors = CODE_ANCHOR_TOKENS.get(code, set())
    for size in (3, 2, 1):
        for idx in range(0, len(tokens) - size + 1):
            window = tokens[idx : idx + size]
            while window and window[0] in NOISE_TOKENS:
                window = window[1:]
            while window and window[-1] in NOISE_TOKENS:
                window = window[:-1]
            if not _valid_phrase_tokens(window, brand_tokens, model_tokens):
                continue
            if anchors and not any(token in anchors for token in window):
                continue
            out.add(" ".join(window))
    return out


def matched_seed_phrases(normalized_title: str, seed_items: tuple[tuple[str, str], ...]) -> set[str]:
    padded = f" {normalized_title} "
    out: set[str] = set()
    for phrase, _code in seed_items:
        if len(phrase.split()) < 2:
            continue
        if f" {phrase} " in padded:
            out.add(phrase)
        if len(out) >= 6:
            break
    return out


def _is_safe_singleton(phrase: str, code: str) -> bool:
    safe = {
        "battery": "BATT",
        "batt": "BATT",
        "earpiece": "ES",
        "loudspeaker": "LS",
        "vibrator": "VIB",
        "fingerprint": "FR",
        "charging": "CP",
    }
    return safe.get(phrase, "") == code


def _required_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("Product Name", "Product SKU", "Product Web SKU"):
        if col not in df.columns:
            df[col] = ""
    return df


def train_patterns_from_dataframe(
    df: pd.DataFrame,
    min_count: int = 2,
    min_confidence: float = 0.8,
) -> tuple[dict[str, str], dict[str, object]]:
    df = _required_columns(df.copy())
    seed_items = build_seed_mapping()
    seed_lookup = {phrase: code for phrase, code in seed_items}

    phrase_code_counts: dict[str, Counter[str]] = defaultdict(Counter)
    brand_counter: Counter[str] = Counter()
    model_counter: Counter[str] = Counter()
    part_counter: Counter[str] = Counter()
    trained_rows = 0

    for row in df[["Product Name", "Product SKU", "Product Web SKU"]].fillna("").itertuples(index=False):
        title, product_sku, product_web_sku = (str(row[0]), str(row[1]), str(row[2]))
        normalized_title = normalize_phrase(title)
        if not normalized_title:
            continue

        tokens = tokenize(normalized_title)
        if not tokens:
            continue

        brands = detect_brands(tokens)
        model_codes = detect_model_codes(title)
        brand_counter.update(brands)
        model_counter.update(model_codes)

        code = infer_part_code(
            title=title,
            product_sku=product_sku,
            product_web_sku=product_web_sku,
            normalized_title=normalized_title,
            seed_items=seed_items,
        )
        if not code or code == "GEN":
            continue

        trained_rows += 1
        part_counter[code] += 1

        model_tokens = {token.lower() for token in model_codes}
        candidates = matched_seed_phrases(normalized_title, seed_items)
        if not candidates:
            candidates = candidate_phrases_from_tokens(tokens, brands, model_tokens, code=code)

        for phrase in candidates:
            phrase_code_counts[phrase][code] += 1

    learned_patterns: dict[str, str] = {}
    for phrase, code_counts in phrase_code_counts.items():
        total = sum(code_counts.values())
        if total <= 0:
            continue
        best_code, best_count = code_counts.most_common(1)[0]
        confidence = best_count / total
        if best_count < min_count:
            continue
        if confidence < min_confidence:
            continue
        seeded_code = seed_lookup.get(phrase)
        if seeded_code and seeded_code != best_code:
            continue
        if len(phrase.split()) == 1 and not _is_safe_singleton(phrase, best_code):
            continue
        if phrase in {"sim", "cable", "board", "reader", "tray", "with", "for"}:
            continue
        learned_patterns[phrase] = best_code

    sorted_patterns = dict(
        sorted(
            learned_patterns.items(),
            key=lambda item: (-len(item[0].split()), item[0]),
        )
    )

    stats: dict[str, object] = {
        "rows_total": int(len(df)),
        "rows_used_for_learning": trained_rows,
        "learned_patterns": len(sorted_patterns),
        "top_brands": brand_counter.most_common(10),
        "top_model_codes": model_counter.most_common(10),
        "top_part_codes": part_counter.most_common(15),
    }
    return sorted_patterns, stats


def _read_input_dataframe(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    return pd.read_excel(path, engine="openpyxl")


def train_patterns_file(
    input_file: str | Path,
    output_file: str | Path = LEARNED_TITLE_PATTERNS_FILE,
    min_count: int = 2,
    min_confidence: float = 0.8,
) -> tuple[dict[str, str], dict[str, object]]:
    input_path = Path(input_file)
    output_path = Path(output_file)
    df = _read_input_dataframe(input_path)
    patterns, stats = train_patterns_from_dataframe(
        df=df,
        min_count=min_count,
        min_confidence=min_confidence,
    )
    with output_path.open("w", encoding="utf-8") as file_obj:
        json.dump(patterns, file_obj, indent=2, ensure_ascii=True)
    return patterns, stats


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train learned title patterns for the SKU parser.")
    parser.add_argument("input_file", help="Inventory file (.xlsx or .csv) containing Product Name/SKU columns.")
    parser.add_argument(
        "-o",
        "--output",
        default=str(LEARNED_TITLE_PATTERNS_FILE),
        help="Output JSON file path (default: learned_title_patterns.json).",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=2,
        help="Minimum occurrences required per phrase/code pair.",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.8,
        help="Minimum dominant-code confidence ratio per phrase.",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()
    patterns, stats = train_patterns_file(
        input_file=args.input_file,
        output_file=args.output,
        min_count=max(1, int(args.min_count)),
        min_confidence=max(0.0, min(1.0, float(args.min_confidence))),
    )
    print(
        json.dumps(
            {
                "output_file": str(args.output),
                "learned_patterns": len(patterns),
                **stats,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
