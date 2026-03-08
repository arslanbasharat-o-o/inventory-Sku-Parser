#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.sku_intelligence_engine import (
    BRAND_DATASET_FILE,
    BRAND_FAMILY_MAP,
    CORE_SMARTPHONE_BRANDS,
    PHONEDB_MODELS_FILE,
    PHONEDB_MODEL_OVERLAY_FILE,
    RUNTIME_DATA_DIR,
)

RE_MULTI_SPACE = re.compile(r"\s+")
RE_NORMALIZE = re.compile(r"[^a-z0-9]+")
RE_MODEL_LABEL_PARENS = re.compile(r"\([^)]*\)")
RE_MODEL_YEAR = re.compile(r"\b20\d{2}\b", re.IGNORECASE)
RE_MODEL_CODE = re.compile(
    r"\b(?:SM-[A-Z0-9-]+|XT\d{4,5}(?:-\d+)?|CPH\d{4,5}|RMX\d{4,5}|TA-\d{3,5}|"
    r"[AGMNSFXV]\d{3,5}[A-Z]?|NE\d{4}|LM-[A-Z0-9-]+|M\d{4}[A-Z0-9-]*)\b",
    re.IGNORECASE,
)
RE_STORAGE = re.compile(r"\b(?:\d{2,4}\s*GB|\d\s*TB)\b", re.IGNORECASE)
RE_LONG_HW_CODE = re.compile(r"\b[A-Z]{0,3}\d{5,}[A-Z0-9-]*\b", re.IGNORECASE)

PHONE_HINTS = {
    "IPHONE",
    "GALAXY",
    "PIXEL",
    "NEXUS",
    "REDMI",
    "POCO",
    "ONEPLUS",
    "MOTO",
    "XPERIA",
    "LUMIA",
    "HONOR",
    "REALME",
    "VIVO",
    "OPPO",
    "HUAWEI",
    "NOKIA",
    "TECNO",
    "INFINIX",
    "ZTE",
    "LENOVO",
    "ASUS",
    "LG",
}

NON_PHONE_HINTS = {
    "TABLET",
    "WATCH",
    "SMARTWATCH",
    "LAPTOP",
    "NOTEBOOK",
    "PDA",
    "POCKET PC",
    "JORNADA",
    "MYPAL",
    "IPAQ",
    "MOBILEPRO",
    "CASSIOPEIA",
    "PALM",
}

PHONEDB_MODEL_SUMMARY_FILE = RUNTIME_DATA_DIR / "phonedb_model_training_summary.json"


def normalize_phrase(value: str) -> str:
    text = RE_NORMALIZE.sub(" ", str(value).lower())
    return RE_MULTI_SPACE.sub(" ", text).strip()


def normalize_code(value: str) -> str:
    text = re.sub(r"[^A-Z0-9/\-\s]", " ", str(value).upper())
    return RE_MULTI_SPACE.sub(" ", text).strip()


def sku_brand_from_dataset_brand(brand_value: str) -> str:
    brand_norm = normalize_phrase(brand_value)
    if not brand_norm:
        return ""
    sku_brand = BRAND_FAMILY_MAP.get(brand_norm, "")
    if sku_brand:
        return normalize_code(sku_brand)
    compact = brand_norm.replace(" ", "")
    sku_brand = BRAND_FAMILY_MAP.get(compact, "")
    if sku_brand:
        return normalize_code(sku_brand)
    return normalize_code(brand_value)


def load_brand_alias_map() -> dict[str, str]:
    try:
        data = json.loads(BRAND_DATASET_FILE.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    if not isinstance(data, dict):
        data = {}

    alias_lookup: dict[str, str] = {}
    brands = data.get("brands", {})
    if isinstance(brands, dict):
        for brand_key, brand_info in brands.items():
            canonical = str(brand_key).strip()
            canonical_norm = normalize_phrase(canonical)
            if canonical_norm:
                alias_lookup[canonical_norm] = canonical
            if isinstance(brand_info, dict):
                for alias in brand_info.get("aliases", []):
                    alias_norm = normalize_phrase(str(alias))
                    if alias_norm:
                        alias_lookup[alias_norm] = canonical

    alias_map = data.get("brand_alias_map", {})
    if isinstance(alias_map, dict):
        for alias, canonical in alias_map.items():
            alias_norm = normalize_phrase(str(alias))
            canonical_text = str(canonical).strip()
            if alias_norm and canonical_text:
                alias_lookup[alias_norm] = canonical_text

    for alias in BRAND_FAMILY_MAP:
        alias_norm = normalize_phrase(str(alias))
        if alias_norm:
            alias_lookup.setdefault(alias_norm, str(alias))
    return alias_lookup


def resolve_dataset_brand(alias_lookup: dict[str, str], raw_brand: str, model_raw: str) -> str:
    brand_norm = normalize_phrase(raw_brand)
    dataset_brand = alias_lookup.get(brand_norm, raw_brand)

    # Keep Xiaomi sub-brands explicit to match real title hints.
    normalized_model = normalize_code(model_raw)
    dataset_brand_norm = normalize_phrase(dataset_brand)
    if dataset_brand_norm in {"xiaomi", "mi"}:
        if normalized_model.startswith("REDMI "):
            return "Redmi"
        if normalized_model.startswith("POCO "):
            return "Poco"
    return str(dataset_brand).strip() or raw_brand


def clean_model_label(model: str) -> str:
    text = str(model).replace("\xa0", " ")
    text = RE_MODEL_LABEL_PARENS.sub(" ", text)
    text = RE_MULTI_SPACE.sub(" ", text).strip(" -/")
    return text


def compact_model_alias(model: str) -> str:
    code = normalize_code(model)
    if not code:
        return ""
    code = code.replace("TD-LTE", " ").replace("TD LTE", " ")
    code = re.sub(
        r"\b(?:PREMIUM|STANDARD|EDITION|GLOBAL|INTERNATIONAL|DUAL|SINGLE|SIM|LTE|NR|CDMA|GSM|UMTS|HSPA|"
        r"UNLOCKED|CARRIER|VERSION|REFURB|REFURBISHED|CN|IN|EU|US|JP)\b",
        " ",
        code,
    )
    code = RE_STORAGE.sub(" ", code)
    code = RE_LONG_HW_CODE.sub(" ", code)
    code = RE_MULTI_SPACE.sub(" ", code).strip(" -/")
    return code


def is_phone_like_model(model: str) -> bool:
    code = normalize_code(model)
    if not code:
        return False
    for hint in NON_PHONE_HINTS:
        if hint in code:
            return False
    if any(ch.isdigit() for ch in code):
        return True
    return any(hint in code for hint in PHONE_HINTS)


def extract_model_codes(*values: str) -> list[str]:
    codes: list[str] = []
    seen: set[str] = set()
    for value in values:
        for match in RE_MODEL_CODE.findall(str(value)):
            code = normalize_code(match)
            if not code or code in seen:
                continue
            if code in {"4G", "5G"} or code.isdigit():
                continue
            if code.endswith("GB") or code.endswith("HZ"):
                continue
            if RE_MODEL_YEAR.fullmatch(code):
                continue
            seen.add(code)
            codes.append(code)
    return codes


def build_aliases(dataset_brand: str, model: str, model_codes: list[str]) -> list[str]:
    aliases: list[str] = []

    def add(value: str) -> None:
        candidate = RE_MULTI_SPACE.sub(" ", str(value)).strip()
        if candidate and candidate not in aliases:
            aliases.append(candidate)

    dataset_brand_clean = str(dataset_brand).strip()
    model_clean = clean_model_label(model)
    compact = compact_model_alias(model_clean)
    add(model_clean)
    add(compact)
    brand_code = normalize_code(dataset_brand_clean)
    model_code = normalize_code(model_clean)
    compact_code = normalize_code(compact)
    add(f"{dataset_brand_clean} {model_clean}".strip())
    if compact_code and not compact_code.startswith(f"{brand_code} "):
        add(f"{dataset_brand_clean} {compact}".strip())
    if compact.endswith(" 5G"):
        compact_without_generation = compact.replace(" 5G", "").strip()
        add(compact_without_generation)
        compact_without_generation_code = normalize_code(compact_without_generation)
        if compact_without_generation_code and not compact_without_generation_code.startswith(
            f"{brand_code} "
        ):
            add(f"{dataset_brand_clean} {compact_without_generation}".strip())
    if compact.endswith(" 4G"):
        compact_without_generation = compact.replace(" 4G", "").strip()
        add(compact_without_generation)
        compact_without_generation_code = normalize_code(compact_without_generation)
        if compact_without_generation_code and not compact_without_generation_code.startswith(
            f"{brand_code} "
        ):
            add(f"{dataset_brand_clean} {compact_without_generation}".strip())
    for code in model_codes:
        add(f"{model_clean} {code}")
        add(f"{dataset_brand_clean} {model_clean} {code}".strip())

    model_code = normalize_code(model_clean)
    brand_norm = normalize_phrase(dataset_brand_clean)
    if brand_norm == "samsung" and model_code and not model_code.startswith("GALAXY "):
        add(f"Galaxy {model_clean}")
    elif brand_norm == "google" and model_code and not model_code.startswith(("PIXEL ", "NEXUS ")):
        add(f"Pixel {model_clean}")
    elif brand_norm == "motorola" and model_code and not model_code.startswith("MOTO "):
        add(f"Moto {model_clean}")
    elif brand_norm == "apple" and model_code and not model_code.startswith("IPHONE "):
        add(f"iPhone {model_clean}")
    return aliases


def train_phonedb_models(
    input_file: str | Path,
    overlay_file: str | Path = PHONEDB_MODEL_OVERLAY_FILE,
    summary_file: str | Path = PHONEDB_MODEL_SUMMARY_FILE,
) -> dict[str, Any]:
    input_path = Path(input_file)
    overlay_path = Path(overlay_file)
    summary_path = Path(summary_file)

    payload = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Input JSON must be an object with a `models` array.")
    rows = payload.get("models", [])
    if not isinstance(rows, list):
        raise ValueError("Input JSON `models` must be an array.")

    alias_lookup = load_brand_alias_map()
    stats = Counter()
    model_index: dict[tuple[str, str], dict[str, Any]] = {}

    for row in rows:
        stats["rows_total"] += 1
        if not isinstance(row, dict):
            stats["rows_invalid"] += 1
            continue
        raw_name = str(row.get("name", "")).replace("\xa0", " ").strip()
        if not raw_name:
            stats["rows_missing_name"] += 1
            continue
        parts = raw_name.split()
        if len(parts) < 2:
            stats["rows_short_name"] += 1
            continue

        raw_brand = parts[0]
        model_raw = " ".join(parts[1:]).strip()
        model_label = clean_model_label(model_raw)
        if not model_label:
            stats["rows_missing_model"] += 1
            continue
        model_canonical = compact_model_alias(model_label) or normalize_code(model_label)
        if not model_canonical:
            stats["rows_invalid_model_norm"] += 1
            continue

        dataset_brand = resolve_dataset_brand(alias_lookup, raw_brand, model_label)
        sku_brand = sku_brand_from_dataset_brand(dataset_brand)
        if not sku_brand:
            stats["rows_unknown_brand"] += 1
            continue
        if sku_brand not in CORE_SMARTPHONE_BRANDS:
            stats["rows_non_core_brand"] += 1
            continue
        if not is_phone_like_model(model_canonical):
            stats["rows_non_phone_like"] += 1
            continue

        model_norm = normalize_code(model_canonical)
        if not model_norm:
            stats["rows_invalid_model_norm"] += 1
            continue
        if model_norm in {sku_brand, f"{sku_brand}S"}:
            stats["rows_brand_only_model"] += 1
            continue

        model_codes = extract_model_codes(raw_name, model_label)
        aliases = build_aliases(dataset_brand, model_label, model_codes)
        aliases.append(model_canonical)
        aliases.append(f"{dataset_brand} {model_canonical}".strip())

        key = (sku_brand, model_norm)
        entry = model_index.setdefault(
            key,
            {
                "brand": dataset_brand,
                "model": model_canonical,
                "model_codes": [],
                "aliases": [],
                "source": "phonedb_trained",
                "row_count": 0,
            },
        )
        entry["row_count"] += 1
        # Prefer explicit sub-brand labels when available.
        if normalize_code(dataset_brand) in {"REDMI", "POCO"}:
            entry["brand"] = dataset_brand
        for code in model_codes:
            if code not in entry["model_codes"]:
                entry["model_codes"].append(code)
        for alias in aliases:
            if alias not in entry["aliases"]:
                entry["aliases"].append(alias)
        stats["rows_used"] += 1

    models = sorted(
        model_index.values(),
        key=lambda item: (-int(item["row_count"]), str(item["brand"]), str(item["model"])),
    )
    for item in models:
        item.pop("row_count", None)

    overlay_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_file": str(input_path),
        "rows_total": int(stats["rows_total"]),
        "rows_used": int(stats["rows_used"]),
        "models": models,
    }
    overlay_path.parent.mkdir(parents=True, exist_ok=True)
    overlay_path.write_text(json.dumps(overlay_payload, indent=2, ensure_ascii=True), encoding="utf-8")

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_file": str(input_path),
        "overlay_file": str(overlay_path),
        "rows_total": int(stats["rows_total"]),
        "rows_used": int(stats["rows_used"]),
        "models": len(models),
        "top_brands": Counter(normalize_code(str(item["brand"])) for item in models).most_common(30),
        **{key: int(value) for key, value in stats.items()},
    }
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")
    return summary


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Train phone-model overlay from raw phonedb_models.json.",
    )
    parser.add_argument(
        "input_file",
        nargs="?",
        default=str(PHONEDB_MODELS_FILE),
        help="Path to phonedb models JSON file.",
    )
    parser.add_argument(
        "--overlay-output",
        default=str(PHONEDB_MODEL_OVERLAY_FILE),
        help="Output path for trained phonedb overlay JSON.",
    )
    parser.add_argument(
        "--summary-output",
        default=str(PHONEDB_MODEL_SUMMARY_FILE),
        help="Output path for training summary JSON.",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()
    summary = train_phonedb_models(
        input_file=args.input_file,
        overlay_file=args.overlay_output,
        summary_file=args.summary_output,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
