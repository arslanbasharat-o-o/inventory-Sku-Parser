#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from backend.sku_intelligence_engine import (
    BRAND_DATASET_FILE,
    EngineConfig,
    LEARNED_PATTERNS_FILE,
    PARTS_DICTIONARY_FILE,
    PARTS_ONTOLOGY_FILE,
    PART_CODE_RULES_FILE,
    RUNTIME_DATA_DIR,
    SPELLING_CORRECTIONS_FILE,
    TX_PARTS_CATALOG_OVERLAY_FILE,
    TX_PARTS_TITLE_MEMORY_FILE,
)
from backend.train_title_patterns import train_patterns_from_dataframe

TX_PARTS_SKU_AUDIT_FILE = RUNTIME_DATA_DIR / "tx_parts_sku_audit.json"
TX_PARTS_TRAINING_SUMMARY_FILE = RUNTIME_DATA_DIR / "tx_parts_training_summary.json"

RE_MULTI_SPACE = re.compile(r"\s+")
RE_NORMALIZE = re.compile(r"[^a-z0-9]+")
RE_PARENS = re.compile(r"\([^)]*\)")
RE_MODEL_CODE = re.compile(
    r"\b(?:SM-[A-Z0-9-]+|XT\d{4,5}(?:-\d+)?|CPH\d{4,5}|RMX\d{4,5}|TA-\d{3,5}|"
    r"[AGMNSFX]\d{3,4}[A-Z]?|NE\d{4}|V\d{4}[A-Z0-9-]*)\b",
    re.IGNORECASE,
)

SKIPPED_CATEGORY_PREFIXES = {
    "TABLET",
    "GAME CONSOLE",
    "TOOLS AND ACCESSORIES",
}
APPLE_PHONE_HINTS = ("IPHONE",)
GOOGLE_PHONE_HINTS = ("PIXEL", "NEXUS")
LEGACY_AUDIT_STOPWORDS = {
    "all",
    "colors",
    "black",
    "white",
    "blue",
    "gold",
    "silver",
    "green",
    "purple",
    "pink",
    "gray",
    "grey",
    "red",
    "yellow",
    "orange",
    "phantom",
    "aura",
    "prism",
    "midnight",
    "graphite",
    "refurb",
    "refurbished",
    "premium",
    "without",
    "with",
    "frame",
    "outer",
    "inner",
    "logo",
    "big",
    "holes",
    "camera",
    "lens",
    "no",
}


def normalize_phrase(value: str) -> str:
    text = RE_NORMALIZE.sub(" ", str(value).lower())
    return RE_MULTI_SPACE.sub(" ", text).strip()


def normalize_code(value: str) -> str:
    text = re.sub(r"[^A-Z0-9/\-\s]", " ", str(value).upper())
    return RE_MULTI_SPACE.sub(" ", text).strip()


def load_brand_alias_map() -> dict[str, str]:
    data = json.loads(BRAND_DATASET_FILE.read_text(encoding="utf-8"))
    brands = data.get("brands", {}) if isinstance(data, dict) else {}
    alias_lookup: dict[str, str] = {}
    display_names: dict[str, str] = {
        "apple": "Apple",
        "asus": "Asus",
        "google": "Google",
        "honor": "Honor",
        "huawei": "Huawei",
        "infinix": "Infinix",
        "lenovo": "Lenovo",
        "lg": "LG",
        "motorola": "Motorola",
        "nokia": "Nokia",
        "oneplus": "OnePlus",
        "oppo": "Oppo",
        "poco": "Poco",
        "realme": "Realme",
        "redmi": "Redmi",
        "samsung": "Samsung",
        "sony": "Sony",
        "tecno": "Tecno",
        "vivo": "Vivo",
        "xiaomi": "Xiaomi",
        "zte": "ZTE",
    }
    for brand_key, info in brands.items():
        canonical = display_names.get(str(brand_key).lower(), str(brand_key).title())
        alias_lookup[normalize_phrase(str(brand_key))] = canonical
        if isinstance(info, dict):
            for alias in info.get("aliases", []):
                alias_lookup[normalize_phrase(str(alias))] = canonical
    for alias, canonical in data.get("brand_alias_map", {}).items():
        canonical_display = display_names.get(str(canonical).lower(), str(canonical).title())
        alias_lookup[normalize_phrase(str(alias))] = canonical_display
    return alias_lookup


def split_category_entries(category: str) -> list[tuple[str, str, str]]:
    entries: list[tuple[str, str, str]] = []
    for raw_entry in str(category).split("|"):
        entry = raw_entry.strip()
        if not entry:
            continue
        parts = [part.strip() for part in entry.split(",") if part.strip()]
        if not parts:
            continue
        if parts[0].lower() in {"other brands", "devices"} and len(parts) >= 4:
            brand = parts[1]
            series = parts[2]
            model = ", ".join(parts[3:])
        elif len(parts) >= 3:
            brand = parts[0]
            series = parts[1]
            model = ", ".join(parts[2:])
        elif len(parts) == 2:
            brand = parts[0]
            series = ""
            model = parts[1]
        else:
            brand = parts[0]
            series = ""
            model = parts[0]
        entries.append((brand.strip(), series.strip(), model.strip()))
    return entries


def resolve_brand(
    brand_alias_map: dict[str, str],
    primary: str,
    series: str,
    model: str,
    title: str,
) -> str:
    exact_key = normalize_phrase(primary)
    if exact_key in brand_alias_map:
        return brand_alias_map[exact_key]

    search_spaces = [primary, series, model, title]
    alias_items = sorted(brand_alias_map.items(), key=lambda item: len(item[0]), reverse=True)
    for text in search_spaces:
        norm_text = normalize_phrase(text)
        if not norm_text:
            continue
        padded = f" {norm_text} "
        for alias, canonical in alias_items:
            if not alias:
                continue
            if f" {alias} " in padded:
                return canonical
    return ""


def is_phone_like_entry(brand: str, series: str, model: str, title: str) -> bool:
    brand_code = normalize_code(brand)
    if not brand_code:
        return False
    if brand_code in SKIPPED_CATEGORY_PREFIXES:
        return False

    series_code = normalize_code(series)
    model_code = normalize_code(model)
    title_code = normalize_code(title)
    combined = f"{brand_code} {series_code} {model_code} {title_code}".strip()

    if brand_code == "APPLE":
        return any(hint in combined for hint in APPLE_PHONE_HINTS)
    if brand_code == "GOOGLE":
        return any(hint in combined for hint in GOOGLE_PHONE_HINTS)
    if "IPAD" in combined or "IWATCH" in combined or "WATCH" in combined or "MACBOOK" in combined:
        return False
    return True


def extract_model_codes(*values: str) -> list[str]:
    codes: list[str] = []
    seen: set[str] = set()
    for value in values:
        for match in RE_MODEL_CODE.findall(str(value)):
            code = normalize_code(match)
            if len(code) < 4:
                continue
            if code.isdigit():
                continue
            if re.fullmatch(r"20\d{2}", code):
                continue
            if code in {"4G", "5G"}:
                continue
            if code.endswith("GB") or code.endswith("HZ"):
                continue
            if "SERIES" in code:
                continue
            if code in seen:
                continue
            seen.add(code)
            codes.append(code)
    return codes


def clean_model_label(model: str) -> str:
    text = str(model)
    text = re.sub(r"\|\s*APPLE\b", "", text, flags=re.IGNORECASE)
    text = RE_PARENS.sub(" ", text)
    text = RE_MULTI_SPACE.sub(" ", text).strip(" -/")
    return text


def build_aliases(brand: str, model: str, model_codes: list[str]) -> list[str]:
    base = clean_model_label(model)
    aliases: list[str] = []

    def add(value: str) -> None:
        value = RE_MULTI_SPACE.sub(" ", str(value)).strip()
        if not value:
            return
        if value not in aliases:
            aliases.append(value)

    add(model)
    add(base)
    for code in model_codes:
        add(f"{base} {code}")

    upper_base = normalize_code(base)
    if brand == "Samsung" and upper_base and not upper_base.startswith("GALAXY "):
        add(f"Galaxy {base}")
        for code in model_codes:
            add(f"Galaxy {base} {code}")
    elif brand == "Google" and upper_base and not upper_base.startswith(("PIXEL ", "NEXUS ")):
        add(f"Pixel {base}")
        for code in model_codes:
            add(f"Pixel {base} {code}")
    elif brand == "Motorola" and upper_base and not upper_base.startswith("MOTO "):
        add(f"Moto {base}")
        for code in model_codes:
            add(f"Moto {base} {code}")
    elif brand == "Apple":
        add(base.replace("IPhone", "iPhone"))
        add(base.replace("IPHONE", "iPhone"))

    return aliases


def build_catalog_overlay_from_dataframe(df: pd.DataFrame) -> tuple[dict[str, Any], dict[str, Any]]:
    brand_alias_map = load_brand_alias_map()
    model_index: dict[tuple[str, str], dict[str, Any]] = {}
    brand_aliases: dict[str, str] = {}
    stats = Counter()

    for row in df.fillna("").itertuples(index=False):
        title = str(getattr(row, "Product_Name", "") or row[df.columns.get_loc("Product Name")])
        category = str(getattr(row, "Category", "") or row[df.columns.get_loc("Category")])
        product_code = str(getattr(row, "Product_Code", "") or row[df.columns.get_loc("Product Code")])
        stats["rows_total"] += 1

        if not title or not category:
            stats["rows_missing_title_or_category"] += 1
            continue

        category_entries = split_category_entries(category)
        if not category_entries:
            stats["rows_unparsed_category"] += 1
            continue

        row_added = False
        for primary_brand, series, model in category_entries:
            brand = resolve_brand(brand_alias_map, primary_brand, series, model, title)
            if not brand or not is_phone_like_entry(brand, series, model, title):
                continue

            model_clean = clean_model_label(model)
            if not model_clean:
                continue
            key = (brand, normalize_code(model_clean))
            model_codes = extract_model_codes(model)
            aliases = build_aliases(brand, model, model_codes)

            entry = model_index.setdefault(
                key,
                {
                    "brand": brand,
                    "model": model_clean,
                    "model_codes": [],
                    "aliases": [],
                    "series": normalize_code(series),
                    "row_count": 0,
                    "source": "tx_parts_catalog",
                },
            )
            entry["row_count"] += 1
            for code in model_codes:
                if code not in entry["model_codes"]:
                    entry["model_codes"].append(code)
            for alias in aliases:
                if alias not in entry["aliases"]:
                    entry["aliases"].append(alias)

            primary_brand_norm = normalize_phrase(primary_brand)
            if primary_brand_norm and primary_brand_norm in brand_alias_map:
                brand_aliases[primary_brand_norm] = brand
            row_added = True

        if row_added:
            stats["rows_used_for_overlay"] += 1
        else:
            stats["rows_skipped_non_phone_or_unknown_brand"] += 1

    models = sorted(
        model_index.values(),
        key=lambda item: (-int(item["row_count"]), str(item["brand"]), str(item["model"])),
    )
    overlay = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows_total": int(stats["rows_total"]),
        "rows_used_for_overlay": int(stats["rows_used_for_overlay"]),
        "brand_aliases": dict(sorted((k, v) for k, v in brand_aliases.items() if k and v)),
        "models": models,
    }
    summary = {
        **{key: int(value) for key, value in stats.items()},
        "brand_aliases": len(overlay["brand_aliases"]),
        "models": len(models),
        "top_brands": Counter(item["brand"] for item in models).most_common(20),
    }
    return overlay, summary


def build_title_memory_from_dataframe(df: pd.DataFrame) -> tuple[dict[str, Any], dict[str, int]]:
    brand_alias_map = load_brand_alias_map()
    title_candidates: dict[str, dict[tuple[str, str], dict[str, Any]]] = defaultdict(dict)
    stats = Counter()

    for row in df.fillna("").itertuples(index=False):
        title = str(getattr(row, "Product_Name", "") or row[df.columns.get_loc("Product Name")]).strip()
        category = str(getattr(row, "Category", "") or row[df.columns.get_loc("Category")]).strip()
        if not title or not category:
            continue
        stats["rows_total"] += 1
        title_key = normalize_phrase(title)
        if not title_key:
            continue

        resolved_rows: list[tuple[str, str, list[str]]] = []
        for primary_brand, series, model in split_category_entries(category):
            brand = resolve_brand(brand_alias_map, primary_brand, series, model, title)
            if not brand or not is_phone_like_entry(brand, series, model, title):
                continue
            model_clean = clean_model_label(model)
            if not model_clean:
                continue
            model_codes = extract_model_codes(model)
            resolved_rows.append((brand, model_clean, model_codes))

        unique_targets: dict[tuple[str, str], list[str]] = {}
        for brand, model_clean, model_codes in resolved_rows:
            key = (brand, model_clean)
            existing = unique_targets.setdefault(key, [])
            for code in model_codes:
                if code not in existing:
                    existing.append(code)

        if len(unique_targets) != 1:
            if unique_targets:
                stats["ambiguous_title_rows"] += 1
            continue

        (brand, model_clean), model_codes = next(iter(unique_targets.items()))
        target_bucket = title_candidates[title_key].setdefault(
            (brand, model_clean),
            {
                "brand": brand,
                "model": model_clean,
                "model_codes": [],
                "count": 0,
                "canonical_title": title,
            },
        )
        target_bucket["count"] += 1
        for code in model_codes:
            if code not in target_bucket["model_codes"]:
                target_bucket["model_codes"].append(code)

    titles: dict[str, dict[str, Any]] = {}
    for title_key, candidate_map in title_candidates.items():
        if len(candidate_map) != 1:
            stats["ambiguous_title_keys"] += 1
            continue
        payload = next(iter(candidate_map.values()))
        titles[title_key] = {
            "brand": payload["brand"],
            "model": payload["model"],
            "model_codes": payload["model_codes"],
            "count": int(payload["count"]),
            "canonical_title": payload["canonical_title"],
        }

    title_memory = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "titles": dict(sorted(titles.items())),
    }
    stats["title_memory_entries"] = len(titles)
    return title_memory, {key: int(value) for key, value in stats.items()}


def merge_learned_patterns_from_catalog(
    df: pd.DataFrame,
    learned_patterns_file: Path = LEARNED_PATTERNS_FILE,
) -> dict[str, Any]:
    training_df = pd.DataFrame(
        {
            "Product Name": df.get("Product Name", pd.Series(dtype=str)).fillna("").astype(str),
            "Product SKU": df.get("Product Code", pd.Series(dtype=str)).fillna("").astype(str),
            "Product Web SKU": df.get("Product Code", pd.Series(dtype=str)).fillna("").astype(str),
        }
    )
    learned_patterns, stats = train_patterns_from_dataframe(
        training_df,
        min_count=4,
        min_confidence=0.92,
    )

    try:
        existing = json.loads(learned_patterns_file.read_text(encoding="utf-8"))
    except Exception:
        existing = {}
    if not isinstance(existing, dict):
        existing = {}

    added = 0
    skipped_conflicts = 0
    for phrase, code in learned_patterns.items():
        current = existing.get(phrase, "")
        if not current:
            existing[phrase] = code
            added += 1
            continue
        if current != code:
            skipped_conflicts += 1

    ordered = dict(sorted(existing.items(), key=lambda item: (-len(item[0]), item[0])))
    learned_patterns_file.write_text(
        json.dumps(ordered, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    return {
        "trainer_stats": stats,
        "patterns_added": added,
        "patterns_total": len(ordered),
        "patterns_candidate_count": len(learned_patterns),
        "patterns_conflicts_skipped": skipped_conflicts,
    }


def normalize_legacy_code(code: str) -> str:
    return normalize_code(str(code))


def _legacy_group_key(title: str) -> str:
    tokens = [
        token
        for token in normalize_phrase(title).split()
        if token and token not in LEGACY_AUDIT_STOPWORDS
    ]
    return " ".join(tokens[:12])


def build_legacy_sku_audit(df: pd.DataFrame) -> dict[str, Any]:
    groups: dict[str, Counter[str]] = defaultdict(Counter)
    sample_titles: dict[str, list[str]] = defaultdict(list)
    rows_with_legacy = 0

    for row in df.fillna("").itertuples(index=False):
        title = str(getattr(row, "Product_Name", "") or row[df.columns.get_loc("Product Name")])
        legacy_code = normalize_legacy_code(
            str(getattr(row, "Product_Code", "") or row[df.columns.get_loc("Product Code")])
        )
        if not title or not legacy_code:
            continue
        rows_with_legacy += 1
        group_key = _legacy_group_key(title)
        if not group_key:
            continue
        groups[group_key][legacy_code] += 1
        if len(sample_titles[group_key]) < 5:
            sample_titles[group_key].append(title)

    inconsistent_groups = []
    for title_group, counter in groups.items():
        if len(counter) <= 1:
            continue
        inconsistent_groups.append(
            {
                "title_group": title_group,
                "legacy_variants": counter.most_common(8),
                "rows": int(sum(counter.values())),
                "sample_titles": sample_titles[title_group],
            }
        )
    inconsistent_groups.sort(key=lambda item: (-int(item["rows"]), item["title_group"]))
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows_with_legacy_code": rows_with_legacy,
        "title_groups_with_multiple_legacy_formats": len(inconsistent_groups),
        "top_inconsistent_groups": inconsistent_groups[:250],
    }


def _read_input_dataframe(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    return pd.read_excel(path)


def train_tx_parts_catalog(
    input_file: str | Path,
    overlay_file: str | Path = TX_PARTS_CATALOG_OVERLAY_FILE,
    title_memory_file: str | Path = TX_PARTS_TITLE_MEMORY_FILE,
    audit_file: str | Path = TX_PARTS_SKU_AUDIT_FILE,
    summary_file: str | Path = TX_PARTS_TRAINING_SUMMARY_FILE,
) -> dict[str, Any]:
    input_path = Path(input_file)
    overlay_path = Path(overlay_file)
    title_memory_path = Path(title_memory_file)
    audit_path = Path(audit_file)
    summary_path = Path(summary_file)

    df = _read_input_dataframe(input_path)
    overlay, overlay_stats = build_catalog_overlay_from_dataframe(df)
    overlay["source_file"] = str(input_path)
    overlay_path.parent.mkdir(parents=True, exist_ok=True)
    overlay_path.write_text(json.dumps(overlay, indent=2, ensure_ascii=True), encoding="utf-8")

    title_memory, title_memory_stats = build_title_memory_from_dataframe(df)
    title_memory["source_file"] = str(input_path)
    title_memory_path.write_text(json.dumps(title_memory, indent=2, ensure_ascii=True), encoding="utf-8")

    pattern_stats = merge_learned_patterns_from_catalog(df)
    audit = build_legacy_sku_audit(df)
    audit["source_file"] = str(input_path)
    audit_path.write_text(json.dumps(audit, indent=2, ensure_ascii=True), encoding="utf-8")

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_file": str(input_path),
        "overlay_file": str(overlay_path),
        "title_memory_file": str(title_memory_path),
        "audit_file": str(audit_path),
        **overlay_stats,
        **title_memory_stats,
        **pattern_stats,
        "legacy_groups_with_inconsistencies": audit["title_groups_with_multiple_legacy_formats"],
    }
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=True), encoding="utf-8")
    return summary


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Train TX Parts brand/model overlay and audit legacy SKU consistency.",
    )
    parser.add_argument("input_file", help="TX Parts workbook (.xlsx or .csv)")
    parser.add_argument(
        "--overlay-output",
        default=str(TX_PARTS_CATALOG_OVERLAY_FILE),
        help="Runtime catalog overlay JSON output path.",
    )
    parser.add_argument(
        "--title-memory-output",
        default=str(TX_PARTS_TITLE_MEMORY_FILE),
        help="Exact title-memory JSON output path.",
    )
    parser.add_argument(
        "--audit-output",
        default=str(TX_PARTS_SKU_AUDIT_FILE),
        help="Legacy SKU audit JSON output path.",
    )
    parser.add_argument(
        "--summary-output",
        default=str(TX_PARTS_TRAINING_SUMMARY_FILE),
        help="Training summary JSON output path.",
    )
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()
    summary = train_tx_parts_catalog(
        input_file=args.input_file,
        overlay_file=args.overlay_output,
        title_memory_file=args.title_memory_output,
        audit_file=args.audit_output,
        summary_file=args.summary_output,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
