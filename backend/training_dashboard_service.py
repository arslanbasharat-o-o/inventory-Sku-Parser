#!/usr/bin/env python3
"""Admin training service for SKU parser datasets."""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from . import sku_parser
from .sku_intelligence_engine import (
    COLOR_DATASET_FILE,
    LEARNED_PATTERNS_FILE,
    LEARNED_SKU_CORRECTIONS_FILE,
    LEARNED_SPELLING_VARIATIONS_FILE,
    PART_ONTOLOGY_DATASET_FILE,
    PHRASE_NORMALIZATION_FILE,
    RUNTIME_DATA_DIR,
    UNKNOWN_LOG_FILE,
)
from .sku_parser import NOT_UNDERSTANDABLE


class TrainingDashboardService:
    """Persist and apply parser training signals from admin dashboard."""

    def __init__(
        self,
        *,
        base_dir: str | Path | None = None,
        structured_log_db_path: str | Path = "outputs/structured_sku_results.db",
    ) -> None:
        self.base_dir = Path(base_dir).resolve() if base_dir else RUNTIME_DATA_DIR
        self.structured_log_db_path = Path(structured_log_db_path)

        self.training_examples_file = self.base_dir / "training_examples.json"
        self.learned_sku_corrections_file = LEARNED_SKU_CORRECTIONS_FILE
        self.learned_rules_file = self.base_dir / "learned_rules.json"
        self.learned_patterns_file = LEARNED_PATTERNS_FILE
        self.learned_spelling_file = LEARNED_SPELLING_VARIATIONS_FILE
        self.phrase_normalization_file = PHRASE_NORMALIZATION_FILE
        self.part_ontology_file = PART_ONTOLOGY_DATASET_FILE
        self.color_dataset_file = COLOR_DATASET_FILE
        self.unknown_log_file = UNKNOWN_LOG_FILE

        self._ensure_seed_files()

    def _engine(self):
        return getattr(sku_parser, "_ENGINE", None)

    def _ensure_seed_files(self) -> None:
        self._ensure_json_file(self.training_examples_file, [])
        self._ensure_json_file(
            self.learned_sku_corrections_file,
            {"sku_overrides": {}, "title_overrides": {}},
        )
        self._ensure_json_file(self.learned_rules_file, [])

    def _ensure_json_file(self, path: Path, seed: object) -> None:
        if path.exists():
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(seed, indent=2, ensure_ascii=True), encoding="utf-8")

    def _read_json(self, path: Path, default: Any) -> Any:
        try:
            with path.open("r", encoding="utf-8") as file_obj:
                payload = json.load(file_obj)
        except Exception:
            return default
        if isinstance(default, dict) and not isinstance(payload, dict):
            return default
        if isinstance(default, list) and not isinstance(payload, list):
            return default
        return payload

    def _write_json(self, path: Path, payload: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")

    def _normalize_phrase(self, value: object) -> str:
        engine = self._engine()
        text = str(value or "").strip()
        if not text:
            return ""
        if engine is not None:
            return engine.normalize_phrase(text)
        text = re.sub(r"[^a-z0-9\s]", " ", text.lower())
        return re.sub(r"\s+", " ", text).strip()

    def _normalize_code(self, value: object) -> str:
        engine = self._engine()
        text = str(value or "").strip().upper()
        if not text:
            return ""
        if engine is not None:
            return engine.normalize_code(text)
        text = re.sub(r"[^A-Z0-9/\-\s]", " ", text)
        return re.sub(r"\s+", " ", text).strip()

    def _reload_parser(self) -> None:
        reload_fn = getattr(sku_parser, "reload_runtime_resources", None)
        if callable(reload_fn):
            reload_fn()
            return
        refresh_fn = getattr(sku_parser, "_refresh_parts_lookup", None)
        if callable(refresh_fn):
            refresh_fn()

    def _load_learned_patterns(self) -> dict[str, str]:
        data = self._read_json(self.learned_patterns_file, {})
        out: dict[str, str] = {}
        for phrase, code in data.items():
            key = self._normalize_phrase(phrase)
            value = self._normalize_code(code)
            if key and value:
                out[key] = value
        return out

    def _save_learned_patterns(self, mapping: dict[str, str]) -> None:
        ordered = dict(sorted(mapping.items(), key=lambda item: (-len(item[0]), item[0])))
        self._write_json(self.learned_patterns_file, ordered)

    def _load_phrase_normalizations(self) -> tuple[dict[str, Any], dict[str, str]]:
        payload = self._read_json(self.phrase_normalization_file, {})
        if not isinstance(payload, dict):
            payload = {}
        normalizations = payload.get("normalizations", {})
        if not isinstance(normalizations, dict):
            normalizations = {}
        out: dict[str, str] = {}
        for src, dst in normalizations.items():
            key = self._normalize_phrase(src)
            value = self._normalize_phrase(dst)
            if key and value:
                out[key] = value
        payload["normalizations"] = out
        return payload, out

    def _load_sku_corrections_payload(self) -> dict[str, dict[str, str]]:
        payload = self._read_json(
            self.learned_sku_corrections_file,
            {"sku_overrides": {}, "title_overrides": {}},
        )
        if not isinstance(payload, dict):
            payload = {"sku_overrides": {}, "title_overrides": {}}
        sku_overrides = payload.get("sku_overrides", {})
        title_overrides = payload.get("title_overrides", {})
        if not isinstance(sku_overrides, dict):
            sku_overrides = {}
        if not isinstance(title_overrides, dict):
            title_overrides = {}

        clean_sku_overrides: dict[str, str] = {}
        for src, dst in sku_overrides.items():
            left = self._normalize_code(src)
            right = self._normalize_code(dst)
            if left and right:
                clean_sku_overrides[left] = right

        clean_title_overrides: dict[str, str] = {}
        for src, dst in title_overrides.items():
            left = self._normalize_phrase(src)
            right = self._normalize_code(dst)
            if left and right:
                clean_title_overrides[left] = right

        return {
            "sku_overrides": clean_sku_overrides,
            "title_overrides": clean_title_overrides,
        }

    def _save_sku_corrections_payload(self, payload: dict[str, dict[str, str]]) -> None:
        payload["sku_overrides"] = dict(sorted(payload.get("sku_overrides", {}).items()))
        payload["title_overrides"] = dict(sorted(payload.get("title_overrides", {}).items()))
        self._write_json(self.learned_sku_corrections_file, payload)

    def _extract_code_from_sku(self, sku: str) -> str:
        normalized_sku = self._normalize_code(sku)
        if not normalized_sku:
            return ""
        engine = self._engine()
        if engine is None:
            return ""

        known_codes = sorted({self._normalize_code(code) for code in engine.known_codes}, key=len, reverse=True)
        padded = f" {normalized_sku} "
        for code in known_codes:
            if not code:
                continue
            if f" {code} " in padded:
                return code
        return ""

    def _part_ontology_entries(self) -> list[dict[str, str]]:
        payload = self._read_json(self.part_ontology_file, {})
        if not isinstance(payload, dict):
            return []

        entries: dict[str, str] = {}

        part_code_map = payload.get("part_code_map", {})
        if isinstance(part_code_map, dict):
            for phrase, code in part_code_map.items():
                key = self._normalize_phrase(phrase)
                value = self._normalize_code(code)
                if key and value:
                    entries[key] = value

        part_aliases = payload.get("part_aliases", {})
        if isinstance(part_aliases, dict):
            for part_name, aliases in part_aliases.items():
                resolved_code = ""
                if isinstance(part_code_map, dict):
                    resolved_code = self._normalize_code(part_code_map.get(part_name, ""))
                if not resolved_code:
                    resolved_code = self._normalize_code(part_name)
                if not resolved_code or not isinstance(aliases, list):
                    continue
                for alias in aliases:
                    alias_key = self._normalize_phrase(alias)
                    if alias_key:
                        entries[alias_key] = resolved_code

        ordered = sorted(entries.items(), key=lambda item: (item[1], item[0]))
        return [{"phrase": phrase.upper(), "sku_code": code} for phrase, code in ordered]

    def _color_mappings(self) -> list[dict[str, str]]:
        payload = self._read_json(self.color_dataset_file, {})
        if not isinstance(payload, dict):
            return []

        mappings: dict[str, str] = {}

        synonyms = payload.get("synonyms", {})
        if isinstance(synonyms, dict):
            for supplier, standard in synonyms.items():
                left = self._normalize_phrase(supplier)
                right = self._normalize_code(standard)
                if left and right:
                    mappings[left] = right

        color_families = payload.get("color_families", {})
        if isinstance(color_families, dict):
            for family, values in color_families.items():
                family_code = self._normalize_code(family)
                if not family_code:
                    continue
                if isinstance(values, list):
                    for alias in values:
                        alias_key = self._normalize_phrase(alias)
                        if alias_key:
                            mappings.setdefault(alias_key, family_code)

        ordered = sorted(mappings.items(), key=lambda item: item[0])
        return [
            {
                "supplier_color": supplier.upper(),
                "standard_color": color,
            }
            for supplier, color in ordered
        ]

    def get_bootstrap(self) -> dict[str, object]:
        examples = self._read_json(self.training_examples_file, [])
        if not isinstance(examples, list):
            examples = []

        phrase_payload, normalizations = self._load_phrase_normalizations()
        spelling_data = self._read_json(self.learned_spelling_file, {})
        if not isinstance(spelling_data, dict):
            spelling_data = {}

        spelling_rows = [
            {"incorrect": wrong.upper(), "correct": self._normalize_code(right)}
            for wrong, right in sorted(spelling_data.items())
            if self._normalize_phrase(wrong) and self._normalize_phrase(right)
        ]

        synonym_rows = [
            {"supplier_phrase": src.upper(), "standard_term": dst.upper()}
            for src, dst in sorted(normalizations.items())
        ]

        corrections_payload = self._load_sku_corrections_payload()
        sku_rows = [
            {"generated_sku": src, "correct_sku": dst}
            for src, dst in sorted(corrections_payload.get("sku_overrides", {}).items())
        ]

        title_override_rows = [
            {"title": src.upper(), "correct_sku": dst}
            for src, dst in sorted(corrections_payload.get("title_overrides", {}).items())
        ]

        rules_rows = self._read_json(self.learned_rules_file, [])
        if not isinstance(rules_rows, list):
            rules_rows = []

        learned_pattern_rows = [
            {"pattern": phrase.upper(), "sku_code": code}
            for phrase, code in list(self._load_learned_patterns().items())[:250]
        ]

        analytics = self.get_analytics()

        return {
            "analytics": analytics,
            "title_training_samples": examples[-200:],
            "synonym_mappings": synonym_rows[:500],
            "spelling_corrections": spelling_rows[:500],
            "part_ontology": self._part_ontology_entries()[:1000],
            "color_mappings": self._color_mappings()[:1000],
            "sku_corrections": sku_rows[:1000],
            "title_overrides": title_override_rows[:1000],
            "rule_definitions": rules_rows[-200:],
            "learned_pattern_preview": learned_pattern_rows,
            "meta": {
                "synonym_count": len(normalizations),
                "spelling_count": len(spelling_rows),
                "part_mapping_count": len(self._part_ontology_entries()),
                "color_mapping_count": len(self._color_mappings()),
                "sku_correction_count": len(sku_rows),
                "title_override_count": len(title_override_rows),
                "rule_count": len(rules_rows),
                "training_example_count": len(examples),
                "normalization_dataset_loaded": bool(phrase_payload.get("normalizations", {})),
            },
        }

    def add_title_training_sample(
        self,
        *,
        product_title: str,
        detected_model: str,
        detected_part: str,
        detected_color: str,
        expected_sku: str,
    ) -> dict[str, object]:
        title = str(product_title or "").strip()
        expected = self._normalize_code(expected_sku)
        if not title:
            raise ValueError("Product title is required.")
        if not expected:
            raise ValueError("Expected SKU is required.")

        examples = self._read_json(self.training_examples_file, [])
        if not isinstance(examples, list):
            examples = []

        row = {
            "product_title": title,
            "detected_model": self._normalize_code(detected_model),
            "detected_part": self._normalize_code(detected_part),
            "detected_color": self._normalize_code(detected_color),
            "expected_sku": expected,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        examples.append(row)
        self._write_json(self.training_examples_file, examples[-5000:])

        corrections_payload = self._load_sku_corrections_payload()
        title_key = self._normalize_phrase(title)
        if title_key:
            corrections_payload["title_overrides"][title_key] = expected
        self._save_sku_corrections_payload(corrections_payload)

        self._reload_parser()
        return row

    def add_synonym_mapping(self, *, supplier_phrase: str, standard_term: str) -> dict[str, str]:
        src = self._normalize_phrase(supplier_phrase)
        dst = self._normalize_phrase(standard_term)
        if not src or not dst:
            raise ValueError("Supplier phrase and standard term are required.")

        payload, normalizations = self._load_phrase_normalizations()
        normalizations[src] = dst
        payload["normalizations"] = dict(sorted(normalizations.items()))
        self._write_json(self.phrase_normalization_file, payload)

        # Promote phrase to learned patterns when standard term maps to a known part code.
        engine = self._engine()
        if engine is not None:
            std_code = self._normalize_code(engine.part_dictionary.get(dst, ""))
            if std_code:
                learned = self._load_learned_patterns()
                learned[src] = std_code
                self._save_learned_patterns(learned)

        self._reload_parser()
        return {"supplier_phrase": src.upper(), "standard_term": dst.upper()}

    def add_spelling_correction(self, *, incorrect_word: str, correct_word: str) -> dict[str, str]:
        wrong = self._normalize_phrase(incorrect_word)
        right = self._normalize_phrase(correct_word)
        if not wrong or not right:
            raise ValueError("Incorrect word and correct word are required.")

        payload = self._read_json(self.learned_spelling_file, {})
        if not isinstance(payload, dict):
            payload = {}
        payload[wrong] = right
        self._write_json(self.learned_spelling_file, dict(sorted(payload.items())))

        self._reload_parser()
        return {"incorrect": wrong.upper(), "correct": right.upper()}

    def add_part_mapping(self, *, phrase: str, sku_code: str) -> dict[str, str]:
        key = self._normalize_phrase(phrase)
        code = self._normalize_code(sku_code)
        if not key or not code:
            raise ValueError("Phrase and SKU code are required.")

        payload = self._read_json(self.part_ontology_file, {})
        if not isinstance(payload, dict):
            payload = {}

        part_code_map = payload.get("part_code_map", {})
        if not isinstance(part_code_map, dict):
            part_code_map = {}
        part_code_map[key.upper()] = code
        payload["part_code_map"] = dict(sorted(part_code_map.items()))
        payload["updated_at"] = datetime.now(timezone.utc).isoformat()
        self._write_json(self.part_ontology_file, payload)

        self._reload_parser()
        return {"phrase": key.upper(), "sku_code": code}

    def add_color_mapping(self, *, supplier_color: str, standard_color: str) -> dict[str, str]:
        supplier = self._normalize_phrase(supplier_color)
        standard = self._normalize_code(standard_color)
        if not supplier or not standard:
            raise ValueError("Supplier color and standard color are required.")

        payload = self._read_json(self.color_dataset_file, {})
        if not isinstance(payload, dict):
            payload = {}

        synonyms = payload.get("synonyms", {})
        if not isinstance(synonyms, dict):
            synonyms = {}
        synonyms[supplier] = standard
        payload["synonyms"] = dict(sorted(synonyms.items()))

        normalized_colors = payload.get("normalized_colors", [])
        if not isinstance(normalized_colors, list):
            normalized_colors = []
        if standard not in {self._normalize_code(color) for color in normalized_colors}:
            normalized_colors.append(standard)
        payload["normalized_colors"] = sorted(
            [self._normalize_code(color) for color in normalized_colors if self._normalize_code(color)]
        )

        self._write_json(self.color_dataset_file, payload)
        self._reload_parser()
        return {"supplier_color": supplier.upper(), "standard_color": standard}

    def add_sku_correction(
        self,
        *,
        generated_sku: str,
        correct_sku: str,
        title: str = "",
    ) -> dict[str, str]:
        generated = self._normalize_code(generated_sku)
        corrected = self._normalize_code(correct_sku)
        if not generated:
            raise ValueError("Generated SKU is required.")
        if not corrected:
            raise ValueError("Correct SKU is required.")

        payload = self._load_sku_corrections_payload()
        payload["sku_overrides"][generated] = corrected
        title_key = self._normalize_phrase(title)
        if title_key:
            payload["title_overrides"][title_key] = corrected
        self._save_sku_corrections_payload(payload)

        self._reload_parser()
        return {
            "generated_sku": generated,
            "correct_sku": corrected,
            "title": title_key.upper() if title_key else "",
        }

    def add_rule(self, *, rule_text: str) -> dict[str, str]:
        raw = str(rule_text or "").strip()
        if not raw:
            raise ValueError("Rule text is required.")

        patterns = [
            re.compile(
                r"if\s+title\s+contains\s+[\"'](?P<phrase>.+?)[\"']\s+then\s+part\s*=\s*(?P<code>[A-Za-z0-9/\-\s]+)",
                re.IGNORECASE,
            ),
            re.compile(
                r"contains\s+[\"'](?P<phrase>.+?)[\"']\s*[,;]?\s*part\s*=\s*(?P<code>[A-Za-z0-9/\-\s]+)",
                re.IGNORECASE,
            ),
        ]

        phrase = ""
        code = ""
        for pattern in patterns:
            match = pattern.search(raw)
            if match:
                phrase = self._normalize_phrase(match.group("phrase"))
                code = self._normalize_code(match.group("code"))
                break

        if not phrase or not code:
            raise ValueError("Rule must follow format: If title contains 'phrase' then part = CODE")

        learned = self._load_learned_patterns()
        learned[phrase] = code
        self._save_learned_patterns(learned)

        rule_rows = self._read_json(self.learned_rules_file, [])
        if not isinstance(rule_rows, list):
            rule_rows = []
        rule_rows.append(
            {
                "rule_text": raw,
                "phrase": phrase.upper(),
                "sku_code": code,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        self._write_json(self.learned_rules_file, rule_rows[-5000:])

        self._reload_parser()
        return {"rule_text": raw, "phrase": phrase.upper(), "sku_code": code}

    def upload_training_dataset(self, file_path: Path) -> dict[str, object]:
        suffix = file_path.suffix.lower()
        if suffix == ".csv":
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path, engine="openpyxl")

        if df.empty:
            raise ValueError("Uploaded dataset is empty.")

        normalized_columns = {str(col).strip().lower(): str(col) for col in df.columns}

        title_col = ""
        for candidate in (
            "product title",
            "product name",
            "title",
            "name",
        ):
            if candidate in normalized_columns:
                title_col = normalized_columns[candidate]
                break

        correct_sku_col = ""
        for candidate in (
            "correct sku",
            "expected sku",
            "sku",
            "target sku",
        ):
            if candidate in normalized_columns:
                correct_sku_col = normalized_columns[candidate]
                break

        if not title_col or not correct_sku_col:
            raise ValueError("Dataset must contain Product Title and Correct SKU columns.")

        total_rows = int(len(df))
        compared_rows = 0
        matched_rows = 0
        mismatch_rows = 0
        learned_title_overrides = 0
        sample_differences: list[dict[str, str]] = []

        corrections_payload = self._load_sku_corrections_payload()

        for row in df[[title_col, correct_sku_col]].fillna("").itertuples(index=False):
            title = str(row[0] or "").strip()
            correct_sku = self._normalize_code(row[1])
            if not title or not correct_sku:
                continue

            compared_rows += 1
            parsed = sku_parser.analyze_title(title)
            generated_sku = self._normalize_code(parsed.get("sku", ""))

            if generated_sku == correct_sku:
                matched_rows += 1
                continue

            mismatch_rows += 1
            title_key = self._normalize_phrase(title)
            if title_key:
                existing = corrections_payload["title_overrides"].get(title_key, "")
                if existing != correct_sku:
                    corrections_payload["title_overrides"][title_key] = correct_sku
                    learned_title_overrides += 1

            if generated_sku:
                corrections_payload["sku_overrides"][generated_sku] = correct_sku

            if len(sample_differences) < 100:
                sample_differences.append(
                    {
                        "title": title,
                        "generated_sku": generated_sku,
                        "correct_sku": correct_sku,
                    }
                )

        self._save_sku_corrections_payload(corrections_payload)
        self._reload_parser()

        accuracy = (matched_rows / compared_rows) if compared_rows else 0.0
        return {
            "rows_total": total_rows,
            "rows_compared": compared_rows,
            "matched_rows": matched_rows,
            "mismatch_rows": mismatch_rows,
            "accuracy": round(accuracy, 4),
            "learned_title_overrides": learned_title_overrides,
            "sample_differences": sample_differences,
        }

    def get_analytics(self) -> dict[str, object]:
        total_titles_parsed = 0
        parsed_titles = 0
        duplicate_skus = 0

        if self.structured_log_db_path.exists():
            try:
                with sqlite3.connect(self.structured_log_db_path) as conn:
                    cursor = conn.cursor()
                    row = cursor.execute(
                        "SELECT COUNT(*) FROM structured_parse_logs"
                    ).fetchone()
                    total_titles_parsed = int((row or [0])[0] or 0)

                    row = cursor.execute(
                        "SELECT COUNT(*) FROM structured_parse_logs WHERE generated_sku != ?",
                        (NOT_UNDERSTANDABLE,),
                    ).fetchone()
                    parsed_titles = int((row or [0])[0] or 0)

                    row = cursor.execute(
                        """
                        SELECT COUNT(*)
                        FROM (
                            SELECT generated_sku
                            FROM structured_parse_logs
                            WHERE generated_sku != ?
                            GROUP BY generated_sku
                            HAVING COUNT(*) > 1
                        )
                        """,
                        (NOT_UNDERSTANDABLE,),
                    ).fetchone()
                    duplicate_skus = int((row or [0])[0] or 0)
            except Exception:
                total_titles_parsed = 0
                parsed_titles = 0
                duplicate_skus = 0

        unknown_parts_detected = 0
        unknown_rows = self._read_json(self.unknown_log_file, [])
        if isinstance(unknown_rows, list):
            unknown_parts_detected = len(unknown_rows)

        accuracy = (parsed_titles / total_titles_parsed) if total_titles_parsed else 0.0
        duplicate_rate = (duplicate_skus / parsed_titles) if parsed_titles else 0.0

        return {
            "total_titles_parsed": total_titles_parsed,
            "parsing_accuracy": round(accuracy, 4),
            "duplicate_skus": duplicate_skus,
            "duplicate_rate": round(duplicate_rate, 4),
            "unknown_parts_detected": unknown_parts_detected,
        }

    def live_test(self, title: str) -> dict[str, object]:
        parsed = sku_parser.analyze_title(title)
        return {
            "title": title,
            "detected_model": parsed.get("model", ""),
            "detected_part": parsed.get("part", ""),
            "detected_color": parsed.get("color", ""),
            "generated_sku": parsed.get("sku", NOT_UNDERSTANDABLE),
            "confidence": float(parsed.get("confidence", 0.0) or 0.0),
            "corrections": parsed.get("corrections", []),
            "parse_status": parsed.get("parse_status", "not_understandable"),
            "reason": parsed.get("reason", ""),
        }
