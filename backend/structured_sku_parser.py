#!/usr/bin/env python3
"""Structured SKU parsing service with Gemini/OpenAI structured fallback."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import ssl
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel, Field, ValidationError

from . import sku_parser
from .sku_parser import NOT_UNDERSTANDABLE, analyze_title as rule_analyze_title
from .sku_intelligence_engine import (
    APPROVED_LEARNED_PATTERNS_FILE,
    APPROVED_LEARNED_SPELLING_VARIATIONS_FILE,
    LEARNED_PATTERNS_FILE,
    LEARNED_SPELLING_VARIATIONS_FILE,
    SKU_VARIANT_TOKENS,
)

try:
    from openai import OpenAI  # type: ignore

    OPENAI_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    OpenAI = None  # type: ignore[assignment]
    OPENAI_AVAILABLE = False

try:
    import certifi

    CERTIFI_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    certifi = None  # type: ignore[assignment]
    CERTIFI_AVAILABLE = False


LOGGER = logging.getLogger(__name__)

RE_MULTI_SPACE = re.compile(r"\s+")
RE_SEPARATORS = re.compile(r"[-_/]+")
RE_NON_TITLE_TEXT = re.compile(r"[^A-Za-z0-9\s+]")
UNRESOLVED_PART = "UNRESOLVED"
FLEX_RULE_CODES = {
    "CF",
    "CAM-F",
    "PB-F",
    "VOL-F",
    "PV-F",
    "MIC-FC",
    "L-FLEX",
    "LCD-F",
    "MFC",
    "MB-FC",
    "VB-F",
    "HB-FC",
    "NFC-CF",
    "P/V-F",
    "FPC",
}


def _load_project_env_file() -> None:
    """Load local .env once for local/dev runs without shell exports."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key:
                os.environ.setdefault(key, value)
    except Exception:
        LOGGER.exception("Failed to load .env file from %s", env_path)


_load_project_env_file()


class ParsedSKUResult(BaseModel):
    brand: str = ""
    model: str = ""
    model_code: str = ""
    primary_part: str = ""
    secondary_part: str | None = None
    variant: str | None = None
    color: str | None = None
    sku: str = NOT_UNDERSTANDABLE
    confidence: float = 0.0
    rule_confidence: float = 0.0
    final_confidence: float = 0.0
    corrections: list[str] = Field(default_factory=list)
    parse_stage: str = ""
    ai_used: bool = False
    validation_failed_reason: str = ""


class AIInterpretationResult(BaseModel):
    brand: str = ""
    model: str = ""
    model_code: str = ""
    primary_part: str = ""
    secondary_part: str | None = None
    variant: str | None = None
    color: str | None = None
    corrections: list[str] = Field(default_factory=list)


@dataclass
class StructuredParseExecution:
    parsed: ParsedSKUResult
    source: Literal["rule", "ai", "cache"]
    parser_reason: str
    parse_status: Literal["parsed", "partial", "not_understandable"]
    review_required: bool
    correction_pairs: list[dict[str, str]]
    parse_stage: Literal["rule_only", "rule_normalized", "ai_assisted"]
    ai_used: bool
    validation_failed_reason: str


class StructuredParseError(RuntimeError):
    """Raised when structured parsing cannot produce a valid schema result."""


class StructuredSKUParserService:
    """Rule-first SKU parser with Gemini/OpenAI structured fallback and caching."""

    def __init__(
        self,
        *,
        ai_model: str | None = None,
        ai_threshold: float = 0.40,
        review_threshold: float = 0.75,
        rule_accept_threshold: float = 0.80,
        cache_size: int = 50_000,
        db_path: str | Path = "data/runtime/structured_sku_results.db",
        enable_ai: bool = True,
    ) -> None:
        self.ai_threshold = float(ai_threshold)
        self.rule_accept_threshold = float(rule_accept_threshold)
        self.review_threshold = float(review_threshold)
        self.cache_size = int(cache_size)

        self._cache: OrderedDict[str, ParsedSKUResult] = OrderedDict()
        self._cache_lock = threading.Lock()

        self.db_path = Path(db_path)
        self._init_db()

        requested_provider = str(os.getenv("SKU_AI_PROVIDER", "auto") or "auto").strip().lower()
        self._gemini_api_key = str(os.getenv("GEMINI_API_KEY", "")).strip()
        self._openai_api_key = str(os.getenv("OPENAI_API_KEY", "")).strip()
        self._gemini_base_url = str(
            os.getenv("GEMINI_API_BASE_URL", "https://generativelanguage.googleapis.com/v1beta")
        ).strip().rstrip("/")
        try:
            self._gemini_timeout_seconds = float(os.getenv("GEMINI_TIMEOUT_SECONDS", "20"))
        except ValueError:
            self._gemini_timeout_seconds = 20.0
        self.batch_ai_enabled = str(os.getenv("SKU_AI_BATCH_ENABLED", "false")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        try:
            self._ai_max_concurrent_requests = max(
                1,
                int(os.getenv("SKU_AI_MAX_CONCURRENT_REQUESTS", "2")),
            )
        except ValueError:
            self._ai_max_concurrent_requests = 2
        self._ai_request_semaphore = threading.BoundedSemaphore(self._ai_max_concurrent_requests)

        self._openai_client: Any | None = None
        self._ai_provider: Literal["gemini", "openai", "disabled"] = "disabled"
        if bool(enable_ai):
            if requested_provider in {"off", "none", "disabled"}:
                self._ai_provider = "disabled"
            elif requested_provider == "gemini":
                self._ai_provider = "gemini" if self._gemini_api_key else "disabled"
            elif requested_provider == "openai":
                self._ai_provider = (
                    "openai" if OPENAI_AVAILABLE and self._openai_api_key else "disabled"
                )
            else:
                if self._gemini_api_key:
                    self._ai_provider = "gemini"
                elif OPENAI_AVAILABLE and self._openai_api_key:
                    self._ai_provider = "openai"

        if ai_model:
            self.single_ai_model = ai_model
        elif self._ai_provider == "gemini":
            self.single_ai_model = os.getenv("SKU_AI_SINGLE_MODEL", "gemini-2.5-pro")
        elif self._ai_provider == "openai":
            self.single_ai_model = os.getenv("SKU_AI_SINGLE_MODEL", "gpt-5")
        else:
            self.single_ai_model = os.getenv("SKU_AI_SINGLE_MODEL", "gemini-2.5-pro")
        if self._ai_provider == "gemini":
            self.batch_ai_model = os.getenv("SKU_AI_BATCH_MODEL", "gemini-2.5-flash")
        elif self._ai_provider == "openai":
            self.batch_ai_model = os.getenv("SKU_AI_BATCH_MODEL", self.single_ai_model)
        else:
            self.batch_ai_model = os.getenv("SKU_AI_BATCH_MODEL", "gemini-2.5-flash")
        self.ai_model = self.single_ai_model

        self._ai_enabled = self._ai_provider in {"gemini", "openai"}
        if self._ai_provider == "openai":
            try:
                self._openai_client = OpenAI(api_key=self._openai_api_key)
            except Exception:
                LOGGER.exception("Failed to initialize OpenAI client")
                self._openai_client = None
                self._ai_provider = "disabled"
                self._ai_enabled = False

        self._rule_allowed_codes = self._load_rule_allowed_codes()
        self._rule_engine = sku_parser.get_engine()

    @property
    def ai_enabled(self) -> bool:
        return bool(self._ai_enabled)

    @property
    def ai_provider(self) -> str:
        return self._ai_provider if self._ai_enabled else "disabled"

    @property
    def ai_max_concurrent_requests(self) -> int:
        return int(self._ai_max_concurrent_requests)

    def learning_status(self) -> dict[str, int]:
        def _count_dict(path: Path) -> int:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                return 0
            return len(payload) if isinstance(payload, dict) else 0

        return {
            "approved_pattern_count": _count_dict(APPROVED_LEARNED_PATTERNS_FILE),
            "candidate_pattern_count": _count_dict(LEARNED_PATTERNS_FILE),
            "approved_spelling_count": _count_dict(APPROVED_LEARNED_SPELLING_VARIATIONS_FILE),
            "candidate_spelling_count": _count_dict(LEARNED_SPELLING_VARIATIONS_FILE),
        }

    @staticmethod
    def _normalize_rule_code(value: object) -> str:
        text = str(value or "").upper().strip()
        text = re.sub(r"[^A-Z0-9/\-\s]", " ", text)
        return RE_MULTI_SPACE.sub(" ", text).strip()

    def _load_rule_allowed_codes(self) -> set[str]:
        codes: set[str] = set()
        try:
            engine = sku_parser.get_engine()
            known_codes = getattr(engine, "known_codes", [])
            normalize_code = getattr(engine, "normalize_code", None)
            for code in known_codes:
                normalized = (
                    normalize_code(code) if callable(normalize_code) else self._normalize_rule_code(code)
                )
                if normalized:
                    codes.add(str(normalized).strip())
        except Exception:
            LOGGER.exception("Failed to load known part codes from rule engine")

        # Safety fallback for essential part codes if engine codes are unavailable.
        if not codes:
            codes.update(
                {
                    "BATT",
                    "CP",
                    "CF",
                    "HJ",
                    "FS",
                    "ES",
                    "LS",
                    "ST",
                    "STD",
                    "SR",
                    "SC-R",
                    "BC",
                    "FC",
                    "BCL",
                    "BDR",
                    "BACKDOOR",
                    "VIB",
                    "PS",
                    "MIC",
                    "NFC",
                    "NFC-CF",
                }
            )
        return codes

    def _is_allowed_rule_code(self, code: str) -> bool:
        normalized = self._normalize_rule_code(code)
        if not normalized:
            return False
        return normalized in self._rule_allowed_codes

    @staticmethod
    def normalize_title(value: object) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        text = RE_SEPARATORS.sub(" ", text)
        text = RE_NON_TITLE_TEXT.sub(" ", text)
        text = RE_MULTI_SPACE.sub(" ", text)
        return text.strip()

    def _cache_key(
        self,
        title: str,
        product_sku: str,
        product_web_sku: str,
        product_description: str,
    ) -> str:
        digest_src = (
            f"{title}|{product_sku.strip()}|{product_web_sku.strip()}|"
            f"{product_description.strip()}"
        )
        return hashlib.sha256(digest_src.encode("utf-8")).hexdigest()

    def _cache_get(self, key: str) -> ParsedSKUResult | None:
        with self._cache_lock:
            value = self._cache.get(key)
            if value is None:
                return None
            self._cache.move_to_end(key)
            return value.model_copy(deep=True)

    def _cache_set(self, key: str, parsed: ParsedSKUResult) -> None:
        with self._cache_lock:
            self._cache[key] = parsed.model_copy(deep=True)
            self._cache.move_to_end(key)
            while len(self._cache) > self.cache_size:
                self._cache.popitem(last=False)

    def cache_entry_count(self) -> int:
        with self._cache_lock:
            return len(self._cache)

    def clear_cache(self) -> None:
        with self._cache_lock:
            self._cache.clear()

    def _init_db(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS structured_parse_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    title TEXT NOT NULL,
                    generated_sku TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    corrections TEXT NOT NULL,
                    source TEXT NOT NULL,
                    parser_reason TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def _log_result(
        self,
        *,
        title: str,
        parsed: ParsedSKUResult,
        source: str,
        parser_reason: str,
    ) -> None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO structured_parse_logs
                    (created_at, title, generated_sku, confidence, corrections, source, parser_reason)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                        title,
                        parsed.sku,
                        float(parsed.confidence),
                        json.dumps(parsed.corrections, ensure_ascii=True),
                        source,
                        parser_reason,
                    ),
                )
                conn.commit()
        except Exception:
            LOGGER.exception("Failed to write structured parse log")

    @staticmethod
    def _format_correction_pairs(raw_pairs: list[dict[str, Any]] | Any) -> tuple[list[dict[str, str]], list[str]]:
        if not isinstance(raw_pairs, list):
            return [], []

        pairs: list[dict[str, str]] = []
        corrections: list[str] = []
        for item in raw_pairs:
            if not isinstance(item, dict):
                continue
            from_token = str(item.get("from", "")).strip()
            to_token = str(item.get("to", "")).strip()
            if not from_token and not to_token:
                continue
            pairs.append({"from": from_token, "to": to_token})
            if from_token and to_token:
                corrections.append(f"{from_token}->{to_token}")
            elif to_token:
                corrections.append(to_token)
            else:
                corrections.append(from_token)
        return pairs, corrections

    @staticmethod
    def _normalize_structured_result(parsed: ParsedSKUResult) -> ParsedSKUResult:
        normalized = parsed.model_copy(deep=True)
        normalized.brand = str(normalized.brand or "").upper().strip()
        normalized.model = str(normalized.model or "").upper().strip()
        normalized.model_code = str(normalized.model_code or "").upper().strip()
        normalized.primary_part = str(normalized.primary_part or "").upper().strip()
        if normalized.secondary_part is not None:
            secondary = str(normalized.secondary_part).upper().strip()
            normalized.secondary_part = secondary or None
        if normalized.variant is not None:
            variant = str(normalized.variant or "").upper().strip()
            normalized.variant = variant or None
        if normalized.color is not None:
            color = str(normalized.color or "").upper().strip()
            normalized.color = color or None
        normalized.sku = str(normalized.sku or "").upper().strip() or NOT_UNDERSTANDABLE
        normalized.corrections = [str(item).strip() for item in normalized.corrections if str(item).strip()]
        normalized.parse_stage = str(normalized.parse_stage or "").strip()
        normalized.ai_used = bool(normalized.ai_used)
        normalized.validation_failed_reason = str(normalized.validation_failed_reason or "").strip()
        if len(normalized.sku) > 31:
            normalized.sku = normalized.sku[:31].rstrip()
        for field_name in ("confidence", "rule_confidence", "final_confidence"):
            value = float(getattr(normalized, field_name, 0.0) or 0.0)
            value = min(1.0, max(0.0, value))
            setattr(normalized, field_name, value)
        if normalized.final_confidence == 0.0 and normalized.confidence:
            normalized.final_confidence = normalized.confidence
        if normalized.confidence == 0.0 and normalized.final_confidence:
            normalized.confidence = normalized.final_confidence
        normalized.confidence = normalized.final_confidence
        return normalized

    @staticmethod
    def _confidence_for_rule(raw_confidence: float, parser_reason: str, corrections: list[str]) -> float:
        if raw_confidence > 0:
            return min(1.0, max(0.0, float(raw_confidence)))
        if raw_confidence <= 0:
            return 0.0
        reason = parser_reason.lower()
        fuzzy_markers = ("fuzzy", "dictionary", "phonetic", "vector", "semantic")
        if any(marker in reason for marker in fuzzy_markers) or corrections:
            return 0.72
        return 0.98

    @staticmethod
    def _confidence_for_ai() -> float:
        return 0.35

    @staticmethod
    def derive_parse_status(parsed: ParsedSKUResult) -> Literal["parsed", "partial", "not_understandable"]:
        normalized_sku = str(parsed.sku or "").strip().upper()
        if normalized_sku and normalized_sku != NOT_UNDERSTANDABLE:
            return "parsed"
        for value in (
            parsed.brand,
            parsed.model,
            parsed.model_code,
            parsed.primary_part,
            parsed.secondary_part or "",
        ):
            if str(value or "").strip():
                return "partial"
        return "not_understandable"

    def _run_rule_parser(
        self,
        *,
        title: str,
        product_sku: str,
        product_web_sku: str,
        product_description: str,
    ) -> tuple[ParsedSKUResult, str, list[dict[str, str]]]:
        payload = rule_analyze_title(
            title,
            product_sku_hint=product_sku,
            product_web_sku_hint=product_web_sku,
            product_description_hint=product_description,
        )
        parser_reason = str(payload.get("reason", "rule"))
        correction_pairs, correction_strings = self._format_correction_pairs(payload.get("corrections", []))
        raw_confidence = float(payload.get("confidence", 0.0) or 0.0)
        rule_confidence = self._confidence_for_rule(raw_confidence, parser_reason, correction_strings)

        parsed = ParsedSKUResult(
            brand=str(payload.get("brand", "")),
            model=str(payload.get("model", "")),
            model_code=str(payload.get("model_code", "")),
            primary_part=str(payload.get("part", "")),
            secondary_part=(str(payload.get("secondary_part", "")).strip() or None),
            variant=(str(payload.get("variant", "")).strip() or None),
            color=(str(payload.get("color", "")).strip() or None),
            sku=str(payload.get("sku", "")) or NOT_UNDERSTANDABLE,
            confidence=rule_confidence,
            rule_confidence=rule_confidence,
            final_confidence=rule_confidence,
            corrections=correction_strings,
        )
        return self._normalize_structured_result(parsed), parser_reason, correction_pairs

    def _ai_prompt(self, *, title: str, rule_result: ParsedSKUResult) -> str:
        allowed_codes = sorted(self._rule_allowed_codes, key=len, reverse=True)
        allowed_variants = sorted(set(SKU_VARIANT_TOKENS) | {"HLD"})
        return (
            "You are a rule-driven TX Parts AI title interpreter.\n"
            "The rule parser remains authoritative and will rebuild the final SKU.\n"
            "ABSOLUTE RULES:\n"
            "- never invent abbreviations\n"
            "- use only official rule codes from the allowed list\n"
            "- do not generate or guess a SKU string\n"
            "- the final SKU must be <= 31 characters including spaces after the rule engine rebuilds it\n"
            "- if no rule code is available, set primary_part='UNRESOLVED'\n"
            "- output uppercase values for brand/model/model_code/primary_part/secondary_part/variant/color\n"
            "NORMALIZATION RULES:\n"
            "- collapse part synonyms into one canonical rule code\n"
            "- never repeat duplicate attributes already covered by the rule code\n"
            "- if a flex rule code is used, do not append extra FPC/FLEX/MB-FC style codes\n"
            "Examples:\n"
            "- HOME BUTTON FLEX -> HB-FC\n"
            "- HOME BUTTON FLEX CABLE -> HB-FC\n"
            "- HOME BUTTON FPC -> HB-FC\n"
            "- HOME BUTTON RIBBON CABLE -> HB-FC\n"
            "- invalid 'GALAXY NOTE 20 HB FPC MB-FC' must normalize to 'GALAXY NOTE 20 HB-FC'\n"
            "MODEL NORMALIZATION EXAMPLES:\n"
            "- SAMSUNG GALAXY NOTE 20 -> model='GALAXY NOTE 20'\n"
            "- SAMSUNG NOTE 20 -> model='GALAXY NOTE 20'\n"
            "- GALAXY NOTE20 -> model='GALAXY NOTE 20'\n"
            "Official allowed rule codes:\n"
            f"{', '.join(allowed_codes)}\n"
            "Official allowed variant tokens:\n"
            f"{', '.join(allowed_variants)}\n"
            "Return ONLY valid JSON matching this exact shape:\n"
            '{"brand":"","model":"","model_code":"","primary_part":"","secondary_part":null,'
            '"variant":null,"color":null,"corrections":[]}\n'
            "Field rules:\n"
            "- primary_part must be one allowed rule code OR UNRESOLVED\n"
            "- secondary_part must be null or one allowed rule code\n"
            "- variant must be null or one or more allowed variant tokens separated by spaces\n"
            "- color must be null or a known approved color phrase\n"
            "- corrections entries must look like WRONG->RIGHT\n"
            "- no markdown, no comments, no extra keys\n"
            f"Product title: {title}\n"
            f"Rule parser candidate JSON: {rule_result.model_dump_json()}"
        )

    @staticmethod
    def _is_flex_rule_code(code: str) -> bool:
        normalized = StructuredSKUParserService._normalize_rule_code(code)
        if not normalized:
            return False
        if normalized in FLEX_RULE_CODES:
            return True
        return normalized.endswith("-F") or normalized.endswith("-FC")

    def _normalize_ai_interpretation(self, parsed: AIInterpretationResult) -> AIInterpretationResult:
        normalized = parsed.model_copy(deep=True)
        normalized.brand = str(normalized.brand or "").upper().strip()
        normalized.model = str(normalized.model or "").upper().strip()
        normalized.model_code = str(normalized.model_code or "").upper().strip()
        normalized.primary_part = str(normalized.primary_part or "").upper().strip()
        if normalized.secondary_part is not None:
            normalized.secondary_part = str(normalized.secondary_part or "").upper().strip() or None
        if normalized.variant is not None:
            normalized.variant = str(normalized.variant or "").upper().strip() or None
        if normalized.color is not None:
            normalized.color = str(normalized.color or "").upper().strip() or None
        normalized.corrections = [str(item).strip() for item in normalized.corrections if str(item).strip()]
        return normalized

    def _normalize_variant_value(self, value: str) -> str:
        allowed_tokens = set(SKU_VARIANT_TOKENS) | {"HLD"}
        tokens = [
            token
            for token in self._normalize_rule_code(value).split()
            if token in allowed_tokens
        ]
        return " ".join(tokens)

    def _validate_ai_color(self, color_value: str, *, brand: str) -> str:
        if not color_value:
            return ""
        engine = self._rule_engine or sku_parser.get_engine()
        normalized = self.normalize_title(color_value)
        if not normalized:
            return ""
        pixel_brand = str(brand or "").upper().strip() == "PIXEL"
        return str(engine._detect_color(normalized, compress=not pixel_brand) or "").upper().strip()

    def _validate_ai_brand_model(
        self,
        *,
        brand: str,
        model: str,
        model_code: str,
        rule_result: ParsedSKUResult,
    ) -> tuple[str, str, str] | None:
        engine = self._rule_engine or sku_parser.get_engine()
        brand_norm = str(brand or "").upper().strip()
        model_norm = str(model or "").upper().strip()
        if not brand_norm or not model_norm:
            return None
        dataset_brand, dataset_model, dataset_model_code = engine._detect_brand_model_from_dataset(
            self.normalize_title(" ".join(part for part in (brand_norm, model_norm, model_code) if part))
        )
        if dataset_brand and dataset_model:
            resolved_code = str(model_code or dataset_model_code or rule_result.model_code or "").upper().strip()
            return dataset_brand, dataset_model, resolved_code
        if (
            brand_norm == str(rule_result.brand or "").upper().strip()
            and model_norm == str(rule_result.model or "").upper().strip()
        ):
            resolved_code = str(model_code or rule_result.model_code or "").upper().strip()
            return brand_norm, model_norm, resolved_code
        return None

    def _validate_ai_result(
        self,
        parsed: AIInterpretationResult,
        *,
        title: str,
        rule_result: ParsedSKUResult,
    ) -> ParsedSKUResult | None:
        normalized = self._normalize_ai_interpretation(parsed)
        engine = self._rule_engine or sku_parser.get_engine()

        brand = normalized.brand or str(rule_result.brand or "").upper().strip()
        model = normalized.model or str(rule_result.model or "").upper().strip()
        model_code = normalized.model_code or str(rule_result.model_code or "").upper().strip()
        primary_norm = self._normalize_rule_code(normalized.primary_part or rule_result.primary_part)
        secondary_norm = self._normalize_rule_code(
            normalized.secondary_part
            if normalized.secondary_part is not None
            else (rule_result.secondary_part or "")
        )

        if primary_norm in {"", UNRESOLVED_PART}:
            return None
        if not self._is_allowed_rule_code(primary_norm):
            LOGGER.warning("Rejecting AI result due to non-rule primary_part code: %s", primary_norm)
            return None
        if secondary_norm and not self._is_allowed_rule_code(secondary_norm):
            LOGGER.warning("Rejecting AI result due to non-rule secondary_part code: %s", secondary_norm)
            return None
        if secondary_norm == primary_norm:
            secondary_norm = ""
        elif self._is_flex_rule_code(primary_norm) and self._is_flex_rule_code(secondary_norm):
            secondary_norm = ""

        validated_brand_model = self._validate_ai_brand_model(
            brand=brand,
            model=model,
            model_code=model_code,
            rule_result=rule_result,
        )
        if validated_brand_model is None:
            LOGGER.warning("Rejecting AI result because brand/model did not validate against the dataset")
            return None
        brand, model, model_code = validated_brand_model

        variant = self._normalize_variant_value(normalized.variant or "")
        color = self._validate_ai_color(normalized.color or "", brand=brand)

        normalized_title = engine.normalize_text(title)
        part_code = " ".join(token for token in (primary_norm, secondary_norm) if token).strip()
        part_code = engine._apply_backdoor_attributes(part_code, normalized_title)
        part_code = engine._apply_contextual_part_code_rules(part_code, brand, normalized_title)
        part_code = engine._apply_sim_tray_mode(part_code, normalized_title)
        part_code = engine._apply_pixel_part_overrides(part_code, brand, normalized_title)
        primary_part, secondary_part = engine._split_primary_secondary_part(part_code)
        rebuilt_sku = engine._build_sku(brand, model, model_code, part_code, variant, color)
        if not rebuilt_sku or rebuilt_sku == NOT_UNDERSTANDABLE or len(rebuilt_sku) > 31:
            LOGGER.warning(
                "Rejecting AI result because structured fields could not produce a valid SKU: %s",
                normalized.model_dump_json(),
            )
            return None

        final_confidence = max(self._confidence_for_ai(), min(0.60, float(rule_result.rule_confidence or rule_result.confidence or 0.0) + 0.10))
        return self._normalize_structured_result(
            ParsedSKUResult(
                brand=brand,
                model=model,
                model_code=model_code,
                primary_part=primary_part,
                secondary_part=secondary_part or None,
                variant=variant or None,
                color=color or None,
                sku=rebuilt_sku,
                confidence=final_confidence,
                rule_confidence=float(rule_result.rule_confidence or rule_result.confidence or 0.0),
                final_confidence=final_confidence,
                corrections=list(dict.fromkeys([*rule_result.corrections, *normalized.corrections])),
            )
        )

    @staticmethod
    def _strip_json_wrappers(text: str) -> str:
        content = (text or "").strip()
        if content.startswith("```"):
            content = re.sub(r"^```(?:json)?\s*", "", content, flags=re.IGNORECASE)
            content = re.sub(r"\s*```$", "", content).strip()
        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end != -1 and end > start:
            return content[start : end + 1]
        return content

    def _gemini_request_url(self, model_name: str) -> str:
        clean_model = str(model_name or "").strip()
        if clean_model.startswith("models/"):
            clean_model = clean_model.split("/", 1)[1]
        if not clean_model:
            clean_model = "gemini-2.5-flash"
        model_path = urllib.parse.quote(clean_model, safe="")
        key = urllib.parse.quote(self._gemini_api_key, safe="")
        return f"{self._gemini_base_url}/models/{model_path}:generateContent?key={key}"

    def _ssl_context(self) -> ssl.SSLContext:
        if CERTIFI_AVAILABLE:
            return ssl.create_default_context(cafile=certifi.where())
        return ssl.create_default_context()

    def _parse_with_gemini_once(
        self,
        *,
        title: str,
        rule_result: ParsedSKUResult,
        model_name: str,
    ) -> ParsedSKUResult | None:
        if not self._gemini_api_key:
            return None

        prompt = self._ai_prompt(title=title, rule_result=rule_result)
        request_payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature": 0,
                "responseMimeType": "application/json",
            },
        }
        encoded = json.dumps(request_payload).encode("utf-8")
        model_candidates = [model_name]
        if "gemini-2.5-flash" not in model_candidates:
            model_candidates.append("gemini-2.5-flash")

        for model_name in model_candidates:
            req = urllib.request.Request(
                self._gemini_request_url(model_name),
                data=encoded,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(
                    req,
                    timeout=self._gemini_timeout_seconds,
                    context=self._ssl_context(),
                ) as response:
                    response_text = response.read().decode("utf-8", errors="replace")
            except urllib.error.HTTPError as exc:
                try:
                    error_body = exc.read().decode("utf-8", errors="replace")
                except Exception:
                    error_body = ""
                LOGGER.warning(
                    "Gemini structured parse request failed (model=%s, status=%s): %s",
                    model_name,
                    exc.code,
                    error_body[:220],
                )
                continue
            except Exception:
                LOGGER.exception("Gemini structured parse request failed (model=%s)", model_name)
                continue

            try:
                response_json = json.loads(response_text)
            except Exception:
                LOGGER.warning("Gemini returned non-JSON response payload")
                continue

            text_candidates: list[str] = []
            for candidate in response_json.get("candidates", []) or []:
                content = candidate.get("content", {}) if isinstance(candidate, dict) else {}
                for part in content.get("parts", []) or []:
                    if isinstance(part, dict) and isinstance(part.get("text"), str):
                        text_candidates.append(part["text"])

            for candidate_text in text_candidates:
                normalized_json_text = self._strip_json_wrappers(candidate_text)
                if not normalized_json_text:
                    continue
                try:
                    candidate_payload = json.loads(normalized_json_text)
                except Exception:
                    continue
                try:
                    parsed = AIInterpretationResult.model_validate(candidate_payload)
                except ValidationError:
                    continue
                validated = self._validate_ai_result(parsed, title=title, rule_result=rule_result)
                if validated is not None:
                    return validated

        return None

    def _parse_with_openai_once(
        self,
        *,
        title: str,
        rule_result: ParsedSKUResult,
        model_name: str,
    ) -> ParsedSKUResult | None:
        if self._openai_client is None:
            return None

        try:
            response = self._openai_client.responses.parse(
                model=model_name,
                input=self._ai_prompt(title=title, rule_result=rule_result),
                response_format=AIInterpretationResult,
            )
        except Exception:
            LOGGER.exception("OpenAI structured parse request failed")
            return None

        candidate = getattr(response, "output_parsed", None)
        if candidate is None:
            output_text = getattr(response, "output_text", "")
            if output_text:
                try:
                    candidate = json.loads(output_text)
                except Exception:
                    candidate = None

        if candidate is None:
            return None

        try:
            parsed = (
                candidate
                if isinstance(candidate, AIInterpretationResult)
                else AIInterpretationResult.model_validate(candidate)
            )
        except ValidationError:
            LOGGER.exception("OpenAI output failed AIInterpretationResult validation")
            return None

        return self._validate_ai_result(parsed, title=title, rule_result=rule_result)

    def _parse_with_ai_once(
        self,
        *,
        title: str,
        rule_result: ParsedSKUResult,
        mode: Literal["single", "batch"],
    ) -> ParsedSKUResult | None:
        if not self._ai_enabled:
            return None
        acquired = self._ai_request_semaphore.acquire(timeout=0.5)
        if not acquired:
            LOGGER.warning("Skipping AI fallback because the AI concurrency limit is saturated")
            return None
        try:
            model_name = self.single_ai_model if mode == "single" else self.batch_ai_model
            if self._ai_provider == "gemini":
                return self._parse_with_gemini_once(
                    title=title,
                    rule_result=rule_result,
                    model_name=model_name,
                )
            if self._ai_provider == "openai":
                return self._parse_with_openai_once(
                    title=title,
                    rule_result=rule_result,
                    model_name=model_name,
                )
            return None
        finally:
            self._ai_request_semaphore.release()

    def _parse_with_ai_retry(
        self,
        *,
        title: str,
        rule_result: ParsedSKUResult,
        mode: Literal["single", "batch"],
    ) -> ParsedSKUResult | None:
        first = self._parse_with_ai_once(title=title, rule_result=rule_result, mode=mode)
        if first is not None:
            return first
        return self._parse_with_ai_once(title=title, rule_result=rule_result, mode=mode)

    def _should_use_ai_fallback(
        self,
        *,
        title: str,
        rule_result: ParsedSKUResult,
        allow_ai: bool,
        mode: Literal["single", "batch"],
        parser_reason: str,
    ) -> bool:
        if not allow_ai or not self._ai_enabled:
            return False
        rule_confidence = float(rule_result.rule_confidence or rule_result.confidence or 0.0)
        if rule_confidence >= self.ai_threshold:
            return False
        parse_status = self.derive_parse_status(rule_result)
        normalized_title = self.normalize_title(title)
        if (
            parse_status == "partial"
            and str(rule_result.model or "").strip()
            and not str(rule_result.primary_part or "").strip()
            and normalized_title
        ):
            # Model-only titles should remain partial rather than letting AI guess a part.
            return False
        if mode == "batch" and not self.batch_ai_enabled:
            return False
        if self._is_simple_rule_result(rule_result=rule_result, parser_reason=parser_reason):
            return False
        return True

    def _is_simple_rule_result(self, *, rule_result: ParsedSKUResult, parser_reason: str) -> bool:
        if self.derive_parse_status(rule_result) != "parsed":
            return False
        if not str(rule_result.brand or "").strip():
            return False
        if not str(rule_result.model or "").strip():
            return False
        if not str(rule_result.primary_part or "").strip():
            return False
        if str(rule_result.secondary_part or "").strip():
            return False
        lowered_reason = str(parser_reason or "").lower()
        if any(marker in lowered_reason for marker in ("fuzzy", "vector", "semantic", "catalog_title_fuzzy", "unresolved")):
            return False
        return True

    def _derive_parse_stage(
        self,
        *,
        source: Literal["rule", "ai", "cache"],
        rule_result: ParsedSKUResult,
        final_result: ParsedSKUResult,
    ) -> Literal["rule_only", "rule_normalized", "ai_assisted"]:
        if source == "ai":
            return "ai_assisted"
        rule_confidence = float(rule_result.rule_confidence or rule_result.confidence or 0.0)
        if rule_confidence >= self.rule_accept_threshold:
            return "rule_only"
        return "rule_normalized"

    def analyze_title(
        self,
        *,
        title: str,
        product_sku: str = "",
        product_web_sku: str = "",
        product_description: str = "",
        allow_ai: bool = True,
        mode: Literal["single", "batch"] = "single",
    ) -> StructuredParseExecution:
        normalized_title = self.normalize_title(title)
        normalized_description = self.normalize_title(product_description)
        if (
            not normalized_title
            and not product_sku.strip()
            and not product_web_sku.strip()
            and not normalized_description
        ):
            raise ValueError("Provide at least a title or SKU hint.")

        cache_key = self._cache_key(
            normalized_title,
            product_sku,
            product_web_sku,
            normalized_description,
        )
        cached = self._cache_get(cache_key)
        if cached is not None:
            parse_status = self.derive_parse_status(cached)
            return StructuredParseExecution(
                parsed=cached,
                source="cache",
                parser_reason="cache_hit",
                parse_status=parse_status,
                review_required=float(cached.confidence) < self.review_threshold,
                correction_pairs=[],
                parse_stage=(cached.parse_stage or "rule_only"),  # type: ignore[arg-type]
                ai_used=bool(cached.ai_used),
                validation_failed_reason=str(cached.validation_failed_reason or ""),
            )

        rule_result, parser_reason, correction_pairs = self._run_rule_parser(
            title=normalized_title,
            product_sku=product_sku,
            product_web_sku=product_web_sku,
            product_description=normalized_description,
        )

        source: Literal["rule", "ai", "cache"] = "rule"
        final_result = rule_result
        validation_failed_reason = ""

        if self._should_use_ai_fallback(
            title=normalized_title,
            rule_result=rule_result,
            allow_ai=allow_ai,
            mode=mode,
            parser_reason=parser_reason,
        ):
            ai_result = self._parse_with_ai_retry(
                title=normalized_title,
                rule_result=rule_result,
                mode=mode,
            )
            if ai_result is not None:
                final_result = ai_result
                source = "ai"
                parser_reason = "ai_structured_inference"
                correction_pairs = []
            else:
                parser_reason = f"{parser_reason}+ai_fallback_missed"
                validation_failed_reason = "ai_validation_failed"

        parse_status = self.derive_parse_status(final_result)
        if parse_status != "parsed" and not str(final_result.primary_part or "").strip():
            final_result.variant = None
            final_result.color = None
        parse_stage = self._derive_parse_stage(
            source=source,
            rule_result=rule_result,
            final_result=final_result,
        )
        final_result.rule_confidence = float(rule_result.rule_confidence or rule_result.confidence or 0.0)
        if source == "ai":
            final_result.final_confidence = float(final_result.final_confidence or final_result.confidence or 0.0)
        else:
            final_result.final_confidence = float(rule_result.rule_confidence or rule_result.confidence or 0.0)
        final_result.confidence = float(final_result.final_confidence)
        final_result.parse_stage = parse_stage
        final_result.ai_used = source == "ai"
        final_result.validation_failed_reason = validation_failed_reason

        self._cache_set(cache_key, final_result)
        self._log_result(
            title=title,
            parsed=final_result,
            source=source,
            parser_reason=parser_reason,
        )

        return StructuredParseExecution(
            parsed=final_result,
            source=source,
            parser_reason=parser_reason,
            parse_status=parse_status,
            review_required=float(final_result.confidence) < self.review_threshold,
            correction_pairs=correction_pairs,
            parse_stage=parse_stage,
            ai_used=source == "ai",
            validation_failed_reason=validation_failed_reason,
        )

    def process_inventory_excel(
        self,
        *,
        input_file: str | Path,
        output_file: str | Path,
        title_column: str = "Product Name",
    ) -> pd.DataFrame:
        input_path = Path(input_file)
        output_path = Path(output_file)

        if input_path.suffix.lower() == ".csv":
            df = pd.read_csv(input_path)
        else:
            df = pd.read_excel(input_path, engine="openpyxl")

        if title_column not in df.columns:
            raise ValueError(f"Missing title column: {title_column}")

        product_sku_col = "Product SKU" if "Product SKU" in df.columns else ""
        web_sku_col = "Product Web SKU" if "Product Web SKU" in df.columns else ""
        description_col = ""
        for candidate in ("Product Description", "Description"):
            if candidate in df.columns:
                description_col = candidate
                break

        rows: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            title = str(row.get(title_column, "") or "")
            product_sku = str(row.get(product_sku_col, "") or "") if product_sku_col else ""
            product_web_sku = str(row.get(web_sku_col, "") or "") if web_sku_col else ""
            product_description = str(row.get(description_col, "") or "") if description_col else ""

            try:
                execution = self.analyze_title(
                    title=title,
                    product_sku=product_sku,
                    product_web_sku=product_web_sku,
                    product_description=product_description,
                    allow_ai=self.batch_ai_enabled,
                    mode="batch",
                )
                parsed = execution.parsed
                parse_status = execution.parse_status
                parser_reason = execution.parser_reason
                source = execution.source
                error = ""
            except StructuredParseError:
                parsed = ParsedSKUResult(
                    sku=NOT_UNDERSTANDABLE,
                    confidence=0.0,
                    corrections=[],
                )
                parse_status = "not_understandable"
                parser_reason = "ai_structured_parse_failed"
                source = "rule"
                error = "Unable to parse title"

            rows.append(
                {
                    "Parsed Brand": parsed.brand,
                    "Parsed Model": parsed.model,
                    "Parsed Model Code": parsed.model_code,
                    "Parsed Primary Part": parsed.primary_part,
                    "Parsed Secondary Part": parsed.secondary_part or "",
                    "Parsed Variant": parsed.variant or "",
                    "Parsed Color": parsed.color or "",
                    "Generated SKU": parsed.sku,
                    "Rule Confidence": float(parsed.rule_confidence),
                    "Final Confidence": float(parsed.final_confidence or parsed.confidence),
                    "Confidence": float(parsed.final_confidence or parsed.confidence),
                    "Corrections": " | ".join(parsed.corrections),
                    "Parser Source": source,
                    "Parser Reason": parser_reason,
                    "Parse Stage": execution.parse_stage,
                    "AI Used": "YES" if execution.ai_used else "",
                    "Validation Failed Reason": execution.validation_failed_reason,
                    "Parse Status": parse_status,
                    "Review Required": "YES" if float(parsed.confidence) < self.review_threshold else "",
                    "Parse Error": error,
                }
            )

        result_df = pd.concat([df.reset_index(drop=True), pd.DataFrame(rows)], axis=1)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result_df.to_excel(output_path, index=False, engine="openpyxl")
        return result_df


def process_inventory_excel(
    input_file: str | Path,
    output_file: str | Path,
    *,
    title_column: str = "Product Name",
    service: StructuredSKUParserService | None = None,
) -> pd.DataFrame:
    """Batch helper for processing Excel/CSV inventory with structured outputs."""
    parser_service = service or StructuredSKUParserService()
    return parser_service.process_inventory_excel(
        input_file=input_file,
        output_file=output_file,
        title_column=title_column,
    )
