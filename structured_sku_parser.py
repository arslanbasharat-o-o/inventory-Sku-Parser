#!/usr/bin/env python3
"""Structured SKU parsing service with OpenAI Responses API fallback."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from pydantic import BaseModel, Field, ValidationError

from sku_parser import NOT_UNDERSTANDABLE, analyze_title as rule_analyze_title

try:
    from openai import OpenAI  # type: ignore

    OPENAI_AVAILABLE = True
except Exception:  # pragma: no cover - optional dependency
    OpenAI = None  # type: ignore[assignment]
    OPENAI_AVAILABLE = False


LOGGER = logging.getLogger(__name__)

RE_MULTI_SPACE = re.compile(r"\s+")
RE_SEPARATORS = re.compile(r"[-_/]+")
RE_NON_TITLE_TEXT = re.compile(r"[^A-Za-z0-9\s+]")


class ParsedSKUResult(BaseModel):
    brand: str = ""
    model: str = ""
    model_code: str = ""
    primary_part: str = ""
    secondary_part: str | None = None
    sku: str = NOT_UNDERSTANDABLE
    confidence: float = 0.0
    corrections: list[str] = Field(default_factory=list)


@dataclass
class StructuredParseExecution:
    parsed: ParsedSKUResult
    source: Literal["rule", "ai", "cache"]
    parser_reason: str
    parse_status: Literal["parsed", "not_understandable"]
    review_required: bool
    correction_pairs: list[dict[str, str]]


class StructuredParseError(RuntimeError):
    """Raised when structured parsing cannot produce a valid schema result."""


class StructuredSKUParserService:
    """Rule-first SKU parser with OpenAI structured fallback and caching."""

    def __init__(
        self,
        *,
        ai_model: str | None = None,
        ai_threshold: float = 0.85,
        review_threshold: float = 0.75,
        cache_size: int = 50_000,
        db_path: str | Path = "outputs/structured_sku_results.db",
        enable_ai: bool = True,
    ) -> None:
        self.ai_model = ai_model or os.getenv("OPENAI_STRUCTURED_MODEL", "gpt-5")
        self.ai_threshold = float(ai_threshold)
        self.review_threshold = float(review_threshold)
        self.cache_size = int(cache_size)

        self._cache: OrderedDict[str, ParsedSKUResult] = OrderedDict()
        self._cache_lock = threading.Lock()

        self.db_path = Path(db_path)
        self._init_db()

        self._openai_client: Any | None = None
        self._ai_enabled = bool(enable_ai)
        if self._ai_enabled and OPENAI_AVAILABLE and os.getenv("OPENAI_API_KEY"):
            try:
                self._openai_client = OpenAI()
            except Exception:
                LOGGER.exception("Failed to initialize OpenAI client")
                self._openai_client = None

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
        normalized.sku = str(normalized.sku or "").upper().strip() or NOT_UNDERSTANDABLE
        normalized.corrections = [str(item).strip() for item in normalized.corrections if str(item).strip()]
        if len(normalized.sku) > 31:
            normalized.sku = normalized.sku[:31].rstrip()
        if normalized.confidence < 0:
            normalized.confidence = 0.0
        if normalized.confidence > 1:
            normalized.confidence = 1.0
        return normalized

    @staticmethod
    def _confidence_for_rule(parser_reason: str, corrections: list[str]) -> float:
        reason = parser_reason.lower()
        fuzzy_markers = ("fuzzy", "dictionary", "phonetic", "vector", "semantic")
        if corrections or any(marker in reason for marker in fuzzy_markers):
            return 0.90
        return 0.98

    @staticmethod
    def _confidence_for_ai() -> float:
        return 0.80

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

        parsed = ParsedSKUResult(
            brand=str(payload.get("brand", "")),
            model=str(payload.get("model", "")),
            model_code=str(payload.get("model_code", "")),
            primary_part=str(payload.get("part", "")),
            secondary_part=(str(payload.get("secondary_part", "")).strip() or None),
            sku=str(payload.get("sku", "")) or NOT_UNDERSTANDABLE,
            confidence=self._confidence_for_rule(parser_reason, correction_strings),
            corrections=correction_strings,
        )
        return self._normalize_structured_result(parsed), parser_reason, correction_pairs

    def _ai_prompt(self, *, title: str, rule_result: ParsedSKUResult) -> str:
        return (
            "Parse this mobile repair part title and generate SKU data. "
            "Return strict schema-compatible values only. "
            "Use uppercase for brand/model/model_code/part codes, and keep SKU length <= 31 characters.\n"
            f"Title: {title}\n"
            f"Rule parser candidate: {rule_result.model_dump_json()}\n"
            "Field rules:\n"
            "- primary_part must be the main component code\n"
            "- secondary_part can be null\n"
            "- corrections must be short strings such as 'battry->battery'\n"
            f"- unknown titles should return sku='{NOT_UNDERSTANDABLE}'"
        )

    def _parse_with_ai_once(self, *, title: str, rule_result: ParsedSKUResult) -> ParsedSKUResult | None:
        if self._openai_client is None:
            return None

        try:
            response = self._openai_client.responses.parse(
                model=self.ai_model,
                input=self._ai_prompt(title=title, rule_result=rule_result),
                response_format=ParsedSKUResult,
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
            parsed = candidate if isinstance(candidate, ParsedSKUResult) else ParsedSKUResult.model_validate(candidate)
        except ValidationError:
            LOGGER.exception("OpenAI output failed ParsedSKUResult validation")
            return None

        parsed = self._normalize_structured_result(parsed)
        parsed.confidence = self._confidence_for_ai()
        return parsed

    def _parse_with_ai_retry(self, *, title: str, rule_result: ParsedSKUResult) -> ParsedSKUResult | None:
        first = self._parse_with_ai_once(title=title, rule_result=rule_result)
        if first is not None:
            return first
        return self._parse_with_ai_once(title=title, rule_result=rule_result)

    def analyze_title(
        self,
        *,
        title: str,
        product_sku: str = "",
        product_web_sku: str = "",
        product_description: str = "",
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
            parse_status: Literal["parsed", "not_understandable"] = (
                "parsed" if cached.sku != NOT_UNDERSTANDABLE else "not_understandable"
            )
            return StructuredParseExecution(
                parsed=cached,
                source="cache",
                parser_reason="cache_hit",
                parse_status=parse_status,
                review_required=float(cached.confidence) < self.review_threshold,
                correction_pairs=[],
            )

        rule_result, parser_reason, correction_pairs = self._run_rule_parser(
            title=normalized_title,
            product_sku=product_sku,
            product_web_sku=product_web_sku,
            product_description=normalized_description,
        )

        source: Literal["rule", "ai", "cache"] = "rule"
        final_result = rule_result

        if rule_result.confidence < self.ai_threshold:
            ai_result = self._parse_with_ai_retry(title=normalized_title, rule_result=rule_result)
            if ai_result is None:
                raise StructuredParseError("Unable to parse title")
            final_result = ai_result
            source = "ai"
            parser_reason = "ai_structured_inference"
            correction_pairs = []

        parse_status: Literal["parsed", "not_understandable"] = (
            "parsed" if final_result.sku != NOT_UNDERSTANDABLE else "not_understandable"
        )

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
                    "Generated SKU": parsed.sku,
                    "Confidence": float(parsed.confidence),
                    "Corrections": " | ".join(parsed.corrections),
                    "Parser Source": source,
                    "Parser Reason": parser_reason,
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
