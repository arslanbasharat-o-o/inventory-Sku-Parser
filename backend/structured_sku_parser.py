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
    """Rule-first SKU parser with Gemini/OpenAI structured fallback and caching."""

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
        self.ai_threshold = float(ai_threshold)
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
            self.ai_model = ai_model
        elif self._ai_provider == "gemini":
            self.ai_model = os.getenv("GEMINI_STRUCTURED_MODEL", "gemini-2.5-flash")
        elif self._ai_provider == "openai":
            self.ai_model = os.getenv("OPENAI_STRUCTURED_MODEL", "gpt-5")
        else:
            self.ai_model = os.getenv("GEMINI_STRUCTURED_MODEL", "gemini-2.5-flash")

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

    @property
    def ai_enabled(self) -> bool:
        return bool(self._ai_enabled)

    @property
    def ai_provider(self) -> str:
        return self._ai_provider if self._ai_enabled else "disabled"

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
        allowed_codes = sorted(self._rule_allowed_codes, key=len, reverse=True)
        return (
            "You are a rule-driven TX Parts SKU parser.\n"
            "ABSOLUTE RULE: Never invent abbreviations. Use only codes from the official rule list.\n"
            "If no rule code is available, set primary_part='UNRESOLVED' and sku='"
            f"{NOT_UNDERSTANDABLE}'.\n"
            "SKU hard limit: 31 characters including spaces.\n"
            "Use uppercase for brand/model/model_code/primary_part/secondary_part/sku.\n"
            "Official allowed rule codes:\n"
            f"{', '.join(allowed_codes)}\n"
            "Return ONLY valid JSON matching this exact shape:\n"
            '{"brand":"","model":"","model_code":"","primary_part":"","secondary_part":null,'
            '"sku":"","confidence":0.0,"corrections":[]}\n'
            "Field rules:\n"
            "- primary_part must be one allowed rule code OR UNRESOLVED\n"
            "- secondary_part must be null or one allowed rule code\n"
            "- corrections entries must look like WRONG->RIGHT\n"
            "- confidence must be 0..1\n"
            f"- unknown/ambiguous titles must return sku='{NOT_UNDERSTANDABLE}'\n"
            "- no markdown, no comments, no extra keys\n"
            f"Product title: {title}\n"
            f"Rule parser candidate JSON: {rule_result.model_dump_json()}"
        )

    def _validate_ai_result(self, parsed: ParsedSKUResult) -> ParsedSKUResult | None:
        raw_sku = str(parsed.sku or "").upper().strip()
        if raw_sku and raw_sku != NOT_UNDERSTANDABLE and len(raw_sku) > 31:
            LOGGER.warning("Rejecting AI result due to SKU length > 31: %s", raw_sku)
            return None

        normalized = self._normalize_structured_result(parsed)
        primary_norm = self._normalize_rule_code(normalized.primary_part)

        if primary_norm in {"", UNRESOLVED_PART}:
            normalized.primary_part = UNRESOLVED_PART
            normalized.secondary_part = None
            normalized.sku = NOT_UNDERSTANDABLE
            normalized.confidence = min(float(normalized.confidence or 0.0), 0.55)
            return normalized

        if not self._is_allowed_rule_code(normalized.primary_part):
            LOGGER.warning(
                "Rejecting AI result due to non-rule primary_part code: %s",
                normalized.primary_part,
            )
            return None

        if normalized.secondary_part and not self._is_allowed_rule_code(normalized.secondary_part):
            LOGGER.warning(
                "Rejecting AI result due to non-rule secondary_part code: %s",
                normalized.secondary_part,
            )
            return None

        if normalized.sku != NOT_UNDERSTANDABLE and not normalized.primary_part:
            LOGGER.warning("Rejecting AI result due to missing primary_part with non-empty SKU")
            return None

        normalized.confidence = self._confidence_for_ai()
        return normalized

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

    def _parse_with_gemini_once(self, *, title: str, rule_result: ParsedSKUResult) -> ParsedSKUResult | None:
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
        model_candidates = [self.ai_model]
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
                    parsed = ParsedSKUResult.model_validate(candidate_payload)
                except ValidationError:
                    continue
                validated = self._validate_ai_result(parsed)
                if validated is not None:
                    return validated

        return None

    def _parse_with_openai_once(self, *, title: str, rule_result: ParsedSKUResult) -> ParsedSKUResult | None:
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

        return self._validate_ai_result(parsed)

    def _parse_with_ai_once(self, *, title: str, rule_result: ParsedSKUResult) -> ParsedSKUResult | None:
        if not self._ai_enabled:
            return None
        if self._ai_provider == "gemini":
            return self._parse_with_gemini_once(title=title, rule_result=rule_result)
        if self._ai_provider == "openai":
            return self._parse_with_openai_once(title=title, rule_result=rule_result)
        return None

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
