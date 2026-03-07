#!/usr/bin/env python3
"""Deprecated compatibility wrapper for structured SKU parsing.

Public entry points delegate to backend.structured_sku_parser so the
rule engine stays authoritative and AI only assists interpretation.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Optional OpenAI import — graceful fallback when key / package absent
# ---------------------------------------------------------------------------
try:
    from openai import OpenAI  # type: ignore[import]

    _OPENAI_AVAILABLE = True
except ImportError:  # pragma: no cover
    _OPENAI_AVAILABLE = False
    OpenAI = None  # type: ignore[assignment,misc]

# ---------------------------------------------------------------------------
# Internal rule-based engine
# ---------------------------------------------------------------------------
from .sku_parser import NOT_UNDERSTANDABLE, analyze_title as _rule_analyze
from .structured_sku_parser import StructuredSKUParserService

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AI_FALLBACK_THRESHOLD: float = float(os.getenv("SKU_AI_THRESHOLD", "0.85"))
AI_MODEL: str = os.getenv("SKU_AI_MODEL", "gpt-4o")
AI_MAX_RETRIES: int = 2
REVIEW_FLAG_THRESHOLD: float = 0.75

# Confidence constants
CONFIDENCE_EXACT_RULE = 0.98
CONFIDENCE_FUZZY = 0.90
CONFIDENCE_AI = 0.80

# ---------------------------------------------------------------------------
# Structured Output Schema
# ---------------------------------------------------------------------------


class CorrectionItem(BaseModel):
    """A single spelling correction applied to the title."""

    from_token: str = Field(alias="from")
    to_token: str = Field(alias="to")

    model_config = {"populate_by_name": True}


class ParsedSKUResult(BaseModel):
    """Strict structured output schema for a parsed mobile-parts SKU.

    This schema is enforced by the OpenAI Responses API — the model will
    always return valid JSON matching this structure.
    """

    brand: str = Field(description="Manufacturer brand code, e.g. SAMSUNG, APPLE, GOOGLE")
    model: str = Field(description="Device model name, e.g. GALAXY A52")
    model_code: str = Field(description="Internal model code, e.g. A525")
    primary_part: str = Field(description="Part abbreviation code, e.g. CP, BATT, BC")
    secondary_part: Optional[str] = Field(
        default=None,
        description="Secondary part code if present, e.g. HJ for Headphone Jack",
    )
    sku: str = Field(description="Full generated SKU string, e.g. GALAXY A52 A525 CP HJ")
    confidence: float = Field(ge=0.0, le=1.0, description="Parser confidence 0-1")
    corrections: list[str] = Field(
        default_factory=list,
        description="List of spelling corrections applied, e.g. ['battry→battery']",
    )
    needs_review: bool = Field(
        default=False,
        description="True when confidence < 0.75 — flag row for manual review",
    )
    source: str = Field(
        default="rule",
        description="Parser source: 'rule', 'rule_fuzzy', 'ai', or 'fallback'",
    )


# ---------------------------------------------------------------------------
# In-memory title cache
# ---------------------------------------------------------------------------

_TITLE_CACHE: dict[str, ParsedSKUResult] = {}
MAX_CACHE_SIZE = 10_000


def _cache_key(title: str) -> str:
    return hashlib.sha256(title.strip().lower().encode()).hexdigest()


def _cache_get(title: str) -> ParsedSKUResult | None:
    return _TITLE_CACHE.get(_cache_key(title))


def _cache_set(title: str, result: ParsedSKUResult) -> None:
    if len(_TITLE_CACHE) >= MAX_CACHE_SIZE:
        # Evict the oldest 10 % of entries (simple FIFO eviction)
        evict = list(_TITLE_CACHE.keys())[: MAX_CACHE_SIZE // 10]
        for k in evict:
            _TITLE_CACHE.pop(k, None)
    _TITLE_CACHE[_cache_key(title)] = result


def clear_cache() -> None:
    """Clear the in-memory title cache."""
    _TITLE_CACHE.clear()


# ---------------------------------------------------------------------------
# OpenAI client (lazy singleton)
# ---------------------------------------------------------------------------

_client: OpenAI | None = None
_structured_service: StructuredSKUParserService | None = None


def _get_client() -> OpenAI | None:
    global _client
    if not _OPENAI_AVAILABLE:
        return None
    if _client is None:
        api_key = os.getenv("OPENAI_API_KEY", "")
        if not api_key:
            logger.warning(
                "OPENAI_API_KEY is not set. AI fallback disabled; "
                "rule-based parser will be used exclusively."
            )
            return None
        _client = OpenAI(api_key=api_key)
    return _client


def _get_structured_service() -> StructuredSKUParserService:
    global _structured_service
    if _structured_service is None:
        _structured_service = StructuredSKUParserService(enable_ai=True)
    return _structured_service


# ---------------------------------------------------------------------------
# AI structured parser
# ---------------------------------------------------------------------------

_AI_SYSTEM_PROMPT = """You are a rule-driven mobile phone parts SKU parser.
You must never invent abbreviations.
Use only official SKU rule codes from the parser context.
If a code is missing in rules, set primary_part to UNRESOLVED and sku to NOT_UNDERSTANDABLE.
All string outputs must be uppercase.
SKU length must be <= 31 characters including spaces."""


def _ai_parse_title(title: str) -> ParsedSKUResult | None:
    """Call the OpenAI Responses API with structured output enforcement.

    Retries once on failure. Returns None if AI is unavailable or both
    attempts fail.
    """
    client = _get_client()
    if client is None:
        return None

    prompt = f"Parse this mobile repair part title and generate SKU data:\n\n{title}"

    for attempt in range(AI_MAX_RETRIES):
        try:
            response = client.responses.parse(
                model=AI_MODEL,
                input=[
                    {"role": "system", "content": _AI_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                response_format=ParsedSKUResult,
            )
            parsed: ParsedSKUResult | None = response.output_parsed
            if parsed is not None:
                parsed.source = "ai"
                parsed.confidence = CONFIDENCE_AI
                parsed.needs_review = parsed.confidence < REVIEW_FLAG_THRESHOLD
                return parsed
        except Exception as exc:
            if attempt == 0:
                logger.warning(
                    "AI parse attempt %d failed for %r: %s — retrying…",
                    attempt + 1,
                    title[:60],
                    exc,
                )
                time.sleep(0.5)
            else:
                logger.error(
                    "AI parse attempt %d failed for %r: %s — giving up.",
                    attempt + 1,
                    title[:60],
                    exc,
                )
    return None


# ---------------------------------------------------------------------------
# Rule result → ParsedSKUResult converter
# ---------------------------------------------------------------------------


def _rule_to_parsed(rule: dict[str, Any], title: str) -> ParsedSKUResult:
    """Convert rule-engine dict output to a ParsedSKUResult."""
    sku = str(rule.get("sku", "") or NOT_UNDERSTANDABLE)
    raw_confidence: float = float(rule.get("confidence", 0.0) or 0.0)
    reason: str = str(rule.get("reason", "")).lower()

    # Map rule reason → confidence constant
    if "fuzzy" in reason or "correction" in reason:
        confidence = CONFIDENCE_FUZZY
    elif raw_confidence >= CONFIDENCE_EXACT_RULE - 0.01:
        confidence = CONFIDENCE_EXACT_RULE
    else:
        confidence = raw_confidence

    source = "rule_fuzzy" if "fuzzy" in reason or "correction" in reason else "rule"

    raw_corrections = rule.get("corrections", []) or []
    corrections_strs: list[str] = []
    for c in raw_corrections:
        if isinstance(c, dict):
            f = c.get("from", c.get("from_token", ""))
            t = c.get("to", c.get("to_token", ""))
            if f and t:
                corrections_strs.append(f"{f}→{t}")
        elif isinstance(c, str):
            corrections_strs.append(c)

    return ParsedSKUResult(
        brand=str(rule.get("brand", "") or ""),
        model=str(rule.get("model", "") or ""),
        model_code=str(rule.get("model_code", "") or ""),
        primary_part=str(rule.get("part", "") or ""),
        secondary_part=str(rule.get("secondary_part", "") or "") or None,
        sku=sku,
        confidence=confidence,
        corrections=corrections_strs,
        needs_review=confidence < REVIEW_FLAG_THRESHOLD,
        source=source,
    )


# ---------------------------------------------------------------------------
# Public hybrid orchestrator
# ---------------------------------------------------------------------------


def get_hybrid_result(
    title: str,
    product_sku_hint: str = "",
    product_web_sku_hint: str = "",
) -> ParsedSKUResult:
    """Return a ParsedSKUResult using the hybrid rule → AI pipeline.

    Flow:
      1. Check in-memory cache.
      2. Run rule-based engine.
      3. If rule confidence >= threshold → return rule result.
      4. Otherwise call AI; if AI succeeds → return AI result.
      5. On AI failure → return rule result with lowered confidence.

    Args:
        title: Raw product title string.
        product_sku_hint: Optional existing SKU to aid parsing.
        product_web_sku_hint: Optional web SKU to aid parsing.

    Returns:
        ParsedSKUResult with structured fields.
    """
    execution = _get_structured_service().analyze_title(
        title=title,
        product_sku=product_sku_hint,
        product_web_sku=product_web_sku_hint,
        allow_ai=True,
        mode="single",
    )
    parsed = execution.parsed
    return ParsedSKUResult(
        brand=parsed.brand,
        model=parsed.model,
        model_code=parsed.model_code,
        primary_part=parsed.primary_part,
        secondary_part=parsed.secondary_part,
        sku=parsed.sku,
        confidence=float(parsed.final_confidence or parsed.confidence),
        corrections=list(parsed.corrections),
        needs_review=bool(execution.review_required),
        source=execution.source,
    )


# ---------------------------------------------------------------------------
# Batch Excel processor
# ---------------------------------------------------------------------------


def process_inventory_excel(
    input_path: str | Path,
    output_path: str | Path | None = None,
) -> pd.DataFrame:
    """Parse product titles in an Excel/CSV inventory file and append SKU columns.

    Args:
        input_path: Path to .xlsx / .xls / .csv file.
        output_path: Optional destination path. Defaults to
            ``<stem>_sku_structured<suffix>.xlsx`` beside the input file.

    Returns:
        DataFrame with appended SKU columns.
    """
    input_path = Path(input_path)
    if output_path is None:
        output_path = input_path.with_name(f"{input_path.stem}_sku_structured.xlsx")
    return _get_structured_service().process_inventory_excel(
        input_file=input_path,
        output_file=Path(output_path),
        title_column="Product Name",
    )


def _find_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first column name that exists (case-insensitive) in df."""
    lower_map = {c.lower(): c for c in df.columns}
    for candidate in candidates:
        match = lower_map.get(candidate.lower())
        if match is not None:
            return match
    return None


# ---------------------------------------------------------------------------
# Structured log helper
# ---------------------------------------------------------------------------


def log_result(
    title: str,
    result: ParsedSKUResult,
    log_file: str | Path = "outputs/structured_parse_log.jsonl",
) -> None:
    """Append a structured JSON log entry to a JSONL file."""
    log_file = Path(log_file)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "generated_sku": result.sku,
        "confidence": result.confidence,
        "corrections": result.corrections,
        "needs_review": result.needs_review,
        "source": result.source,
    }
    with log_file.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")
