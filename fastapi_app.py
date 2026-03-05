#!/usr/bin/env python3
"""FastAPI backend for live SKU title analysis — with OpenAI Structured Outputs support."""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Literal, Optional

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from ai_parser import (
    AI_FALLBACK_THRESHOLD,
    AI_MODEL,
    ParsedSKUResult,
    clear_cache,
    get_hybrid_result,
    log_result,
    process_inventory_excel,
)
from sku_parser import NOT_UNDERSTANDABLE

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class AnalyzeTitleRequest(BaseModel):
    title: str = Field(default="", description="Raw product title")
    product_sku: str = Field(default="", description="Optional existing SKU hint")
    product_web_sku: str = Field(default="", description="Optional web SKU hint")


class CorrectionItem(BaseModel):
    from_token: str = Field(alias="from")
    to_token: str = Field(alias="to")

    model_config = {"populate_by_name": True}


class AnalyzeTitleResponse(BaseModel):
    """Full structured response for a single title analysis."""

    brand: str
    model: str
    model_code: str
    part: str
    secondary_part: str
    sku: str
    confidence: float
    corrections: list[CorrectionItem]
    interpreted_title: str
    parser_reason: str
    parse_status: Literal["parsed", "not_understandable"]
    needs_review: bool
    source: str


class BatchResponse(BaseModel):
    output_file: str
    rows_processed: int
    message: str


class CacheStatusResponse(BaseModel):
    cached_entries: int
    ai_enabled: bool
    ai_model: str
    ai_threshold: float


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="SKU Parser Analyzer API",
    version="2.0.0",
    description=(
        "Hybrid SKU parser: rule-based engine + OpenAI Structured Outputs fallback. "
        "AI is invoked only when rule confidence < threshold."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


@app.get("/healthz")
def healthz() -> dict[str, str]:
    ai_status = "enabled" if os.getenv("OPENAI_API_KEY") else "disabled (no API key)"
    return {
        "status": "ok",
        "service": "sku-parser-analyzer-api",
        "ai": ai_status,
    }


# ---------------------------------------------------------------------------
# Single title analysis  (hybrid: rule + optional AI)
# ---------------------------------------------------------------------------


@app.post("/analyze-title", response_model=AnalyzeTitleResponse)
def analyze_title_api(payload: AnalyzeTitleRequest) -> AnalyzeTitleResponse:
    """Analyze a single product title and return a structured SKU result.

    Processing flow:
      1. Run rule-based parser.
      2. If confidence < threshold → call AI with structured output.
      3. Return the best result.
    """
    title = payload.title.strip()
    if not title and not payload.product_sku.strip() and not payload.product_web_sku.strip():
        raise HTTPException(status_code=400, detail="Provide at least a title or SKU hint.")

    try:
        result: ParsedSKUResult = get_hybrid_result(
            title=title,
            product_sku_hint=payload.product_sku,
            product_web_sku_hint=payload.product_web_sku,
        )
    except Exception as exc:
        logger.exception("Hybrid parse failed for %r", title[:80])
        raise HTTPException(status_code=500, detail=f"Failed to analyze title: {exc}") from exc

    # Log the result to JSONL store
    try:
        log_result(title, result)
    except Exception:
        pass  # never block the response for logging failures

    # Derive parse_status
    parse_status: Literal["parsed", "not_understandable"] = (
        "not_understandable" if result.sku == NOT_UNDERSTANDABLE or not result.sku
        else "parsed"
    )

    # Build CorrectionItem objects from flat strings  ("wrong→correct")
    correction_items: list[CorrectionItem] = []
    for c in result.corrections:
        if "→" in c:
            parts = c.split("→", 1)
            correction_items.append(
                CorrectionItem(**{"from": parts[0].strip(), "to": parts[1].strip()})
            )
        else:
            correction_items.append(CorrectionItem(**{"from": c, "to": c}))

    return AnalyzeTitleResponse(
        brand=result.brand,
        model=result.model,
        model_code=result.model_code,
        part=result.primary_part,
        secondary_part=result.secondary_part or "",
        sku=result.sku or NOT_UNDERSTANDABLE,
        confidence=result.confidence,
        corrections=correction_items,
        interpreted_title=title,
        parser_reason=result.source,
        parse_status=parse_status,
        needs_review=result.needs_review,
        source=result.source,
    )


# ---------------------------------------------------------------------------
# Batch Excel endpoint
# ---------------------------------------------------------------------------


@app.post("/analyze-title/batch", response_class=FileResponse)
async def analyze_batch(file: UploadFile = File(...)) -> FileResponse:
    """Batch-process an uploaded Excel/CSV inventory file.

    Uploads should be .xlsx, .xls, or .csv. Returns a processed Excel file
    with appended AI_* columns for brand, model, part, SKU, confidence, etc.
    """
    allowed_suffixes = {".xlsx", ".xls", ".csv"}
    suffix = Path(file.filename or "upload.xlsx").suffix.lower()
    if suffix not in allowed_suffixes:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{suffix}'. Upload .xlsx, .xls, or .csv.",
        )

    # Save the upload to a temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_in:
        shutil.copyfileobj(file.file, tmp_in)
        tmp_in_path = Path(tmp_in.name)

    tmp_out_path = tmp_in_path.with_stem(tmp_in_path.stem + "_sku_structured")

    try:
        process_inventory_excel(tmp_in_path, tmp_out_path)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Batch processing failed")
        raise HTTPException(status_code=500, detail=f"Batch processing failed: {exc}") from exc
    finally:
        tmp_in_path.unlink(missing_ok=True)

    return FileResponse(
        path=str(tmp_out_path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"sku_structured_output{suffix}",
        background=None,
    )


# ---------------------------------------------------------------------------
# Cache management
# ---------------------------------------------------------------------------


@app.get("/cache/status", response_model=CacheStatusResponse)
def cache_status() -> CacheStatusResponse:
    """Return current cache stats and AI configuration."""
    from ai_parser import _TITLE_CACHE  # local import to avoid circular

    return CacheStatusResponse(
        cached_entries=len(_TITLE_CACHE),
        ai_enabled=bool(os.getenv("OPENAI_API_KEY")),
        ai_model=AI_MODEL,
        ai_threshold=AI_FALLBACK_THRESHOLD,
    )


@app.delete("/cache")
def clear_title_cache() -> dict[str, str]:
    """Clear the in-memory title parse cache."""
    clear_cache()
    return {"status": "ok", "message": "Cache cleared."}
