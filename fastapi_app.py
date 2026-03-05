#!/usr/bin/env python3
"""FastAPI backend for structured SKU parsing and batch processing."""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from structured_sku_parser import (
    ParsedSKUResult,
    StructuredParseError,
    StructuredSKUParserService,
)

LOGGER = logging.getLogger(__name__)


class AnalyzeTitleRequest(BaseModel):
    title: str = Field(default="", description="Raw product title")
    product_sku: str = Field(default="", description="Optional SKU hint")
    product_web_sku: str = Field(default="", description="Optional web SKU hint")


class CorrectionItem(BaseModel):
    from_token: str = Field(alias="from")
    to_token: str = Field(alias="to")

    model_config = {"populate_by_name": True}


class AnalyzeTitleResponse(BaseModel):
    brand: str
    model: str
    model_code: str
    primary_part: str
    secondary_part: str | None
    sku: str
    confidence: float
    corrections: list[str]

    # Useful operational metadata for API clients.
    parse_status: Literal["parsed", "not_understandable"]
    parser_reason: str
    source: Literal["rule", "ai", "cache"]
    review_required: bool
    needs_review: bool
    interpreted_title: str

    # Backward-compatible fields used by existing UI.
    part: str
    correction_pairs: list[CorrectionItem]


class BatchPathRequest(BaseModel):
    input_file: str
    output_file: str = "outputs/structured_inventory_output.xlsx"
    title_column: str = "Product Name"


class BatchPathResponse(BaseModel):
    output_file: str
    rows_processed: int


class CacheStatusResponse(BaseModel):
    cached_entries: int
    ai_enabled: bool
    ai_model: str
    ai_threshold: float


AI_THRESHOLD = float(os.getenv("SKU_AI_THRESHOLD", "0.85"))
REVIEW_THRESHOLD = float(os.getenv("SKU_REVIEW_THRESHOLD", "0.75"))
AI_MODEL = os.getenv("OPENAI_STRUCTURED_MODEL", "gpt-5")

PARSER_SERVICE = StructuredSKUParserService(
    ai_model=AI_MODEL,
    ai_threshold=AI_THRESHOLD,
    review_threshold=REVIEW_THRESHOLD,
    cache_size=int(os.getenv("SKU_PARSE_CACHE_SIZE", "50000")),
    db_path=os.getenv("SKU_PARSE_DB_PATH", "outputs/structured_sku_results.db"),
    enable_ai=True,
)

app = FastAPI(
    title="SKU Parser Analyzer API",
    version="3.0.0",
    description=(
        "Rule-first SKU parser with OpenAI Responses API structured-output fallback. "
        "When rule confidence < 0.85, AI structured parsing is used."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "sku-parser-analyzer-api",
        "ai": "enabled" if bool(os.getenv("OPENAI_API_KEY")) else "disabled",
    }


@app.post("/analyze-title", response_model=AnalyzeTitleResponse)
def analyze_title_api(payload: AnalyzeTitleRequest) -> AnalyzeTitleResponse:
    title = payload.title.strip()
    if not title and not payload.product_sku.strip() and not payload.product_web_sku.strip():
        raise HTTPException(status_code=400, detail="Provide at least a title or SKU hint.")

    try:
        execution = PARSER_SERVICE.analyze_title(
            title=title,
            product_sku=payload.product_sku,
            product_web_sku=payload.product_web_sku,
        )
    except StructuredParseError:
        # Required failsafe for invalid structured model output after retry.
        return JSONResponse(status_code=422, content={"error": "Unable to parse title"})
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("Structured analyze-title failed")
        raise HTTPException(status_code=500, detail=f"Failed to analyze title: {exc}") from exc

    parsed: ParsedSKUResult = execution.parsed
    correction_pairs = [
        CorrectionItem(**{"from": src, "to": dst})
        for src, dst in (
            (
                str(pair.get("from", "")).strip(),
                str(pair.get("to", "")).strip(),
            )
            for pair in execution.correction_pairs
            if isinstance(pair, dict)
        )
        if src or dst
    ]

    return AnalyzeTitleResponse(
        brand=parsed.brand,
        model=parsed.model,
        model_code=parsed.model_code,
        primary_part=parsed.primary_part,
        secondary_part=parsed.secondary_part,
        sku=parsed.sku,
        confidence=float(parsed.confidence),
        corrections=list(parsed.corrections),
        parse_status=execution.parse_status,
        parser_reason=execution.parser_reason,
        source=execution.source,
        review_required=bool(execution.review_required),
        needs_review=bool(execution.review_required),
        interpreted_title=title,
        part=parsed.primary_part,
        correction_pairs=correction_pairs,
    )


@app.post("/process-inventory-excel", response_model=BatchPathResponse)
def process_inventory_excel_by_path(payload: BatchPathRequest) -> BatchPathResponse:
    input_path = Path(payload.input_file)
    output_path = Path(payload.output_file)

    if not input_path.exists():
        raise HTTPException(status_code=404, detail=f"Input file not found: {input_path}")

    try:
        result_df = PARSER_SERVICE.process_inventory_excel(
            input_file=input_path,
            output_file=output_path,
            title_column=payload.title_column,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("process_inventory_excel_by_path failed")
        raise HTTPException(status_code=500, detail=f"Batch processing failed: {exc}") from exc

    return BatchPathResponse(
        output_file=str(output_path),
        rows_processed=int(len(result_df)),
    )


@app.post("/analyze-title/batch", response_class=FileResponse)
async def analyze_batch(file: UploadFile = File(...)) -> FileResponse:
    allowed_suffixes = {".xlsx", ".xls", ".csv"}
    suffix = Path(file.filename or "upload.xlsx").suffix.lower()
    if suffix not in allowed_suffixes:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{suffix}'. Upload .xlsx, .xls, or .csv.",
        )

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_in:
        shutil.copyfileobj(file.file, tmp_in)
        tmp_in_path = Path(tmp_in.name)

    tmp_out_path = tmp_in_path.with_stem(tmp_in_path.stem + "_structured")
    if tmp_out_path.suffix.lower() == ".csv":
        tmp_out_path = tmp_out_path.with_suffix(".xlsx")

    try:
        PARSER_SERVICE.process_inventory_excel(
            input_file=tmp_in_path,
            output_file=tmp_out_path,
            title_column="Product Name",
        )
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("Batch upload processing failed")
        raise HTTPException(status_code=500, detail=f"Batch processing failed: {exc}") from exc
    finally:
        tmp_in_path.unlink(missing_ok=True)

    return FileResponse(
        path=str(tmp_out_path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="structured_inventory_output.xlsx",
    )


@app.get("/cache/status", response_model=CacheStatusResponse)
def cache_status() -> CacheStatusResponse:
    return CacheStatusResponse(
        cached_entries=PARSER_SERVICE.cache_entry_count(),
        ai_enabled=bool(os.getenv("OPENAI_API_KEY")),
        ai_model=AI_MODEL,
        ai_threshold=AI_THRESHOLD,
    )


@app.delete("/cache")
def clear_title_cache() -> dict[str, str]:
    PARSER_SERVICE.clear_cache()
    return {"status": "ok", "message": "Cache cleared."}
