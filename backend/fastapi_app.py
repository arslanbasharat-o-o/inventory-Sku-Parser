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

from .structured_sku_parser import (
    ParsedSKUResult,
    StructuredParseError,
    StructuredSKUParserService,
)
from .training_dashboard_service import TrainingDashboardService

LOGGER = logging.getLogger(__name__)


class AnalyzeTitleRequest(BaseModel):
    title: str = Field(default="", description="Raw product title")
    product_sku: str = Field(default="", description="Optional SKU hint")
    product_web_sku: str = Field(default="", description="Optional web SKU hint")
    product_description: str = Field(default="", description="Optional product description")


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


class TitleTrainingRequest(BaseModel):
    product_title: str
    detected_model: str = ""
    detected_part: str = ""
    detected_color: str = ""
    expected_sku: str


class SynonymTrainingRequest(BaseModel):
    supplier_phrase: str
    standard_term: str


class SpellingTrainingRequest(BaseModel):
    incorrect_word: str
    correct_word: str


class PartOntologyTrainingRequest(BaseModel):
    phrase: str
    sku_code: str


class ColorTrainingRequest(BaseModel):
    supplier_color: str
    standard_color: str


class SKUCorrectionRequest(BaseModel):
    generated_sku: str
    correct_sku: str
    title: str = ""


class RuleTrainingRequest(BaseModel):
    rule_text: str


class LiveTestRequest(BaseModel):
    title: str


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
TRAINING_SERVICE = TrainingDashboardService(structured_log_db_path=PARSER_SERVICE.db_path)


def _refresh_structured_cache() -> None:
    PARSER_SERVICE.clear_cache()

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
    if (
        not title
        and not payload.product_sku.strip()
        and not payload.product_web_sku.strip()
        and not payload.product_description.strip()
    ):
        raise HTTPException(status_code=400, detail="Provide at least a title or SKU hint.")

    try:
        execution = PARSER_SERVICE.analyze_title(
            title=title,
            product_sku=payload.product_sku,
            product_web_sku=payload.product_web_sku,
            product_description=payload.product_description,
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


@app.get("/admin/training/bootstrap")
def training_bootstrap() -> dict[str, object]:
    try:
        return TRAINING_SERVICE.get_bootstrap()
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("training_bootstrap failed")
        raise HTTPException(status_code=500, detail=f"Failed to load training dashboard: {exc}") from exc


@app.get("/admin/training/analytics")
def training_analytics() -> dict[str, object]:
    try:
        return TRAINING_SERVICE.get_analytics()
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("training_analytics failed")
        raise HTTPException(status_code=500, detail=f"Failed to load analytics: {exc}") from exc


@app.post("/admin/training/title-training")
def training_add_title_sample(payload: TitleTrainingRequest) -> dict[str, object]:
    try:
        result = TRAINING_SERVICE.add_title_training_sample(
            product_title=payload.product_title,
            detected_model=payload.detected_model,
            detected_part=payload.detected_part,
            detected_color=payload.detected_color,
            expected_sku=payload.expected_sku,
        )
        _refresh_structured_cache()
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("training_add_title_sample failed")
        raise HTTPException(status_code=500, detail=f"Failed to save title training sample: {exc}") from exc


@app.post("/admin/training/synonym")
def training_add_synonym(payload: SynonymTrainingRequest) -> dict[str, object]:
    try:
        result = TRAINING_SERVICE.add_synonym_mapping(
            supplier_phrase=payload.supplier_phrase,
            standard_term=payload.standard_term,
        )
        _refresh_structured_cache()
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("training_add_synonym failed")
        raise HTTPException(status_code=500, detail=f"Failed to save synonym: {exc}") from exc


@app.post("/admin/training/spelling")
def training_add_spelling(payload: SpellingTrainingRequest) -> dict[str, object]:
    try:
        result = TRAINING_SERVICE.add_spelling_correction(
            incorrect_word=payload.incorrect_word,
            correct_word=payload.correct_word,
        )
        _refresh_structured_cache()
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("training_add_spelling failed")
        raise HTTPException(status_code=500, detail=f"Failed to save spelling correction: {exc}") from exc


@app.post("/admin/training/part-ontology")
def training_add_part_mapping(payload: PartOntologyTrainingRequest) -> dict[str, object]:
    try:
        result = TRAINING_SERVICE.add_part_mapping(
            phrase=payload.phrase,
            sku_code=payload.sku_code,
        )
        _refresh_structured_cache()
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("training_add_part_mapping failed")
        raise HTTPException(status_code=500, detail=f"Failed to save part mapping: {exc}") from exc


@app.post("/admin/training/color")
def training_add_color_mapping(payload: ColorTrainingRequest) -> dict[str, object]:
    try:
        result = TRAINING_SERVICE.add_color_mapping(
            supplier_color=payload.supplier_color,
            standard_color=payload.standard_color,
        )
        _refresh_structured_cache()
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("training_add_color_mapping failed")
        raise HTTPException(status_code=500, detail=f"Failed to save color mapping: {exc}") from exc


@app.post("/admin/training/sku-correction")
def training_add_sku_correction(payload: SKUCorrectionRequest) -> dict[str, object]:
    try:
        result = TRAINING_SERVICE.add_sku_correction(
            generated_sku=payload.generated_sku,
            correct_sku=payload.correct_sku,
            title=payload.title,
        )
        _refresh_structured_cache()
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("training_add_sku_correction failed")
        raise HTTPException(status_code=500, detail=f"Failed to save SKU correction: {exc}") from exc


@app.post("/admin/training/rule")
def training_add_rule(payload: RuleTrainingRequest) -> dict[str, object]:
    try:
        result = TRAINING_SERVICE.add_rule(rule_text=payload.rule_text)
        _refresh_structured_cache()
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("training_add_rule failed")
        raise HTTPException(status_code=500, detail=f"Failed to save rule: {exc}") from exc


@app.post("/admin/training/live-test")
def training_live_test(payload: LiveTestRequest) -> dict[str, object]:
    title = payload.title.strip()
    if not title:
        raise HTTPException(status_code=400, detail="Title is required.")
    try:
        return TRAINING_SERVICE.live_test(title)
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("training_live_test failed")
        raise HTTPException(status_code=500, detail=f"Live test failed: {exc}") from exc


@app.post("/admin/training/upload-dataset")
async def training_upload_dataset(file: UploadFile = File(...)) -> dict[str, object]:
    allowed_suffixes = {".xlsx", ".xls", ".csv"}
    suffix = Path(file.filename or "training_upload.xlsx").suffix.lower()
    if suffix not in allowed_suffixes:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{suffix}'. Upload .xlsx, .xls, or .csv.",
        )

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_in:
        shutil.copyfileobj(file.file, tmp_in)
        tmp_in_path = Path(tmp_in.name)

    try:
        result = TRAINING_SERVICE.upload_training_dataset(tmp_in_path)
        _refresh_structured_cache()
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("training_upload_dataset failed")
        raise HTTPException(status_code=500, detail=f"Dataset upload training failed: {exc}") from exc
    finally:
        tmp_in_path.unlink(missing_ok=True)
