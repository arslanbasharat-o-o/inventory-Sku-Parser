#!/usr/bin/env python3
"""FastAPI backend for structured SKU parsing and batch processing."""

from __future__ import annotations

import logging
import os
import re
import shutil
import tempfile
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from .logging_utils import configure_backend_logging
from .bulk_job_runner import (
    BulkJobError,
    BulkJobTimeoutError,
    MAX_CONCURRENT_BULK_JOBS,
    bulk_job_slot,
    load_processed_inventory_preview,
    run_legacy_inventory_job,
    run_structured_inventory_job,
    warm_bulk_worker_pool,
)
from .bulk_job_queue import BulkJobQueueManager
from .structured_sku_parser import (
    ParsedSKUResult,
    StructuredParseError,
    StructuredSKUParserService,
)
from .sku_parser import NOT_UNDERSTANDABLE as LEGACY_NOT_UNDERSTANDABLE
from .training_dashboard_service import TrainingDashboardService

BACKEND_LOG_FILE, BACKEND_ERROR_LOG_FILE = configure_backend_logging()
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
    variant: str | None = None
    color: str | None = None
    sku: str
    confidence: float
    rule_confidence: float
    final_confidence: float
    corrections: list[str]

    # Useful operational metadata for API clients.
    parse_status: Literal["parsed", "partial", "not_understandable"]
    parse_stage: Literal["rule_only", "rule_normalized", "ai_assisted"]
    parser_reason: str
    source: Literal["rule", "ai", "cache"]
    ai_used: bool
    review_required: bool
    needs_review: bool
    interpreted_title: str
    validation_failed_reason: str

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
    ai_provider: str
    ai_model: str
    single_ai_model: str
    batch_ai_model: str
    ai_threshold: float
    ai_single_threshold: float
    rule_accept_threshold: float
    ai_batch_enabled: bool
    ai_max_concurrent_requests: int
    approved_pattern_count: int
    candidate_pattern_count: int
    approved_spelling_count: int
    candidate_spelling_count: int
    structured_db_path: str
    backend_log_file: str
    backend_error_log_file: str


class CandidateLearningItem(BaseModel):
    candidate_type: Literal["pattern", "spelling"]
    normalized_source: str
    mapped_value: str
    review_status: Literal["PENDING", "APPROVED", "REJECTED"]
    review_note: str = ""


class CandidateLearningResponse(BaseModel):
    patterns: list[CandidateLearningItem]
    spellings: list[CandidateLearningItem]


class CandidateReviewRequest(BaseModel):
    candidate_type: Literal["pattern", "spelling"]
    normalized_source: str
    mapped_value: str
    review_status: Literal["approved", "rejected"]
    review_note: str = ""


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


AI_THRESHOLD = float(os.getenv("SKU_AI_SINGLE_THRESHOLD", os.getenv("SKU_AI_THRESHOLD", "0.40")))
RULE_ACCEPT_THRESHOLD = float(os.getenv("SKU_RULE_ACCEPT_THRESHOLD", "0.80"))
REVIEW_THRESHOLD = float(os.getenv("SKU_REVIEW_THRESHOLD", "0.75"))
BASE_DIR = Path(__file__).resolve().parent.parent
UPLOAD_DIR = Path(os.getenv("SKU_UPLOAD_DIR", str(BASE_DIR / "uploads"))).resolve()
OUTPUT_DIR = Path(os.getenv("SKU_OUTPUT_DIR", str(BASE_DIR / "outputs"))).resolve()
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PARSER_SERVICE = StructuredSKUParserService(
    ai_threshold=AI_THRESHOLD,
    review_threshold=REVIEW_THRESHOLD,
    rule_accept_threshold=RULE_ACCEPT_THRESHOLD,
    cache_size=int(os.getenv("SKU_PARSE_CACHE_SIZE", "50000")),
    db_path=os.getenv("SKU_PARSE_DB_PATH", "data/runtime/structured_sku_results.db"),
    enable_ai=True,
)
TRAINING_SERVICE = TrainingDashboardService(structured_log_db_path=PARSER_SERVICE.db_path)
BULK_JOB_QUEUE = BulkJobQueueManager(worker_count=MAX_CONCURRENT_BULK_JOBS)


def _refresh_structured_cache() -> None:
    PARSER_SERVICE.clear_cache()


def _safe_filename(raw_name: str) -> str:
    name = Path(raw_name or "inventory.xlsx").name
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._")
    return safe or "inventory.xlsx"


def _build_legacy_stats(df: pd.DataFrame) -> dict[str, int | float]:
    parsed_mask = df.get("Product New SKU", pd.Series(dtype=object)).astype(str).ne(
        LEGACY_NOT_UNDERSTANDABLE
    )
    sku_dup_mask = df.get("SKU Duplicate", pd.Series(dtype=object)).astype(str).eq("DUPLICATED")
    title_dup_mask = df.get("Title Duplicate", pd.Series(dtype=object)).astype(str).eq("DUPLICATED")
    total_rows = int(len(df))
    parsed_rows = int(parsed_mask.sum()) if total_rows else 0
    parse_rate = round((parsed_rows / total_rows) * 100, 2) if total_rows else 0.0
    return {
        "total_rows": total_rows,
        "parsed_rows": parsed_rows,
        "unparsed_rows": int(total_rows - parsed_rows),
        "parse_rate": parse_rate,
        "sku_duplicates": int(sku_dup_mask.sum()),
        "title_duplicates": int(title_dup_mask.sum()),
    }

@asynccontextmanager
async def app_lifespan(_app: FastAPI):
    try:
        warm_bulk_worker_pool()
    except Exception:
        LOGGER.exception("Failed to warm bulk worker pool on startup")
    yield


app = FastAPI(
    title="SKU Parser Analyzer API",
    version="3.0.0",
    description=(
        "Rule-first SKU parser with Gemini/OpenAI structured-output fallback. "
        "Rule results are accepted at high confidence, and AI is only used for low-confidence interpretation."
    ),
    lifespan=app_lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/healthz")
def healthz() -> dict[str, str | int]:
    return {
        "status": "ok",
        "service": "sku-parser-analyzer-api",
        "ai": "enabled" if PARSER_SERVICE.ai_enabled else "disabled",
        "ai_provider": PARSER_SERVICE.ai_provider,
        "bulk_workers": MAX_CONCURRENT_BULK_JOBS,
        "structured_db_path": str(PARSER_SERVICE.db_path),
        "backend_log_file": str(BACKEND_LOG_FILE),
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
        # Graceful fallback: if AI structured output is unavailable/invalid,
        # return rule-parser output instead of hard-failing the API.
        try:
            parsed, parser_reason, correction_pairs_raw = PARSER_SERVICE._run_rule_parser(  # noqa: SLF001
                title=PARSER_SERVICE.normalize_title(title),
                product_sku=payload.product_sku,
                product_web_sku=payload.product_web_sku,
                product_description=PARSER_SERVICE.normalize_title(payload.product_description),
            )
            parse_status = PARSER_SERVICE.derive_parse_status(parsed)
            execution = type("FallbackExecution", (), {})()
            execution.parsed = parsed
            execution.parse_status = parse_status
            execution.parser_reason = f"{parser_reason}+ai_unavailable_fallback"
            execution.source = "rule"
            execution.review_required = bool(float(parsed.confidence) < REVIEW_THRESHOLD)
            execution.correction_pairs = correction_pairs_raw
            execution.parse_stage = "rule_only" if float(parsed.rule_confidence or parsed.confidence) >= RULE_ACCEPT_THRESHOLD else "rule_normalized"
            execution.ai_used = False
            execution.validation_failed_reason = "ai_unavailable_fallback"
        except Exception:
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
        variant=parsed.variant,
        color=parsed.color,
        sku="" if execution.parse_status == "partial" else parsed.sku,
        confidence=float(parsed.confidence),
        rule_confidence=float(parsed.rule_confidence),
        final_confidence=float(parsed.final_confidence or parsed.confidence),
        corrections=list(parsed.corrections),
        parse_status=execution.parse_status,
        parse_stage=execution.parse_stage,
        parser_reason=execution.parser_reason,
        source=execution.source,
        ai_used=bool(execution.ai_used),
        review_required=bool(execution.review_required),
        needs_review=bool(execution.review_required),
        interpreted_title=title,
        validation_failed_reason=execution.validation_failed_reason,
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
        with bulk_job_slot() as acquired:
            if not acquired:
                raise HTTPException(
                    status_code=429,
                    detail="Server is busy. Too many concurrent bulk parse jobs. Retry shortly.",
                )
            result = run_structured_inventory_job(
                input_file=input_path,
                output_file=output_path,
                title_column=payload.title_column,
            )
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except BulkJobTimeoutError as exc:
        LOGGER.exception("process_inventory_excel_by_path timed out")
        output_path.unlink(missing_ok=True)
        raise HTTPException(status_code=504, detail=str(exc)) from exc
    except BulkJobError as exc:
        LOGGER.exception("process_inventory_excel_by_path failed in worker")
        output_path.unlink(missing_ok=True)
        raise HTTPException(status_code=500, detail=f"Batch processing failed: {exc}") from exc
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("process_inventory_excel_by_path failed")
        raise HTTPException(status_code=500, detail=f"Batch processing failed: {exc}") from exc

    return BatchPathResponse(
        output_file=str(output_path),
        rows_processed=int(result.get("rows_processed", 0)),
    )


@app.post("/analyze-title/batch", response_class=FileResponse)
def analyze_batch(file: UploadFile = File(...)) -> FileResponse:
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
        with bulk_job_slot() as acquired:
            if not acquired:
                raise HTTPException(
                    status_code=429,
                    detail="Server is busy. Too many concurrent bulk parse jobs. Retry shortly.",
                )
            run_structured_inventory_job(
                input_file=tmp_in_path,
                output_file=tmp_out_path,
                title_column="Product Name",
            )
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except BulkJobTimeoutError as exc:
        LOGGER.exception("Batch upload processing timed out")
        tmp_out_path.unlink(missing_ok=True)
        raise HTTPException(status_code=504, detail=str(exc)) from exc
    except BulkJobError as exc:
        LOGGER.exception("Batch upload processing failed in worker")
        tmp_out_path.unlink(missing_ok=True)
        raise HTTPException(status_code=500, detail=f"Batch processing failed: {exc}") from exc
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


@app.post("/parse-inventory-api")
def parse_inventory_api(inventory_file: UploadFile = File(...)) -> dict[str, object]:
    """Compatibility endpoint used by the existing Next.js bulk parser UI."""
    suffix = Path(inventory_file.filename or "inventory.xlsx").suffix.lower()
    if suffix not in {".xlsx", ".xlsm", ".xls", ".csv"}:
        raise HTTPException(status_code=400, detail="Invalid file type")

    file_id = uuid.uuid4().hex
    input_name = _safe_filename(inventory_file.filename or "inventory.xlsx")
    input_path = UPLOAD_DIR / f"{file_id}_{input_name}"
    output_path = OUTPUT_DIR / f"{file_id}_products_sku_processed.xlsx"

    try:
        with input_path.open("wb") as out_fp:
            shutil.copyfileobj(inventory_file.file, out_fp)
        job_snapshot = BULK_JOB_QUEUE.submit_legacy_job(
            input_file=input_path,
            output_file=output_path,
        )
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("parse_inventory_api failed")
        raise HTTPException(status_code=500, detail=f"Processing failed: {exc}") from exc

    return JSONResponse(status_code=202, content=job_snapshot)


@app.get("/parse-inventory-api/{job_id}")
def parse_inventory_job_status(job_id: str) -> dict[str, object]:
    snapshot = BULK_JOB_QUEUE.get_job(job_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Bulk parse job not found")

    status = str(snapshot.get("status", "")).strip().lower()
    if status == "completed":
        output_path = OUTPUT_DIR / str(snapshot.get("download_file", ""))
        if not output_path.exists():
            raise HTTPException(status_code=500, detail="Bulk parse output file is missing")
        result_df = load_processed_inventory_preview(output_path)
        preview_df = result_df.fillna("")
        return {
            **snapshot,
            "columns": list(preview_df.columns),
            "rows": preview_df.to_dict(orient="records"),
            "stats": _build_legacy_stats(result_df),
            "download_file": output_path.name,
        }

    if status == "failed":
        return snapshot

    return snapshot


@app.get("/download/{file_name}", response_class=FileResponse)
def download_file(file_name: str) -> FileResponse:
    safe_name = _safe_filename(file_name)
    file_path = OUTPUT_DIR / safe_name
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Processed file not found")
    return FileResponse(
        path=str(file_path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="products_sku_processed.xlsx",
    )


@app.get("/cache/status", response_model=CacheStatusResponse)
def cache_status() -> CacheStatusResponse:
    learning_status = PARSER_SERVICE.learning_status()
    return CacheStatusResponse(
        cached_entries=PARSER_SERVICE.cache_entry_count(),
        ai_enabled=PARSER_SERVICE.ai_enabled,
        ai_provider=PARSER_SERVICE.ai_provider,
        ai_model=PARSER_SERVICE.ai_model,
        single_ai_model=PARSER_SERVICE.single_ai_model,
        batch_ai_model=PARSER_SERVICE.batch_ai_model,
        ai_threshold=AI_THRESHOLD,
        ai_single_threshold=AI_THRESHOLD,
        rule_accept_threshold=RULE_ACCEPT_THRESHOLD,
        ai_batch_enabled=bool(PARSER_SERVICE.batch_ai_enabled),
        ai_max_concurrent_requests=PARSER_SERVICE.ai_max_concurrent_requests,
        approved_pattern_count=int(learning_status["approved_pattern_count"]),
        candidate_pattern_count=int(learning_status["candidate_pattern_count"]),
        approved_spelling_count=int(learning_status["approved_spelling_count"]),
        candidate_spelling_count=int(learning_status["candidate_spelling_count"]),
        structured_db_path=str(PARSER_SERVICE.db_path),
        backend_log_file=str(BACKEND_LOG_FILE),
        backend_error_log_file=str(BACKEND_ERROR_LOG_FILE),
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


@app.post("/admin/training/promote-candidates")
def training_promote_candidates() -> dict[str, int]:
    try:
        result = TRAINING_SERVICE.promote_candidate_learning()
        _refresh_structured_cache()
        return result
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("training_promote_candidates failed")
        raise HTTPException(status_code=500, detail=f"Failed to promote candidate learning: {exc}") from exc


@app.get("/admin/training/candidates", response_model=CandidateLearningResponse)
def training_candidate_learning() -> CandidateLearningResponse:
    try:
        payload = TRAINING_SERVICE.list_candidate_learning()
        return CandidateLearningResponse(
            patterns=[CandidateLearningItem(**row) for row in payload.get("patterns", [])],
            spellings=[CandidateLearningItem(**row) for row in payload.get("spellings", [])],
        )
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("training_candidate_learning failed")
        raise HTTPException(status_code=500, detail=f"Failed to load candidate learning: {exc}") from exc


@app.post("/admin/training/review-candidate")
def training_review_candidate(payload: CandidateReviewRequest) -> dict[str, str]:
    try:
        result = TRAINING_SERVICE.review_candidate_learning(
            candidate_type=payload.candidate_type,
            normalized_source=payload.normalized_source,
            mapped_value=payload.mapped_value,
            review_status=payload.review_status,
            review_note=payload.review_note,
        )
        _refresh_structured_cache()
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - runtime safety
        LOGGER.exception("training_review_candidate failed")
        raise HTTPException(status_code=500, detail=f"Failed to review candidate learning: {exc}") from exc


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
