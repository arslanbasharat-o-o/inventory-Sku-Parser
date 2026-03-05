#!/usr/bin/env python3
"""Web frontend for the SKU parser."""

from __future__ import annotations

import os
import uuid
import json
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from threading import BoundedSemaphore

import pandas as pd
from flask import Flask, flash, redirect, render_template, request, send_file, url_for
from werkzeug.utils import secure_filename

from sku_parser import NOT_UNDERSTANDABLE, UNKNOWN_LOG_FILE, generate_sku, process_inventory


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("SKU_PARSER_DATA_DIR", str(BASE_DIR))).resolve()
UPLOAD_DIR = DATA_DIR / "uploads"
OUTPUT_DIR = DATA_DIR / "outputs"
LEGACY_SUGGESTION_FILE = BASE_DIR / "semantic_mapping_suggestions.log"
ALLOWED_EXTENSIONS = {".xlsx", ".xlsm", ".xls"}
MAX_UPLOAD_SIZE_BYTES = 20 * 1024 * 1024  # 20 MB
MAX_CONCURRENT_PARSE_JOBS = max(1, int(os.getenv("MAX_CONCURRENT_PARSE_JOBS", "2")))
PARSE_JOB_SEMAPHORE = BoundedSemaphore(value=MAX_CONCURRENT_PARSE_JOBS)


@contextmanager
def parse_job_slot():
    """Best-effort backpressure guard for expensive parse jobs."""
    acquired = PARSE_JOB_SEMAPHORE.acquire(blocking=False)
    try:
        yield acquired
    finally:
        if acquired:
            PARSE_JOB_SEMAPHORE.release()


def create_app() -> Flask:
    """App factory for local/prod usage."""
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_SIZE_BYTES
    app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "sku-parser-dev-key")
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    @app.route("/", methods=["GET", "POST"])
    def index():
        cleanup_old_files()
        if request.method == "GET":
            return render_template(
                "index.html",
                processed=False,
                columns=[],
                rows=[],
                stats={},
                semantic_suggestions=load_semantic_suggestions(limit=20),
                preview_meta={"row_count": 0, "column_count": 0},
                generated_at="",
                download_url="",
            )

        uploaded_file = request.files.get("inventory_file")
        if uploaded_file is None or not uploaded_file.filename:
            flash("Please select an Excel file before processing.", "error")
            return redirect(url_for("index"))

        input_name = secure_filename(uploaded_file.filename)
        extension = Path(input_name).suffix.lower()
        if extension not in ALLOWED_EXTENSIONS:
            flash("Invalid file type. Upload .xlsx, .xlsm, or .xls file.", "error")
            return redirect(url_for("index"))

        file_id = uuid.uuid4().hex
        input_path = UPLOAD_DIR / f"{file_id}_{input_name}"
        output_path = OUTPUT_DIR / f"{file_id}_products_sku_processed.xlsx"

        with parse_job_slot() as acquired:
            if not acquired:
                flash(
                    "Server is currently busy processing other files. Please retry in a moment.",
                    "error",
                )
                return redirect(url_for("index"))
            try:
                uploaded_file.save(input_path)
                output_df = process_inventory(input_path, output_path)
            except Exception as exc:  # pragma: no cover - graceful UI error path
                flash(f"Processing failed: {exc}", "error")
                return redirect(url_for("index"))

        preview_df = output_df.fillna("")
        stats = build_stats(output_df)
        semantic_suggestions = load_semantic_suggestions(limit=20)
        preview_meta = {
            "row_count": int(len(preview_df)),
            "column_count": int(len(preview_df.columns)),
        }
        return render_template(
            "index.html",
            processed=True,
            columns=list(preview_df.columns),
            rows=preview_df.to_dict(orient="records"),
            stats=stats,
            semantic_suggestions=semantic_suggestions,
            preview_meta=preview_meta,
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            download_url=url_for("download_file", file_name=output_path.name),
        )

    @app.route("/parse-inventory-api", methods=["POST"])
    def parse_inventory_api():
        cleanup_old_files()
        uploaded_file = request.files.get("inventory_file")
        if uploaded_file is None or not uploaded_file.filename:
            return {"error": "No file included"}, 400

        input_name = secure_filename(uploaded_file.filename)
        extension = Path(input_name).suffix.lower()
        if extension not in ALLOWED_EXTENSIONS and extension != ".csv":
            return {"error": "Invalid file type"}, 400

        file_id = uuid.uuid4().hex
        input_path = UPLOAD_DIR / f"{file_id}_{input_name}"
        output_path = OUTPUT_DIR / f"{file_id}_products_sku_processed.xlsx"

        with parse_job_slot() as acquired:
            if not acquired:
                return {
                    "error": "Server is busy. Too many concurrent parse jobs. Retry shortly."
                }, 429
            try:
                uploaded_file.save(input_path)
                output_df = process_inventory(input_path, output_path)
            except Exception as exc:
                return {"error": f"Processing failed: {exc}"}, 500

        preview_df = output_df.fillna("")
        stats = build_stats(output_df)
        
        return {
            "columns": list(preview_df.columns),
            "rows": preview_df.to_dict(orient="records"),
            "stats": stats,
            "download_file": output_path.name
        }

    @app.route("/download/<path:file_name>", methods=["GET"])
    def download_file(file_name: str):
        safe_name = secure_filename(file_name)
        file_path = OUTPUT_DIR / safe_name
        if not file_path.exists():
            flash("Processed file was not found. Please process again.", "error")
            return redirect(url_for("index"))
        return send_file(
            file_path,
            as_attachment=True,
            download_name="products_sku_processed.xlsx",
        )

    @app.route("/generate-sku-api", methods=["POST"])
    def generate_sku_api():
        payload = request.get_json(silent=True) or {}
        title = str(payload.get("title", "")).strip()
        product_sku = str(payload.get("product_sku", "")).strip()
        product_web_sku = str(payload.get("product_web_sku", "")).strip()

        if not title and not product_sku and not product_web_sku:
            return {"error": "Provide at least a title or SKU hint."}, 400

        try:
            sku = generate_sku(title, product_sku, product_web_sku)
        except Exception as exc:
            return {"error": f"Failed to generate SKU: {exc}"}, 500

        return {
            "title": title,
            "product_sku": product_sku,
            "product_web_sku": product_web_sku,
            "generated_sku": sku,
            "parse_status": "parsed" if sku != NOT_UNDERSTANDABLE else "not_understandable",
        }

    @app.route("/healthz", methods=["GET"])
    def healthz():
        return {
            "status": "ok",
            "service": "sku-parser-api",
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        }

    @app.route("/readyz", methods=["GET"])
    def readyz():
        ready = UPLOAD_DIR.exists() and OUTPUT_DIR.exists()
        status_code = 200 if ready else 503
        return {
            "status": "ready" if ready else "not_ready",
            "max_concurrent_parse_jobs": MAX_CONCURRENT_PARSE_JOBS,
        }, status_code

    @app.errorhandler(413)
    def file_too_large(_error):
        flash("File is too large. Max allowed size is 20 MB.", "error")
        return redirect(url_for("index"))

    return app


def build_stats(df: pd.DataFrame) -> dict[str, int | float]:
    """Build UI summary metrics for quick validation."""
    parsed_mask = df["Product New SKU"].astype(str).ne(NOT_UNDERSTANDABLE)
    sku_dup_mask = df["SKU Duplicate"].astype(str).eq("DUPLICATED")
    title_dup_mask = df["Title Duplicate"].astype(str).eq("DUPLICATED")
    total_rows = int(len(df))
    parsed_rows = int(parsed_mask.sum())
    parse_rate = round((parsed_rows / total_rows) * 100, 2) if total_rows else 0.0
    return {
        "total_rows": total_rows,
        "parsed_rows": parsed_rows,
        "unparsed_rows": int((~parsed_mask).sum()),
        "parse_rate": parse_rate,
        "sku_duplicates": int(sku_dup_mask.sum()),
        "title_duplicates": int(title_dup_mask.sum()),
    }


def load_semantic_suggestions(limit: int = 20) -> list[dict[str, str]]:
    """Load repeated semantic patterns inferred by the parser."""
    suggestions = _load_semantic_suggestions_from_json(limit=limit)
    if suggestions:
        return suggestions
    return _load_semantic_suggestions_from_legacy_log(limit=limit)


def _load_semantic_suggestions_from_json(limit: int) -> list[dict[str, str]]:
    if not UNKNOWN_LOG_FILE.exists():
        return []

    try:
        with UNKNOWN_LOG_FILE.open("r", encoding="utf-8") as file_obj:
            data = json.load(file_obj)
    except Exception:
        return []

    if not isinstance(data, list) or not data:
        return []

    suggestions: list[dict[str, str]] = []
    for row in reversed(data[-limit:]):
        pattern = str(row.get("title_pattern", "")).strip()
        part = str(row.get("suggested_code", "")).strip()
        count = str(row.get("count", "")).strip()
        if not pattern or not part:
            continue
        suggestions.append({"pattern": pattern, "part": part, "count": count or "1"})
    return suggestions


def _load_semantic_suggestions_from_legacy_log(limit: int) -> list[dict[str, str]]:
    if not LEGACY_SUGGESTION_FILE.exists():
        return []

    try:
        raw_lines = LEGACY_SUGGESTION_FILE.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []

    suggestions: list[dict[str, str]] = []
    for line in reversed(raw_lines[-limit:]):
        parts = [segment.strip() for segment in line.split("\t") if segment.strip()]
        if len(parts) < 2:
            continue
        pattern = parts[0]
        part = parts[1]
        count = "1"
        for segment in parts[2:]:
            if segment.lower().startswith("count="):
                count = segment.split("=", 1)[1].strip() or "1"
                break
        suggestions.append({"pattern": pattern, "part": part, "count": count})
    return suggestions


def cleanup_old_files(max_age_hours: int = 24) -> None:
    """Remove stale uploads/outputs to keep local disk usage stable."""
    threshold = datetime.now() - timedelta(hours=max_age_hours)
    for directory in (UPLOAD_DIR, OUTPUT_DIR):
        for file_path in directory.glob("*"):
            if not file_path.is_file():
                continue
            modified = datetime.fromtimestamp(file_path.stat().st_mtime)
            if modified < threshold:
                try:
                    file_path.unlink(missing_ok=True)
                except OSError:
                    continue


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
