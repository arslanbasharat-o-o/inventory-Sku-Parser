#!/usr/bin/env python3
"""Web frontend for the SKU parser."""

from __future__ import annotations

import os
import uuid
import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from flask import Flask, flash, redirect, render_template, request, send_file, url_for
from werkzeug.utils import secure_filename

from .bulk_job_runner import (
    BulkJobError,
    BulkJobTimeoutError,
    MAX_CONCURRENT_BULK_JOBS,
    bulk_job_slot as parse_job_slot,
    load_processed_inventory_preview,
    run_legacy_inventory_job,
)
from .structured_sku_parser import StructuredParseError, StructuredSKUParserService
from .sku_parser import (
    NOT_UNDERSTANDABLE,
    UNKNOWN_LOG_FILE,
    analyze_title,
    generate_sku,
)
from .training_dashboard_service import TrainingDashboardService

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.getenv("SKU_PARSER_DATA_DIR", str(BASE_DIR))).resolve()
UPLOAD_DIR = DATA_DIR / "uploads"
OUTPUT_DIR = DATA_DIR / "outputs"
LEGACY_SUGGESTION_FILE = BASE_DIR / "semantic_mapping_suggestions.log"
ALLOWED_EXTENSIONS = {".xlsx", ".xlsm", ".xls"}
MAX_UPLOAD_SIZE_BYTES = 20 * 1024 * 1024  # 20 MB
MAX_CONCURRENT_PARSE_JOBS = MAX_CONCURRENT_BULK_JOBS
AI_THRESHOLD = float(os.getenv("SKU_AI_SINGLE_THRESHOLD", os.getenv("SKU_AI_THRESHOLD", "0.40")))
RULE_ACCEPT_THRESHOLD = float(os.getenv("SKU_RULE_ACCEPT_THRESHOLD", "0.80"))
REVIEW_THRESHOLD = float(os.getenv("SKU_REVIEW_THRESHOLD", "0.75"))

def create_app() -> Flask:
    """App factory for local/prod usage."""
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_SIZE_BYTES
    app.config["SECRET_KEY"] = os.getenv("FLASK_SECRET_KEY", "sku-parser-dev-key")
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    training_service = TrainingDashboardService(
        structured_log_db_path=Path(os.getenv("SKU_PARSE_DB_PATH", "outputs/structured_sku_results.db"))
    )
    structured_parser_service = StructuredSKUParserService(
        ai_threshold=AI_THRESHOLD,
        review_threshold=REVIEW_THRESHOLD,
        rule_accept_threshold=RULE_ACCEPT_THRESHOLD,
        cache_size=int(os.getenv("SKU_PARSE_CACHE_SIZE", "50000")),
        db_path=os.getenv("SKU_PARSE_DB_PATH", "outputs/structured_sku_results.db"),
        enable_ai=True,
    )

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
                run_legacy_inventory_job(input_file=input_path, output_file=output_path)
                output_df = load_processed_inventory_preview(output_path)
            except BulkJobTimeoutError as exc:
                output_path.unlink(missing_ok=True)
                flash(str(exc), "error")
                return redirect(url_for("index"))
            except BulkJobError as exc:
                output_path.unlink(missing_ok=True)
                flash(f"Processing failed: {exc}", "error")
                return redirect(url_for("index"))
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
                run_legacy_inventory_job(input_file=input_path, output_file=output_path)
                output_df = load_processed_inventory_preview(output_path)
            except BulkJobTimeoutError as exc:
                output_path.unlink(missing_ok=True)
                return {"error": str(exc)}, 504
            except BulkJobError as exc:
                output_path.unlink(missing_ok=True)
                return {"error": f"Processing failed: {exc}"}, 500
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
        product_description = str(payload.get("product_description", "")).strip()

        if not title and not product_sku and not product_web_sku and not product_description:
            return {"error": "Provide at least a title or SKU hint."}, 400

        try:
            sku = generate_sku(title, product_sku, product_web_sku, product_description)
        except Exception as exc:
            return {"error": f"Failed to generate SKU: {exc}"}, 500

        return {
            "title": title,
            "product_sku": product_sku,
            "product_web_sku": product_web_sku,
            "product_description": product_description,
            "generated_sku": sku,
            "parse_status": "parsed" if sku != NOT_UNDERSTANDABLE else "not_understandable",
        }

    @app.route("/analyze-title", methods=["POST"])
    def analyze_title_api():
        payload = request.get_json(silent=True) or {}
        title = str(payload.get("title", "")).strip()
        product_sku = str(payload.get("product_sku", "")).strip()
        product_web_sku = str(payload.get("product_web_sku", "")).strip()
        product_description = str(payload.get("product_description", "")).strip()

        if not title and not product_sku and not product_web_sku and not product_description:
            return {"error": "Provide at least a title or SKU hint."}, 400

        try:
            execution = structured_parser_service.analyze_title(
                title=title,
                product_sku=product_sku,
                product_web_sku=product_web_sku,
                product_description=product_description,
            )
        except StructuredParseError:
            try:
                parsed = analyze_title(
                    title,
                    product_sku_hint=product_sku,
                    product_web_sku_hint=product_web_sku,
                    product_description_hint=product_description,
                )
                confidence = float(parsed.get("confidence", 0.0) or 0.0)
                part = str(parsed.get("part", "")).strip().upper()
                secondary_part = str(parsed.get("secondary_part", "")).strip().upper()
                parse_status = str(parsed.get("parse_status", "not_understandable")).strip().lower()
                raw_corrections = parsed.get("corrections", [])
                correction_pairs = []
                if isinstance(raw_corrections, list):
                    for item in raw_corrections:
                        if not isinstance(item, dict):
                            continue
                        from_token = str(item.get("from", "")).strip()
                        to_token = str(item.get("to", "")).strip()
                        if from_token or to_token:
                            correction_pairs.append({"from": from_token, "to": to_token})
                return {
                    "brand": str(parsed.get("brand", "")).strip().upper(),
                    "model": str(parsed.get("model", "")).strip().upper(),
                    "model_code": str(parsed.get("model_code", "")).strip().upper(),
                    "primary_part": part,
                    "part": part,
                    "secondary_part": secondary_part or None,
                    "variant": str(parsed.get("variant", "")).strip().upper() or None,
                    "color": str(parsed.get("color", "")).strip().upper() or None,
                    "sku": "" if parse_status == "partial" else str(parsed.get("sku", NOT_UNDERSTANDABLE)).strip().upper(),
                    "confidence": confidence,
                    "rule_confidence": confidence,
                    "final_confidence": confidence,
                    "corrections": raw_corrections if isinstance(raw_corrections, list) else [],
                    "correction_pairs": correction_pairs,
                    "interpreted_title": str(parsed.get("interpreted_title", title)).strip(),
                    "parser_reason": f"{str(parsed.get('reason', 'rule_parser')).strip()}+ai_unavailable_fallback",
                    "source": "rule",
                    "review_required": confidence < REVIEW_THRESHOLD,
                    "needs_review": confidence < REVIEW_THRESHOLD,
                    "decision": str(parsed.get("decision", "")).strip(),
                    "parse_status": parse_status,
                    "parse_stage": "rule_only" if confidence >= RULE_ACCEPT_THRESHOLD else "rule_normalized",
                    "ai_used": False,
                    "validation_failed_reason": "ai_unavailable_fallback",
                }
            except Exception as exc:
                return {"error": f"Failed to analyze title: {exc}"}, 500
        except ValueError as exc:
            return {"error": str(exc)}, 400
        except Exception as exc:
            return {"error": f"Failed to analyze title: {exc}"}, 500

        parsed = execution.parsed
        correction_pairs = []
        for item in execution.correction_pairs:
            if not isinstance(item, dict):
                continue
            from_token = str(item.get("from", "")).strip()
            to_token = str(item.get("to", "")).strip()
            if from_token or to_token:
                correction_pairs.append({"from": from_token, "to": to_token})

        return {
            "brand": parsed.brand,
            "model": parsed.model,
            "model_code": parsed.model_code,
            "primary_part": parsed.primary_part,
            "part": parsed.primary_part,
            "secondary_part": parsed.secondary_part,
            "variant": parsed.variant,
            "color": parsed.color,
            "sku": "" if execution.parse_status == "partial" else parsed.sku,
            "confidence": float(parsed.confidence),
            "rule_confidence": float(parsed.rule_confidence),
            "final_confidence": float(parsed.final_confidence or parsed.confidence),
            "corrections": list(parsed.corrections),
            "correction_pairs": correction_pairs,
            "interpreted_title": title,
            "parser_reason": execution.parser_reason,
            "source": execution.source,
            "review_required": execution.review_required,
            "needs_review": execution.review_required,
            "decision": "",
            "parse_status": execution.parse_status,
            "parse_stage": execution.parse_stage,
            "ai_used": execution.ai_used,
            "validation_failed_reason": execution.validation_failed_reason,
        }

    @app.route("/healthz", methods=["GET"])
    def healthz():
        return {
            "status": "ok",
            "service": "sku-parser-api",
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        }

    @app.route("/admin/training/bootstrap", methods=["GET"])
    def admin_training_bootstrap():
        try:
            return training_service.get_bootstrap()
        except Exception as exc:
            return {"error": f"Failed to load training dashboard data: {exc}"}, 500

    @app.route("/admin/training/promote-candidates", methods=["POST"])
    def admin_training_promote_candidates():
        try:
            return training_service.promote_candidate_learning()
        except Exception as exc:
            return {"error": f"Failed to promote candidate learning: {exc}"}, 500

    @app.route("/admin/training/analytics", methods=["GET"])
    def admin_training_analytics():
        try:
            return training_service.get_analytics()
        except Exception as exc:
            return {"error": f"Failed to load analytics: {exc}"}, 500

    @app.route("/admin/training/title-training", methods=["POST"])
    def admin_training_title_training():
        payload = request.get_json(silent=True) or {}
        try:
            product_title = str(payload.get("product_title", "")).strip()
            expected_sku = str(payload.get("expected_sku", "")).strip()
            if not product_title or not expected_sku:
                return {"error": "product_title and expected_sku are required."}, 400
            return training_service.add_title_training_sample(
                product_title=product_title,
                detected_model=str(payload.get("detected_model", "")),
                detected_part=str(payload.get("detected_part", "")),
                detected_color=str(payload.get("detected_color", "")),
                expected_sku=expected_sku,
            )
        except ValueError as exc:
            return {"error": str(exc)}, 400
        except Exception as exc:
            return {"error": f"Failed to save title training sample: {exc}"}, 500

    @app.route("/admin/training/synonym", methods=["POST"])
    def admin_training_synonym():
        payload = request.get_json(silent=True) or {}
        try:
            supplier_phrase = str(payload.get("supplier_phrase", "")).strip()
            standard_term = str(payload.get("standard_term", "")).strip()
            if not supplier_phrase or not standard_term:
                return {"error": "supplier_phrase and standard_term are required."}, 400
            return training_service.add_synonym_mapping(
                supplier_phrase=supplier_phrase,
                standard_term=standard_term,
            )
        except ValueError as exc:
            return {"error": str(exc)}, 400
        except Exception as exc:
            return {"error": f"Failed to save synonym mapping: {exc}"}, 500

    @app.route("/admin/training/spelling", methods=["POST"])
    def admin_training_spelling():
        payload = request.get_json(silent=True) or {}
        try:
            incorrect_word = str(payload.get("incorrect_word", "")).strip()
            correct_word = str(payload.get("correct_word", "")).strip()
            if not incorrect_word or not correct_word:
                return {"error": "incorrect_word and correct_word are required."}, 400
            return training_service.add_spelling_correction(
                incorrect_word=incorrect_word,
                correct_word=correct_word,
            )
        except ValueError as exc:
            return {"error": str(exc)}, 400
        except Exception as exc:
            return {"error": f"Failed to save spelling correction: {exc}"}, 500

    @app.route("/admin/training/part-ontology", methods=["POST"])
    def admin_training_part_ontology():
        payload = request.get_json(silent=True) or {}
        try:
            phrase = str(payload.get("phrase", "")).strip()
            sku_code = str(payload.get("sku_code", "")).strip()
            if not phrase or not sku_code:
                return {"error": "phrase and sku_code are required."}, 400
            return training_service.add_part_mapping(phrase=phrase, sku_code=sku_code)
        except ValueError as exc:
            return {"error": str(exc)}, 400
        except Exception as exc:
            return {"error": f"Failed to save part mapping: {exc}"}, 500

    @app.route("/admin/training/color", methods=["POST"])
    def admin_training_color():
        payload = request.get_json(silent=True) or {}
        try:
            supplier_color = str(payload.get("supplier_color", "")).strip()
            standard_color = str(payload.get("standard_color", "")).strip()
            if not supplier_color or not standard_color:
                return {"error": "supplier_color and standard_color are required."}, 400
            return training_service.add_color_mapping(
                supplier_color=supplier_color,
                standard_color=standard_color,
            )
        except ValueError as exc:
            return {"error": str(exc)}, 400
        except Exception as exc:
            return {"error": f"Failed to save color mapping: {exc}"}, 500

    @app.route("/admin/training/sku-correction", methods=["POST"])
    def admin_training_sku_correction():
        payload = request.get_json(silent=True) or {}
        try:
            generated_sku = str(payload.get("generated_sku", "")).strip()
            correct_sku = str(payload.get("correct_sku", "")).strip()
            title = str(payload.get("title", "")).strip()
            if not generated_sku or not correct_sku:
                return {"error": "generated_sku and correct_sku are required."}, 400
            return training_service.add_sku_correction(
                generated_sku=generated_sku,
                correct_sku=correct_sku,
                title=title,
            )
        except ValueError as exc:
            return {"error": str(exc)}, 400
        except Exception as exc:
            return {"error": f"Failed to save SKU correction: {exc}"}, 500

    @app.route("/admin/training/rule", methods=["POST"])
    def admin_training_rule():
        payload = request.get_json(silent=True) or {}
        try:
            rule_text = str(payload.get("rule_text", "")).strip()
            if not rule_text:
                return {"error": "rule_text is required."}, 400
            return training_service.add_rule(rule_text=rule_text)
        except ValueError as exc:
            return {"error": str(exc)}, 400
        except Exception as exc:
            return {"error": f"Failed to save rule: {exc}"}, 500

    @app.route("/admin/training/live-test", methods=["POST"])
    def admin_training_live_test():
        payload = request.get_json(silent=True) or {}
        title = str(payload.get("title", "")).strip()
        if not title:
            return {"error": "title is required."}, 400
        try:
            return training_service.live_test(title)
        except Exception as exc:
            return {"error": f"Live test failed: {exc}"}, 500

    @app.route("/admin/training/upload-dataset", methods=["POST"])
    def admin_training_upload_dataset():
        uploaded_file = request.files.get("file")
        if uploaded_file is None or not uploaded_file.filename:
            return {"error": "No file included."}, 400

        input_name = secure_filename(uploaded_file.filename)
        extension = Path(input_name).suffix.lower()
        if extension not in {".xlsx", ".xls", ".csv"}:
            return {"error": "Invalid file type. Upload .xlsx, .xls, or .csv."}, 400

        file_id = uuid.uuid4().hex
        input_path = UPLOAD_DIR / f"{file_id}_{input_name}"
        try:
            uploaded_file.save(input_path)
            return training_service.upload_training_dataset(input_path)
        except ValueError as exc:
            return {"error": str(exc)}, 400
        except Exception as exc:
            return {"error": f"Dataset upload training failed: {exc}"}, 500
        finally:
            input_path.unlink(missing_ok=True)

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
