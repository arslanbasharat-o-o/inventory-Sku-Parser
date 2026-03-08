#!/usr/bin/env python3
"""Persistent bulk inventory execution helpers."""

from __future__ import annotations

import atexit
import logging
import multiprocessing as mp
import os
import threading
import traceback
from concurrent.futures import ProcessPoolExecutor, TimeoutError as FutureTimeoutError
from concurrent.futures.process import BrokenProcessPool
from contextlib import contextmanager
from pathlib import Path
from threading import BoundedSemaphore
from typing import Any, Callable

import pandas as pd

from .logging_utils import configure_backend_logging

configure_backend_logging()

DEFAULT_BULK_JOB_TIMEOUT_SECONDS = max(
    30,
    int(os.getenv("SKU_BULK_JOB_TIMEOUT_SECONDS", "180")),
)
MAX_CONCURRENT_BULK_JOBS = max(
    1,
    int(
        os.getenv(
            "MAX_CONCURRENT_BULK_JOBS",
            os.getenv("MAX_CONCURRENT_PARSE_JOBS", str(min(4, os.cpu_count() or 4))),
        )
    ),
)

_BULK_JOB_SEMAPHORE = BoundedSemaphore(value=MAX_CONCURRENT_BULK_JOBS)
_POOL_LOCK = threading.Lock()
_POOL_EXECUTOR: ProcessPoolExecutor | None = None
_WORKER_STRUCTURED_SERVICE: Any | None = None
LOGGER = logging.getLogger(__name__)


class BulkJobError(RuntimeError):
    """Raised when an isolated bulk job fails."""


class BulkJobTimeoutError(TimeoutError):
    """Raised when an isolated bulk job exceeds its wall-clock budget."""


@contextmanager
def bulk_job_slot():
    """Best-effort backpressure guard for expensive direct bulk jobs."""
    acquired = _BULK_JOB_SEMAPHORE.acquire(blocking=False)
    try:
        yield acquired
    finally:
        if acquired:
            _BULK_JOB_SEMAPHORE.release()


def _terminate_executor(executor: ProcessPoolExecutor | None) -> None:
    if executor is None:
        return
    processes = getattr(executor, "_processes", None)
    if isinstance(processes, dict):
        for process in tuple(processes.values()):
            try:
                if process.is_alive():
                    process.terminate()
            except Exception:
                pass
        for process in tuple(processes.values()):
            try:
                process.join(timeout=1)
            except Exception:
                pass
        for process in tuple(processes.values()):
            try:
                if process.is_alive():
                    process.kill()
            except Exception:
                pass
    try:
        executor.shutdown(wait=False, cancel_futures=True)
    except Exception:
        pass


def _shutdown_pool() -> None:
    global _POOL_EXECUTOR
    with _POOL_LOCK:
        executor = _POOL_EXECUTOR
        _POOL_EXECUTOR = None
    _terminate_executor(executor)


atexit.register(_shutdown_pool)


def _get_pool() -> ProcessPoolExecutor:
    global _POOL_EXECUTOR
    with _POOL_LOCK:
        if _POOL_EXECUTOR is None:
            _POOL_EXECUTOR = ProcessPoolExecutor(
                max_workers=MAX_CONCURRENT_BULK_JOBS,
                mp_context=mp.get_context("spawn"),
                initializer=_pool_initializer,
            )
        return _POOL_EXECUTOR


def _pool_initializer() -> None:
    global _WORKER_STRUCTURED_SERVICE
    configure_backend_logging()
    from . import sku_parser  # noqa: F401

    _WORKER_STRUCTURED_SERVICE = None


def _warmup_task() -> int:
    from . import sku_parser  # noqa: F401

    return os.getpid()


def _run_legacy_inventory_task(input_file: str, output_file: str) -> dict[str, Any]:
    try:
        from .sku_parser import process_inventory

        result_df = process_inventory(input_file, output_file)
        return {"ok": True, "rows_processed": int(len(result_df))}
    except Exception as exc:  # pragma: no cover - worker safety
        return {
            "ok": False,
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }


def _run_structured_inventory_task(
    input_file: str,
    output_file: str,
    title_column: str,
) -> dict[str, Any]:
    try:
        global _WORKER_STRUCTURED_SERVICE
        from .structured_sku_parser import StructuredSKUParserService

        if _WORKER_STRUCTURED_SERVICE is None:
            _WORKER_STRUCTURED_SERVICE = StructuredSKUParserService()
        result_df = _WORKER_STRUCTURED_SERVICE.process_inventory_excel(
            input_file=input_file,
            output_file=output_file,
            title_column=title_column,
        )
        return {"ok": True, "rows_processed": int(len(result_df))}
    except Exception as exc:  # pragma: no cover - worker safety
        return {
            "ok": False,
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }


def _coerce_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if not payload:
        raise BulkJobError("Bulk parser worker finished without a result payload.")
    if payload.get("ok"):
        return payload
    detail = str(payload.get("error", "Unknown bulk parser error"))
    tb = str(payload.get("traceback", "")).strip()
    if tb:
        raise BulkJobError(f"{detail}\n{tb}")
    raise BulkJobError(detail)


def _run_bulk_job(
    worker: Callable[..., dict[str, Any]],
    *worker_args: object,
    timeout_seconds: int | None = None,
) -> dict[str, Any]:
    timeout_budget = timeout_seconds or DEFAULT_BULK_JOB_TIMEOUT_SECONDS
    executor = _get_pool()
    LOGGER.info(
        "Submitting bulk task worker=%s timeout=%ss args=%s",
        getattr(worker, "__name__", "unknown"),
        timeout_budget,
        tuple(str(arg) for arg in worker_args),
    )
    future = executor.submit(worker, *worker_args)
    try:
        payload = _coerce_payload(future.result(timeout=timeout_budget))
        LOGGER.info(
            "Bulk task finished worker=%s rows=%s",
            getattr(worker, "__name__", "unknown"),
            payload.get("rows_processed", 0),
        )
        return payload
    except FutureTimeoutError as exc:
        LOGGER.exception(
            "Bulk task timed out worker=%s timeout=%ss",
            getattr(worker, "__name__", "unknown"),
            timeout_budget,
        )
        _shutdown_pool()
        raise BulkJobTimeoutError(f"Bulk parsing exceeded {timeout_budget} seconds.") from exc
    except BrokenProcessPool as exc:
        LOGGER.exception(
            "Bulk worker pool crashed while running worker=%s",
            getattr(worker, "__name__", "unknown"),
        )
        _shutdown_pool()
        raise BulkJobError("Bulk parser worker pool crashed and was reset.") from exc
    except Exception:
        LOGGER.exception(
            "Bulk task failed worker=%s",
            getattr(worker, "__name__", "unknown"),
        )
        if future.cancel():
            raise
        raise


def run_legacy_inventory_job(
    input_file: str | Path,
    output_file: str | Path,
    *,
    timeout_seconds: int | None = None,
) -> dict[str, Any]:
    return _run_bulk_job(
        _run_legacy_inventory_task,
        str(Path(input_file)),
        str(Path(output_file)),
        timeout_seconds=timeout_seconds,
    )


def run_structured_inventory_job(
    input_file: str | Path,
    output_file: str | Path,
    *,
    title_column: str = "Product Name",
    timeout_seconds: int | None = None,
) -> dict[str, Any]:
    return _run_bulk_job(
        _run_structured_inventory_task,
        str(Path(input_file)),
        str(Path(output_file)),
        title_column,
        timeout_seconds=timeout_seconds,
    )


def load_processed_inventory_preview(output_file: str | Path) -> pd.DataFrame:
    output_path = Path(output_file)
    if output_path.suffix.lower() == ".csv":
        return pd.read_csv(output_path)
    return pd.read_excel(output_path, engine="openpyxl")


def warm_bulk_worker_pool() -> None:
    executor = _get_pool()
    warm_count = max(1, MAX_CONCURRENT_BULK_JOBS)
    futures = [executor.submit(_warmup_task) for _ in range(warm_count)]
    for future in futures:
        future.result(timeout=DEFAULT_BULK_JOB_TIMEOUT_SECONDS)
