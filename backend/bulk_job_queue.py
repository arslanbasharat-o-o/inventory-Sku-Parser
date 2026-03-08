#!/usr/bin/env python3
"""In-process queued bulk job manager for upload endpoints."""

from __future__ import annotations

import logging
import os
import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from .bulk_job_runner import run_legacy_inventory_job, run_structured_inventory_job

BulkJobKind = Literal["legacy", "structured"]
BulkJobStatus = Literal["queued", "running", "completed", "failed"]
LOGGER = logging.getLogger(__name__)

DEFAULT_BULK_JOB_RETENTION_SECONDS = max(
    300,
    int(os.getenv("SKU_BULK_JOB_RETENTION_SECONDS", "3600")),
)


@dataclass
class BulkJob:
    job_id: str
    kind: BulkJobKind
    input_file: str
    output_file: str
    title_column: str = "Product Name"
    status: BulkJobStatus = "queued"
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    error: str = ""
    rows_processed: int = 0


class BulkJobQueueManager:
    """Background queue for bulk jobs so uploads can be accepted immediately."""

    def __init__(self, *, worker_count: int, retention_seconds: int = DEFAULT_BULK_JOB_RETENTION_SECONDS) -> None:
        self.worker_count = max(1, int(worker_count))
        self.retention_seconds = max(300, int(retention_seconds))
        self._jobs: dict[str, BulkJob] = {}
        self._order: list[str] = []
        self._lock = threading.Lock()
        self._queue: queue.Queue[str] = queue.Queue()
        self._workers_started = False
        self._workers: list[threading.Thread] = []

    def _ensure_workers(self) -> None:
        with self._lock:
            if self._workers_started:
                return
            for index in range(self.worker_count):
                worker = threading.Thread(
                    target=self._worker_loop,
                    name=f"bulk-job-worker-{index+1}",
                    daemon=True,
                )
                worker.start()
                self._workers.append(worker)
            self._workers_started = True

    def _purge_expired_jobs(self) -> None:
        now = time.time()
        keep_order: list[str] = []
        for job_id in self._order:
            job = self._jobs.get(job_id)
            if job is None:
                continue
            finished_at = job.finished_at or job.created_at
            if job.status in {"completed", "failed"} and (now - finished_at) > self.retention_seconds:
                try:
                    Path(job.output_file).unlink(missing_ok=True)
                except Exception:
                    pass
                self._jobs.pop(job_id, None)
                continue
            keep_order.append(job_id)
        self._order = keep_order

    def submit_legacy_job(self, *, input_file: str | Path, output_file: str | Path) -> dict[str, Any]:
        return self._submit_job(
            kind="legacy",
            input_file=input_file,
            output_file=output_file,
        )

    def submit_structured_job(
        self,
        *,
        input_file: str | Path,
        output_file: str | Path,
        title_column: str = "Product Name",
    ) -> dict[str, Any]:
        return self._submit_job(
            kind="structured",
            input_file=input_file,
            output_file=output_file,
            title_column=title_column,
        )

    def _submit_job(
        self,
        *,
        kind: BulkJobKind,
        input_file: str | Path,
        output_file: str | Path,
        title_column: str = "Product Name",
    ) -> dict[str, Any]:
        self._ensure_workers()
        input_path = str(Path(input_file))
        output_path = str(Path(output_file))
        job = BulkJob(
            job_id=uuid.uuid4().hex,
            kind=kind,
            input_file=input_path,
            output_file=output_path,
            title_column=title_column,
        )
        with self._lock:
            self._purge_expired_jobs()
            self._jobs[job.job_id] = job
            self._order.append(job.job_id)
            queue_position = self._queued_jobs_ahead(job.job_id)
        LOGGER.info(
            "Queued bulk job id=%s kind=%s queue_position=%s input=%s output=%s",
            job.job_id,
            job.kind,
            queue_position,
            input_path,
            output_path,
        )
        self._queue.put(job.job_id)
        return self._snapshot(job, queue_position=queue_position)

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            self._purge_expired_jobs()
            job = self._jobs.get(job_id)
            if job is None:
                return None
            queue_position = self._queued_jobs_ahead(job_id) if job.status == "queued" else 0
            return self._snapshot(job, queue_position=queue_position)

    def _queued_jobs_ahead(self, job_id: str) -> int:
        position = 0
        for current_id in self._order:
            if current_id == job_id:
                break
            current = self._jobs.get(current_id)
            if current and current.status == "queued":
                position += 1
        return position

    def _snapshot(self, job: BulkJob, *, queue_position: int = 0) -> dict[str, Any]:
        return {
            "job_id": job.job_id,
            "status": job.status,
            "kind": job.kind,
            "queue_position": max(0, int(queue_position)),
            "created_at": job.created_at,
            "started_at": job.started_at,
            "finished_at": job.finished_at,
            "error": job.error,
            "rows_processed": job.rows_processed,
            "download_file": Path(job.output_file).name if job.status == "completed" else "",
            "poll_interval_ms": 1000,
        }

    def _worker_loop(self) -> None:
        while True:
            job_id = self._queue.get()
            try:
                with self._lock:
                    job = self._jobs.get(job_id)
                    if job is None:
                        continue
                    job.status = "running"
                    job.started_at = time.time()
                LOGGER.info(
                    "Starting bulk job id=%s kind=%s input=%s output=%s",
                    job.job_id,
                    job.kind,
                    job.input_file,
                    job.output_file,
                )

                if job.kind == "legacy":
                    result = run_legacy_inventory_job(
                        input_file=job.input_file,
                        output_file=job.output_file,
                    )
                else:
                    result = run_structured_inventory_job(
                        input_file=job.input_file,
                        output_file=job.output_file,
                        title_column=job.title_column,
                    )

                with self._lock:
                    current = self._jobs.get(job_id)
                    if current is None:
                        continue
                    current.status = "completed"
                    current.rows_processed = int(result.get("rows_processed", 0))
                    current.finished_at = time.time()
                LOGGER.info(
                    "Completed bulk job id=%s kind=%s rows=%s elapsed=%.3fs",
                    job.job_id,
                    job.kind,
                    int(result.get("rows_processed", 0)),
                    (current.finished_at or time.time()) - (job.started_at or job.created_at),
                )
            except Exception as exc:  # pragma: no cover - worker safety
                with self._lock:
                    current = self._jobs.get(job_id)
                    if current is not None:
                        current.status = "failed"
                        current.error = str(exc)
                        current.finished_at = time.time()
                LOGGER.exception(
                    "Bulk job failed id=%s kind=%s input=%s output=%s",
                    job.job_id,
                    job.kind,
                    job.input_file,
                    job.output_file,
                )
                try:
                    Path(job.output_file).unlink(missing_ok=True)
                except Exception:
                    pass
            finally:
                try:
                    Path(job.input_file).unlink(missing_ok=True)
                except Exception:
                    pass
                self._queue.task_done()
