#!/usr/bin/env python3
"""Shared backend logging configuration."""

from __future__ import annotations

import logging
import os
from pathlib import Path

DEFAULT_LOG_DIR = Path(os.getenv("SKU_LOG_DIR", "logs"))
DEFAULT_BACKEND_LOG_FILE = DEFAULT_LOG_DIR / "backend.log"
DEFAULT_BACKEND_ERROR_LOG_FILE = DEFAULT_LOG_DIR / "backend.error.log"

_CONFIGURED = False


def configure_backend_logging() -> tuple[Path, Path]:
    """Configure root/backend/uvicorn logging once."""
    global _CONFIGURED

    log_dir = DEFAULT_LOG_DIR.resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    backend_log_file = DEFAULT_BACKEND_LOG_FILE.resolve()
    backend_error_log_file = DEFAULT_BACKEND_ERROR_LOG_FILE.resolve()

    if _CONFIGURED:
        return backend_log_file, backend_error_log_file

    log_level_name = str(os.getenv("SKU_LOG_LEVEL", "INFO")).strip().upper() or "INFO"
    log_level = getattr(logging, log_level_name, logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(processName)s:%(threadName)s] %(name)s: %(message)s"
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    file_handler = logging.FileHandler(backend_log_file, encoding="utf-8")
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    error_handler = logging.FileHandler(backend_error_log_file, encoding="utf-8")
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)

    existing_paths = {
        Path(getattr(handler, "baseFilename", "")).resolve()
        for handler in root_logger.handlers
        if getattr(handler, "baseFilename", "")
    }
    if backend_log_file not in existing_paths:
        root_logger.addHandler(file_handler)
    if backend_error_log_file not in existing_paths:
        root_logger.addHandler(error_handler)

    for logger_name in ("backend", "uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(logger_name)
        logger.setLevel(log_level)
        logger.propagate = True

    logging.captureWarnings(True)
    _CONFIGURED = True
    logging.getLogger(__name__).info(
        "Backend logging configured: log=%s error_log=%s level=%s",
        backend_log_file,
        backend_error_log_file,
        log_level_name,
    )
    return backend_log_file, backend_error_log_file

