"""Gunicorn config with CPU-aware defaults for horizontal scaling."""

from __future__ import annotations

import multiprocessing
import os


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


cpu_count = multiprocessing.cpu_count()
default_workers = min(max((cpu_count * 2) + 1, 2), 16)

bind = os.getenv("BIND", "0.0.0.0:5000")
workers = _env_int("WEB_CONCURRENCY", default_workers)
threads = _env_int("GUNICORN_THREADS", 2)
worker_class = os.getenv("GUNICORN_WORKER_CLASS", "gthread")
timeout = _env_int("GUNICORN_TIMEOUT", 180)
graceful_timeout = _env_int("GUNICORN_GRACEFUL_TIMEOUT", 30)
keepalive = _env_int("GUNICORN_KEEPALIVE", 5)
max_requests = _env_int("GUNICORN_MAX_REQUESTS", 1000)
max_requests_jitter = _env_int("GUNICORN_MAX_REQUESTS_JITTER", 100)
preload_app = os.getenv("GUNICORN_PRELOAD", "1") == "1"
loglevel = os.getenv("GUNICORN_LOG_LEVEL", "info")
accesslog = "-"
errorlog = "-"
