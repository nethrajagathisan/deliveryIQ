"""Structured JSON logging for the DeliveryIQ ingestion pipeline.

Every log line is emitted as a single JSON object so it can be parsed
directly by Cloud Logging / log shippers. A ``pipeline_run_id`` is attached
to every record to correlate all logs from a single pipeline execution.

Usage::

    from ingestion.utils.logger import get_logger

    log = get_logger(__name__)
    log.info("uploaded file", extra={"extra_fields": {"gcs_uri": uri, "rows": 1200}})
"""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

# Resolve the pipeline run id once at import time. Prefer the value injected
# by the orchestrator (Airflow) via PIPELINE_RUN_ID; otherwise generate one so
# logs are always correlatable, even in ad-hoc local runs.
PIPELINE_RUN_ID: str = os.getenv("PIPELINE_RUN_ID") or str(uuid.uuid4())

# Reserved attributes already present on every LogRecord. Anything outside this
# set that a caller attaches lands in the JSON "extra" object.
_RESERVED_RECORD_ATTRS = frozenset(
    {
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "module", "msecs",
        "message", "msg", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "thread", "threadName", "taskName",
        "extra_fields",
    }
)


class JsonFormatter(logging.Formatter):
    """Render a ``LogRecord`` as a single-line JSON document."""

    def format(self, record: logging.LogRecord) -> str:
        extra: dict = {}

        # Preferred path: caller passed extra={"extra_fields": {...}}.
        if isinstance(getattr(record, "extra_fields", None), dict):
            extra.update(record.extra_fields)

        # Convenience path: caller passed loose extra={...} kwargs.
        for key, value in record.__dict__.items():
            if key not in _RESERVED_RECORD_ATTRS and not key.startswith("_"):
                extra[key] = value

        payload = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "pipeline_run_id": PIPELINE_RUN_ID,
            "extra": extra,
        }

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, ensure_ascii=False)


def get_logger(name: str, level: int | str | None = None) -> logging.Logger:
    """Return a logger that emits structured JSON to stdout.

    Args:
        name: Logger name, conventionally ``__name__`` of the calling module.
        level: Optional log level (int or name). Defaults to the ``LOG_LEVEL``
            env var, or ``INFO``.

    Returns:
        A configured :class:`logging.Logger`. Repeated calls with the same name
        reuse the same logger without stacking duplicate handlers.
    """
    logger = logging.getLogger(name)

    resolved_level = level or os.getenv("LOG_LEVEL", "INFO")
    logger.setLevel(resolved_level)

    # Attach our handler exactly once per logger.
    if not any(getattr(h, "_deliveryiq_json", False) for h in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        handler._deliveryiq_json = True  # type: ignore[attr-defined]
        logger.addHandler(handler)

    # Do not also propagate to the root logger (avoids duplicate plain lines).
    logger.propagate = False
    return logger
