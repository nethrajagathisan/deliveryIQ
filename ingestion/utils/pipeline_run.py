"""Context manager that records pipeline run metrics to ``pipeline_audit``.

Wrap an ingestion task's work in :class:`PipelineRun` to automatically time
it, assign it a ``run_id``, and write a row to
``deliveryiq_bronze.pipeline_audit`` when the block exits — whether it
succeeds, partially succeeds, or raises.

Usage::

    with PipelineRun(bq_loader, dag_id="ingestion_zomato", source="restaurants") as run:
        rows = loader.load_parquet_to_table(...)
        run.record_rows(processed=rows, rejected=0)
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from ingestion.utils.logger import get_logger

if TYPE_CHECKING:
    from ingestion.loaders.bq_loader import BigQueryLoader

log = get_logger(__name__)

AUDIT_TABLE_ID = "pipeline_audit"


class PipelineRun:
    """Times a pipeline task and records its outcome to ``pipeline_audit``."""

    def __init__(
        self,
        bq_loader: "BigQueryLoader",
        dag_id: str,
        source: str,
        task_id: str | None = None,
        gcs_uri: str | None = None,
    ) -> None:
        self.bq_loader = bq_loader
        self.dag_id = dag_id
        self.source_name = source
        self.task_id = task_id or source
        self.gcs_uri = gcs_uri

        self.run_id = str(uuid.uuid4())
        self.rows_processed = 0
        self.rows_rejected = 0

    def record_rows(self, processed: int, rejected: int = 0) -> None:
        """Record the number of rows processed and rejected by the task."""
        self.rows_processed = processed
        self.rows_rejected = rejected

    def __enter__(self) -> "PipelineRun":
        self._start = time.monotonic()
        self._run_date = datetime.now(timezone.utc)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        duration_seconds = time.monotonic() - self._start

        if exc_type is not None:
            status = "FAILURE"
            error_message = str(exc_val)
        elif self.rows_rejected > 0:
            status = "PARTIAL"
            error_message = None
        else:
            status = "SUCCESS"
            error_message = None

        row = {
            "run_id": self.run_id,
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "source_name": self.source_name,
            "run_date": self._run_date.isoformat(),
            "rows_processed": self.rows_processed,
            "rows_rejected": self.rows_rejected,
            "status": status,
            "error_message": error_message,
            "duration_seconds": duration_seconds,
            "gcs_uri": self.gcs_uri,
        }

        table_ref = self.bq_loader.dataset_ref.table(AUDIT_TABLE_ID)
        errors = self.bq_loader.client.insert_rows_json(table_ref, [row])

        if errors:
            log.error(
                "failed to write pipeline_audit row",
                extra={"extra_fields": {"run_id": self.run_id, "errors": errors}},
            )
        else:
            log.info("recorded pipeline run", extra={"extra_fields": row})

        return False
