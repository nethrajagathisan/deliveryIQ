"""Pipeline watermark tracking for incremental ingestion.

Watermarks are stored as a single JSON file in GCS at
``{bucket}/metadata/watermarks.json``, mapping source name to the last
successful run date. DAGs use this to decide how far back to re-ingest.

Static seed sources (IPL schedule, festival calendar) are reloaded in
full on every run (WRITE_TRUNCATE) and don't need watermarks for
correctness, but a watermark is still recorded for audit/freshness
tracking consistency with the other sources.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from ingestion.utils.exceptions import WatermarkError
from ingestion.utils.logger import get_logger

if TYPE_CHECKING:
    from ingestion.loaders.gcs_loader import GCSLoader

log = get_logger(__name__)

WATERMARK_BLOB_PATH = "metadata/watermarks.json"


class PipelineWatermark:
    """Reads and writes pipeline source watermarks stored as JSON in GCS."""

    def __init__(self, gcs_loader: "GCSLoader") -> None:
        self.gcs_loader = gcs_loader

    def _load(self) -> dict:
        blob = self.gcs_loader.bucket.blob(WATERMARK_BLOB_PATH)
        if not blob.exists():
            return {}

        try:
            return json.loads(blob.download_as_text())
        except json.JSONDecodeError as exc:
            raise WatermarkError(f"Watermark file at {WATERMARK_BLOB_PATH} is not valid JSON") from exc

    def _save(self, watermarks: dict) -> None:
        blob = self.gcs_loader.bucket.blob(WATERMARK_BLOB_PATH)
        blob.upload_from_string(json.dumps(watermarks, indent=2), content_type="application/json")

    def get_watermark(self, source_name: str) -> str | None:
        """Return the last recorded run date for a source, or None if unset."""
        watermarks = self._load()
        return watermarks.get(source_name)

    def set_watermark(self, source_name: str, date_str: str) -> None:
        """Record the last successful run date for a source."""
        watermarks = self._load()
        watermarks[source_name] = date_str
        self._save(watermarks)

        log.info(
            "set pipeline watermark",
            extra={"extra_fields": {"source_name": source_name, "date": date_str}},
        )

    def get_all_watermarks(self) -> dict:
        """Return the full {source_name: last_run_date} watermark mapping."""
        return self._load()
