"""Custom exceptions for the DeliveryIQ ingestion pipeline."""

from __future__ import annotations


class SchemaValidationError(Exception):
    """Raised when a source file does not match the expected schema."""


class ExtractionError(Exception):
    """Raised when extraction of a source file fails."""


class LoadError(Exception):
    """Raised when loading data to GCS or BigQuery fails."""


class WatermarkError(Exception):
    """Raised when reading or writing an incremental load watermark fails."""
