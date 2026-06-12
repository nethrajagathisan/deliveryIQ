"""Extractor for the Indian festival calendar seed dataset.

Reads the checked-in festival calendar CSV, normalises categorical
columns, casts ``is_major`` to boolean, and lands a Parquet file for the
Bronze layer.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ingestion.utils.exceptions import SchemaValidationError
from ingestion.utils.logger import get_logger

log = get_logger(__name__)

REQUIRED_COLUMNS: list[str] = [
    "festival_date",
    "festival_name",
    "festival_type",
    "region",
    "is_major",
]

DEFAULT_SOURCE_PATH = Path("data/raw/festival_calendar/indian_festivals_2020_2024.csv")


class FestivalCalendarExtractor:
    """Extracts and lands the Indian festival calendar for the Bronze layer."""

    def __init__(self, source_path: Path = DEFAULT_SOURCE_PATH) -> None:
        self.source_path = Path(source_path)

    def validate_schema(self) -> bool:
        """Validate the source CSV has all required columns.

        Raises:
            SchemaValidationError: If any required column is missing.

        Returns:
            True if the schema is valid.
        """
        df = pd.read_csv(self.source_path, nrows=5)

        missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        if missing:
            raise SchemaValidationError(
                f"Festival calendar source file is missing required columns: {missing}"
            )

        log.info(
            "validated festival calendar schema",
            extra={
                "extra_fields": {
                    "source_file": str(self.source_path),
                    "column_count": len(df.columns),
                }
            },
        )
        return True

    def clean_for_landing(self, ingestion_date: str) -> pd.DataFrame:
        """Read, normalise, and annotate the festival calendar for Bronze landing."""
        df = pd.read_csv(self.source_path)

        df["festival_date"] = pd.to_datetime(df["festival_date"]).dt.date

        df["festival_type"] = df["festival_type"].str.upper()
        df["region"] = df["region"].str.upper()

        df["is_major"] = (
            df["is_major"].astype(str).str.strip().str.upper().map({"TRUE": True, "FALSE": False})
        )

        df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        df["_source_file"] = self.source_path.name
        df["_ingestion_date"] = ingestion_date

        return df

    def extract(self, ingestion_date: str) -> dict:
        """Run the full extraction pipeline and land a Parquet file.

        Returns:
            A manifest dict: ``source_file``, ``row_count``,
            ``major_festival_count``, ``parquet_path``, ``ingestion_date``.
        """
        self.validate_schema()
        cleaned_df = self.clean_for_landing(ingestion_date)

        parquet_path = Path(f"/tmp/festival_calendar_{ingestion_date}.parquet")
        cleaned_df.to_parquet(parquet_path, index=False)

        manifest = {
            "source_file": str(self.source_path),
            "row_count": len(cleaned_df),
            "major_festival_count": int(cleaned_df["is_major"].sum()),
            "parquet_path": str(parquet_path),
            "ingestion_date": ingestion_date,
        }

        log.info(
            "extracted festival calendar",
            extra={"extra_fields": manifest},
        )
        return manifest
