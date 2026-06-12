"""Extractor for the Zomato restaurant dataset.

Reads the raw Zomato CSV, validates its schema, filters to Indian
restaurants, and lands a cleaned (but untransformed) Parquet file for
the Bronze layer. Bronze = raw as-received: this module only renames
columns to snake_case, strips whitespace, normalizes empty strings to
``None``, and attaches ingestion metadata. No business transformations.
"""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from ingestion.utils.exceptions import SchemaValidationError
from ingestion.utils.logger import get_logger

if TYPE_CHECKING:
    from ingestion.loaders.gcs_loader import GCSLoader

log = get_logger(__name__)

REQUIRED_COLUMNS: list[str] = [
    "Restaurant ID",
    "Restaurant Name",
    "Country Code",
    "City",
    "Address",
    "Locality",
    "Locality Verbose",
    "Longitude",
    "Latitude",
    "Cuisines",
    "Average Cost for two",
    "Currency",
    "Has Table booking",
    "Has Online delivery",
    "Is delivering now",
    "Switch to order menu",
    "Price range",
    "Aggregate rating",
    "Rating color",
    "Rating text",
    "Votes",
]

INDIA_COUNTRY_CODE = 1

_MD5_CHUNK_SIZE = 8192


def _to_snake_case(column: str) -> str:
    """Convert a Zomato CSV header to snake_case (e.g. "Restaurant ID" -> "restaurant_id")."""
    column = column.strip()
    column = re.sub(r"[^0-9a-zA-Z]+", " ", column)
    column = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", column)
    return "_".join(column.split()).lower()


class ZomatoExtractor:
    """Extracts and lands the Zomato restaurant dataset for the Bronze layer."""

    def __init__(
        self,
        gcs_loader: "GCSLoader",
        source_path: Path,
        ingestion_date: str,
    ) -> None:
        self.gcs_loader = gcs_loader
        self.source_path = Path(source_path)
        self.ingestion_date = ingestion_date

    def validate_schema(self) -> bool:
        """Validate the source CSV has all required columns.

        Reads only the first 5 rows for a cheap schema check.

        Raises:
            SchemaValidationError: If any required column is missing.

        Returns:
            True if the schema is valid.
        """
        sample = pd.read_csv(self.source_path, encoding="latin-1", nrows=5)

        missing = [col for col in REQUIRED_COLUMNS if col not in sample.columns]
        if missing:
            raise SchemaValidationError(
                f"Zomato source file is missing required columns: {missing}"
            )

        log.info(
            "validated zomato schema",
            extra={
                "extra_fields": {
                    "source_file": str(self.source_path),
                    "column_count": len(sample.columns),
                    "sample_row_count": len(sample),
                }
            },
        )
        return True

    def compute_file_md5(self) -> str:
        """Compute the MD5 hash of the source file for idempotent deduplication."""
        md5 = hashlib.md5()
        with open(self.source_path, "rb") as f:
            for chunk in iter(lambda: f.read(_MD5_CHUNK_SIZE), b""):
                md5.update(chunk)
        return md5.hexdigest()

    def filter_indian_restaurants(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter the DataFrame to restaurants with Country Code == 1 (India)."""
        total_rows = len(df)
        filtered = df[df["Country Code"] == INDIA_COUNTRY_CODE].copy()
        kept_rows = len(filtered)

        log.info(
            "filtered to indian restaurants",
            extra={
                "extra_fields": {
                    "rows_kept": kept_rows,
                    "rows_dropped": total_rows - kept_rows,
                    "total_rows": total_rows,
                }
            },
        )
        return filtered

    def clean_for_landing(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename columns to snake_case and attach ingestion metadata.

        Bronze = raw as-received: only column renaming, whitespace
        stripping, and empty-string normalization happen here. No
        business transformations.
        """
        df = df.copy()
        df.columns = [_to_snake_case(col) for col in df.columns]

        string_columns = df.select_dtypes(include="object").columns
        for col in string_columns:
            df[col] = df[col].str.strip()
            df[col] = df[col].replace("", None)

        df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        df["_source_file"] = self.source_path.name
        df["_ingestion_date"] = self.ingestion_date

        return df

    def extract(self) -> dict:
        """Run the full extraction pipeline and land a Parquet file.

        Returns:
            A manifest dict describing the extraction result:
            ``source_file``, ``row_count``, ``indian_rows``, ``md5``,
            ``parquet_path``, ``ingestion_date``.
        """
        self.validate_schema()

        df = pd.read_csv(self.source_path, encoding="latin-1")
        row_count = len(df)

        indian_df = self.filter_indian_restaurants(df)
        cleaned_df = self.clean_for_landing(indian_df)

        parquet_path = Path(f"/tmp/zomato_raw_{self.ingestion_date}.parquet")
        cleaned_df.to_parquet(parquet_path, index=False)

        manifest = {
            "source_file": str(self.source_path),
            "row_count": row_count,
            "indian_rows": len(cleaned_df),
            "md5": self.compute_file_md5(),
            "parquet_path": str(parquet_path),
            "ingestion_date": self.ingestion_date,
        }

        log.info(
            "extracted zomato dataset",
            extra={"extra_fields": manifest},
        )
        return manifest
