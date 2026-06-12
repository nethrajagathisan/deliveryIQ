"""Extractor for the IPL match schedule seed dataset.

Reads the checked-in IPL schedule CSV, standardises venue city names to
match the ``stg_restaurants`` city convention, flags matches hosted in
the six DeliveryIQ cities, and lands a Parquet file for the Bronze layer.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ingestion.utils.exceptions import SchemaValidationError
from ingestion.utils.logger import get_logger

log = get_logger(__name__)

REQUIRED_COLUMNS: list[str] = [
    "match_date",
    "season",
    "team1",
    "team2",
    "venue_city",
    "match_type",
]

# DeliveryIQ's six cities, as used by stg_restaurants.
DELIVERYIQ_CITIES: set[str] = {
    "Delhi",
    "Mumbai",
    "Bangalore",
    "Chennai",
    "Hyderabad",
    "Pune",
}

# Maps alternate/legacy spellings of venue cities to the stg_restaurants
# city convention.
CITY_NAME_STANDARDIZATION: dict[str, str] = {
    "Bengaluru": "Bangalore",
    "New Delhi": "Delhi",
}

DEFAULT_SOURCE_PATH = Path("data/raw/ipl_schedule/ipl_matches_2020_2024.csv")


class IPLScheduleExtractor:
    """Extracts and lands the IPL match schedule for the Bronze layer."""

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
                f"IPL schedule source file is missing required columns: {missing}"
            )

        log.info(
            "validated ipl schedule schema",
            extra={
                "extra_fields": {
                    "source_file": str(self.source_path),
                    "column_count": len(df.columns),
                }
            },
        )
        return True

    def clean_for_landing(self, ingestion_date: str) -> pd.DataFrame:
        """Read, standardise, and annotate the IPL schedule for Bronze landing."""
        df = pd.read_csv(self.source_path)

        df["match_date"] = pd.to_datetime(df["match_date"]).dt.date

        df["venue_city"] = df["venue_city"].replace(CITY_NAME_STANDARDIZATION)

        df["is_deliveryiq_city"] = df["venue_city"].isin(DELIVERYIQ_CITIES)

        df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        df["_source_file"] = self.source_path.name
        df["_ingestion_date"] = ingestion_date

        return df

    def extract(self, ingestion_date: str) -> dict:
        """Run the full extraction pipeline and land a Parquet file.

        Returns:
            A manifest dict: ``source_file``, ``row_count``,
            ``deliveryiq_city_rows``, ``parquet_path``, ``ingestion_date``.
        """
        self.validate_schema()
        cleaned_df = self.clean_for_landing(ingestion_date)

        parquet_path = Path(f"/tmp/ipl_schedule_{ingestion_date}.parquet")
        cleaned_df.to_parquet(parquet_path, index=False)

        manifest = {
            "source_file": str(self.source_path),
            "row_count": len(cleaned_df),
            "deliveryiq_city_rows": int(cleaned_df["is_deliveryiq_city"].sum()),
            "parquet_path": str(parquet_path),
            "ingestion_date": ingestion_date,
        }

        log.info(
            "extracted ipl schedule",
            extra={"extra_fields": manifest},
        )
        return manifest
