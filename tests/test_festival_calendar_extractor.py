"""Unit tests for FestivalCalendarExtractor. Uses small in-memory CSV fixtures, not the real seed file."""

from __future__ import annotations

import pytest

from ingestion.extractors.festival_calendar_extractor import FestivalCalendarExtractor
from ingestion.utils.exceptions import SchemaValidationError


@pytest.fixture
def valid_csv(tmp_path):
    csv_path = tmp_path / "indian_festivals_2020_2024.csv"
    csv_path.write_text(
        "festival_date,festival_name,festival_type,region,is_major\n"
        "2023-11-12,Diwali,religious,all,TRUE\n"
        "2023-01-26,Republic Day,national,all,FALSE\n"
        "2023-01-15,Pongal,regional,south,FALSE\n"
    )
    return csv_path


@pytest.fixture
def extractor(valid_csv) -> FestivalCalendarExtractor:
    return FestivalCalendarExtractor(source_path=valid_csv)


class TestValidateSchema:
    def test_validate_schema_passes_with_valid_df(self, extractor: FestivalCalendarExtractor):
        assert extractor.validate_schema() is True

    def test_validate_schema_raises_when_columns_missing(self, tmp_path):
        csv_path = tmp_path / "bad.csv"
        csv_path.write_text("festival_date,festival_name\n2023-11-12,Diwali\n")

        extractor = FestivalCalendarExtractor(source_path=csv_path)
        with pytest.raises(SchemaValidationError):
            extractor.validate_schema()


class TestCleanForLanding:
    def test_clean_for_landing_adds_metadata_columns(self, extractor: FestivalCalendarExtractor):
        df = extractor.clean_for_landing(ingestion_date="2026-06-12")

        assert "_ingested_at" in df.columns
        assert "_source_file" in df.columns
        assert "_ingestion_date" in df.columns

        assert (df["_ingestion_date"] == "2026-06-12").all()
        assert df["_ingested_at"].notna().all()

    def test_festival_type_and_region_normalised_to_upper(
        self, extractor: FestivalCalendarExtractor
    ):
        df = extractor.clean_for_landing(ingestion_date="2026-06-12")

        assert set(df["festival_type"]) == {"RELIGIOUS", "NATIONAL", "REGIONAL"}
        assert set(df["region"]) == {"ALL", "SOUTH"}

    def test_is_major_cast_to_bool(self, extractor: FestivalCalendarExtractor):
        df = extractor.clean_for_landing(ingestion_date="2026-06-12")

        assert df["is_major"].dtype == bool

        diwali_row = df[df["festival_name"] == "Diwali"].iloc[0]
        republic_day_row = df[df["festival_name"] == "Republic Day"].iloc[0]

        assert bool(diwali_row["is_major"]) is True
        assert bool(republic_day_row["is_major"]) is False
