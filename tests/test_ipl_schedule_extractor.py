"""Unit tests for IPLScheduleExtractor. Uses small in-memory CSV fixtures, not the real seed file."""

from __future__ import annotations

import pytest

from ingestion.extractors.ipl_schedule_extractor import IPLScheduleExtractor
from ingestion.utils.exceptions import SchemaValidationError


@pytest.fixture
def valid_csv(tmp_path):
    csv_path = tmp_path / "ipl_matches_2020_2024.csv"
    csv_path.write_text(
        "match_date,season,match_number,team1,team2,venue_city,match_type,winner\n"
        "2023-04-02,2023,3,Royal Challengers Bangalore,Mumbai Indians,Bengaluru,league,Mumbai Indians\n"
        "2023-04-09,2023,10,Delhi Capitals,Punjab Kings,New Delhi,league,Punjab Kings\n"
        "2023-05-28,2023,70,Chennai Super Kings,Gujarat Titans,Ahmedabad,final,Chennai Super Kings\n"
    )
    return csv_path


@pytest.fixture
def extractor(valid_csv) -> IPLScheduleExtractor:
    return IPLScheduleExtractor(source_path=valid_csv)


class TestValidateSchema:
    def test_validate_schema_passes_with_valid_df(self, extractor: IPLScheduleExtractor):
        assert extractor.validate_schema() is True

    def test_validate_schema_raises_when_columns_missing(self, tmp_path):
        csv_path = tmp_path / "bad.csv"
        csv_path.write_text("match_date,season,team1,team2\n2023-04-02,2023,A,B\n")

        extractor = IPLScheduleExtractor(source_path=csv_path)
        with pytest.raises(SchemaValidationError):
            extractor.validate_schema()


class TestCleanForLanding:
    def test_clean_for_landing_adds_metadata_columns(self, extractor: IPLScheduleExtractor):
        df = extractor.clean_for_landing(ingestion_date="2026-06-12")

        assert "_ingested_at" in df.columns
        assert "_source_file" in df.columns
        assert "_ingestion_date" in df.columns

        assert (df["_ingestion_date"] == "2026-06-12").all()
        assert df["_ingested_at"].notna().all()

    def test_clean_for_landing_standardises_city_names(self, extractor: IPLScheduleExtractor):
        df = extractor.clean_for_landing(ingestion_date="2026-06-12")

        assert "Bengaluru" not in df["venue_city"].values
        assert "New Delhi" not in df["venue_city"].values
        assert "Bangalore" in df["venue_city"].values
        assert "Delhi" in df["venue_city"].values

    def test_is_deliveryiq_city_flag_set_correctly(self, extractor: IPLScheduleExtractor):
        df = extractor.clean_for_landing(ingestion_date="2026-06-12")

        bangalore_row = df[df["venue_city"] == "Bangalore"].iloc[0]
        delhi_row = df[df["venue_city"] == "Delhi"].iloc[0]
        ahmedabad_row = df[df["venue_city"] == "Ahmedabad"].iloc[0]

        assert bool(bangalore_row["is_deliveryiq_city"]) is True
        assert bool(delhi_row["is_deliveryiq_city"]) is True
        assert bool(ahmedabad_row["is_deliveryiq_city"]) is False
