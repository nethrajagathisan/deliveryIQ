"""Unit tests for ZomatoExtractor. No real file I/O — GCSLoader and source files are mocked."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from ingestion.extractors.zomato_extractor import REQUIRED_COLUMNS, ZomatoExtractor
from ingestion.utils.exceptions import SchemaValidationError


@pytest.fixture
def mock_gcs_loader() -> MagicMock:
    return MagicMock()


@pytest.fixture
def extractor(mock_gcs_loader: MagicMock) -> ZomatoExtractor:
    return ZomatoExtractor(
        gcs_loader=mock_gcs_loader,
        source_path=Path("data/raw/zomato/zomato.csv"),
        ingestion_date="2026-06-12",
    )


@pytest.fixture
def valid_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Restaurant ID": [1, 2, 3],
            "Restaurant Name": [" Le Petit Souffle ", "Restaurant B", "Restaurant C"],
            "Country Code": [1, 1, 216],
            "City": ["New Delhi", "Mumbai", "Manama"],
            "Address": ["Addr 1", "Addr 2", "Addr 3"],
            "Locality": ["Loc 1", "", "Loc 3"],
            "Locality Verbose": ["Loc 1 Verbose", "Loc 2 Verbose", "Loc 3 Verbose"],
            "Longitude": [77.2167, 72.8354, 50.5860],
            "Latitude": [28.6448, 18.9388, 26.2285],
            "Cuisines": ["North Indian", "Chinese", "Arabic"],
            "Average Cost for two": [1000, 500, 300],
            "Currency": ["Indian Rupees(Rs.)", "Indian Rupees(Rs.)", "Bahraini Dinar(BD)"],
            "Has Table booking": ["Yes", "No", "No"],
            "Has Online delivery": ["Yes", "Yes", "No"],
            "Is delivering now": ["No", "No", "No"],
            "Switch to order menu": ["No", "No", "No"],
            "Price range": [3, 2, 1],
            "Aggregate rating": [4.5, 4.0, 3.5],
            "Rating color": ["Dark Green", "Green", "Yellow"],
            "Rating text": ["Excellent", "Very Good", "Good"],
            "Votes": [500, 300, 100],
        }
    )


class TestValidateSchema:
    def test_validate_schema_passes_with_valid_df(self, extractor, valid_df, monkeypatch):
        monkeypatch.setattr(
            pd, "read_csv", lambda *args, **kwargs: valid_df.head(5)
        )
        assert extractor.validate_schema() is True

    def test_validate_schema_raises_when_columns_missing(self, extractor, valid_df, monkeypatch):
        incomplete_df = valid_df.drop(columns=["Votes", "Cuisines"])
        monkeypatch.setattr(
            pd, "read_csv", lambda *args, **kwargs: incomplete_df.head(5)
        )
        with pytest.raises(SchemaValidationError):
            extractor.validate_schema()


class TestFilterIndianRestaurants:
    def test_filter_indian_restaurants_keeps_only_india(self, extractor, valid_df):
        filtered = extractor.filter_indian_restaurants(valid_df)

        assert len(filtered) == 2
        assert (filtered["Country Code"] == 1).all()
        assert set(filtered["Restaurant Name"].str.strip()) == {
            "Le Petit Souffle",
            "Restaurant B",
        }


class TestCleanForLanding:
    def test_clean_for_landing_renames_columns_to_snake_case(self, extractor, valid_df):
        indian_df = extractor.filter_indian_restaurants(valid_df)
        cleaned = extractor.clean_for_landing(indian_df)

        for expected_col in [
            "restaurant_id",
            "restaurant_name",
            "country_code",
            "average_cost_for_two",
            "has_table_booking",
            "has_online_delivery",
            "aggregate_rating",
        ]:
            assert expected_col in cleaned.columns

        assert "Restaurant ID" not in cleaned.columns
        assert "Average Cost for two" not in cleaned.columns

    def test_clean_for_landing_strips_whitespace_and_nulls_empty_strings(
        self, extractor, valid_df
    ):
        indian_df = extractor.filter_indian_restaurants(valid_df)
        cleaned = extractor.clean_for_landing(indian_df)

        assert cleaned.loc[cleaned["restaurant_id"] == 1, "restaurant_name"].iloc[0] == (
            "Le Petit Souffle"
        )
        assert cleaned.loc[cleaned["restaurant_id"] == 2, "locality"].iloc[0] is None

    def test_clean_for_landing_adds_metadata_columns(self, extractor, valid_df):
        indian_df = extractor.filter_indian_restaurants(valid_df)
        cleaned = extractor.clean_for_landing(indian_df)

        assert "_ingested_at" in cleaned.columns
        assert "_source_file" in cleaned.columns
        assert "_ingestion_date" in cleaned.columns

        assert (cleaned["_source_file"] == "zomato.csv").all()
        assert (cleaned["_ingestion_date"] == "2026-06-12").all()
        assert cleaned["_ingested_at"].notna().all()
