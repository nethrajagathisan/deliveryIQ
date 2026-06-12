"""Unit tests for FoodOrdersGenerator. No real file I/O for generation tests."""

from __future__ import annotations

import pandas as pd
import pytest

from ingestion.extractors.orders_generator import FoodOrdersGenerator


@pytest.fixture
def restaurants_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "restaurant_id": [1, 2, 3, 4],
            "restaurant_name": ["Restaurant A", "Restaurant B", "Restaurant C", "Restaurant D"],
            "city": ["Bangalore", "Delhi", "Mumbai", "Pune"],
            "cuisines": ["North Indian, Chinese", "Biryani", "Pizza, Fast Food", "South Indian"],
            "average_cost_for_two": [600, 500, 800, 400],
            "aggregate_rating": [4.2, 3.8, 4.5, 4.0],
        }
    )


@pytest.fixture
def generator(restaurants_df: pd.DataFrame) -> FoodOrdersGenerator:
    return FoodOrdersGenerator(
        restaurants_df=restaurants_df,
        start_date="2023-01-01",
        end_date="2023-01-07",
        seed=42,
    )


EXPECTED_COLUMNS = [
    "order_id",
    "restaurant_id",
    "customer_id",
    "city",
    "order_date",
    "order_datetime",
    "order_amount_inr",
    "payment_method",
    "platform",
    "order_status",
    "delivery_time_mins",
    "customer_rating",
    "cuisine_category",
    "_generated_at",
]


class TestGenerateOrders:
    def test_generate_orders_produces_correct_columns(self, generator: FoodOrdersGenerator):
        df = generator.generate_orders()

        for col in EXPECTED_COLUMNS:
            assert col in df.columns

        assert "_generator_version" in df.columns
        assert (df["_generator_version"] == "1.0").all()
        assert len(df) > 0

    def test_order_amounts_within_valid_range(self, generator: FoodOrdersGenerator):
        df = generator.generate_orders()

        assert (df["order_amount_inr"] >= FoodOrdersGenerator.MIN_ORDER_AMOUNT).all()
        assert (df["order_amount_inr"] <= FoodOrdersGenerator.MAX_ORDER_AMOUNT).all()

    def test_delivery_time_zero_for_cancelled_orders(self, generator: FoodOrdersGenerator):
        df = generator.generate_orders()

        cancelled = df[df["order_status"] == "cancelled"]
        assert len(cancelled) > 0
        assert (cancelled["delivery_time_mins"] >= 0).all()
        assert (cancelled["delivery_time_mins"] <= FoodOrdersGenerator.CANCELLED_MAX_MINS).all()

    def test_payment_method_distribution(self, restaurants_df: pd.DataFrame):
        generator = FoodOrdersGenerator(
            restaurants_df=restaurants_df,
            start_date="2023-01-01",
            end_date="2023-03-31",
            seed=42,
        )
        df = generator.generate_orders(orders_per_restaurant_per_day=5.0)

        counts = df["payment_method"].value_counts(normalize=True)
        assert counts.idxmax() == "UPI"
        assert counts["UPI"] > 0.35

    def test_generate_orders_is_reproducible_with_same_seed(self, restaurants_df: pd.DataFrame):
        gen_a = FoodOrdersGenerator(
            restaurants_df=restaurants_df,
            start_date="2023-01-01",
            end_date="2023-01-07",
            seed=123,
        )
        gen_b = FoodOrdersGenerator(
            restaurants_df=restaurants_df,
            start_date="2023-01-01",
            end_date="2023-01-07",
            seed=123,
        )

        df_a = gen_a.generate_orders()
        df_b = gen_b.generate_orders()

        pd.testing.assert_frame_equal(
            df_a.drop(columns=["_generated_at"]),
            df_b.drop(columns=["_generated_at"]),
        )


class TestSavePartitioned:
    def test_save_partitioned_writes_one_file_per_date(
        self, generator: FoodOrdersGenerator, tmp_path
    ):
        df = generator.generate_orders()
        paths = generator.save_partitioned(df, tmp_path)

        distinct_dates = df["order_date"].nunique()
        assert len(paths) == distinct_dates

        for path in paths:
            assert path.exists()
            assert path.name == "orders.parquet"
            assert path.parent.name.startswith("order_date=")
