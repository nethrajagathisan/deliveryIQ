"""Seed synthetic food order data from the Zomato restaurant dataset.

Loads the raw Zomato CSV, filters to Indian restaurants, generates
synthetic orders across a date range, and writes them to
``data/raw/orders/`` partitioned by ``order_date``.

Usage:
    python scripts/seed_data.py
    python scripts/seed_data.py --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.extractors.orders_generator import FoodOrdersGenerator  # noqa: E402
from ingestion.extractors.zomato_extractor import (  # noqa: E402
    INDIA_COUNTRY_CODE,
    _to_snake_case,
)

ZOMATO_CSV_PATH = PROJECT_ROOT / "data" / "raw" / "zomato" / "zomato.csv"
ORDERS_OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "orders"

FULL_START_DATE = "2020-01-01"
FULL_END_DATE = "2023-12-31"
DRY_RUN_START_DATE = "2023-12-25"
DRY_RUN_END_DATE = "2023-12-31"

ORDERS_PER_RESTAURANT_PER_DAY = 3.5
SEED = 42


def load_indian_restaurants(csv_path: Path) -> pd.DataFrame:
    """Load the Zomato CSV, filter to India, and rename columns to snake_case."""
    df = pd.read_csv(csv_path, encoding="latin-1")
    indian_df = df[df["Country Code"] == INDIA_COUNTRY_CODE].copy()
    indian_df.columns = [_to_snake_case(col) for col in indian_df.columns]
    return indian_df


def print_summary(df: pd.DataFrame) -> None:
    print(f"Total orders: {len(df):,}")

    print("\nOrders by city:")
    for city, count in df["city"].value_counts().items():
        print(f"  {city}: {count:,}")

    print("\nOrders by platform:")
    for platform, count in df["platform"].value_counts().items():
        print(f"  {platform}: {count:,}")

    min_date = df["order_date"].min()
    max_date = df["order_date"].max()
    print(f"\nDate range: {min_date} to {max_date}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate only 7 days of orders and print stats without writing files.",
    )
    args = parser.parse_args()

    if args.dry_run:
        start_date, end_date = DRY_RUN_START_DATE, DRY_RUN_END_DATE
    else:
        start_date, end_date = FULL_START_DATE, FULL_END_DATE

    print(f"Loading Zomato restaurants from {ZOMATO_CSV_PATH} ...")
    restaurants_df = load_indian_restaurants(ZOMATO_CSV_PATH)
    print(f"Loaded {len(restaurants_df):,} Indian restaurants.")

    print(f"Generating orders from {start_date} to {end_date} ...")
    generator = FoodOrdersGenerator(
        restaurants_df=restaurants_df,
        start_date=start_date,
        end_date=end_date,
        seed=SEED,
    )
    orders_df = generator.generate_orders(
        orders_per_restaurant_per_day=ORDERS_PER_RESTAURANT_PER_DAY
    )

    print_summary(orders_df)

    if args.dry_run:
        print("\nDry run: skipping write to disk.")
        return

    print(f"\nWriting partitioned orders to {ORDERS_OUTPUT_DIR} ...")
    paths = generator.save_partitioned(orders_df, ORDERS_OUTPUT_DIR)
    print(f"Wrote {len(paths)} partition files.")


if __name__ == "__main__":
    main()
