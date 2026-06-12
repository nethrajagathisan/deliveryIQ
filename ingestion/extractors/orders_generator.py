"""Synthetic food order generator for the DeliveryIQ pipeline.

Generates realistic India-specific food delivery orders for the Zomato
restaurant dataset: order volume varies by city, cuisine, weekday vs.
weekend, and time of day, and amounts/payment methods/delivery times
follow distributions typical of Indian food delivery platforms.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from ingestion.utils.logger import get_logger

log = get_logger(__name__)

_GENERATOR_VERSION = "1.0"

_CUSTOMER_POOL_SIZE = 50_000


class FoodOrdersGenerator:
    """Generates synthetic food order data based on Zomato restaurant data."""

    # Payment method distribution typical of Indian food delivery.
    INDIAN_PAYMENT_METHODS: dict[str, float] = {
        "UPI": 0.45,
        "card": 0.22,
        "cash": 0.18,
        "wallet": 0.15,
    }

    # Platform market share.
    PLATFORMS: dict[str, float] = {
        "zomato": 0.55,
        "swiggy": 0.45,
    }

    # Order outcome distribution.
    ORDER_STATUS_DISTRIBUTION: dict[str, float] = {
        "delivered": 0.87,
        "cancelled": 0.09,
        "refund_requested": 0.04,
    }

    # Order frequency multipliers by cuisine type. Matched case-insensitively
    # against the restaurant's primary cuisine; unmatched cuisines fall back
    # to "others".
    CUISINE_ORDER_MULTIPLIERS: dict[str, float] = {
        "indian": 2.5,
        "north indian": 2.5,
        "chinese": 1.8,
        "south indian": 2.0,
        "pizza": 1.4,
        "biryani": 2.2,
        "fast food": 1.6,
        "others": 1.0,
    }

    # Daily order volume multipliers by city.
    CITY_DAILY_ORDER_MULTIPLIERS: dict[str, float] = {
        "Delhi": 1.8,
        "New Delhi": 1.8,
        "Mumbai": 2.0,
        "Bangalore": 2.2,
        "Chennai": 1.4,
        "Hyderabad": 1.6,
        "Pune": 1.5,
    }
    CITY_DAILY_ORDER_MULTIPLIER_DEFAULT = 1.0

    # Hour-of-day weights: lunch (12-14) and dinner (19-22) are peak.
    PEAK_HOURS: dict[int, float] = {hour: 1.0 for hour in range(24)}
    for _hour in (12, 13):
        PEAK_HOURS[_hour] = 3.0
    for _hour in (19, 20, 21):
        PEAK_HOURS[_hour] = 4.0
    del _hour

    WEEKEND_MULTIPLIER = 1.35
    RAINY_MULTIPLIER = 1.25

    # Customer rating distribution: 5-star is most common.
    CUSTOMER_RATING_DISTRIBUTION: dict[int, float] = {
        5: 0.35,
        4: 0.30,
        3: 0.20,
        2: 0.10,
        1: 0.05,
    }

    # Order amount bounds, in INR.
    MIN_ORDER_AMOUNT = 80
    MAX_ORDER_AMOUNT = 3500

    # Delivery time bounds, in minutes.
    DELIVERED_MEAN_MINS = 37.5
    DELIVERED_STD_MINS = 10.0
    DELIVERED_MIN_MINS = 20
    DELIVERED_MAX_MINS = 75
    CANCELLED_MIN_MINS = 0
    CANCELLED_MAX_MINS = 15

    # Per-city adjustments to delivered delivery time (minutes).
    CITY_DELIVERY_TIME_OFFSETS: dict[str, float] = {
        "Bangalore": -4.0,
        "Delhi": 3.0,
        "New Delhi": 3.0,
    }

    def __init__(
        self,
        restaurants_df: pd.DataFrame,
        start_date: str,
        end_date: str,
        seed: int = 42,
    ) -> None:
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        self.seed = seed
        self.rng = np.random.default_rng(seed)

        self.restaurants = self._build_restaurant_lookup(restaurants_df)

    @staticmethod
    def _build_restaurant_lookup(restaurants_df: pd.DataFrame) -> dict[int, dict]:
        """Build a {restaurant_id: {city, cuisines, avg_cost, rating}} lookup."""
        lookup: dict[int, dict] = {}
        for _, row in restaurants_df.iterrows():
            restaurant_id = int(row["restaurant_id"])
            cuisines = str(row.get("cuisines") or "")
            lookup[restaurant_id] = {
                "city": row.get("city"),
                "cuisines": cuisines,
                "cuisine_category": cuisines.split(",")[0].strip() if cuisines else "others",
                "avg_cost_for_two": float(row.get("average_cost_for_two") or 0),
                "rating": float(row.get("aggregate_rating") or 0),
            }
        return lookup

    def _get_order_hour(self) -> int:
        """Sample an order hour (0-23) weighted toward lunch and dinner peaks."""
        hours = list(self.PEAK_HOURS.keys())
        weights = np.array(list(self.PEAK_HOURS.values()), dtype=float)
        probabilities = weights / weights.sum()
        return int(self.rng.choice(hours, p=probabilities))

    def _calculate_order_amount(self, restaurant: dict) -> float:
        """Derive an order amount from the restaurant's avg cost for two."""
        base = restaurant["avg_cost_for_two"]
        if base <= 0:
            base = 300.0

        factor = self.rng.uniform(0.4, 2.2)
        amount = base * factor

        amount = round(amount / 10) * 10

        return float(min(max(amount, self.MIN_ORDER_AMOUNT), self.MAX_ORDER_AMOUNT))

    def _calculate_delivery_time(self, city: str, order_status: str) -> int:
        """Sample a delivery time in minutes based on city and order status."""
        if order_status == "cancelled":
            return int(self.rng.integers(self.CANCELLED_MIN_MINS, self.CANCELLED_MAX_MINS + 1))

        city_offset = self.CITY_DELIVERY_TIME_OFFSETS.get(city, 0.0)
        mean = self.DELIVERED_MEAN_MINS + city_offset

        minutes = self.rng.normal(mean, self.DELIVERED_STD_MINS)
        minutes = np.clip(minutes, self.DELIVERED_MIN_MINS, self.DELIVERED_MAX_MINS)
        return int(round(minutes))

    def _sample_weighted(self, distribution: dict) -> object:
        """Sample a single key from a {key: probability} distribution."""
        keys = list(distribution.keys())
        weights = np.array(list(distribution.values()), dtype=float)
        probabilities = weights / weights.sum()
        index = self.rng.choice(len(keys), p=probabilities)
        return keys[index]

    def _cuisine_multiplier(self, cuisine_category: str) -> float:
        return self.CUISINE_ORDER_MULTIPLIERS.get(
            cuisine_category.lower(), self.CUISINE_ORDER_MULTIPLIERS["others"]
        )

    def _city_multiplier(self, city: str) -> float:
        return self.CITY_DAILY_ORDER_MULTIPLIERS.get(
            city, self.CITY_DAILY_ORDER_MULTIPLIER_DEFAULT
        )

    def generate_orders(self, orders_per_restaurant_per_day: float = 3.5) -> pd.DataFrame:
        """Generate synthetic orders for every restaurant across the date range.

        Returns:
            A DataFrame with one row per order, with columns:
            ``order_id``, ``restaurant_id``, ``customer_id``, ``city``,
            ``order_date``, ``order_datetime``, ``order_amount_inr``,
            ``payment_method``, ``platform``, ``order_status``,
            ``delivery_time_mins``, ``customer_rating``,
            ``cuisine_category``, ``_generated_at``.
        """
        records: list[dict] = []
        generated_at = datetime.now(timezone.utc).isoformat()

        current_date = self.start_date
        while current_date <= self.end_date:
            is_weekend = current_date.weekday() >= 5
            weekend_factor = self.WEEKEND_MULTIPLIER if is_weekend else 1.0
            is_rainy = bool(self.rng.random() < 0.15)
            rainy_factor = self.RAINY_MULTIPLIER if is_rainy else 1.0

            for restaurant_id, restaurant in self.restaurants.items():
                city = restaurant["city"]
                cuisine_category = restaurant["cuisine_category"]

                expected_orders = (
                    orders_per_restaurant_per_day
                    * self._city_multiplier(city)
                    * self._cuisine_multiplier(cuisine_category)
                    * weekend_factor
                    * rainy_factor
                )

                order_count = int(self.rng.poisson(lam=expected_orders))

                for _ in range(order_count):
                    order_hour = self._get_order_hour()
                    order_minute = int(self.rng.integers(0, 60))
                    order_datetime = datetime.combine(
                        current_date, datetime.min.time()
                    ) + timedelta(hours=order_hour, minutes=order_minute)

                    order_status = self._sample_weighted(self.ORDER_STATUS_DISTRIBUTION)
                    payment_method = self._sample_weighted(self.INDIAN_PAYMENT_METHODS)
                    platform = self._sample_weighted(self.PLATFORMS)
                    customer_rating = self._sample_weighted(self.CUSTOMER_RATING_DISTRIBUTION)

                    records.append(
                        {
                            "order_id": str(uuid.UUID(bytes=self.rng.bytes(16), version=4)),
                            "restaurant_id": restaurant_id,
                            "customer_id": int(self.rng.integers(0, _CUSTOMER_POOL_SIZE)),
                            "city": city,
                            "order_date": current_date.isoformat(),
                            "order_datetime": order_datetime.isoformat(),
                            "order_amount_inr": self._calculate_order_amount(restaurant),
                            "payment_method": payment_method,
                            "platform": platform,
                            "order_status": order_status,
                            "delivery_time_mins": self._calculate_delivery_time(
                                city, order_status
                            ),
                            "customer_rating": int(customer_rating),
                            "cuisine_category": cuisine_category,
                            "_generated_at": generated_at,
                        }
                    )

            current_date += timedelta(days=1)

        df = pd.DataFrame.from_records(
            records,
            columns=[
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
            ],
        )
        df["_generator_version"] = _GENERATOR_VERSION

        log.info(
            "generated synthetic orders",
            extra={
                "extra_fields": {
                    "order_count": len(df),
                    "restaurant_count": len(self.restaurants),
                    "start_date": self.start_date.isoformat(),
                    "end_date": self.end_date.isoformat(),
                }
            },
        )
        return df

    def save_partitioned(self, df: pd.DataFrame, output_dir: Path) -> list[Path]:
        """Save orders as Parquet files partitioned by order_date.

        Writes ``{output_dir}/order_date={date}/orders.parquet`` for each
        distinct order date present in ``df``.

        Returns:
            The list of Parquet file paths that were written.
        """
        output_dir = Path(output_dir)
        written_paths: list[Path] = []

        for order_date, partition_df in df.groupby("order_date"):
            partition_dir = output_dir / f"order_date={order_date}"
            partition_dir.mkdir(parents=True, exist_ok=True)

            partition_path = partition_dir / "orders.parquet"
            partition_df.to_parquet(partition_path, index=False)
            written_paths.append(partition_path)

        log.info(
            "saved partitioned orders",
            extra={
                "extra_fields": {
                    "output_dir": str(output_dir),
                    "partition_count": len(written_paths),
                    "row_count": len(df),
                }
            },
        )
        return written_paths
