"""Seed a small, schema-correct sample into the Bronze tables for local dbt runs.

Loads a handful of realistic rows into raw_restaurants, raw_orders and raw_weather
so the Silver models and their tests run against real data (not an empty table).
Idempotent-ish: it truncates the three target tables before loading.

Auth: uses Application Default Credentials (gcloud auth application-default login).
Usage: python scripts/seed_bronze_sample.py
"""

from __future__ import annotations

import os
import random
from datetime import date, datetime, time, timedelta, timezone

from google.cloud import bigquery

PROJECT = os.environ.get("GCP_PROJECT_ID", "delivery-iq-499207")
DATASET = "deliveryiq_bronze"
LOCATION = os.environ.get("GCP_REGION", "asia-south1")

CITIES = ["Delhi", "Mumbai", "Bangalore", "Chennai", "Hyderabad", "Pune"]
PAYMENT_METHODS = ["UPI", "card", "cash", "wallet"]
PLATFORMS = ["zomato", "swiggy"]
STATUSES = ["delivered", "cancelled", "refund_requested"]
CUISINES = [
    "North Indian, Chinese",
    "South Indian",
    "Biryani, Mughlai",
    "Italian, Continental",
    "Chinese, Thai",
    "Indian, Fast Food",
]

rng = random.Random(42)


def make_restaurants(n: int = 30) -> list[dict]:
    rows = []
    ingested = datetime.now(timezone.utc)
    for rid in range(1, n + 1):
        cuisines = rng.choice(CUISINES)
        rows.append(
            {
                "restaurant_id": rid,
                "restaurant_name": f"Restaurant {rid}",
                "city": rng.choice(CITIES),
                "address": f"{rid} Some Road",
                "locality": "Central",
                "longitude": round(rng.uniform(72.0, 80.5), 4),
                "latitude": round(rng.uniform(12.0, 28.7), 4),
                "cuisines": cuisines,
                "average_cost_for_two": rng.choice([300, 500, 800, 1200, 2000]),
                "currency": "Indian Rupees(Rs.)",
                "has_table_booking": rng.random() < 0.4,
                "has_online_delivery": rng.random() < 0.8,
                "price_range": rng.randint(1, 4),
                "aggregate_rating": round(rng.uniform(2.8, 4.9), 1),
                "rating_text": "Good",
                "votes": rng.randint(10, 5000),
                "country_code": 1,
                "_ingested_at": ingested.isoformat(),
                "_source_file": "seed_sample.json",
                "_ingestion_date": ingested.date().isoformat(),
            }
        )
    return rows


def make_orders(restaurants: list[dict], days: int = 20) -> list[dict]:
    rows = []
    generated = datetime.now(timezone.utc)
    # Bronze partitions expire after 60 days (expirationMs on the partition spec),
    # so event dates MUST be recent or BigQuery drops them on load. Anchor to today.
    start = date.today() - timedelta(days=days)
    oid = 0
    for d in range(days):
        order_date = start + timedelta(days=d)
        for _ in range(rng.randint(8, 15)):
            oid += 1
            r = rng.choice(restaurants)
            status = rng.choices(STATUSES, weights=[0.87, 0.09, 0.04])[0]
            hour = rng.randint(8, 23)
            odt = datetime.combine(order_date, time(hour, rng.randint(0, 59)), tzinfo=timezone.utc)
            delivery = 0 if status == "cancelled" else rng.randint(18, 55)
            rows.append(
                {
                    "order_id": f"ORD{oid:06d}",
                    "restaurant_id": r["restaurant_id"],
                    "customer_id": f"CUST{rng.randint(1, 200):04d}",
                    "city": r["city"],
                    "order_date": order_date.isoformat(),
                    "order_datetime": odt.isoformat(),
                    "order_amount_inr": round(rng.uniform(80, 3500), 2),
                    "payment_method": rng.choices(PAYMENT_METHODS, weights=[0.45, 0.22, 0.18, 0.15])[0],
                    "platform": rng.choices(PLATFORMS, weights=[0.55, 0.45])[0],
                    "order_status": status,
                    "delivery_time_mins": delivery,
                    "customer_rating": rng.randint(1, 5),
                    "cuisine_category": r["cuisines"].split(",")[0].strip(),
                    "_generated_at": generated.isoformat(),
                }
            )
    return rows


def make_weather(days: int = 20) -> list[dict]:
    rows = []
    fetched = datetime.now(timezone.utc)
    # Recent dates so the daily partitions don't immediately expire (60-day TTL).
    start = date.today() - timedelta(days=days)
    for city in CITIES:
        for d in range(days):
            wdate = start + timedelta(days=d)
            tmax = round(rng.uniform(22, 43), 1)
            precip = round(rng.choice([0, 0, 0, 1.5, 4.0, 18.0]), 1)
            rows.append(
                {
                    "city": city,
                    "weather_date": wdate.isoformat(),
                    "temperature_max": tmax,
                    "temperature_min": round(tmax - rng.uniform(6, 12), 1),
                    "precipitation_sum": precip,
                    "windspeed_max": round(rng.uniform(5, 30), 1),
                    "weather_code": rng.choice([0, 1, 2, 3, 61, 63, 95]),
                    "weather_description": "Sample",
                    "is_rainy": precip > 2.5,
                    "weather_category": "normal",
                    "_fetched_at": fetched.isoformat(),
                }
            )
    return rows


def load(client: bigquery.Client, table: str, rows: list[dict]) -> None:
    # Use a LOAD job (not DML/streaming): free-tier BigQuery blocks DML and the
    # streaming insert API, but load jobs with WRITE_TRUNCATE are allowed.
    table_id = f"{PROJECT}.{DATASET}.{table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema_update_options=[],
    )
    client.load_table_from_json(rows, table_id, job_config=job_config).result()
    print(f"Loaded {len(rows)} rows into {table}")


def main() -> None:
    client = bigquery.Client(project=PROJECT, location=LOCATION)
    restaurants = make_restaurants()
    orders = make_orders(restaurants)
    weather = make_weather()
    load(client, "raw_restaurants", restaurants)
    load(client, "raw_orders", orders)
    load(client, "raw_weather", weather)
    print("Done seeding bronze sample.")


if __name__ == "__main__":
    main()
