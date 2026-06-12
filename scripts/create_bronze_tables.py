"""Create BigQuery Bronze-layer tables for the DeliveryIQ pipeline.

Creates the ``deliveryiq_bronze`` dataset (if missing) and all Bronze raw
tables plus the ``pipeline_audit`` table, with the partitioning and
clustering described in the project design docs. Safe to re-run: existing
tables are left untouched.

Usage:
    python scripts/create_bronze_tables.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.api_core.exceptions import Conflict
from google.cloud import bigquery

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from ingestion.utils.logger import get_logger  # noqa: E402

log = get_logger(__name__)

BRONZE_DATASET = "deliveryiq_bronze"

# Each entry: (table_id, schema, time_partitioning, clustering_fields, require_partition_filter)
TABLE_SPECS: list[dict] = [
    {
        "table_id": "raw_restaurants",
        "schema": [
            bigquery.SchemaField("restaurant_id", "INT64"),
            bigquery.SchemaField("restaurant_name", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("address", "STRING"),
            bigquery.SchemaField("locality", "STRING"),
            bigquery.SchemaField("longitude", "FLOAT64"),
            bigquery.SchemaField("latitude", "FLOAT64"),
            bigquery.SchemaField("cuisines", "STRING"),
            bigquery.SchemaField("average_cost_for_two", "INT64"),
            bigquery.SchemaField("currency", "STRING"),
            bigquery.SchemaField("has_table_booking", "BOOL"),
            bigquery.SchemaField("has_online_delivery", "BOOL"),
            bigquery.SchemaField("price_range", "INT64"),
            bigquery.SchemaField("aggregate_rating", "FLOAT64"),
            bigquery.SchemaField("rating_text", "STRING"),
            bigquery.SchemaField("votes", "INT64"),
            bigquery.SchemaField("country_code", "INT64"),
            bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("_source_file", "STRING"),
            bigquery.SchemaField("_ingestion_date", "DATE"),
        ],
        "time_partitioning": bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="_ingestion_date"
        ),
        "clustering_fields": ["city", "price_range"],
        "require_partition_filter": False,
    },
    {
        "table_id": "raw_orders",
        "schema": [
            bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("restaurant_id", "INT64"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("order_date", "DATE"),
            bigquery.SchemaField("order_datetime", "TIMESTAMP"),
            bigquery.SchemaField("order_amount_inr", "FLOAT64"),
            bigquery.SchemaField("payment_method", "STRING"),
            bigquery.SchemaField("platform", "STRING"),
            bigquery.SchemaField("order_status", "STRING"),
            bigquery.SchemaField("delivery_time_mins", "INT64"),
            bigquery.SchemaField("customer_rating", "INT64"),
            bigquery.SchemaField("cuisine_category", "STRING"),
            bigquery.SchemaField("_generated_at", "TIMESTAMP"),
        ],
        "time_partitioning": bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH, field="order_date"
        ),
        "clustering_fields": ["city", "platform", "order_status"],
        "require_partition_filter": True,
    },
    {
        "table_id": "raw_weather",
        "schema": [
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("weather_date", "DATE"),
            bigquery.SchemaField("temperature_max", "FLOAT64"),
            bigquery.SchemaField("temperature_min", "FLOAT64"),
            bigquery.SchemaField("precipitation_sum", "FLOAT64"),
            bigquery.SchemaField("windspeed_max", "FLOAT64"),
            bigquery.SchemaField("weather_code", "INT64"),
            bigquery.SchemaField("weather_description", "STRING"),
            bigquery.SchemaField("is_rainy", "BOOL"),
            bigquery.SchemaField("weather_category", "STRING"),
            bigquery.SchemaField("_fetched_at", "TIMESTAMP"),
        ],
        "time_partitioning": bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="weather_date"
        ),
        "clustering_fields": ["city"],
        "require_partition_filter": False,
    },
    {
        "table_id": "raw_ipl_schedule",
        "schema": [
            bigquery.SchemaField("match_date", "DATE"),
            bigquery.SchemaField("season", "INT64"),
            bigquery.SchemaField("match_number", "INT64"),
            bigquery.SchemaField("team1", "STRING"),
            bigquery.SchemaField("team2", "STRING"),
            bigquery.SchemaField("venue_city", "STRING"),
            bigquery.SchemaField("match_type", "STRING"),
            bigquery.SchemaField("winner", "STRING"),
            bigquery.SchemaField("is_deliveryiq_city", "BOOL"),
            bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("_source_file", "STRING"),
            bigquery.SchemaField("_ingestion_date", "DATE"),
        ],
        "time_partitioning": bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="_ingestion_date"
        ),
        "clustering_fields": ["venue_city"],
        "require_partition_filter": False,
    },
    {
        "table_id": "raw_festival_calendar",
        "schema": [
            bigquery.SchemaField("festival_date", "DATE"),
            bigquery.SchemaField("festival_name", "STRING"),
            bigquery.SchemaField("festival_type", "STRING"),
            bigquery.SchemaField("region", "STRING"),
            bigquery.SchemaField("is_major", "BOOL"),
            bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("_source_file", "STRING"),
            bigquery.SchemaField("_ingestion_date", "DATE"),
        ],
        "time_partitioning": bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="_ingestion_date"
        ),
        "clustering_fields": ["region"],
        "require_partition_filter": False,
    },
    {
        "table_id": "pipeline_audit",
        "schema": [
            bigquery.SchemaField("run_id", "STRING"),
            bigquery.SchemaField("dag_id", "STRING"),
            bigquery.SchemaField("task_id", "STRING"),
            bigquery.SchemaField("source_name", "STRING"),
            bigquery.SchemaField("run_date", "TIMESTAMP"),
            bigquery.SchemaField("rows_processed", "INT64"),
            bigquery.SchemaField("rows_rejected", "INT64"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("error_message", "STRING"),
            bigquery.SchemaField("duration_seconds", "FLOAT64"),
            bigquery.SchemaField("gcs_uri", "STRING"),
        ],
        "time_partitioning": None,
        "clustering_fields": None,
        "require_partition_filter": False,
    },
]


def ensure_dataset(client: bigquery.Client, dataset_id: str, location: str) -> None:
    """Create the dataset if it does not already exist, in the given location."""
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = location
    try:
        client.create_dataset(dataset, exists_ok=False)
        print(f"Created dataset {dataset_id} in {location}")
    except Conflict:
        print(f"Dataset {dataset_id} already exists — skipped")


def create_table_if_not_exists(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    schema: list[bigquery.SchemaField],
    time_partitioning: bigquery.TimePartitioning | None,
    clustering_fields: list[str] | None,
    require_partition_filter: bool,
) -> None:
    """Create a table from its spec, skipping it if it already exists."""
    table_ref = client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)

    if time_partitioning is not None:
        table.time_partitioning = time_partitioning
        table.require_partition_filter = require_partition_filter

    if clustering_fields:
        table.clustering_fields = clustering_fields

    try:
        client.create_table(table)
        print(f"Created table {table_id}")
    except Conflict:
        print(f"Table {table_id} already exists — skipped")


def main() -> None:
    project_id = os.environ["GCP_PROJECT_ID"]
    region = os.getenv("GCP_REGION", "asia-south1")

    client = bigquery.Client(project=project_id, location=region)

    ensure_dataset(client, BRONZE_DATASET, region)

    for spec in TABLE_SPECS:
        create_table_if_not_exists(
            client,
            BRONZE_DATASET,
            spec["table_id"],
            spec["schema"],
            spec["time_partitioning"],
            spec["clustering_fields"],
            spec["require_partition_filter"],
        )


if __name__ == "__main__":
    main()
