"""End-to-end Bronze layer ingestion test harness.

Runs every Bronze source extractor and loads its output into GCS and
BigQuery, exactly as the future Airflow DAGs will — but as a single local
script so the pipeline can be proven correct before orchestration is added.

Usage:
    python scripts/run_bronze_ingestion.py --source all --date 2023-01-15
    python scripts/run_bronze_ingestion.py --source weather --date 2023-01-15
    python scripts/run_bronze_ingestion.py --source all --date 2023-01-15 --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from ingestion.extractors.festival_calendar_extractor import FestivalCalendarExtractor  # noqa: E402
from ingestion.extractors.ipl_schedule_extractor import IPLScheduleExtractor  # noqa: E402
from ingestion.extractors.orders_generator import FoodOrdersGenerator  # noqa: E402
from ingestion.extractors.weather_extractor import IndianCityWeatherExtractor  # noqa: E402
from ingestion.extractors.zomato_extractor import (  # noqa: E402
    INDIA_COUNTRY_CODE,
    ZomatoExtractor,
    _to_snake_case,
)
from ingestion.loaders.bq_loader import BigQueryLoader  # noqa: E402
from ingestion.loaders.gcs_loader import GCSLoader  # noqa: E402
from ingestion.utils.logger import get_logger  # noqa: E402
from ingestion.utils.pipeline_run import PipelineRun  # noqa: E402
from ingestion.utils.watermark import PipelineWatermark  # noqa: E402

log = get_logger(__name__)

BRONZE_DATASET = "deliveryiq_bronze"
DAG_ID = "bronze_ingestion_harness"

ZOMATO_CSV_PATH = PROJECT_ROOT / "data" / "raw" / "zomato" / "zomato.csv"
IPL_CSV_PATH = PROJECT_ROOT / "data" / "raw" / "ipl_schedule" / "ipl_matches_2020_2024.csv"
FESTIVAL_CSV_PATH = PROJECT_ROOT / "data" / "raw" / "festival_calendar" / "indian_festivals_2020_2024.csv"
ORDERS_DIR = PROJECT_ROOT / "data" / "raw" / "orders"

ALL_SOURCES = ["zomato", "orders", "weather", "ipl", "festivals"]

ORDERS_GENERATION_DAYS = 30
ORDERS_PER_RESTAURANT_PER_DAY = 3.5
ORDERS_SEED = 42

WEATHER_LOOKBACK_DAYS = 7


def _upload_to_gcs(gcs_loader: GCSLoader, local_path: Path, gcs_path: str) -> str:
    """Upload a local file to a specific GCS path and return its gs:// URI."""
    blob = gcs_loader.bucket.blob(gcs_path)
    blob.upload_from_filename(str(local_path))
    return f"gs://{gcs_loader.bucket_name}/{gcs_path}"


def load_indian_restaurants(csv_path: Path) -> pd.DataFrame:
    """Load the Zomato CSV, filter to India, and rename columns to snake_case."""
    df = pd.read_csv(csv_path, encoding="latin-1")
    indian_df = df[df["Country Code"] == INDIA_COUNTRY_CODE].copy()
    indian_df.columns = [_to_snake_case(col) for col in indian_df.columns]
    return indian_df


def _existing_order_ids(bq_loader: BigQueryLoader, order_dates: list[str]) -> set[str]:
    """Return the order_ids already present in raw_orders for the given dates."""
    dates = [datetime.strptime(d, "%Y-%m-%d").date() for d in order_dates]

    sql = f"""
        SELECT order_id
        FROM `{bq_loader.project_id}.{bq_loader.dataset_id}.raw_orders`
        WHERE order_date IN UNNEST(@dates)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("dates", "DATE", dates)]
    )

    try:
        rows = bq_loader.client.query(sql, job_config=job_config).result()
        return {row.order_id for row in rows}
    except Exception:
        log.warning("could not check existing order_ids — assuming none exist")
        return set()


def ingest_zomato(args: argparse.Namespace, gcs_loader: GCSLoader | None, bq_loader: BigQueryLoader | None) -> int:
    """Extract Zomato restaurants and load them into raw_restaurants. Returns row count."""
    extractor = ZomatoExtractor(gcs_loader, ZOMATO_CSV_PATH, args.date)
    manifest = extractor.extract()
    rows = manifest["indian_rows"]

    if not args.dry_run:
        with PipelineRun(bq_loader, dag_id=DAG_ID, source="zomato") as run:
            gcs_path = f"zomato/raw/ingestion_date={args.date}/restaurants.parquet"
            run.gcs_uri = _upload_to_gcs(gcs_loader, Path(manifest["parquet_path"]), gcs_path)
            bq_loader.load_gcs_to_table(run.gcs_uri, "raw_restaurants", write_disposition="WRITE_TRUNCATE")
            run.record_rows(processed=rows)

    print(f"Zomato: {rows:,} restaurants loaded to BQ")
    return rows


def ingest_orders(args: argparse.Namespace, gcs_loader: GCSLoader | None, bq_loader: BigQueryLoader | None) -> int:
    """Load or generate food orders and append new rows to raw_orders. Returns row count."""
    existing_partitions = sorted(ORDERS_DIR.glob("order_date=*/orders.parquet"))

    if existing_partitions:
        orders_df = pd.concat(
            (pd.read_parquet(p) for p in existing_partitions), ignore_index=True
        )
    else:
        restaurants_df = load_indian_restaurants(ZOMATO_CSV_PATH)

        start_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        end_date = start_date + timedelta(days=ORDERS_GENERATION_DAYS - 1)

        generator = FoodOrdersGenerator(
            restaurants_df=restaurants_df,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            seed=ORDERS_SEED,
        )
        orders_df = generator.generate_orders(
            orders_per_restaurant_per_day=ORDERS_PER_RESTAURANT_PER_DAY
        )

    rows = len(orders_df)

    if not args.dry_run:
        with PipelineRun(bq_loader, dag_id=DAG_ID, source="orders") as run:
            order_dates = sorted(orders_df["order_date"].unique().tolist())
            existing_ids = _existing_order_ids(bq_loader, order_dates)

            new_df = orders_df[~orders_df["order_id"].isin(existing_ids)]
            rejected = rows - len(new_df)

            parquet_path = Path(f"/tmp/orders_{args.date}.parquet")
            new_df.to_parquet(parquet_path, index=False)

            gcs_path = f"orders/raw/order_date={args.date}/orders.parquet"
            run.gcs_uri = _upload_to_gcs(gcs_loader, parquet_path, gcs_path)

            if len(new_df) > 0:
                bq_loader.load_gcs_to_table(run.gcs_uri, "raw_orders", write_disposition="WRITE_APPEND")

            run.record_rows(processed=len(new_df), rejected=rejected)

    print(f"Orders: {rows:,} orders loaded to BQ")
    return rows


def ingest_weather(args: argparse.Namespace, gcs_loader: GCSLoader | None, bq_loader: BigQueryLoader | None) -> tuple[int, int]:
    """Fetch weather since the last watermark and upsert into raw_weather. Returns (rows, city_count)."""
    extractor = IndianCityWeatherExtractor(gcs_loader)

    watermark = PipelineWatermark(gcs_loader) if gcs_loader is not None else None
    last_date = watermark.get_watermark("weather") if watermark is not None else None

    end_date = args.date
    if last_date:
        start_date = (datetime.strptime(last_date, "%Y-%m-%d").date() + timedelta(days=1)).isoformat()
    else:
        start_date = (
            datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=WEATHER_LOOKBACK_DAYS - 1)
        ).isoformat()

    if start_date > end_date:
        print("Weather: 0 rows upserted for 0 cities (already up to date)")
        return 0, 0

    raw_responses = extractor.fetch_all_cities(start_date, end_date)
    n_cities = len(raw_responses)
    total_rows = 0

    if not args.dry_run:
        with PipelineRun(bq_loader, dag_id=DAG_ID, source="weather") as run:
            for city, raw_response in raw_responses.items():
                df = extractor.flatten_to_dataframe(city, raw_response)
                total_rows += len(df)

                parquet_path = Path(f"/tmp/weather_{city}_{args.date}.parquet")
                df.to_parquet(parquet_path, index=False)

                gcs_path = f"weather/raw/{city}/date={args.date}/weather.parquet"
                _upload_to_gcs(gcs_loader, parquet_path, gcs_path)

                bq_loader.upsert_from_parquet(
                    parquet_path, "raw_weather", unique_key_columns=["city", "weather_date"]
                )

            watermark.set_watermark("weather", end_date)
            run.record_rows(processed=total_rows)
    else:
        for city, raw_response in raw_responses.items():
            df = extractor.flatten_to_dataframe(city, raw_response)
            total_rows += len(df)

    print(f"Weather: {total_rows} rows upserted for {n_cities} cities")
    return total_rows, n_cities


def ingest_ipl(args: argparse.Namespace, gcs_loader: GCSLoader | None, bq_loader: BigQueryLoader | None) -> tuple[int, int]:
    """Extract the IPL schedule seed and full-reload raw_ipl_schedule. Returns (rows, deliveryiq_city_rows)."""
    extractor = IPLScheduleExtractor(source_path=IPL_CSV_PATH)
    manifest = extractor.extract(args.date)
    rows = manifest["row_count"]
    deliveryiq_rows = manifest["deliveryiq_city_rows"]

    if not args.dry_run:
        with PipelineRun(bq_loader, dag_id=DAG_ID, source="ipl_schedule") as run:
            gcs_path = f"ipl_schedule/raw/ingestion_date={args.date}/ipl_schedule.parquet"
            run.gcs_uri = _upload_to_gcs(gcs_loader, Path(manifest["parquet_path"]), gcs_path)
            bq_loader.load_gcs_to_table(run.gcs_uri, "raw_ipl_schedule", write_disposition="WRITE_TRUNCATE")

            PipelineWatermark(gcs_loader).set_watermark("ipl_schedule", args.date)
            run.record_rows(processed=rows)

    print(f"IPL schedule: {rows} matches loaded to BQ ({deliveryiq_rows} in DeliveryIQ cities)")
    return rows, deliveryiq_rows


def ingest_festivals(args: argparse.Namespace, gcs_loader: GCSLoader | None, bq_loader: BigQueryLoader | None) -> tuple[int, int]:
    """Extract the festival calendar seed and full-reload raw_festival_calendar. Returns (rows, major_count)."""
    extractor = FestivalCalendarExtractor(source_path=FESTIVAL_CSV_PATH)
    manifest = extractor.extract(args.date)
    rows = manifest["row_count"]
    major_rows = manifest["major_festival_count"]

    if not args.dry_run:
        with PipelineRun(bq_loader, dag_id=DAG_ID, source="festival_calendar") as run:
            gcs_path = f"festival_calendar/raw/ingestion_date={args.date}/festival_calendar.parquet"
            run.gcs_uri = _upload_to_gcs(gcs_loader, Path(manifest["parquet_path"]), gcs_path)
            bq_loader.load_gcs_to_table(run.gcs_uri, "raw_festival_calendar", write_disposition="WRITE_TRUNCATE")

            PipelineWatermark(gcs_loader).set_watermark("festival_calendar", args.date)
            run.record_rows(processed=rows)

    print(f"Festival calendar: {rows} festivals loaded to BQ ({major_rows} major)")
    return rows, major_rows


def print_summary(date_str: str, results: dict, duration_seconds: float) -> None:
    print("\n=== Bronze Ingestion Summary ===")
    print(f"Date: {date_str}")

    if "zomato" in results:
        rows = results["zomato"]
        print(f"Zomato restaurants: {rows:>6,} rows  ✓")

    if "orders" in results:
        rows = results["orders"]
        print(f"Food orders:        {rows:>6,} rows  ✓")

    if "weather" in results:
        rows, n_cities = results["weather"]
        days = rows // n_cities if n_cities else 0
        print(f"Weather data:       {rows:>6,} rows  ✓ ({n_cities} cities x {days} days)")

    if "ipl" in results:
        rows, deliveryiq_rows = results["ipl"]
        print(f"IPL schedule:       {rows:>6,} rows  ✓ ({deliveryiq_rows} in DeliveryIQ cities)")

    if "festivals" in results:
        rows, major_rows = results["festivals"]
        print(f"Festival calendar:  {rows:>6,} rows  ✓ ({major_rows} major)")

    print(f"Total duration:     {duration_seconds:.1f}s")


def main() -> None:
    # Ensure the ✓ summary marker prints correctly on terminals (e.g. Windows
    # cp1252) that don't default to UTF-8 stdout.
    if sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=date.today().isoformat(), help="Ingestion date (YYYY-MM-DD).")
    parser.add_argument(
        "--source",
        choices=["zomato", "orders", "weather", "ipl", "festivals", "all"],
        default="all",
        help="Which source to ingest.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Process data locally without uploading to GCS or loading into BigQuery.",
    )
    args = parser.parse_args()

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        parser.error(f"--date must be in YYYY-MM-DD format, got {args.date!r}")

    if args.dry_run:
        gcs_loader: GCSLoader | None = None
        bq_loader: BigQueryLoader | None = None
    else:
        project_id = os.environ["GCP_PROJECT_ID"]
        bucket_name = os.environ["GCS_BUCKET_NAME"]
        gcs_loader = GCSLoader(project_id, bucket_name)
        bq_loader = BigQueryLoader(project_id, BRONZE_DATASET)

    sources = ALL_SOURCES if args.source == "all" else [args.source]

    start = time.monotonic()
    results: dict = {}

    if "zomato" in sources:
        results["zomato"] = ingest_zomato(args, gcs_loader, bq_loader)
    if "orders" in sources:
        results["orders"] = ingest_orders(args, gcs_loader, bq_loader)
    if "weather" in sources:
        results["weather"] = ingest_weather(args, gcs_loader, bq_loader)
    if "ipl" in sources:
        results["ipl"] = ingest_ipl(args, gcs_loader, bq_loader)
    if "festivals" in sources:
        results["festivals"] = ingest_festivals(args, gcs_loader, bq_loader)

    duration_seconds = time.monotonic() - start
    print_summary(args.date, results, duration_seconds)


if __name__ == "__main__":
    main()
