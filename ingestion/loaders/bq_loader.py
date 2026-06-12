"""BigQuery loader for the DeliveryIQ Bronze layer.

Loads Parquet files (local or from GCS) into BigQuery tables and provides
small helpers for running ad-hoc queries and checking table row counts.
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

from ingestion.utils.logger import get_logger

log = get_logger(__name__)

_QUERY_PREVIEW_LENGTH = 200


class BigQueryLoader:
    """Loads data into BigQuery and runs queries against it."""

    def __init__(self, project_id: str, dataset_id: str) -> None:
        self.project_id = project_id
        self.dataset_id = dataset_id

        self.client = bigquery.Client(project=project_id)
        self.dataset_ref = self.client.dataset(dataset_id)

    def _table_ref(self, table_id: str) -> bigquery.TableReference:
        return self.dataset_ref.table(table_id)

    def load_parquet_to_table(
        self,
        parquet_path: Path,
        table_id: str,
        write_disposition: str = "WRITE_TRUNCATE",
    ) -> dict:
        """Load a local Parquet file into a BigQuery table.

        Returns:
            ``{rows_loaded, job_id, table_id, duration_seconds}``.
        """
        parquet_path = Path(parquet_path)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
        )

        start = time.monotonic()
        with open(parquet_path, "rb") as f:
            job = self.client.load_table_from_file(
                f, self._table_ref(table_id), job_config=job_config
            )
        job.result()
        duration_seconds = time.monotonic() - start

        result = {
            "rows_loaded": job.output_rows,
            "job_id": job.job_id,
            "table_id": table_id,
            "duration_seconds": duration_seconds,
        }

        log.info(
            "loaded parquet to bigquery",
            extra={"extra_fields": {**result, "source": str(parquet_path)}},
        )
        return result

    def load_gcs_to_table(
        self,
        gcs_uri: str,
        table_id: str,
        write_disposition: str = "WRITE_TRUNCATE",
    ) -> dict:
        """Load a Parquet file from GCS into a BigQuery table.

        Returns:
            ``{rows_loaded, job_id, table_id, duration_seconds}``.
        """
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
        )

        start = time.monotonic()
        job = self.client.load_table_from_uri(
            gcs_uri, self._table_ref(table_id), job_config=job_config
        )
        job.result()
        duration_seconds = time.monotonic() - start

        result = {
            "rows_loaded": job.output_rows,
            "job_id": job.job_id,
            "table_id": table_id,
            "duration_seconds": duration_seconds,
        }

        log.info(
            "loaded gcs file to bigquery",
            extra={"extra_fields": {**result, "source": gcs_uri}},
        )
        return result

    def upsert_from_parquet(
        self,
        parquet_path: Path,
        table_id: str,
        unique_key_columns: list[str],
    ) -> dict:
        """Upsert a local Parquet file into a BigQuery table.

        Loads the file into a temporary staging table, deduplicates rows
        that share the same ``unique_key_columns`` (keeping one row per
        key), then runs a ``MERGE`` into ``table_id`` — updating matching
        rows and inserting new ones. The staging table is dropped afterwards.

        Returns:
            ``{rows_loaded, rows_affected, table_id, duration_seconds}``.
        """
        parquet_path = Path(parquet_path)
        staging_table_id = f"_staging_{table_id}_{uuid.uuid4().hex[:8]}"

        start = time.monotonic()

        load_result = self.load_parquet_to_table(
            parquet_path, staging_table_id, write_disposition="WRITE_TRUNCATE"
        )

        try:
            staging_table = self.client.get_table(self._table_ref(staging_table_id))
            columns = [field.name for field in staging_table.schema]

            match_clause = " AND ".join(f"T.{col} = S.{col}" for col in unique_key_columns)
            update_clause = ", ".join(f"{col} = S.{col}" for col in columns)
            insert_columns = ", ".join(columns)
            insert_values = ", ".join(f"S.{col}" for col in columns)
            partition_clause = ", ".join(unique_key_columns)

            merge_sql = f"""
            MERGE `{self.project_id}.{self.dataset_id}.{table_id}` T
            USING (
                SELECT * EXCEPT(_row_number) FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_clause}) AS _row_number
                    FROM `{self.project_id}.{self.dataset_id}.{staging_table_id}`
                )
                WHERE _row_number = 1
            ) S
            ON {match_clause}
            WHEN MATCHED THEN UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
            """

            job = self.client.query(merge_sql)
            job.result()
            rows_affected = job.num_dml_affected_rows
        finally:
            self.client.delete_table(self._table_ref(staging_table_id), not_found_ok=True)

        duration_seconds = time.monotonic() - start

        result = {
            "rows_loaded": load_result["rows_loaded"],
            "rows_affected": rows_affected,
            "table_id": table_id,
            "duration_seconds": duration_seconds,
        }

        log.info(
            "upserted parquet to bigquery table",
            extra={"extra_fields": {**result, "source": str(parquet_path)}},
        )
        return result

    def run_query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return the result as a DataFrame."""
        preview = sql[:_QUERY_PREVIEW_LENGTH]

        start = time.monotonic()
        df = self.client.query(sql).to_dataframe()
        duration_seconds = time.monotonic() - start

        log.info(
            "executed bigquery query",
            extra={
                "extra_fields": {
                    "query_preview": preview,
                    "duration_seconds": duration_seconds,
                    "row_count": len(df),
                }
            },
        )
        return df

    def table_row_count(self, table_id: str) -> int:
        """Return the row count for a table, or 0 if it does not exist."""
        table_ref = self._table_ref(table_id)

        try:
            table = self.client.get_table(table_ref)
        except Exception:
            return 0

        sql = (
            "SELECT row_count FROM "
            f"`{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.TABLE_STORAGE` "
            f"WHERE table_name = '{table_id}'"
        )
        result = self.client.query(sql).to_dataframe()
        if result.empty:
            return table.num_rows or 0

        return int(result["row_count"].iloc[0])
