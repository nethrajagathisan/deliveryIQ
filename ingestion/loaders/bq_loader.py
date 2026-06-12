"""BigQuery loader for the DeliveryIQ Bronze layer.

Loads Parquet files (local or from GCS) into BigQuery tables and provides
small helpers for running ad-hoc queries and checking table row counts.
"""

from __future__ import annotations

import time
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
