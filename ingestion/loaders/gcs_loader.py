"""GCS loader for the DeliveryIQ Bronze layer.

Uploads local Parquet files to Google Cloud Storage with MD5-based
idempotent deduplication, so re-running an ingestion job for an
unchanged file is a no-op.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from google.cloud import storage

from ingestion.utils.logger import get_logger

log = get_logger(__name__)

_MD5_CHUNK_SIZE = 8192
_MD5_METADATA_KEY = "md5_checksum"


def _compute_file_md5(local_path: Path) -> str:
    """Compute the MD5 hex digest of a local file."""
    md5 = hashlib.md5()
    with open(local_path, "rb") as f:
        for chunk in iter(lambda: f.read(_MD5_CHUNK_SIZE), b""):
            md5.update(chunk)
    return md5.hexdigest()


class GCSLoader:
    """Uploads Bronze-layer files to a GCS bucket."""

    def __init__(self, project_id: str, bucket_name: str) -> None:
        self.project_id = project_id
        self.bucket_name = bucket_name

        client = storage.Client(project=project_id)
        self.bucket = client.bucket(bucket_name)

    def _build_gcs_path(self, local_path: Path, gcs_prefix: str, partition_key: str | None) -> str:
        if partition_key:
            return f"{gcs_prefix}/ingestion_date={partition_key}/{local_path.name}"
        return f"{gcs_prefix}/{local_path.name}"

    def upload_file(
        self,
        local_path: Path,
        gcs_prefix: str,
        partition_key: str | None = None,
    ) -> str:
        """Upload a local file to GCS, optionally partitioned by ingestion date.

        Returns:
            The full ``gs://`` URI of the uploaded file.
        """
        local_path = Path(local_path)
        gcs_path = self._build_gcs_path(local_path, gcs_prefix, partition_key)

        blob = self.bucket.blob(gcs_path)
        blob.upload_from_filename(str(local_path))

        uri = f"gs://{self.bucket_name}/{gcs_path}"
        log.info(
            "uploaded file to gcs",
            extra={"extra_fields": {"local_path": str(local_path), "gcs_uri": uri}},
        )
        return uri

    def upload_parquet_with_checksum(
        self,
        local_path: Path,
        gcs_prefix: str,
        partition_key: str | None = None,
    ) -> dict:
        """Upload a Parquet file to GCS, skipping the upload if unchanged.

        Computes the local file's MD5 and compares it against the
        ``md5_checksum`` custom metadata on the existing blob (if any).

        Returns:
            ``{uploaded: False, uri}`` if the blob already exists with a
            matching MD5, otherwise ``{uploaded: True, uri, md5, size_bytes}``.
        """
        local_path = Path(local_path)
        gcs_path = self._build_gcs_path(local_path, gcs_prefix, partition_key)
        uri = f"gs://{self.bucket_name}/{gcs_path}"

        md5_hex = _compute_file_md5(local_path)

        blob = self.bucket.blob(gcs_path)
        if blob.exists():
            blob.reload()
            existing_md5 = (blob.metadata or {}).get(_MD5_METADATA_KEY)
            if existing_md5 == md5_hex:
                log.info(
                    "skip — MD5 match",
                    extra={"extra_fields": {"gcs_uri": uri, "md5": md5_hex}},
                )
                return {"uploaded": False, "uri": uri}

        blob.metadata = {_MD5_METADATA_KEY: md5_hex}
        blob.upload_from_filename(str(local_path))

        size_bytes = local_path.stat().st_size

        log.info(
            "uploaded parquet to gcs",
            extra={
                "extra_fields": {
                    "gcs_uri": uri,
                    "md5": md5_hex,
                    "size_bytes": size_bytes,
                }
            },
        )
        return {"uploaded": True, "uri": uri, "md5": md5_hex, "size_bytes": size_bytes}

    def list_blobs(self, prefix: str) -> list[str]:
        """Return the names of all blobs under the given prefix."""
        return [blob.name for blob in self.bucket.list_blobs(prefix=prefix)]

    def blob_exists(self, gcs_path: str) -> bool:
        """Return True if a blob exists at the given path within the bucket."""
        return self.bucket.blob(gcs_path).exists()
