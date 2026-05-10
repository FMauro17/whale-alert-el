"""
StorageClient: MinIO / S3-compatible object storage client using boto3.

Handles bucket creation/validation and CSV upload for the
Whale Alert EL pipeline.
"""

import io
import os
from typing import Optional

import boto3
import botocore.config
from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError

from src.utils.logger import get_logger

logger = get_logger(__name__)


class StorageClient:
    """
    Client for interacting with a MinIO (S3-compatible) bucket.

    Configuration is read from environment variables when explicit
    arguments are not provided:
        - MINIO_ENDPOINT
        - MINIO_ACCESS_KEY
        - MINIO_SECRET_KEY
        - MINIO_BUCKET_NAME

    Args:
        endpoint:    Full URL to the MinIO service  (e.g. http://minio:9000).
        access_key:  MinIO access key.
        secret_key:  MinIO secret key.
        bucket_name: Target bucket name.
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket_name: Optional[str] = None,
    ) -> None:
        self.endpoint: str = endpoint or os.environ["MINIO_ENDPOINT"]
        self.access_key: str = access_key or os.environ["MINIO_ACCESS_KEY"]
        self.secret_key: str = secret_key or os.environ["MINIO_SECRET_KEY"]
        self.bucket_name: str = bucket_name or os.environ["MINIO_BUCKET_NAME"]

        self._client: BaseClient = self._build_client()
        logger.info(
            "StorageClient initialised — endpoint=%s, bucket=%s",
            self.endpoint,
            self.bucket_name,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ensure_bucket_exists(self) -> None:
        """
        Creates the target bucket if it does not already exist.

        Raises:
            ClientError: On unexpected S3/MinIO API errors.
        """
        try:
            self._client.head_bucket(Bucket=self.bucket_name)
            logger.info("Bucket '%s' already exists", self.bucket_name)
        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            if error_code in ("404", "NoSuchBucket"):
                logger.info(
                    "Bucket '%s' not found — creating it now", self.bucket_name
                )
                self._client.create_bucket(Bucket=self.bucket_name)
                logger.info("Bucket '%s' created successfully", self.bucket_name)
            else:
                logger.error(
                    "Unexpected error checking bucket '%s': %s",
                    self.bucket_name,
                    exc,
                )
                raise

    def upload_csv(self, csv_content: str, object_key: str) -> int:
        """
        Uploads a CSV string to the configured bucket.

        Args:
            csv_content: The CSV data as a UTF-8 string.
            object_key:  The S3 object key (file path inside the bucket).

        Returns:
            Number of bytes uploaded.

        Raises:
            ClientError | BotoCoreError: On upload failures.
        """
        encoded: bytes = csv_content.encode("utf-8")
        file_obj = io.BytesIO(encoded)
        byte_count: int = len(encoded)

        logger.info(
            "Uploading '%s' to bucket '%s' (%d bytes)",
            object_key,
            self.bucket_name,
            byte_count,
        )

        try:
            self._client.upload_fileobj(
                Fileobj=file_obj,
                Bucket=self.bucket_name,
                Key=object_key,
                ExtraArgs={"ContentType": "text/csv"},
            )
            logger.info(
                "Upload complete — s3://%s/%s (%d bytes transferred)",
                self.bucket_name,
                object_key,
                byte_count,
            )
            return byte_count

        except (ClientError, BotoCoreError) as exc:
            logger.error(
                "Failed to upload '%s' to bucket '%s': %s",
                object_key,
                self.bucket_name,
                exc,
            )
            raise

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_client(self) -> BaseClient:
        """Instantiates and returns a configured boto3 S3 client."""
        try:
            client = boto3.client(
                "s3",
                endpoint_url=self.endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                # MinIO requires path-style addressing and Signature V4
                config=botocore.config.Config(
                    signature_version="s3v4",
                    s3={"addressing_style": "path"},
                ),
            )
            logger.debug("boto3 S3 client built successfully")
            return client
        except Exception as exc:
            logger.error("Failed to build S3 client: %s", exc)
            raise

