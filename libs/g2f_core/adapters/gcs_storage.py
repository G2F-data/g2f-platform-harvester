"""
Purpose: Google Cloud Storage implementation of the StoragePort.
Usage: Used in production to save Bronze data to the GCS bucket.
Dependencies: google.cloud.storage
"""

import json
import logging
from typing import Any

from google.cloud import storage

logger = logging.getLogger(__name__)


class GCSStorage:
    """Stores raw Bronze JSON files in a Google Cloud Storage bucket."""

    def __init__(self, bucket_name: str):
        """
        Args:
            bucket_name: The target GCS bucket name.
        """
        self.bucket_name = bucket_name
        # The Client automatically looks for
        #  GOOGLE_APPLICATION_CREDENTIALS env var
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def save(self, filename: str, data: dict[str, Any]) -> None:
        """Uploads a dictionary directly to GCS as a JSON blob."""
        # Create a blob (file) object in the bucket
        blob = self.bucket.blob(filename)

        # Upload the JSON string directly
        blob.upload_from_string(
            json.dumps(data, indent=2, default=str),
            content_type="application/json",
        )

    def read(self, filename: str) -> dict[str, Any] | None:
        """Downloads a JSON blob from GCS. Returns None if not found."""
        blob = self.bucket.blob(filename)
        if not blob.exists():
            return None

        content = blob.download_as_string()
        return dict(json.loads(content))

    def delete(self, path: str) -> None:
        """Delete a GCS object. No-op if the object does not exist."""
        blob = self.bucket.blob(path)
        try:
            blob.delete()
        except Exception:
            # google.cloud.exceptions.NotFound or any transient error —
            # log at debug level so terminal-failure cleanup never raises.
            logger.debug("GCSStorage.delete: %s not found or error", path)
