# filepath: libs/g2f_core/adapters/storage_factory.py
"""
Purpose: Factory function for resolving the correct StoragePort adapter.
Usage:   Call get_storage_adapter() at the composition root.
         APP_ENV=cloud  → GCSStorage (requires GCS_RACECARD_BUCKET_NAME)
         APP_ENV=local  → LocalFileStorage (default)
Dependencies: g2f_core.domain.ports, g2f_core.adapters.gcs_storage,
              g2f_core.adapters.storage
"""

import logging
import os

from g2f_core.adapters.gcs_storage import GCSStorage
from g2f_core.adapters.storage import LocalFileStorage
from g2f_core.domain.ports import StoragePort

logger = logging.getLogger(__name__)


def get_storage_adapter() -> StoragePort:
    """Resolve the active StoragePort adapter from environment.

    Returns:
        GCSStorage when APP_ENV=cloud.
        LocalFileStorage otherwise.

    Raises:
        ValueError: If APP_ENV=cloud but GCS_RACECARD_BUCKET_NAME is not set.
    """
    env = os.getenv("APP_ENV", "local")

    if env == "cloud":
        bucket_name = os.getenv("GCS_RACECARD_BUCKET_NAME")
        if not bucket_name:
            raise ValueError("GCS_RACECARD_BUCKET_NAME is not set")

        logger.info("Storage Factory: Using GCS Bucket [%s]", bucket_name)
        return GCSStorage(bucket_name)

    logger.info("Storage Factory: Using Local Disk [data/bronze]")
    return LocalFileStorage()
