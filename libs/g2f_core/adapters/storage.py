"""
Purpose: Local file system implementation of the StoragePort.
Usage: Used for local development and testing to save Bronze data to disk.
Dependencies: json, pathlib
"""

import json
from pathlib import Path
from typing import Any


class LocalFileStorage:
    """Stores raw Bronze JSON files on the local disk."""

    def __init__(self, base_path: str = "data/bronze"):
        """
        Args:
            base_path: The root directory for storage.
        """
        self.base_dir = Path(base_path)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def save(self, filename: str, data: dict[str, Any]) -> None:
        """Saves a dictionary as a JSON file, creating directories if needed."""
        file_path = self.base_dir / filename
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

    def read(self, filename: str) -> dict[str, Any] | None:
        """Reads a JSON file from disk. Returns None if it doesn't exist."""
        file_path = self.base_dir / filename
        if not file_path.exists():
            return None

        with open(file_path, encoding="utf-8") as f:
            return dict(json.load(f))

    def delete(self, path: str) -> None:
        """Delete a local file. No-op if the file does not exist."""
        full_path = self.base_dir / path
        try:  # noqa: SIM105
            full_path.unlink()
        except FileNotFoundError:
            pass  # noqa: SIM105
