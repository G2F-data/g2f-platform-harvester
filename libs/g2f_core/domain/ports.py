# filepath: libs/g2f_core/domain/ports.py
"""
Purpose: Defines the abstract interfaces (Ports) for the Racing domain.
Usage: Used by domain services to interact with
    external systems without coupling.
Dependencies: None.
"""

import abc
from datetime import date
from typing import Any, Protocol

from g2f_core.domain.models import Race


class ScraperPort(Protocol):
    """Interface for web scraping adapters."""

    async def get_race_ids(self, date_str: date) -> list[str]:
        """Fetches a list of unique race IDs for a given date."""
        ...

    async def fetch_race_raw(self, race_id: str) -> dict[str, Any]:
        """
        Navigates to a specific race and harvests all raw HTML/JSON data.

        Returns:
            Dictionary structure containing html_snapshots and dog fragments.
        """
        ...


class StoragePort(Protocol):
    """Interface for key-value doc storage (e.g., GCS, Local File System)."""

    def save(self, path: str, data: dict[str, Any]) -> None:
        """
        Saves a dictionary as a JSON file.

        Args:
            path: Relative path/key (e.g. "2025/12/20/race_123.json")
            data: The dictionary to save
        """
        ...

    def read(self, path: str) -> dict[str, Any] | None:
        """
        Reads a JSON file from storage and returns it as a dictionary.

        Args:
            path: Relative path/key to read.

        Returns:
            The parsed dictionary, or None if the file does not exist.
        """
        ...

    @abc.abstractmethod
    def delete(self, path: str) -> None:
        """Delete the object at path.

        Must be a no-op (not raise) when the object does not exist.
        This contract allows safe use in terminal-failure cleanup without
        requiring callers to check existence first.
        """
        ...


class RaceRepositoryPort(Protocol):
    """Interface for relational database persistence of Race data."""

    def save(self, race: Race) -> None:
        """
        Persists a Race domain object.
        Must handle 'Upsert' logic (Insert if new, Update if exists).
        This includes cascading updates to Tracks, Dogs, and Entries.
        """
        ...

    def exists(self, race_id: str) -> bool:
        """
        Checks if a race already exists in the database.
        Used by the orchestration service to skip already processed races.
        """
        ...
