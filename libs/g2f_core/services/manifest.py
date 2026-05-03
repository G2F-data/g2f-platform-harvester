"""
Purpose: Defines operational models for managing stateful data harvesting.
Usage: Used by the Orchestrator to track successes, failures, and blocks.
Dependencies: Pydantic
"""

from datetime import date, datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class TargetStatus(StrEnum):
    """State machine statuses for a specific scrape target."""

    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"


class RaceTargetState(BaseModel):
    """
    Tracks the lifecycle of a single race target during the daily harvest.
    """

    race_id: str
    track_id: str
    r_date: str
    races_ids: str
    r_time: str = "00:00"
    status: TargetStatus = TargetStatus.PENDING
    retries: int = 0
    error_log: str | None = None


class HarvestManifest(BaseModel):
    """
    The orchestrator's central state machine.
    Acts as a lock and recovery file stored in the Bronze layer.
    """

    run_date: date
    start_time: datetime = Field(default_factory=datetime.now)
    runner: str = "local"
    status: str = "running"
    targets: dict[str, RaceTargetState] = Field(default_factory=dict)
    blocked: bool = False
    duration_seconds: float = 0.0

    @property
    def targets_found(self) -> int:
        """Total targets discovered for the day."""
        return len(self.targets)

    @property
    def success_count(self) -> int:
        """Number of targets successfully scraped."""
        return sum(
            1 for t in self.targets.values() if t.status == TargetStatus.SUCCESS
        )

    @property
    def error_count(self) -> int:
        """Number of targets that repeatedly failed without blocking."""
        return sum(
            1 for t in self.targets.values() if t.status == TargetStatus.FAILED
        )

    def get_pending_targets(
        self, max_retries: int = 3
    ) -> list[RaceTargetState]:
        """
        Retrieves all targets that need processing by the current runner.
        Excludes SUCCESS and permanently FAILED targets.
        """
        return [
            t
            for t in self.targets.values()
            if t.status in (TargetStatus.PENDING, TargetStatus.BLOCKED)
            or (t.status == TargetStatus.FAILED and t.retries < max_retries)
        ]
