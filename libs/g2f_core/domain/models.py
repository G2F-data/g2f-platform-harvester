# filepath: libs/g2f_core/domain/models.py
"""
Purpose: Domain models for the Silver layer — pure Python, zero external deps.
Usage: These are the ACL boundary objects. Both BronzeV2 and any future
       adapters must produce these objects. The repository is source-agnostic.

Structural Rules

Race (Aggregate Root)
  ├─ Track (Entity)
  ├─ WeatherContext (Value Object)
  └─ RaceEntry (Entity) — one per trap, 1-6 per race
       ├─ Greyhound (Entity) — the biological identity
       ├─ PastRun (Value Object) — form history, frozen at scrape time
       └─ RunResult (Value Object) — race outcome, None until finished

Point-in-Time Accuracy

- `form_history`: Exactly what the punter saw on race day. Immutable.
- `dog_name_snapshot`: Preserves the historical name if a dog is renamed.

No Sentinels

`r_time` is `time | None`. When scraped before publish, it is `None`.
Using midnight ("00:00") as a sentinel corrupts ML time-of-day features.
"""

from __future__ import annotations

import re
from datetime import date, time
from enum import StrEnum
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

# ── Enums ──


class Sex(StrEnum):
    DOG = "DOG"
    BITCH = "BITCH"


class RaceType(StrEnum):
    FLAT = "Flat"
    HURDLE = "Hurdle"
    CHASE = "Chase"


class RaceEntryStatus(StrEnum):
    RUNNER = "RUNNER"
    WITHDRAWN = "WITHDRAWN"
    RESERVE = "RESERVE"


# ── Value Objects


class WeatherContext(BaseModel):
    """Atmospheric conditions at race time."""

    model_config = ConfigDict(frozen=True)

    going_allowance: int = 0
    temperature_c: float | None = None
    precipitation_mm: float = 0.0
    humidity_pct: float | None = Field(None, ge=0, le=100)
    wind_speed_kph: float | None = None
    wind_direction: str | None = None


class Track(BaseModel):
    """Racing venue identity."""

    model_config = ConfigDict(frozen=True)

    track_id: str
    name: str | None = None
    latitude: float | None = None
    longitude: float | None = None


class PastRun(BaseModel):
    """One past race as it appeared on the card on race day.

    run_date is date | None:
      - A valid past race -> date object (parsed from "14Mar25")
      - A trial run with no date -> None

    finish_pos is int | None:
      - "1st" -> 1, "2nd" -> 2
      - "DNF", "DIS", "" -> None
    """

    model_config = ConfigDict(frozen=True)

    run_date: date | None = None
    track_short: str = ""
    grade: str = ""
    distance_meters: int = 0
    trap: int = 0
    finish_pos: int | None = None
    finish_pos_raw: str = ""
    distance_beaten: str = ""
    win_time: float = 0.0
    calc_time: float = 0.0
    split_time: float = 0.0
    going: str = ""
    remarks: str = ""
    competitor_name: str = ""
    competitor_id: str = ""
    date_race_id: str = ""
    sp: str = ""
    weight_kg: float = 0.0
    bends: str = ""

    @field_validator("run_date", mode="before")
    @classmethod
    def parse_run_date(cls, v: Any) -> date | None:
        if v is None or v == "":
            return None
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return None
            months = {
                "Jan": 1,
                "Feb": 2,
                "Mar": 3,
                "Apr": 4,
                "May": 5,
                "Jun": 6,
                "Jul": 7,
                "Aug": 8,
                "Sep": 9,
                "Oct": 10,
                "Nov": 11,
                "Dec": 12,
            }
            m = re.match(r"^(\d{1,2})([A-Za-z]{3})(\d{2})$", v)
            if m:
                day = int(m.group(1))
                month = months.get(m.group(2).capitalize())
                year = 2000 + int(m.group(3))
                if month:
                    return date(year, month, day)
            try:
                from datetime import datetime

                return datetime.strptime(v, "%Y-%m-%d").date()
            except ValueError:
                pass
        return None

    @field_validator("finish_pos", mode="before")
    @classmethod
    def parse_finish_pos(cls, v: Any) -> int | None:
        if v is None or v == "":
            return None
        if isinstance(v, int):
            return v
        s = str(v).strip()
        m = re.match(r"^(\d+)(st|nd|rd|th)$", s, re.IGNORECASE)
        if m:
            return int(m.group(1))
        if s.isdigit():
            return int(s)
        return None

    @field_validator("distance_meters", mode="before")
    @classmethod
    def parse_distance(cls, v: Any) -> int:
        if isinstance(v, int):
            return v
        if isinstance(v, str):
            m = re.search(r"(\d+)", v)
            if m:
                d = int(m.group(1))
                return round(d * 0.9144) if "y" in v.lower() else d
        return 0

    @field_validator(
        "win_time",
        "calc_time",
        "split_time",
        "weight_kg",
        mode="before",
    )
    @classmethod
    def parse_float(cls, v: Any) -> float:
        if v is None or v == "":
            return 0.0
        try:
            return float(v)
        except (ValueError, TypeError):
            return 0.0

    @field_validator("trap", mode="before")
    @classmethod
    def parse_trap(cls, v: Any) -> int:
        if v is None or v == "":
            return 0
        s = str(v).strip("[]")
        try:
            return int(s)
        except (ValueError, TypeError):
            return 0


class RunResult(BaseModel):
    """Race outcome for one entry. Only populated after the race.

    Attributes:
        finish_position: 1-8 for placed runners; 0 = NR/withdrawn.
        time_raw: Original time string from source ("16.56" for
            winner, "3/4" for others).
        winning_time: Absolute time in seconds (calculated via
            chain algebra for non-winners).
        distance_beaten: Lengths behind previous finisher.
        behind_first: Cumulative lengths behind winner (0 for 1st).
        starting_price: Raw odds string (e.g. "6/5F").
    """

    model_config = ConfigDict(frozen=True)

    finish_position: int = Field(
        ..., ge=0, le=8, description="0 = NR/withdrawn"
    )
    time_raw: str | None = None
    winning_time: float | None = None
    sectional_time: float | None = None
    distance_beaten: float | None = None
    behind_first: float | None = None
    bends: str | None = None
    comment: str | None = Field(None, description="Remarks")
    weight_kg: float | None = None
    starting_price: str | None = None
    status_code: str | None = None


# ── Core Entities


class Greyhound(BaseModel):
    """The biological identity of a racing dog.

    Holds only what never changes about the dog. Stats, trainer, weight
    are NOT here — they belong on RaceEntry, frozen at scrape time.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    dog_id: str
    name: str
    whelp_date: date | None = None
    sex: Sex | None = None
    color: str | None = None
    sire_name: str | None = None
    dam_name: str | None = None

    @field_validator("whelp_date", mode="before")
    @classmethod
    def parse_whelp_date(cls, v: Any) -> date | None:
        return PastRun.parse_run_date(v)


class RaceEntry(BaseModel):
    """One dog's participation in one race. Pre-race data only.

    greyhound: The dog's biological identity (nested entity).
    dog_name_snapshot: Point-in-time string of the dog's name.
    form_history: Frozen list of PastRun value objects.
    result: The outcome (None until the race finishes).
    """

    model_config = ConfigDict(frozen=True)

    trap: int = Field(..., ge=1, le=8)
    greyhound: Greyhound
    dog_name_snapshot: str = ""

    trainer_name: str | None = None
    status: RaceEntryStatus = RaceEntryStatus.RUNNER
    seeding: str | None = None
    sp_forecast: str | None = None
    topspeed: str | None = None
    form_string: str | None = None
    comment: str = ""

    form_history: list[PastRun] = Field(default_factory=list)
    result: RunResult | None = None


# ── Aggregate Root ──


class Race(BaseModel):
    """Aggregate root for one race event."""

    model_config = ConfigDict(frozen=True)

    race_id: str
    track: Track
    r_date: date
    r_time: time | None = None

    grade: str = ""
    distance_meters: int = 0
    race_type: RaceType = RaceType.FLAT

    weather: WeatherContext | None = None
    entries: list[RaceEntry] = Field(default_factory=list)

    win_prize: str | None = None
    race_class: str | None = None

    stats: dict[str, Any] = Field(default_factory=dict)
    tips: dict[str, Any] = Field(default_factory=dict)

    @field_validator("r_date", mode="before")
    @classmethod
    def parse_r_date(cls, v: Any) -> date:
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            from datetime import datetime

            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue
        raise ValueError(f"Cannot parse r_date: {v!r}")

    @field_validator("r_time", mode="before")
    @classmethod
    def parse_r_time(cls, v: Any) -> time | None:
        """None means time not yet published."""
        if v is None or v == "":
            return None
        if v in ("00:00", "00:00:00"):
            return None
        if isinstance(v, time):
            if v == time(0, 0):
                return None
            return v
        if isinstance(v, str):
            try:
                # Handle HH:MM and HH:MM:SS
                parts = v.strip().split(":")
                return time(int(parts[0]), int(parts[1]))
            except (ValueError, AttributeError, IndexError):
                pass
        return None

    @field_validator("distance_meters", mode="before")
    @classmethod
    def parse_distance_meters(cls, v: Any) -> int:
        """Accept int or distance strings like '503m', '550y'."""
        if isinstance(v, int):
            return v
        if isinstance(v, str):
            m = re.search(r"(\d+)", v)
            if m:
                d = int(m.group(1))
                return round(d * 0.9144) if "y" in v.lower() else d
        return 0

    @model_validator(mode="after")
    def validate_entries(self) -> Race:
        """Reject duplicate traps. Sort entries by trap."""
        traps = [e.trap for e in self.entries if e.trap > 0]
        if len(traps) != len(set(traps)):
            dups = [t for t in set(traps) if traps.count(t) > 1]
            raise ValueError(
                f"Duplicate traps in race {self.race_id}: " f"{dups}"
            )
        sorted_entries = sorted(self.entries, key=lambda e: e.trap)
        object.__setattr__(self, "entries", sorted_entries)
        return self

    @property
    def is_finished(self) -> bool:
        """True if any entry has a RunResult attached."""
        return any(e.result is not None for e in self.entries)
