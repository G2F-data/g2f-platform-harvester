# filepath: libs/g2f_core/adapters/db/schema.py
"""
Purpose: SQLAlchemy 2.0 ORM schema for the Silver PostgreSQL database.
Usage:   Imported by the repository and Alembic migrations.
Dependencies: sqlalchemy

Table Layout:
    tracks          — Racing venue identity
    trainers        — Trainer identity (name-keyed, auto-increment PK)
    greyhounds      — Dog biological identity
    races           — Aggregate root, one per race event
    race_entries    — Pre-race data: one per dog per race (features)
    race_results    — Post-race data: one per dog per race (labels)

ML Separation:
    race_entries holds ONLY pre-race information (features).
    race_results holds ONLY post-race information (labels).
    Physical separation prevents accidental data leakage in
    feature-engineering queries.
"""

from __future__ import annotations

import enum
from typing import Any

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    Time,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
)

# ── Base ──


class Base(DeclarativeBase):
    """Shared base for all ORM models."""

    pass


# ── Enums ──


class BronzeSourceEnum(str, enum.Enum):
    V2_GCS = "v2_gcs"
    V1_POSTGRES = "v1_postgres"


class IngestionStatusEnum(str, enum.Enum):
    PENDING = "pending"
    COMPLETE = "complete"
    FAILED = "failed"


# ── Tables ──


class TrackTable(Base):
    __tablename__ = "tracks"

    track_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    name: Mapped[str | None] = mapped_column(String(100))
    latitude: Mapped[float | None] = mapped_column(Float)
    longitude: Mapped[float | None] = mapped_column(Float)


class TrainerTable(Base):
    """Trainer identity. Name-keyed with auto-increment PK.

    When proper trainer IDs become available from scraping
    trainer profile pages, add an external_id column.
    """

    __tablename__ = "trainers"

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)


class GreyhoundTable(Base):
    __tablename__ = "greyhounds"

    dog_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    whelp_date: Mapped[Date | None] = mapped_column(Date)
    sex: Mapped[str | None] = mapped_column(String(10))
    color: Mapped[str | None] = mapped_column(String(50))
    sire_name: Mapped[str | None] = mapped_column(String(100))
    dam_name: Mapped[str | None] = mapped_column(String(100))
    last_updated: Mapped[DateTime | None] = mapped_column(
        DateTime(timezone=True)
    )


class RaceTable(Base):
    __tablename__ = "races"

    race_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    track_id: Mapped[str] = mapped_column(
        String(20),
        ForeignKey("tracks.track_id"),
        nullable=False,
    )
    r_date: Mapped[Date] = mapped_column(Date, nullable=False)
    r_time: Mapped[Time | None] = mapped_column(Time)
    distance_meters: Mapped[int] = mapped_column(Integer, default=0)
    grade: Mapped[str] = mapped_column(String(20), default="")
    race_type: Mapped[str] = mapped_column(String(20), default="Flat")
    is_finished: Mapped[bool] = mapped_column(Boolean, default=False)
    ingestion_status: Mapped[str] = mapped_column(
        String(20), default=IngestionStatusEnum.PENDING
    )
    bronze_source: Mapped[str | None] = mapped_column(String(20))
    meta_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)


class RaceEntryTable(Base):
    """Pre-race data only. No result columns.

    Trainer stored as FK to trainers table.
    """

    __tablename__ = "race_entries"
    __table_args__ = (UniqueConstraint("race_id", "trap", name="uq_entry"),)

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    race_id: Mapped[str] = mapped_column(
        String(20),
        ForeignKey("races.race_id"),
        nullable=False,
    )
    trap: Mapped[int] = mapped_column(Integer, nullable=False)
    dog_id: Mapped[str] = mapped_column(
        String(20),
        ForeignKey("greyhounds.dog_id"),
        nullable=False,
    )
    dog_name_snapshot: Mapped[str | None] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(20), default="RUNNER")
    trainer_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("trainers.id")
    )
    sp_forecast: Mapped[str | None] = mapped_column(String(20))
    topspeed: Mapped[str | None] = mapped_column(String(20))
    form_string: Mapped[str | None] = mapped_column(String(50))
    comment: Mapped[str | None] = mapped_column(Text)
    form_history: Mapped[list[Any] | None] = mapped_column(JSONB)


class RaceResultTable(Base):
    """Post-race data. One row per dog per race.

    Physical separation from race_entries prevents ML data
    leakage. Can be populated independently of racecards.

    Race-level metadata (forecast, tricast, going) is
    denormalized per-row for single-table query simplicity.
    """

    __tablename__ = "race_results"
    __table_args__ = (UniqueConstraint("race_id", "dog_id", name="uq_result"),)

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    # ── Per-dog result fields ──
    race_id: Mapped[str] = mapped_column(
        String(20),
        ForeignKey("races.race_id"),
        nullable=False,
    )
    dog_id: Mapped[str] = mapped_column(
        String(20),
        ForeignKey("greyhounds.dog_id"),
        nullable=False,
    )
    trap: Mapped[int] = mapped_column(Integer, nullable=False)
    finish_position: Mapped[int] = mapped_column(Integer, nullable=False)
    time_raw: Mapped[str | None] = mapped_column(String(20))
    winning_time: Mapped[float | None] = mapped_column(Float)
    distance_beaten: Mapped[float | None] = mapped_column(Float)
    behind_first: Mapped[float | None] = mapped_column(Float)
    starting_price: Mapped[str | None] = mapped_column(String(20))
    result_comment: Mapped[str | None] = mapped_column(Text)
    trainer_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("trainers.id")
    )

    # ── Race-level metadata (denormalized per row) ──
    forecast: Mapped[str | None] = mapped_column(String(50))
    tricast: Mapped[str | None] = mapped_column(String(50))
    total_sp_pct: Mapped[str | None] = mapped_column(String(20))
    race_head: Mapped[str | None] = mapped_column(String(200))
    going_allowance: Mapped[int | None] = mapped_column(Integer)
    r_datetime: Mapped[str | None] = mapped_column(String(30))
    result_url: Mapped[str | None] = mapped_column(Text)
    result_status: Mapped[str | None] = mapped_column(String(20))
    nr_comments: Mapped[list[Any] | None] = mapped_column(JSONB)
    dead_heat_notes: Mapped[list[Any] | None] = mapped_column(JSONB)


class TrackHourlyWeatherTable(Base):
    """Hourly weather observations and forecasts per track.

    ML queries join to this table on track_id + nearest hour:
        JOIN track_hourly_weather w
          ON w.track_id = r.track_id
         AND w.obs_datetime = date_trunc('hour', r.r_time::timestamptz)

    is_forecast immutability:
        Once a row is written with is_forecast=False (ERA5 actuals),
        subsequent upserts with is_forecast=True cannot overwrite it.
        Enforced via COALESCE at the repository level.
    """

    __tablename__ = "track_hourly_weather"
    __table_args__ = (
        UniqueConstraint("track_id", "obs_datetime", name="uq_track_weather"),
    )

    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    track_id: Mapped[str] = mapped_column(
        String(20),
        ForeignKey("tracks.track_id"),
        nullable=False,
    )
    obs_datetime: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    temperature_c: Mapped[float | None] = mapped_column(Float)
    precipitation_mm: Mapped[float | None] = mapped_column(Float)
    humidity_pct: Mapped[float | None] = mapped_column(Float)
    wind_speed_kph: Mapped[float | None] = mapped_column(Float)
    wind_direction_deg: Mapped[int | None] = mapped_column(Integer)
    is_forecast: Mapped[bool] = mapped_column(Boolean, default=False)
