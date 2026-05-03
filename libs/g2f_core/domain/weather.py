# filepath: libs/g2f_core/domain/weather.py
"""
Purpose: Domain model for the Weather dimension.
Usage:   Produced by OpenMeteoAdapter; consumed by upsert_weather().
Dependencies: pydantic, datetime (stdlib only)

Design:
    TrackHourlyWeather is a Value Object — immutable, identified
    by (track_id, obs_datetime). It is NOT part of the Race aggregate.
    It lives in its own bounded context as an independent dimension.

    is_forecast=True  → Open-Meteo forecast API (overwriteable)
    is_forecast=False → Open-Meteo archive/ERA5 (authoritative, immutable)

    Repository enforces this via COALESCE: a False row can never
    be overwritten by a True row in an ON CONFLICT DO UPDATE.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class TrackHourlyWeather(BaseModel):
    """One hour of meteorological data at a track location.

    Attributes:
        track_id: FK to tracks table.
        obs_datetime: UTC timestamp, always on the hour.
        temperature_c: Air temperature in Celsius.
        precipitation_mm: Precipitation in mm.
        humidity_pct: Relative humidity 0–100.
        wind_speed_kph: Wind speed in km/h.
        wind_direction_deg: Wind direction in degrees (0–360).
        is_forecast: True if predictive; False if ERA5 actuals.
    """

    model_config = ConfigDict(frozen=True)

    track_id: str
    obs_datetime: datetime
    temperature_c: float | None = None
    precipitation_mm: float | None = Field(None, ge=0)
    humidity_pct: float | None = Field(None, ge=0, le=100)
    wind_speed_kph: float | None = Field(None, ge=0)
    wind_direction_deg: int | None = Field(None, ge=0, le=360)
    is_forecast: bool = False
