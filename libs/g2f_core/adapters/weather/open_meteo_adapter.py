# filepath: libs/g2f_core/adapters/weather/open_meteo_adapter.py
"""
Purpose: Fetches hourly weather data from Open-Meteo and produces
         TrackHourlyWeather domain objects.

Usage:
    adapter = OpenMeteoAdapter()
    # Historical backfill — one call per track
    records = adapter.fetch_historical(
        track_id="99", lat=52.62, lon=-2.08,
        start_date=date(2023, 1, 1), end_date=date(2026, 3, 19),
    )
    # Daily two-phase sync — actuals lookback + forecast lookahead
    records = adapter.fetch_sync(track_id="99", lat=52.62, lon=-2.08)
    # Forecast only
    records = adapter.fetch_forecast(track_id="99", lat=52.62, lon=-2.08)

Dependencies: httpx (sync), g2f_core.domain.weather

API contracts:
    Archive:  https://archive-api.open-meteo.com/v1/archive
    Forecast: https://api.open-meteo.com/v1/forecast

Both endpoints return identical JSON shapes:
    {
        "hourly": {
            "time":             ["2026-03-15T10:00", ...],
            "temperature_2m":   [8.4, ...],
            "precipitation":    [0.0, ...],
            "relativehumidity_2m" | "relative_humidity_2m": [72, ...],
            "windspeed_10m" | "wind_speed_10m":   [12.5, ...],
            "winddirection_10m" | "wind_direction_10m": [220, ...],
        }
    }

Racing window filter:
    Only hours 10:00–22:00 (inclusive) are retained. This matches
    the earliest and latest possible UK/Irish race times and reduces
    stored rows by ~50%.

Rate limiting:
    Open-Meteo's free tier enforces both a per-day limit (10,000
    calls) and a per-minute burst limit. Exponential backoff with
    up to MAX_RETRIES attempts is applied on HTTP 429. A courtesy
    sleep of _REQUEST_SLEEP seconds is added between every call.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast

import httpx

from g2f_core.domain.weather import TrackHourlyWeather

logger = logging.getLogger(__name__)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

# Variables requested from both endpoints
_HOURLY_VARS = (
    "temperature_2m,"
    "precipitation,"
    "relative_humidity_2m,"
    "wind_speed_10m,"
    "wind_direction_10m"
)

RACE_HOUR_START = 10
RACE_HOUR_END = 22

# Courtesy sleep between every successful request (seconds)
_REQUEST_SLEEP = 1.5

# Retry config for HTTP 429 (rate limit)
_MAX_RETRIES = 5
_RETRY_BASE_SLEEP = 10.0  # seconds; doubles each attempt


class OpenMeteoError(Exception):
    """Raised when all retries are exhausted or on non-retryable errors."""


class OpenMeteoAdapter:
    """Fetches weather from Open-Meteo for a single track location.

    One instance is safe to reuse across multiple tracks in a loop.
    Not thread-safe (single httpx.Client).
    """

    def __init__(self, timeout: float = 30.0) -> None:
        self._client = httpx.Client(timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> OpenMeteoAdapter:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    # ── Public ──

    def fetch_historical(
        self,
        track_id: str,
        lat: float,
        lon: float,
        start_date: date,
        end_date: date,
    ) -> list[TrackHourlyWeather]:
        """Fetch ERA5 reanalysis data for the full date range.

        One HTTP call regardless of range length.

        Args:
            track_id: Silver track_id string.
            lat: Latitude.
            lon: Longitude.
            start_date: First day (inclusive).
            end_date: Last day (inclusive).

        Returns:
            Filtered list of TrackHourlyWeather (is_forecast=False).

        Raises:
            OpenMeteoError: On HTTP error after all retries.
        """
        params: dict[str, Any] = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "hourly": _HOURLY_VARS,
            "timezone": "UTC",
        }
        raw = self._get(ARCHIVE_URL, params, track_id)
        records = self._parse(raw, track_id, is_forecast=False)
        logger.info(
            "track %s: %d historical records (%s → %s)",
            track_id,
            len(records),
            start_date,
            end_date,
        )
        time.sleep(_REQUEST_SLEEP)
        return records

    def fetch_forecast(
        self,
        track_id: str,
        lat: float,
        lon: float,
        forecast_days: int = 2,
    ) -> list[TrackHourlyWeather]:
        """Fetch GFS/IFS forecast data for upcoming days.

        Args:
            track_id: Silver track_id string.
            lat: Latitude.
            lon: Longitude.
            forecast_days: Number of days ahead (default 2 = today+tomorrow).

        Returns:
            Filtered list of TrackHourlyWeather (is_forecast=True).

        Raises:
            OpenMeteoError: On HTTP error after all retries.
        """
        params: dict[str, Any] = {
            "latitude": lat,
            "longitude": lon,
            "hourly": _HOURLY_VARS,
            "forecast_days": forecast_days,
            "timezone": "UTC",
        }
        raw = self._get(FORECAST_URL, params, track_id)
        records = self._parse(raw, track_id, is_forecast=True)
        logger.info(
            "track %s: %d forecast records",
            track_id,
            len(records),
        )
        time.sleep(_REQUEST_SLEEP)
        return records

    def fetch_sync(
        self,
        track_id: str,
        lat: float,
        lon: float,
        lookback_days: int = 5,
        forecast_days: int = 2,
    ) -> list[TrackHourlyWeather]:
        """Two-phase daily sync — actuals lookback + forecast lookahead.

        Phase 1 (Actuals): Fetches ERA5 archive data for the past
            lookback_days up to yesterday. This overwrites stale
            forecast rows that are now within the ERA5 availability
            window. A 5-day lookback safely covers weekend cron gaps
            and Open-Meteo upstream delays.

        Phase 2 (Forecast): Fetches GFS/IFS forecast for today and
            tomorrow. The DB's COALESCE ensures these cannot overwrite
            any ERA5 actuals already written.

        Combined: 2 HTTP calls per track. For 37 tracks = 74 calls,
        which is 0.74% of the daily free-tier allowance.

        Args:
            track_id: Silver track_id string.
            lat: Latitude.
            lon: Longitude.
            lookback_days: Days of ERA5 actuals to fetch (default 5).
            forecast_days: Days of forecast to fetch (default 2).

        Returns:
            Combined list of TrackHourlyWeather, actuals first.
        """
        today = date.today()
        yesterday = today - timedelta(days=1)
        lookback_start = yesterday - timedelta(days=lookback_days - 1)

        actuals = self.fetch_historical(
            track_id=track_id,
            lat=lat,
            lon=lon,
            start_date=lookback_start,
            end_date=yesterday,
        )
        forecasts = self.fetch_forecast(
            track_id=track_id,
            lat=lat,
            lon=lon,
            forecast_days=forecast_days,
        )
        return actuals + forecasts

    # ── Private ──

    def _get(
        self,
        url: str,
        params: dict[str, Any],
        track_id: str,
    ) -> dict[str, Any]:
        """Execute GET with exponential backoff on HTTP 429.

        Retries up to _MAX_RETRIES times, doubling the sleep each
        attempt starting from _RETRY_BASE_SLEEP seconds.
        """
        sleep = _RETRY_BASE_SLEEP
        for attempt in range(_MAX_RETRIES + 1):
            try:
                resp = self._client.get(url, params=params)
                if resp.status_code == 429:
                    if attempt < _MAX_RETRIES:
                        logger.warning(
                            "track %s: HTTP 429 (attempt %d/%d) "
                            "— sleeping %.0fs",
                            track_id,
                            attempt + 1,
                            _MAX_RETRIES,
                            sleep,
                        )
                        time.sleep(sleep)
                        sleep *= 2
                        continue
                    raise OpenMeteoError(
                        f"track {track_id}: HTTP 429 after "
                        f"{_MAX_RETRIES} retries"
                    )
                resp.raise_for_status()
                return cast(dict[str, Any], resp.json())
            except httpx.HTTPStatusError as exc:
                # Non-429 HTTP errors are not retried
                raise OpenMeteoError(
                    f"track {track_id}: HTTP {exc.response.status_code}"
                ) from exc
            except httpx.RequestError as exc:
                raise OpenMeteoError(
                    f"track {track_id}: request failed — {exc}"
                ) from exc

        # Unreachable — loop always raises or returns
        raise OpenMeteoError(f"track {track_id}: unexpected retry exhaustion")

    def _parse(
        self,
        raw: dict[str, Any],
        track_id: str,
        is_forecast: bool,
    ) -> list[TrackHourlyWeather]:
        """Parse Open-Meteo JSON into domain objects.

        Handles both old variable names (windspeed_10m) and new
        (wind_speed_10m) to be resilient to API version changes.
        """
        hourly = raw.get("hourly", {})
        if not isinstance(hourly, dict):
            return []

        times: list[str] = hourly.get("time", [])
        temps: list[float | None] = hourly.get("temperature_2m", [])
        precips: list[float | None] = hourly.get("precipitation", [])

        # Handle both API naming conventions
        humidities: list[float | None] = (
            hourly.get("relative_humidity_2m")
            or hourly.get("relativehumidity_2m")
            or []
        )
        wind_speeds: list[float | None] = (
            hourly.get("wind_speed_10m") or hourly.get("windspeed_10m") or []
        )
        wind_dirs: list[float | None] = (
            hourly.get("wind_direction_10m")
            or hourly.get("winddirection_10m")
            or []
        )

        records: list[TrackHourlyWeather] = []
        for i, ts in enumerate(times):
            try:
                dt = datetime.fromisoformat(ts).replace(tzinfo=UTC)
            except ValueError:
                continue

            # Drop hours outside the racing window
            if not (RACE_HOUR_START <= dt.hour <= RACE_HOUR_END):
                continue

            records.append(
                TrackHourlyWeather(
                    track_id=track_id,
                    obs_datetime=dt,
                    temperature_c=_safe_float(temps, i),
                    precipitation_mm=_safe_float(precips, i),
                    humidity_pct=_safe_float(humidities, i),
                    wind_speed_kph=_safe_float(wind_speeds, i),
                    wind_direction_deg=_safe_int(wind_dirs, i),
                    is_forecast=is_forecast,
                )
            )

        return records


# ── Helpers ──


def _safe_float(lst: list[float | None], i: int) -> float | None:
    try:
        v = lst[i]
        return float(v) if v is not None else None
    except (IndexError, TypeError, ValueError):
        return None


def _safe_int(lst: list[float | None], i: int) -> int | None:
    v = _safe_float(lst, i)
    return round(v) if v is not None else None
