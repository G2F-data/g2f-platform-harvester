# filepath: libs/g2f_core/adapters/db/repository.py
"""
Purpose: Silver-layer repository — persists Race domain objects.

Table write order (FK constraints):
    tracks → trainers → greyhounds → races →
    race_entries → race_results

ML separation:
    race_entries holds ONLY pre-race features.
    race_results holds ONLY post-race labels.

Two write modes:
    save()             — full Race with entries + results
    save_result_only() — result-only, no race_entries touched
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from g2f_core.adapters.db.schema import (
    BronzeSourceEnum,
    GreyhoundTable,
    IngestionStatusEnum,
    RaceEntryTable,
    RaceResultTable,
    RaceTable,
    TrackHourlyWeatherTable,
    TrackTable,
    TrainerTable,
)
from g2f_core.domain.models import Race
from g2f_core.domain.weather import TrackHourlyWeather


class SqlAlchemyRaceRepository:
    """Persists Race domain objects to Silver PostgreSQL.

    Upsert order respects FK constraints:
        Track → Trainer → Greyhound → Race →
        RaceEntry → RaceResult

    form_history immutability is enforced at SQL level via
    COALESCE: once written, subsequent upserts cannot overwrite.

    Trainer names are cached in-memory for batch performance.
    """

    def __init__(
        self,
        session: Session,
        bronze_source: BronzeSourceEnum = (BronzeSourceEnum.V2_GCS),
    ) -> None:
        self._session = session
        self._bronze_source = bronze_source
        self._trainer_cache: dict[str, int] = {}

    # ── Public ──

    def save(self, race: Race) -> None:
        """Persist a full Race (entries + results)."""
        with self._session.begin_nested():
            self._upsert_track(race)
            self._upsert_trainers(race)
            self._upsert_greyhounds(race)
            self._upsert_race(race)
            self._upsert_entries(race)
            self._upsert_results(race)

    def save_result_only(self, race: Race) -> None:
        """Persist result data without race_entries."""
        with self._session.begin_nested():
            self._upsert_track(race)
            self._upsert_trainers(race)
            self._upsert_greyhounds(race)
            self._upsert_race(race)
            self._upsert_results(race)

    def exists(self, race_id: str) -> bool:
        stmt = select(RaceTable.race_id).where(RaceTable.race_id == race_id)
        return self._session.execute(stmt).scalar() is not None

    def has_results(self, race_id: str) -> bool:
        stmt = select(RaceResultTable.race_id).where(
            RaceResultTable.race_id == race_id
        )
        return self._session.execute(stmt).scalar() is not None

    # ── Trainer cache ──

    def _get_trainer_id(self, trainer_name: str | None) -> int | None:
        """Get or create trainer, return ID. Cached."""
        if not trainer_name:
            return None
        name = trainer_name.strip()
        if not name:
            return None

        if name in self._trainer_cache:
            return self._trainer_cache[name]

        stmt = (
            insert(TrainerTable)
            .values(name=name)
            .on_conflict_do_nothing(index_elements=["name"])
        )
        self._session.execute(stmt)

        row = self._session.execute(
            select(TrainerTable.id).where(TrainerTable.name == name)
        ).scalar()

        if row is not None:
            self._trainer_cache[name] = row
        return row

    # ── Upsert helpers ──

    def _upsert_track(self, race: Race) -> None:
        track = race.track
        stmt = (
            insert(TrackTable)
            .values(
                track_id=track.track_id,
                name=track.name or track.track_id,
            )
            .on_conflict_do_update(
                index_elements=["track_id"],
                set_={
                    "name": text("COALESCE(EXCLUDED.name," " tracks.name)"),
                },
            )
        )
        self._session.execute(stmt)

    def _upsert_trainers(self, race: Race) -> None:
        """Pre-cache all trainers in this race."""
        for entry in race.entries:
            if entry.trainer_name:
                self._get_trainer_id(entry.trainer_name)

    def _upsert_greyhounds(self, race: Race) -> None:
        now = datetime.now(UTC)
        for entry in race.entries:
            g = entry.greyhound
            values: dict[str, Any] = {
                "dog_id": g.dog_id,
                "name": g.name,
                "last_updated": now,
            }
            if g.whelp_date is not None:
                values["whelp_date"] = g.whelp_date
            if g.sex is not None:
                values["sex"] = g.sex.value
            if g.color is not None:
                values["color"] = g.color
            if g.sire_name is not None:
                values["sire_name"] = g.sire_name
            if g.dam_name is not None:
                values["dam_name"] = g.dam_name

            stmt = (
                insert(GreyhoundTable)
                .values(**values)
                .on_conflict_do_update(
                    index_elements=["dog_id"],
                    set_={
                        "name": text(
                            "COALESCE(" "greyhounds.name," " EXCLUDED.name)"
                        ),
                        "whelp_date": text(
                            "COALESCE("
                            "greyhounds.whelp_date,"
                            " EXCLUDED.whelp_date)"
                        ),
                        "sex": text(
                            "COALESCE(" "greyhounds.sex," " EXCLUDED.sex)"
                        ),
                        "color": text(
                            "COALESCE(" "greyhounds.color," " EXCLUDED.color)"
                        ),
                        "sire_name": text(
                            "COALESCE("
                            "greyhounds.sire_name,"
                            " EXCLUDED.sire_name)"
                        ),
                        "dam_name": text(
                            "COALESCE("
                            "greyhounds.dam_name,"
                            " EXCLUDED.dam_name)"
                        ),
                        "last_updated": now,
                    },
                )
            )
            self._session.execute(stmt)

    def _upsert_race(self, race: Race) -> None:
        meta: dict[str, Any] = {}
        if race.stats:
            meta["stats"] = race.stats
        if race.tips:
            meta["tips"] = race.tips
        if race.weather is not None:
            meta["weather"] = race.weather.model_dump()

        stmt = (
            insert(RaceTable)
            .values(
                race_id=race.race_id,
                track_id=race.track.track_id,
                r_date=race.r_date,
                r_time=race.r_time,
                distance_meters=race.distance_meters,
                grade=race.grade,
                race_type=race.race_type.value,
                is_finished=race.is_finished,
                ingestion_status=(IngestionStatusEnum.COMPLETE),
                bronze_source=self._bronze_source,
                meta_json=meta or None,
            )
            .on_conflict_do_update(
                index_elements=["race_id"],
                set_={
                    "r_time": text(
                        "COALESCE(races.r_time," " EXCLUDED.r_time)"
                    ),
                    "distance_meters": text(
                        "COALESCE("
                        "NULLIF("
                        "races.distance_meters, 0),"
                        " EXCLUDED.distance_meters)"
                    ),
                    "grade": text(
                        "COALESCE("
                        "NULLIF(races.grade, ''),"
                        " EXCLUDED.grade)"
                    ),
                    "is_finished": text(
                        "races.is_finished" " OR EXCLUDED.is_finished"
                    ),
                    "ingestion_status": (IngestionStatusEnum.COMPLETE),
                    "meta_json": text(
                        "COALESCE(" "races.meta_json," " EXCLUDED.meta_json)"
                    ),
                },
            )
        )
        self._session.execute(stmt)

    def _upsert_entries(self, race: Race) -> None:
        """Write pre-race data only. No result columns."""
        for entry in race.entries:
            form_json = [
                pr.model_dump(mode="json") for pr in entry.form_history
            ]

            trainer_id = self._get_trainer_id(entry.trainer_name)

            values: dict[str, Any] = {
                "race_id": race.race_id,
                "trap": entry.trap,
                "dog_id": entry.greyhound.dog_id,
                "dog_name_snapshot": (
                    entry.dog_name_snapshot or entry.greyhound.name
                ),
                "status": entry.status,
                "trainer_id": trainer_id,
                "sp_forecast": entry.sp_forecast,
                "topspeed": entry.topspeed,
                "form_string": entry.form_string,
                "comment": entry.comment or None,
                "form_history": form_json or None,
            }

            stmt = (
                insert(RaceEntryTable)
                .values(**values)
                .on_conflict_do_update(
                    constraint="uq_entry",
                    set_={
                        "form_history": text(
                            "COALESCE("
                            "race_entries.form_history,"
                            " EXCLUDED.form_history)"
                        ),
                        "dog_name_snapshot": text("EXCLUDED.dog_name_snapshot"),
                        "trainer_id": text("EXCLUDED.trainer_id"),
                        "sp_forecast": text("EXCLUDED.sp_forecast"),
                        "topspeed": text("EXCLUDED.topspeed"),
                        "form_string": text("EXCLUDED.form_string"),
                        "status": text("EXCLUDED.status"),
                    },
                )
            )
            self._session.execute(stmt)

    def _upsert_results(self, race: Race) -> None:
        """Write post-race data to race_results table.

        Only writes rows for entries that have a RunResult.
        Race-level metadata comes from race.stats["result_meta"].
        """
        # Extract race-level result metadata
        result_meta = race.stats.get("result_meta", {})
        forecast = result_meta.get("result_F/C")
        tricast = result_meta.get("result_T/C")
        total_sp = result_meta.get("result_Total SP%")
        race_head_val = result_meta.get("result_race_head")
        r_datetime = result_meta.get("result_r_datetime")
        result_url = result_meta.get("result_url")
        result_status = result_meta.get("result_status")
        nr_comments = result_meta.get("result_NR Comments")
        dh_notes = result_meta.get("result_Other")

        going_allowance = self._parse_going(race_head_val)

        for entry in race.entries:
            res = entry.result
            if res is None:
                continue

            trainer_id = self._get_trainer_id(entry.trainer_name)

            values: dict[str, Any] = {
                "race_id": race.race_id,
                "dog_id": entry.greyhound.dog_id,
                "trap": entry.trap,
                "finish_position": res.finish_position,
                "time_raw": res.time_raw,
                "winning_time": res.winning_time,
                "distance_beaten": res.distance_beaten,
                "behind_first": res.behind_first,
                "starting_price": res.starting_price,
                "result_comment": res.comment,
                "trainer_id": trainer_id,
                "forecast": forecast,
                "tricast": tricast,
                "total_sp_pct": total_sp,
                "race_head": race_head_val,
                "going_allowance": going_allowance,
                "r_datetime": (str(r_datetime) if r_datetime else None),
                "result_url": result_url,
                "result_status": result_status,
                "nr_comments": nr_comments,
                "dead_heat_notes": dh_notes,
            }

            stmt = (
                insert(RaceResultTable)
                .values(**values)
                .on_conflict_do_update(
                    constraint="uq_result",
                    set_={
                        k: text(f"EXCLUDED.{k}")
                        for k in [
                            "finish_position",
                            "time_raw",
                            "winning_time",
                            "distance_beaten",
                            "behind_first",
                            "starting_price",
                            "result_comment",
                            "trainer_id",
                            "forecast",
                            "tricast",
                            "total_sp_pct",
                            "race_head",
                            "going_allowance",
                            "r_datetime",
                            "result_url",
                            "result_status",
                            "nr_comments",
                            "dead_heat_notes",
                        ]
                    },
                )
            )
            self._session.execute(stmt)

    @staticmethod
    def _parse_going(
        race_head: str | None,
    ) -> int | None:
        """Extract going allowance from race_head.

        "Going: +15" → 15, "Going: -10" → -10,
        "Going: N" → 0
        """
        if not race_head:
            return None
        m = re.search(r"Going:\s*([+-]?\d+)", race_head)
        if m:
            return int(m.group(1))
        m2 = re.search(r"Going:\s*(\w+)", race_head)
        if m2:
            cat = m2.group(1).upper()
            if cat in ("N", "NORMAL", "STD"):
                return 0
        return None

    # Maximum rows per INSERT statement.
    # 8 columns × 1,000 rows = 8,000 bind parameters — safely under
    # the PostgreSQL hard limit of 65,535 per statement.
    _WEATHER_CHUNK_SIZE = 1_000

    def upsert_weather(self, records: list[TrackHourlyWeather]) -> None:
        """Upsert hourly weather records in chunked bulk INSERTs.

        Splits records into batches of _WEATHER_CHUNK_SIZE to stay
        under PostgreSQL's 65,535 bind-parameter limit. Each chunk
        compiles to one INSERT ... ON CONFLICT DO UPDATE statement.

        Immutability rule:
            ERA5 actuals (is_forecast=False) are authoritative.
            A forecast row (is_forecast=True) can never overwrite
            an actuals row. Enforced via COALESCE at SQL level.

        Args:
            records: List of TrackHourlyWeather domain objects.
        """
        if not records:
            return

        for i in range(0, len(records), self._WEATHER_CHUNK_SIZE):
            chunk = records[i : i + self._WEATHER_CHUNK_SIZE]
            values = [
                {
                    "track_id": r.track_id,
                    "obs_datetime": r.obs_datetime,
                    "temperature_c": r.temperature_c,
                    "precipitation_mm": r.precipitation_mm,
                    "humidity_pct": r.humidity_pct,
                    "wind_speed_kph": r.wind_speed_kph,
                    "wind_direction_deg": r.wind_direction_deg,
                    "is_forecast": r.is_forecast,
                }
                for r in chunk
            ]
            stmt = (
                insert(TrackHourlyWeatherTable)
                .values(values)
                .on_conflict_do_update(
                    constraint="uq_track_weather",
                    set_={
                        "temperature_c": text("EXCLUDED.temperature_c"),
                        "precipitation_mm": text("EXCLUDED.precipitation_mm"),
                        "humidity_pct": text("EXCLUDED.humidity_pct"),
                        "wind_speed_kph": text("EXCLUDED.wind_speed_kph"),
                        "wind_direction_deg": text(
                            "EXCLUDED.wind_direction_deg"
                        ),
                        # Actuals (False) can never be overwritten by a
                        # forecast (True). Once False, always False.
                        "is_forecast": text(
                            "track_hourly_weather.is_forecast"
                            " AND EXCLUDED.is_forecast"
                        ),
                    },
                )
            )
            self._session.execute(stmt)
