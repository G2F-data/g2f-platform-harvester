# filepath: libs/g2f_core/services/acl/bronze_result_adapter.py
"""
Purpose: ACL adapter for Bronze V2 result HTML → domain objects.
Usage:   adapter = BronzeResultAdapter()
         race = adapter.to_race(raw_json)

Independent of the racecard pipeline. Can create races,
greyhounds, and results even when no pre-race card exists.

DDD contract:
- Produces a Race aggregate with RaceEntry objects carrying
  RunResult. Repository routes entries/results to correct tables.
- For result-only ingestion (save_result_only), the repository
  skips race_entries entirely.
- 0th finish position = non-runner (filtered from time algebra).
"""

from __future__ import annotations

import contextlib
import logging
from datetime import datetime
from typing import Any

from g2f_core.domain.models import (
    Greyhound,
    Race,
    RaceEntry,
    RunResult,
    Sex,
    Track,
)
from g2f_core.services.acl.parse_result_html import (
    parse_result_page,
)

logger = logging.getLogger(__name__)

TIME_CONVERSION: dict[str, float] = {
    "¾": 0.75,
    "½": 0.5,
    "¼": 0.25,
    "shd": 0.1,
    "nk": 0.2,
    "hd": 0.1,
    "snk": 0.15,
    "dht": 0.0,
    "dist": 20.0,
}


class ResultAdapterError(Exception):
    """Raised for unrecoverable result adapter errors."""

    pass


class BronzeResultAdapter:
    """Converts a Bronze V2 result JSON dict into a Race."""

    def to_race(self, raw: dict[str, Any]) -> Race | None:
        """Convert a result JSON dict into a Race.

        Returns None for void races.
        """
        race_id = str(raw.get("race_id", "")).strip()
        if not race_id:
            raise ResultAdapterError("Result JSON missing race_id")

        status = raw.get("status", "scraped")
        if status == "void":
            logger.info("race %s: void — no result", race_id)
            return None

        track_id = str(raw.get("track_id", "")).strip()
        r_date_raw = str(raw.get("r_date", "")).strip()
        r_time_raw = raw.get("r_time") or None

        snapshots = raw.get("html_snapshots", {})
        result_html = snapshots.get("result", "")
        header_html = snapshots.get("header", "")

        if not result_html:
            raise ResultAdapterError(f"race {race_id}: no result HTML")

        parsed = parse_result_page(result_html, header_html)
        meta = parsed.get("race_meta", {})
        placements = parsed.get("placements", [])

        if not placements:
            raise ResultAdapterError(f"race {race_id}: no placements parsed")

        grade = meta.get("grade", "")
        distance_m = meta.get("distance_meters", 0)
        track_name = meta.get("track_name") or None

        result_meta = self._build_result_meta(meta, raw)

        # Run time algebra (handles 0th non-runners)
        results_with_times = self._calculate_times(placements)

        entries: list[RaceEntry] = []
        for p in results_with_times:
            entry = self._build_entry(p)
            if entry:
                entries.append(entry)

        if not entries:
            raise ResultAdapterError(f"race {race_id}: no valid entries built")

        stats: dict[str, Any] = {}
        if result_meta:
            stats["result_meta"] = result_meta

        return Race(
            race_id=race_id,
            track=Track(track_id=track_id, name=track_name),
            r_date=r_date_raw,
            r_time=r_time_raw,
            distance_meters=distance_m,
            grade=grade,
            race_type="Flat",
            entries=entries,
            stats=stats,
            tips={},
            weather=None,
        )

    def _build_entry(self, p: dict[str, Any]) -> RaceEntry | None:
        dog_id = str(p.get("dog_id", "")).strip()
        if not dog_id:
            return None

        dog_name = str(p.get("dog_name", "")).strip()
        trap = int(p.get("trap", 0))
        if trap == 0:
            return None

        greyhound = self._build_greyhound(p)

        result = RunResult(
            finish_position=p.get("finish_position", 0),
            time_raw=p.get("time_raw") or None,
            winning_time=p.get("winning_time"),
            distance_beaten=p.get("distance_beaten"),
            behind_first=p.get("behind_first"),
            starting_price=(p.get("starting_price") or None),
            comment=p.get("result_comment") or None,
            sectional_time=p.get("sectional_time"),
        )

        return RaceEntry(
            trap=trap,
            greyhound=greyhound,
            dog_name_snapshot=dog_name,
            trainer_name=p.get("trainer_name") or None,
            form_history=[],
            result=result,
        )

    def _build_greyhound(self, p: dict[str, Any]) -> Greyhound:
        dog_id = str(p["dog_id"])
        dog_name = str(p.get("dog_name", dog_id))

        sex_raw = p.get("dog_sex", "")
        sex = None
        if sex_raw.lower() in ("b", "bitch"):
            sex = Sex.BITCH
        elif sex_raw.lower() in ("d", "dog"):
            sex = Sex.DOG

        sire = None
        dam = None
        sire_dam = p.get("dog_sire_dam", "")
        if "-" in sire_dam:
            parts = sire_dam.split("-", 1)
            sire = parts[0].strip() or None
            dam = parts[1].strip() or None

        whelp_date = None
        dob_raw = p.get("dog_dob", "")
        if dob_raw:
            whelp_date = self._parse_short_dob(dob_raw)

        color = p.get("dog_color") or None

        return Greyhound(
            dog_id=dog_id,
            name=dog_name,
            whelp_date=whelp_date,
            sex=sex,
            color=color,
            sire_name=sire,
            dam_name=dam,
        )

    @staticmethod
    def _parse_short_dob(raw: str) -> Any:
        """Parse "May 23" or "Oct 24" to date."""
        raw = raw.strip()
        if not raw:
            return None
        try:
            dt = datetime.strptime(raw, "%b %y")
            return dt.date()
        except ValueError:
            return None

    def _calculate_times(
        self, placements: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Apply chain time algebra to placements.

        Non-runners (finish_position == 0) are separated
        before sorting to prevent them from corrupting
        the winner's base time calculation.
        """
        if not placements:
            return placements

        # Separate non-runners from actual runners
        runners = [p for p in placements if p.get("finish_position", 99) > 0]
        non_runners = [
            p for p in placements if p.get("finish_position", 99) == 0
        ]

        # Non-runners get no time data
        for nr in non_runners:
            nr["winning_time"] = None
            nr["distance_beaten"] = None
            nr["behind_first"] = None

        if not runners:
            return non_runners

        # Sort runners by finish position
        sorted_p = sorted(
            runners,
            key=lambda x: x.get("finish_position", 99),
        )

        # First place: absolute time
        first = sorted_p[0]
        first_time_raw = first.get("time_raw", "")
        base_time: float | None = None
        with contextlib.suppress(ValueError, TypeError):
            base_time = float(first_time_raw)

        if base_time and base_time > 0:
            first["winning_time"] = round(base_time, 2)
            first["behind_first"] = 0.0
            first["distance_beaten"] = None
        else:
            first["winning_time"] = None
            first["behind_first"] = None
            first["distance_beaten"] = None

        # Chain algebra for subsequent places
        prev_time = base_time
        cumulative_behind = 0.0

        for p in sorted_p[1:]:
            time_str = p.get("time_raw", "")
            lengths = _convert_fractional_time(time_str)

            if lengths is not None:
                cumulative_behind += lengths
                p["distance_beaten"] = lengths
                p["behind_first"] = round(cumulative_behind, 4)
            else:
                p["distance_beaten"] = None
                p["behind_first"] = None

            if lengths is not None and prev_time is not None:
                calc_time = prev_time + (lengths * 0.08)
                prev_time = calc_time
                p["winning_time"] = round(calc_time, 2)
            else:
                p["winning_time"] = None

        # Return non-runners + sorted runners
        return non_runners + sorted_p

    @staticmethod
    def _build_result_meta(
        meta: dict[str, Any],
        raw: dict[str, Any],
    ) -> dict[str, Any]:
        result_meta: dict[str, Any] = {}

        if meta.get("forecast"):
            result_meta["result_F/C"] = meta["forecast"]
        if meta.get("tricast"):
            result_meta["result_T/C"] = meta["tricast"]
        if meta.get("total_sp_pct"):
            result_meta["result_Total SP%"] = meta["total_sp_pct"]
        if meta.get("race_head"):
            result_meta["result_race_head"] = meta["race_head"]
        if meta.get("going_raw"):
            result_meta["result_going_raw"] = meta["going_raw"]
        ts = raw.get("scrape_timestamp")
        if ts:
            result_meta["result_r_datetime"] = ts

        return result_meta


def _convert_fractional_time(
    time_str: str,
) -> float | None:
    """Parse fractional distance string to lengths."""
    if not time_str:
        return None
    lower = time_str.lower().strip()
    if lower in ("dnf", "dis", ""):
        return None

    if lower in TIME_CONVERSION:
        return TIME_CONVERSION[lower]

    parts = lower.split()
    total = 0.0
    for part in parts:
        if "/" in part:
            try:
                num, den = part.split("/")
                total += float(num) / float(den)
            except (ValueError, ZeroDivisionError):
                pass
        elif part.isdigit():
            total += float(part)
        elif part in TIME_CONVERSION:
            total += TIME_CONVERSION[part]

    return total if total > 0 else None
