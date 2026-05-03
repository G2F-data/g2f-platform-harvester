# filepath: libs/g2f_core/services/acl/bronze_v2_adapter.py
"""
Purpose: Anti-Corruption Layer adapter for Bronze V2 (Playwright GCS JSON).
Usage:   adapter = BronzeV2Adapter(); race = adapter.to_race(raw_json_dict)
Dependencies: g2f_core.domain.models, g2f_core.services.acl.parse_html
"""

from __future__ import annotations

import contextlib
import logging
from typing import Any

from g2f_core.domain.models import (
    Greyhound,
    PastRun,
    Race,
    RaceEntry,
    Sex,
    Track,
)
from g2f_core.services.acl.parse_html import (
    parse_card_html,
    parse_dog_html,
    parse_header_html,
    parse_stats_html,
    parse_tips_html,
)

logger = logging.getLogger(__name__)


class AdapterError(Exception):
    """Raised for unrecoverable Bronze V2 adapter errors."""

    pass


class BronzeV2Adapter:
    """Converts a raw Bronze V2 JSON dict into a Race domain object."""

    def to_race(self, raw: dict[str, Any]) -> Race:
        """Entry point. Returns a fully populated Race domain object."""
        race_id = str(raw.get("race_id", "")).strip()
        if not race_id:
            raise AdapterError("Bronze V2 JSON missing race_id")

        track_id = str(raw.get("track_id", "")).strip()
        r_date_raw = str(raw.get("r_date", "")).strip()
        r_time_raw = raw.get("race_time") or None

        snapshots: dict[str, str] = raw.get("html_snapshots", {})
        dogs_raw: list[dict[str, Any]] = raw.get("dogs", [])

        scan_report = raw.get("scan_report", {})
        reported_count = int(scan_report.get("dogs_count", len(dogs_raw)))
        if reported_count == 0 and not dogs_raw:
            raise AdapterError(
                f"race {race_id}: dogs_count=0 in scan_report " "— ghost race"
            )

        # Parse header for metadata (fixes 0m / NULL grade bug)
        header_html = snapshots.get("header", "")
        header_meta = parse_header_html(header_html) if header_html else {}

        # Parse card for entries
        card_html = snapshots.get("card", "")
        card_data = (
            parse_card_html(card_html)
            if card_html
            else {"entries": [], "race_meta": {}}
        )

        # Merge metas: header wins (titleColumn2 lives there)
        race_meta = {
            **card_data.get("race_meta", {}),
            **header_meta,
        }
        card_entries: list[dict[str, Any]] = card_data.get("entries", [])

        if not card_entries and dogs_raw:
            logger.warning(
                "race %s: card HTML — no entries; " "falling back to dogs[]",
                race_id,
            )

        stats = parse_stats_html(snapshots.get("stats", ""))
        tips = parse_tips_html(snapshots.get("tips", ""))

        dog_html_by_id: dict[str, str] = {
            str(d.get("dog_id", "")): d.get("html", "")
            for d in dogs_raw
            if d.get("dog_id")
        }
        dog_stub_by_trap: dict[int, dict[str, Any]] = {
            int(str(d.get("trap", 0))): d for d in dogs_raw if d.get("trap")
        }

        entries = self._build_entries(
            race_id=race_id,
            card_entries=card_entries,
            dog_stub_by_trap=dog_stub_by_trap,
            dog_html_by_id=dog_html_by_id,
        )

        if not entries:
            raise AdapterError(f"race {race_id}: no valid entries built")

        grade = race_meta.get("grade") or str(raw.get("grade", ""))
        distance_m = race_meta.get("distance_meters") or _safe_int(
            raw.get("distance")
        )

        return Race(
            race_id=race_id,
            track=Track(track_id=track_id),
            r_date=r_date_raw,
            r_time=r_time_raw,
            distance_meters=distance_m,
            grade=grade,
            race_type=race_meta.get("race_type", "Flat"),
            entries=entries,
            stats=stats,
            tips=tips,
            weather=None,
        )

    # ── Private ──

    def _build_entries(
        self,
        race_id: str,
        card_entries: list[dict[str, Any]],
        dog_stub_by_trap: dict[int, dict[str, Any]],
        dog_html_by_id: dict[str, str],
    ) -> list[RaceEntry]:
        entries: list[RaceEntry] = []
        processed_traps: set[int] = set()

        for c in card_entries:
            trap = int(c.get("trap", 0))
            if trap == 0:
                continue
            processed_traps.add(trap)

            dog_id = str(c.get("dog_id", ""))
            dog_name = str(c.get("dog_name", ""))

            stub = dog_stub_by_trap.get(trap, {})
            if not dog_id:
                dog_id = str(stub.get("dog_id", ""))
            if not dog_name:
                dog_name = str(stub.get("name", ""))

            entry = self._construct_entry(
                race_id=race_id,
                trap=trap,
                dog_id=dog_id,
                dog_name=dog_name,
                card_data=c,
                dog_html=dog_html_by_id.get(dog_id, ""),
            )
            if entry:
                entries.append(entry)

        for trap, stub in dog_stub_by_trap.items():
            if trap in processed_traps:
                continue
            logger.warning(
                "race %s trap %d: not in card HTML, " "using dogs[] stub only",
                race_id,
                trap,
            )
            dog_id = str(stub.get("dog_id", ""))
            dog_name = str(stub.get("name", ""))
            entry = self._construct_entry(
                race_id=race_id,
                trap=trap,
                dog_id=dog_id,
                dog_name=dog_name,
                card_data={},
                dog_html=dog_html_by_id.get(dog_id, ""),
            )
            if entry:
                entries.append(entry)

        return entries

    def _construct_entry(
        self,
        race_id: str,
        trap: int,
        dog_id: str,
        dog_name: str,
        card_data: dict[str, Any],
        dog_html: str,
    ) -> RaceEntry | None:
        if not dog_id:
            return None

        dog_data = (
            parse_dog_html(dog_html)
            if dog_html
            else {"identity": {}, "past_runs": []}
        )
        identity = dog_data.get("identity", {})

        greyhound = Greyhound(
            dog_id=dog_id,
            name=dog_name or identity.get("name", dog_id),
            whelp_date=identity.get("dob") or None,
            sire_name=identity.get("sire") or None,
            dam_name=identity.get("dam") or None,
            sex=_parse_sex(identity.get("sex", "")),
            color=identity.get("color") or None,
        )

        form_history: list[PastRun] = []
        for pr_raw in dog_data.get("past_runs", []):
            with contextlib.suppress(Exception):
                form_history.append(PastRun(**pr_raw))

        return RaceEntry(
            trap=trap,
            greyhound=greyhound,
            dog_name_snapshot=dog_name,
            trainer_name=card_data.get("trainer") or None,
            seeding=None,
            sp_forecast=card_data.get("sp_forecast") or None,
            topspeed=card_data.get("topspeed") or None,
            form_string=card_data.get("form_string") or None,
            comment=card_data.get("comment", ""),
            form_history=form_history,
            result=None,
        )


def _parse_sex(raw: str) -> Sex | None:
    if not raw:
        return None
    lower = raw.lower()
    if "bitch" in lower:
        return Sex.BITCH
    if "dog" in lower:
        return Sex.DOG
    return None


def _safe_int(v: Any) -> int:
    if v is None:
        return 0
    try:
        return int(str(v).split(".")[0])
    except (ValueError, TypeError):
        return 0
