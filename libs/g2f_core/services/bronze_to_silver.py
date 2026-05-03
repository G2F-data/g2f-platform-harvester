"""
Purpose: Orchestrates the Bronze-to-Silver ETL pipeline for V2 data.
Usage:
    service = BronzeToSilverService(storage=storage_port, repo=repo_port)
    service.ingest_date(date(2025, 12, 29))

Dependencies: domain ports only — no SQLAlchemy, no GCS, no ORM.

Clean Architecture compliance
------------------------------
This Application Service imports ONLY domain Port abstractions.
The concrete adapters (SqlAlchemyRaceRepository, GCSStorage) are wired
at the composition root — never here.

Error strategy
--------------
- Bronze JSON missing:     log warning, return False, count as failure.
- Adapter raises:          log error, return False, count as failure.
- repo.save() raises:      log exception, return False, count as failure.
- No failure state written to Silver. Silver is a pristine Fact Store.
  Failure tracking belongs to the GCS HarvestManifest orchestrator.
- Re-running is safe — repo.exists() skips already-ingested races, and
  repo.save() uses upsert semantics when skip_existing=False.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from g2f_core.domain.ports import RaceRepositoryPort, StoragePort
from g2f_core.services.acl.bronze_v2_adapter import (
    AdapterError,
    BronzeV2Adapter,
)

logger = logging.getLogger(__name__)


class BronzeToSilverService:
    """Reads Bronze V2 JSON from GCS and persists Race objects to Silver.

    Injected with StoragePort and RaceRepositoryPort — never with concrete
    adapters or SQLAlchemy sessions.
    """

    def __init__(
        self,
        storage: StoragePort,
        repo: RaceRepositoryPort,
    ) -> None:
        self._storage = storage
        self._repo = repo
        self._adapter = BronzeV2Adapter()

    # ── Public API ────────────────────────────────────────────────────────────

    def ingest_date(
        self,
        r_date: date,
        skip_existing: bool = True,
    ) -> dict[str, Any]:
        """Ingest all SUCCESS races from the GCS manifest for r_date.

        Returns {"total": int, "success": int, "failed": int, "skipped": int}.

        skipped counts:
          - races whose manifest status is not SUCCESS
          - races already in Silver when skip_existing=True
        """
        manifest_path = f"{r_date}/_manifest.json"
        manifest = self._storage.read(manifest_path)

        if not manifest:
            logger.warning(
                "ingest_date %s: manifest not found at %s",
                r_date,
                manifest_path,
            )
            return {"total": 0, "success": 0, "failed": 0, "skipped": 0}

        targets = manifest.get("targets", {})
        total = success = failed = skipped = 0

        for race_id, target in targets.items():
            if target.get("status") != "SUCCESS":
                logger.debug(
                    "ingest_date %s: skipping %s (manifest status=%s)",
                    r_date,
                    race_id,
                    target.get("status"),
                )
                skipped += 1
                continue

            if skip_existing and self._repo.exists(race_id):
                logger.debug(
                    "ingest_date %s: skipping %s (already in Silver)",
                    r_date,
                    race_id,
                )
                skipped += 1
                continue

            total += 1
            if self.ingest_race(
                race_id=race_id, r_date=r_date, skip_existing=False
            ):
                success += 1
            else:
                failed += 1

        logger.info(
            "ingest_date %s: total=%d success=%d failed=%d skipped=%d",
            r_date,
            total,
            success,
            failed,
            skipped,
        )
        return {
            "total": total,
            "success": success,
            "failed": failed,
            "skipped": skipped,
        }

    def ingest_race(
        self,
        race_id: str,
        r_date: date,
        skip_existing: bool = True,
    ) -> bool:
        """Ingest one race from GCS Bronze into Silver. Returns True on success.

        Never raises — all exceptions are caught and logged.
        False means the race was not persisted; the caller counts failures.
        """
        try:
            if skip_existing and self._repo.exists(race_id):
                logger.debug(
                    "ingest_race %s: already in Silver, skipping", race_id
                )
                return True

            raw = self._storage.read(f"{r_date}/{race_id}.json")
            if not raw:
                logger.warning(
                    "ingest_race %s: JSON not found in GCS at %s/%s.json",
                    race_id,
                    r_date,
                    race_id,
                )
                return False

            try:
                race = self._adapter.to_race(raw)
            except AdapterError as exc:
                logger.error("ingest_race %s: adapter error — %s", race_id, exc)
                return False

            self._repo.save(race)
            logger.info("ingest_race %s: SUCCESS", race_id)
            return True

        except Exception as exc:
            logger.exception(
                "ingest_race %s: unexpected error — %s", race_id, exc
            )
            return False
