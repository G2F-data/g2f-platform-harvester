"""
Purpose: The Cloud Ingestion worker for GitHub Actions.
Usage: Triggered daily via cron. Orchestrates stateful harvesting.
Dependencies: PlaywrightScraper, GCSStorage
"""

import asyncio
import logging
import os
import random
import sys
import time
from datetime import date
from typing import Any, cast

from g2f_core.adapters.playwright_scraper import (
    PlaywrightScraper,
    RaceTarget,
    ScraperBlockedError,
)
from g2f_core.adapters.storage_factory import get_storage_adapter
from g2f_core.domain.ports import StoragePort
from g2f_core.services.manifest import (
    HarvestManifest,
    RaceTargetState,
    TargetStatus,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s[%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("Harvester")

EXIT_OK = 0
EXIT_ERROR = 1
# EXIT_IP_BLOCKED: the site refused our connection (403/Captcha) at any
# point. The workflow chainer triggers a new GitHub Actions run, which
# lands on a fresh VM with a different IP ("runner hop"). The manifest
# is always in a safe, resumable state when this code is used.
EXIT_IP_BLOCKED = 11
# EXIT_BUDGET: clean exit when time budget is exhausted but targets
# remain. The chainer triggers another run that loads the manifest
# and resumes.
EXIT_BUDGET = 20

# Tabs that must be OK for a race to be considered fully captured.
# tips is editorial and may succeed even when card data is unavailable,
# so it is intentionally excluded from the quality gate.
_QUALITY_TABS = ("card", "form", "stats")

# scan_report status values that count as successful tab capture.
_OK_STATUSES = frozenset(
    {"OK", "OK_FALLBACK_1", "OK_FALLBACK_WRAPPER", "SKIPPED_NO_TAB"}
)

# [H] Consecutive-failure escalation ladder thresholds.
_FAIL_CYCLE = 3
_FAIL_RESTART = 6
_FAIL_BAIL = 10
_MAX_RESTARTS = 2
# [I] Per-track cascade deflection threshold.
_PER_TRACK_FAIL_LIMIT = 3

# [J] Retry and loopback parameters
_MAX_GLOBAL_RETRIES = 12
_MAX_RUN_ATTEMPTS = 3


def _classify_payload(result: dict[str, Any]) -> tuple[bool, str | None]:
    """Inspect scan_report and classify the payload quality.

    Returns (is_critical_fail, error_msg).

    is_critical_fail=True  → target must be marked FAILED and retried.
    is_critical_fail=False → target is saved as SUCCESS; error_msg
                             is set only for PARTIAL_FAILURE.

    Ghost hierarchy (all result in FAILED):
      GHOST_ALL_TABS_FAILED  — dogs=0, every tab produced nothing useful
      GHOST_NO_DOGS          — dogs=0, at least tips was ok

    Partial success (saved, flagged):
      PARTIAL_FAILURE        — dogs present but ≥1 quality tab failed;
                               the dogs data is the high-value asset and
                               is retained even when tab snapshots are
                               incomplete
    """
    if "error" in result:
        return True, str(result["error"])

    scan = result.get("scan_report", {})
    dogs_count = int(scan.get("dogs_count", "0"))

    failed_quality_tabs = [
        t for t in _QUALITY_TABS if scan.get(t, "FAILED") not in _OK_STATUSES
    ]

    if dogs_count == 0:
        all_bad = all(
            scan.get(t, "FAILED") not in _OK_STATUSES
            for t in ("card", "form", "stats", "tips")
        )
        if all_bad:
            return True, (
                "GHOST_ALL_TABS_FAILED: all tabs hidden or missing — "
                "SPA state failure or abandoned race"
            )
        return True, (
            f"GHOST_NO_DOGS: no runner data found — "
            f"failed_tabs={failed_quality_tabs}"
        )

    if failed_quality_tabs:
        msg = (
            f"PARTIAL_FAILURE: dogs captured but "
            f"tabs={failed_quality_tabs} failed"
        )
        return False, msg

    return False, None


class HarvestOrchestrator:
    def __init__(
        self,
        scraper: PlaywrightScraper,
        storage: StoragePort,
        env_name: str,
        time_budget_seconds: float | None = None,
    ) -> None:
        self.scraper = scraper
        self.storage = storage
        self.env_name = env_name
        self.target_date = date.today()
        self.manifest_path = f"{self.target_date}/_manifest.json"
        self._active_manifest: HarvestManifest | None = None
        self._deadline: float | None = (
            time.monotonic() + time_budget_seconds
            if time_budget_seconds
            else None
        )

    def _budget_exceeded(self) -> bool:
        """Returns True when the wall-clock deadline has passed."""
        return self._deadline is not None and time.monotonic() >= self._deadline

    async def run(self) -> None:
        start_time = time.time()
        logger.info(
            f"🚀 Starting Harvester Orchestrator (Env: {self.env_name})"
        )

        startup_delay = random.uniform(5.0, 30.0)
        logger.info(f"⏳ Delaying startup for {startup_delay:.1f} seconds...")
        await asyncio.sleep(startup_delay)

        budget_exit = False

        try:
            try:
                await self.scraper.start()
            except ScraperBlockedError as e:
                logger.error(f"⛔ STARTUP BLOCKED: {e}")
                # No manifest interaction — bypass finally entirely.
                os._exit(EXIT_IP_BLOCKED)

            manifest = await self._load_or_create_manifest()

            if manifest.targets_found == 0:
                logger.warning("⚠️ No races found. Exiting gracefully.")
                sys.exit(EXIT_OK)

            pending_targets = manifest.get_pending_targets(
                max_retries=_MAX_GLOBAL_RETRIES
            )
            logger.info(
                f"🌾 Resuming Harvest: {len(pending_targets)} pending "
                f"targets out of {manifest.targets_found} total."
            )

            if not pending_targets:
                logger.info("🎉 All targets are already SUCCESS. Exiting.")
                sys.exit(EXIT_OK)

            pending_targets.sort(
                key=lambda t: (t.r_date, t.track_id, t.race_id)
            )

            # Escalation ladder + per-track cascade deflection.
            #
            # [H] Consecutive-failure ladder:
            #   3 fails → cycle_page(warmup=True)
            #   6 fails → full browser restart (cap MAX_RESTARTS per run)
            #  10 fails → bail with EXIT_BUDGET to trigger a fresh runner
            #
            # [I] Per-track skip: if a single track_id accumulates 3 fails
            # its remaining races are deflected to the end of the queue.
            # Each track can be deferred at most once to prevent infinite
            # loops when every remaining track is broken.
            consecutive_failures = 0
            per_track_failures: dict[str, int] = {}
            deferred_tracks: set[str] = set()
            restart_count = 0

            targets = list(pending_targets)
            run_attempts: dict[str, int] = {t.race_id: 0 for t in targets}
            i = 0
            while i < len(targets):
                target = targets[i]

                if manifest.blocked:
                    logger.warning(
                        "🛑 Global Block detected. Stopping task queue."
                    )
                    break

                # TIME BUDGET GUARD: stop clean before GitHub Actions kills us.
                # Manifest is fully up-to-date (saved after every target below)
                # so the next chained run resumes exactly where we stopped.
                if self._budget_exceeded():
                    remaining = manifest.get_pending_targets(
                        max_retries=_MAX_GLOBAL_RETRIES
                    )
                    logger.info(
                        f"⏰ Time budget reached. "
                        f"{len(remaining)} targets remain — "
                        "signalling chainer (exit 20)."
                    )
                    budget_exit = True
                    break

                await self._process_target(target, manifest)

                failed = target.status == TargetStatus.FAILED
                track_id = target.track_id

                if failed:
                    consecutive_failures += 1
                    per_track_failures[track_id] = (
                        per_track_failures.get(track_id, 0) + 1
                    )
                    run_attempts[target.race_id] += 1

                    # [K] Loopback: Append back to targets to batch retry
                    if (
                        run_attempts[target.race_id] < _MAX_RUN_ATTEMPTS
                        and target.retries < _MAX_GLOBAL_RETRIES
                    ):
                        targets.append(target)
                        logger.warning(
                            f"🔄 Queuing {target.race_id} for local retry "
                            f"({run_attempts[target.race_id]}/{_MAX_RUN_ATTEMPTS})."
                        )

                    # [I] Per-track cascade deflection (once per track).
                    if (
                        per_track_failures[track_id] >= 3
                        and track_id not in deferred_tracks
                    ):
                        tail = targets[i + 1 :]
                        same = [t for t in tail if t.track_id == track_id]
                        other = [t for t in tail if t.track_id != track_id]
                        targets = targets[: i + 1] + other + same
                        deferred_tracks.add(track_id)
                        per_track_failures[track_id] = 0
                        logger.warning(
                            f"🛤 Track {track_id} cascade — deferring "
                            f"{len(same)} remaining races to end of queue."
                        )

                    # [H] Escalation ladder.
                    if consecutive_failures >= _FAIL_BAIL:
                        remaining = manifest.get_pending_targets(
                            max_retries=_MAX_GLOBAL_RETRIES
                        )
                        logger.error(
                            f"🚨 {consecutive_failures} consecutive failures — "
                            f"session appears dead. Bailing with EXIT_BUDGET "
                            f"to hop runners. {len(remaining)} targets remain."
                        )
                        budget_exit = True
                        self.storage.save(
                            self.manifest_path,
                            manifest.model_dump(mode="json"),
                        )
                        break
                    elif (
                        consecutive_failures >= _FAIL_RESTART
                        and restart_count < _MAX_RESTARTS
                    ):
                        logger.warning(
                            f"⚠️ {consecutive_failures} consecutive failures — "
                            f"full browser restart "
                            f"({restart_count + 1}/{_MAX_RESTARTS})."
                        )
                        ok = await self.scraper.restart()
                        restart_count += 1
                        if ok:
                            consecutive_failures = 0
                        else:
                            logger.error(
                                "Post-restart warmup failed — SPA "
                                "still unreachable."
                            )
                    elif consecutive_failures >= _FAIL_CYCLE:
                        logger.warning(
                            f"{consecutive_failures} consecutive failures — "
                            "forcing cycle_page(warmup=True)."
                        )
                        ok = await self.scraper.cycle_page(warmup=True)
                        if ok:
                            consecutive_failures = 0
                        else:
                            logger.warning(
                                "cycle_page warmup failed — will escalate "
                                "on next failure."
                            )
                else:
                    consecutive_failures = 0
                    # [F] Periodic maintenance recycle every 20 successful
                    # iterations to reduce hidden-DOM accumulation.
                    if i > 0 and i % 20 == 0:
                        await self.scraper.cycle_page()

                self.storage.save(
                    self.manifest_path,
                    manifest.model_dump(mode="json"),
                )
                i += 1

            if (
                not budget_exit
                and not manifest.blocked
                and manifest.get_pending_targets(
                    max_retries=_MAX_GLOBAL_RETRIES
                )
            ):
                logger.info(
                    "Pool complete, but retries remain. Triggering chain."
                )
                budget_exit = True

        except Exception as e:
            logger.exception(f"💥 UNHANDLED EXCEPTION: {e}")
            sys.exit(EXIT_ERROR)

        finally:
            await self.scraper.stop()

            duration = time.time() - start_time
            final_manifest = self._active_manifest
            if not final_manifest:
                # This path is only reached if the manifest was never loaded
                # (e.g. discovery block already called os._exit above, so
                # this case should not occur in normal operation).
                final_manifest = HarvestManifest(
                    run_date=self.target_date, runner=self.env_name
                )

            final_manifest.duration_seconds += round(duration, 2)

            if final_manifest.blocked:
                # Mid-scrape block. Manifest has been saved after every race,
                # so all completed races are preserved. Reset to 'running' so
                # the next chained runner resumes correctly.
                final_manifest.status = "running"
                final_manifest.blocked = False
                for t in final_manifest.targets.values():
                    if t.status == TargetStatus.BLOCKED:
                        t.status = TargetStatus.PENDING
                self.storage.save(
                    self.manifest_path,
                    final_manifest.model_dump(mode="json"),
                )
                logger.warning(
                    "⛔ Mid-scrape block. Manifest reset to PENDING. "
                    "Exiting with IP_BLOCKED (11) for runner hop."
                )
                sys.exit(EXIT_IP_BLOCKED)

            remaining_after = final_manifest.get_pending_targets(
                max_retries=_MAX_GLOBAL_RETRIES
            )

            if budget_exit and remaining_after:
                # Clean time-budget exit. Leave status as 'running' so the
                # next chained runner loads this manifest and resumes.
                final_manifest.status = "running"
                self.storage.save(
                    self.manifest_path,
                    final_manifest.model_dump(mode="json"),
                )
                logger.info(
                    f"⏰ Budget exit. Success so far: "
                    f"{final_manifest.success_count}, "
                    f"Errors: {final_manifest.error_count}, "
                    f"Remaining: {len(remaining_after)}"
                )
                sys.exit(EXIT_BUDGET)

            elif final_manifest.error_count > 0:
                final_manifest.status = "completed_with_errors"
            else:
                final_manifest.status = "completed"

            self.storage.save(
                self.manifest_path,
                final_manifest.model_dump(mode="json"),
            )
            logger.info(
                f"🏁 Finished. Success: {final_manifest.success_count}, "
                f"Errors: {final_manifest.error_count}"
            )

    async def _load_or_create_manifest(self) -> HarvestManifest:
        data = self.storage.read(self.manifest_path)

        if data and data.get("targets"):
            manifest = HarvestManifest.model_validate(data)
            logger.info("📄 Existing manifest loaded for stateful recovery.")
            manifest.blocked = False
            manifest.status = "running"
            for t in manifest.targets.values():
                if t.status == TargetStatus.BLOCKED:
                    t.status = TargetStatus.PENDING
            self._active_manifest = manifest
            return manifest

        if data:
            logger.info(
                "⚠️ Legacy manifest format detected. " "Forcing fresh discovery."
            )
        else:
            logger.info(f"🔎 Fetching Metadata for {self.target_date}...")

        try:
            targets_raw = await self.scraper.get_races_metadata(
                self.target_date
            )
        except ScraperBlockedError as e:
            logger.error(f"⛔ BLOCKED DURING DISCOVERY: {e}")
            # No manifest written — bypass finally entirely so we don't
            # write a misleading "completed" manifest with zero targets.
            os._exit(EXIT_IP_BLOCKED)

        manifest = HarvestManifest(
            run_date=self.target_date,
            runner=self.env_name,
        )
        for t in targets_raw:
            manifest.targets[t["race_id"]] = RaceTargetState(**t)

        self._active_manifest = manifest
        self.storage.save(self.manifest_path, manifest.model_dump(mode="json"))
        return manifest

    async def _process_target(
        self, target: RaceTargetState, manifest: HarvestManifest
    ) -> None:
        target_dict = cast(
            RaceTarget,
            {
                "race_id": target.race_id,
                "track_id": target.track_id,
                "r_date": target.r_date,
                "races_ids": target.races_ids,
                "r_time": target.r_time,
            },
        )

        try:
            jitter = abs(random.gauss(2.5, 1.0))
            await asyncio.sleep(jitter)

            result = await self.scraper.fetch_race_raw(target_dict)

            # [A+B] Validity gate — classify payload quality before
            # deciding success or failure. A ghost SUCCESS (dogs=0
            # saved as success) is unrecoverable; a FAILED record is
            # retried on the next chain run.
            is_critical_fail, err_msg = _classify_payload(result)

            if is_critical_fail:
                target.status = TargetStatus.FAILED
                target.retries += 1
                target.error_log = err_msg or "UNKNOWN_FAILURE"
                self.storage.save(
                    f"{self.target_date}/error_{target.race_id}.json",
                    result,
                )
                logger.error(f"❌ Failed {target.race_id}: {target.error_log}")

                # [G] Terminal ghost cleanup: once a ghost race has
                # exhausted all retries, delete any stale success JSON
                # that may exist from a previous run before this fix was
                # deployed. Ensures bronze is never silently incomplete.
                if target.retries >= _MAX_GLOBAL_RETRIES:  # max_retries
                    ghost_path = f"{self.target_date}/{target.race_id}.json"
                    try:
                        self.storage.delete(ghost_path)
                        logger.warning(
                            f"🗑 Deleted terminal ghost: {ghost_path}"
                        )
                    except Exception:
                        pass  # no-op if file did not exist

            else:
                target.status = TargetStatus.SUCCESS
                target.error_log = err_msg  # None or PARTIAL_FAILURE msg
                if err_msg:
                    # PARTIAL_FAILURE — annotate the payload before saving
                    result.setdefault("scan_report", {})["quality_flag"] = (
                        err_msg
                    )
                    logger.warning(f"⚠️ Partial {target.race_id}: {err_msg}")
                else:
                    logger.info(f"✅ Saved {target.race_id}")
                self.storage.save(
                    f"{self.target_date}/{target.race_id}.json", result
                )

        except ScraperBlockedError as e:
            manifest.blocked = True
            target.status = TargetStatus.BLOCKED
            target.error_log = str(e)
            logger.error(f"⛔ Blocked on {target.race_id}: {e}")
        except Exception as e:
            target.status = TargetStatus.FAILED
            target.retries += 1
            target.error_log = str(e)
            logger.error(f"❌ Exception on {target.race_id}: {e}")


async def main() -> None:
    headless = os.getenv("HEADLESS", "true").lower() == "true"
    env_name = os.getenv("APP_ENV", "local")

    budget_str = os.getenv("SCRAPER_TIME_BUDGET_SECONDS")
    time_budget: float | None = float(budget_str) if budget_str else None

    proxy = os.getenv("SCRAPER_PROXY") or None

    scraper = PlaywrightScraper(headless=headless, proxy=proxy)
    storage = get_storage_adapter()

    orchestrator = HarvestOrchestrator(
        scraper, storage, env_name, time_budget_seconds=time_budget
    )
    await orchestrator.run()


if __name__ == "__main__":
    asyncio.run(main())
