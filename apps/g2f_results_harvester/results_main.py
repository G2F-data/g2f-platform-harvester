# filepath: apps/g2f_results_harvester/results_main.py
"""
Purpose: Cloud orchestrator for results scraping.
Usage:   python apps/g2f_results_harvester/results_main.py --date 2026-03-15
         python apps/g2f_results_harvester/results_main.py
         (defaults to yesterday)

Runs in GitHub Actions. Writes Bronze result JSONs to GCS.
Uses manifests to track progress across interrupted runs.

Manifest format (per race_id):
    {
        "status":   "pending" | "scraped" | "void" | "failed",
        "track_id": "99",
        "r_date":   "2025-12-01",
        "r_time":   "11:03",
    }

The full race metadata is stored at discovery time so that
fetch_result() can reconstruct the correct SPA hash URL on
any subsequent chained run, even if discovery is skipped.

Exit codes:
    0  — all targets scraped successfully
    11 — WAF 403 detected, trigger runner hop
    20 — time budget exceeded, resume on next run
    1  — unrecoverable error
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, cast

# Bootstrap
_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(_ROOT / "libs"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("results_runner")

TIME_BUDGET_SECONDS = int(
    os.environ.get("SCRAPER_TIME_BUDGET_SECONDS", "19800")
)

GCS_BUCKET = os.environ.get(
    "GCS_RESULTS_BUCKET_NAME", "g2f-bronze-results-prod"
)


def _upload_to_gcs(blob_name: str, data: str) -> None:
    """Upload string data to GCS."""
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data, content_type="application/json")


def _download_from_gcs(blob_name: str) -> str | None:
    """Download string from GCS. Returns None if not found."""
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(blob_name)
    if not blob.exists():
        return None
    return cast(str, blob.download_as_text())


def _load_manifest(date_str: str) -> dict[str, Any]:
    """Load or create manifest for a date.

    Returns dict of race_id → {status, track_id, r_date, r_time}.
    Status values: "pending", "scraped", "void", "failed".
    """
    blob_name = f"{date_str}/_manifest.json"
    content = _download_from_gcs(blob_name)
    if content:
        return cast(dict[str, Any], json.loads(content))
    return {}


def _save_manifest(date_str: str, manifest: dict[str, Any]) -> None:
    """Save manifest to GCS."""
    blob_name = f"{date_str}/_manifest.json"
    _upload_to_gcs(blob_name, json.dumps(manifest))


def run(target_dates: list[date]) -> int:
    """Execute results scraping for given dates.

    Returns exit code. The scraper is always stopped in the finally
    block — never inside the loop — to prevent double-close of the
    Playwright event loop which raises "Event loop is closed!".
    """
    from results_scraper import ResultsScraper, WAFBlockError

    start_time = time.time()
    scraper = ResultsScraper()

    try:
        scraper.start()
    except Exception as exc:
        logger.error("Failed to start scraper: %s", exc)
        return 1

    # exit_code is set by break conditions inside the loop.
    # The finally block stops the scraper exactly once regardless
    # of which path exits the try block.
    exit_code = 0
    total_scraped = 0
    total_failed = 0
    total_void = 0

    try:
        for target_date in target_dates:
            date_str = target_date.isoformat()
            elapsed = time.time() - start_time

            if elapsed > TIME_BUDGET_SECONDS:
                logger.info("Time budget reached at %s", date_str)
                exit_code = 20
                break

            logger.info("Processing date: %s", date_str)

            manifest = _load_manifest(date_str)

            # Discover races if manifest is empty
            if not manifest:
                try:
                    races = scraper.discover_results(target_date)
                except WAFBlockError:
                    logger.error("WAF block on discovery")
                    exit_code = 11
                    break
                except Exception as exc:
                    logger.error("Discovery failed: %s", exc)
                    continue

                if not races:
                    logger.info("%s: no races found", date_str)
                    continue

                # Store full metadata so chained runs can reconstruct
                # the correct SPA hash URL without re-discovering.
                for race in races:
                    rid = race["race_id"]
                    if rid not in manifest:
                        manifest[rid] = {
                            "status": "pending",
                            "track_id": race["track_id"],
                            "r_date": race["r_date"],
                            "r_time": race["r_time"],
                        }
                _save_manifest(date_str, manifest)

            # Collect pending entries with their full metadata
            pending: list[dict[str, str]] = [
                {
                    "race_id": rid,
                    "track_id": meta["track_id"],
                    "r_date": meta["r_date"],
                    "r_time": meta["r_time"],
                }
                for rid, meta in manifest.items()
                if isinstance(meta, dict) and meta.get("status") == "pending"
            ]

            if not pending:
                logger.info(
                    "%s: all %d races already processed",
                    date_str,
                    len(manifest),
                )
                continue

            logger.info(
                "%s: %d pending of %d total",
                date_str,
                len(pending),
                len(manifest),
            )

            budget_hit = False
            for race_info in pending:
                elapsed = time.time() - start_time
                if elapsed > TIME_BUDGET_SECONDS:
                    logger.info("Time budget reached")
                    _save_manifest(date_str, manifest)
                    exit_code = 20
                    budget_hit = True
                    break

                race_id = race_info["race_id"]

                try:
                    result = scraper.fetch_result(
                        race_id=race_id,
                        track_id=race_info["track_id"],
                        r_date=race_info["r_date"],
                        r_time=race_info["r_time"],
                    )
                except WAFBlockError:
                    logger.error("WAF block on race %s", race_id)
                    _save_manifest(date_str, manifest)
                    exit_code = 11
                    budget_hit = True
                    break
                except Exception as exc:
                    logger.warning("race %s failed: %s", race_id, exc)
                    manifest[race_id]["status"] = "failed"
                    total_failed += 1
                    continue

                if result is None:
                    manifest[race_id]["status"] = "void"
                    total_void += 1
                else:
                    blob_name = f"{date_str}/{race_id}_result.json"
                    _upload_to_gcs(blob_name, json.dumps(result))
                    manifest[race_id]["status"] = "scraped"
                    total_scraped += 1

            # Propagate inner-loop budget/WAF break to the outer loop
            if budget_hit:
                break

            _save_manifest(date_str, manifest)
            logger.info(
                "%s complete: scraped=%d void=%d failed=%d",
                date_str,
                sum(
                    1
                    for m in manifest.values()
                    if isinstance(m, dict) and m.get("status") == "scraped"
                ),
                sum(
                    1
                    for m in manifest.values()
                    if isinstance(m, dict) and m.get("status") == "void"
                ),
                sum(
                    1
                    for m in manifest.values()
                    if isinstance(m, dict) and m.get("status") == "failed"
                ),
            )

    finally:
        # Single authoritative stop. Never called inside the loop.
        # Playwright's event loop is torn down exactly once here.
        scraper.stop()

    logger.info(
        "== COMPLETE == scraped=%d failed=%d void=%d exit=%d",
        total_scraped,
        total_failed,
        total_void,
        exit_code,
    )
    return exit_code


def _parse_date_range(
    target_date: str | None,
    from_date: str | None,
    to_date: str | None,
) -> list[date]:
    """Build list of dates to process."""
    if target_date:
        return [date.fromisoformat(target_date)]

    if from_date and to_date:
        start = date.fromisoformat(from_date)
        end = date.fromisoformat(to_date)
        dates: list[date] = []
        current = start
        while current <= end:
            dates.append(current)
            current += timedelta(days=1)
        return dates

    if from_date:
        return [date.fromisoformat(from_date)]

    # Default: yesterday
    return [date.today() - timedelta(days=1)]


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None)
    parser.add_argument("--from-date", default=None)
    parser.add_argument("--to-date", default=None)
    args = parser.parse_args()

    target_date_env = os.environ.get("TARGET_DATE")
    from_date_env = os.environ.get("FROM_DATE")
    to_date_env = os.environ.get("TO_DATE")

    dates = _parse_date_range(
        args.date or target_date_env,
        args.from_date or from_date_env,
        args.to_date or to_date_env,
    )

    logger.info("Target dates: %s", dates)
    exit_code = run(dates)
    sys.exit(exit_code)
