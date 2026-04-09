# filepath: apps/g2f_results_harvester/results_scraper.py
"""
Purpose: Playwright-based scraper for greyhound race results.
Usage:   Called by runners/results_main.py in GitHub Actions.

WAF-evasion patterns:
- Jittered human pauses (3-8s) between requests
- cycle_page() every 15 targets to reset SPA state
- Exit code 11 on WAF 403 (triggers runner hop)
- Exit code 20 on time budget exceeded

Two main methods:
    discover_results(date) — get all race_ids for a date
    fetch_result(race_id, ...) — capture one result page
"""

from __future__ import annotations

import logging
import random
import re
import time
from datetime import date
from typing import Any

from playwright.sync_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    sync_playwright,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://greyhoundbet.racingpost.com/"
SPA_LOAD_SELECTOR = "#firstLoadingAnimation"
RESULT_CONTENT_SELECTOR = ".meetingResultsList"

MIN_DELAY = 3.0
MAX_DELAY = 8.0
CYCLE_EVERY = 15


class WAFBlockError(Exception):
    """Raised when Azure WAF returns 403."""

    pass


class ResultsScraper:
    """Scrapes race results from the Greyhound Bet SPA."""

    def __init__(self) -> None:
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None
        self._request_count = 0
        self._spa_loaded = False

    def start(self) -> None:
        """Launch browser and load the SPA."""
        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features" "=AutomationControlled",
                "--no-sandbox",
            ],
        )
        self._context = self._browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
        )
        self._page = self._context.new_page()
        self._load_spa()

    def stop(self) -> None:
        """Close browser and playwright."""
        if self._context:
            self._context.close()
        if self._browser:
            self._browser.close()
        if self._playwright:
            self._playwright.stop()

    def discover_results(self, target_date: date) -> list[dict[str, str]]:
        """Get all race_ids with results for a date."""
        page = self._ensure_page()
        date_str = target_date.isoformat()

        page.evaluate(
            "window.location.hash = " f"'results-list/r_date={date_str}'"
        )
        logger.info("Discovering results for %s", date_str)

        self._human_delay(2.0, 4.0)

        try:
            page.wait_for_selector(
                "a[href*='race_id=']",
                state="attached",
                timeout=15000,
            )
        except Exception:
            logger.warning("No results list for %s", date_str)
            return []

        self._human_delay(1.0, 2.0)

        html = page.content()
        races = self._extract_race_links(html, date_str)

        logger.info(
            "Found %d races for %s",
            len(races),
            date_str,
        )
        self._request_count += 1
        return races

    def fetch_result(
        self,
        race_id: str,
        track_id: str,
        r_date: str,
        r_time: str,
    ) -> dict[str, Any] | None:
        """Fetch one race result page.

        Returns Bronze-format dict or None on failure.
        """
        page = self._ensure_page()
        self._request_count += 1

        if self._request_count % CYCLE_EVERY == 0:
            self._cycle_page()

        hash_path = (
            f"result-meeting-result/"
            f"race_id={race_id}&track_id={track_id}"
            f"&r_date={r_date}&r_time={r_time}"
        )
        page.evaluate(f"window.location.hash = '{hash_path}'")

        self._human_delay()

        try:
            page.wait_for_selector(
                RESULT_CONTENT_SELECTOR,
                state="visible",
                timeout=12000,
            )
        except Exception as err:
            content = page.content()
            if "403" in content or "Access Denied" in content:
                raise WAFBlockError(f"WAF block on race {race_id}") from err
            logger.warning("race %s: content not found", race_id)
            return None

        self._human_delay(0.5, 1.5)

        # Capture result HTML
        result_html = page.evaluate("""
            () => {
                const el = document.querySelector(
                    '.meetingResultsList'
                );
                return el ? el.outerHTML : '';
            }
        """)

        # Capture header — use .meetingHeader for
        # robustness against SPA ad-load wrapping
        header_html = page.evaluate("""
            () => {
                const parts = [];
                const mh = document.querySelector(
                    '.meetingHeader'
                );
                if (mh) parts.push(mh.outerHTML);
                const rt = document.querySelector(
                    '.raceTitle'
                );
                if (rt) parts.push(rt.outerHTML);
                return parts.join('');
            }
        """)

        if not result_html:
            logger.warning("race %s: empty result HTML", race_id)
            return None

        return {
            "race_id": race_id,
            "track_id": track_id,
            "r_date": r_date,
            "r_time": r_time,
            "status": "scraped",
            "html_snapshots": {
                "result": result_html,
                "header": header_html,
            },
            "scrape_timestamp": (time.strftime("%Y-%m-%dT%H:%M:%SZ")),
        }

    # ── Private ──

    def _ensure_page(self) -> Page:
        if not self._page:
            raise RuntimeError("Scraper not started")
        if not self._spa_loaded:
            self._load_spa()
        return self._page

    def _load_spa(self) -> None:
        page = self._page
        if not page:
            raise RuntimeError("No page")
        page.goto(BASE_URL)
        try:
            page.wait_for_selector(
                SPA_LOAD_SELECTOR,
                state="hidden",
                timeout=30000,
            )
        except Exception:
            logger.warning("SPA loading slow")
        self._human_delay(1.0, 3.0)
        self._spa_loaded = True

    def _cycle_page(self) -> None:
        page = self._page
        if not page:
            return
        logger.debug("Cycling page")
        page.goto(f"{BASE_URL}#news-home")
        self._human_delay(2.0, 4.0)
        self._spa_loaded = True

    @staticmethod
    def _human_delay(
        min_s: float = MIN_DELAY,
        max_s: float = MAX_DELAY,
    ) -> None:
        delay = random.uniform(min_s, max_s)
        time.sleep(delay)

    @staticmethod
    def _extract_race_links(
        html: str, fallback_date: str
    ) -> list[dict[str, str]]:
        races: list[dict[str, str]] = []
        seen: set[str] = set()

        for m in re.finditer(
            # Accept both #result-meeting-result/ and #card-meeting-result/
            r'href="#(?:result|card)-meeting-result/'
            r"race_id=(\d+)"
            r"(?:&amp;|&)track_id=(\d+)"
            r"(?:&amp;|&)r_date=([\d-]+)"
            r"(?:&amp;|&)r_time=([\d:%]+)",
            html,
        ):
            race_id = m.group(1)
            if race_id in seen:
                continue
            seen.add(race_id)

            races.append(
                {
                    "race_id": race_id,
                    "track_id": m.group(2),
                    "r_date": m.group(3),
                    "r_time": m.group(4).replace("%3A", ":"),
                }
            )

        return races
