"""
Purpose: Playwright adapter for scraping Greyhound Racing Post.
Usage: Navigates the SPA, clicks tabs, and extracts raw HTML fragments.
Dependencies: playwright, asyncio, bs4

Timing philosophy
-----------------
Human-like behaviour requires *variation* and *presence* of pauses, not
large absolute values.  The WAF fingerprints cadence patterns; it does not
require any specific minimum sleep.  Key changes from the prior version:

* ``_simulate_human_reading`` reduced to a single Gaussian pause with
  tighter parameters.  The second pause and ``steps=5`` mouse-move (which
  adds ~250 ms of synchronous Playwright overhead) are removed.
* ``count()`` / ``is_visible()`` guard calls replaced with ``try/except``
  around the direct action — this eliminates 40-60 serial CDP roundtrips
  per race (50-80 ms each on a GitHub Actions runner).
* ``_build_runner_manifest`` replaced with a single ``page.evaluate()``
  call that extracts all runner data in one JS execution, cutting 2-3 s
  of serial roundtrip overhead per race.
* Hover pauses reduced from ``gauss(0.2, 0.05)`` to ``gauss(0.12, 0.03)``.
  A human can comfortably hover-then-click in 80-150 ms; 200 ms is
  over-cautious and multiplies across every dog link and tab.
* ``get_races_metadata`` now uses a single ``page.evaluate()`` to extract
  both href and link text from the meeting list, providing ``r_time``
  directly from the discovery phase instead of relying on the unreliable
  ``#pagerCardTime`` live-ticker element.
* ``HiddenDOMError`` added: raised when a tab container is present in
  the DOM but permanently CSS-hidden (``locator resolved to hidden``).
  Caught by ``fetch_race_raw`` to trigger ``cycle_page()`` + one free
  retry before the retry counter is incremented.
"""

import asyncio  # noqa: I001
import json
import logging
import random
import re
from collections.abc import Callable
from datetime import date, datetime
from typing import Any, TypedDict
from urllib.parse import parse_qs, urlencode

from playwright.async_api import (
    Browser,
    BrowserContext,
    Locator,
    Page,
    Playwright,
    ProxySettings,
    Request,
    Response,
)
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import (
    async_playwright,
)

BASE_URL = "https://greyhoundbet.racingpost.com"
# fmt: off
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
# fmt: on

logger = logging.getLogger(__name__)


class RaceTarget(TypedDict):
    race_id: str
    track_id: str
    r_date: str
    races_ids: str
    r_time: str  # [D] from discovery, e.g. "19:45"; "00:00" if unknown


class ScraperBlockedError(Exception):
    """Raised when the site refuses connection (403/406/Captcha)."""

    pass


class StaleSPAError(Exception):
    """Raised when URL changes but the SPA fails to render the DOM."""

    pass


class HiddenDOMError(Exception):
    """Raised when a tab container is present but CSS-hidden.

    Distinct from StaleSPAError (which is a navigation/URL failure).
    HiddenDOMError means the route loaded correctly but the SPA's
    internal component tree did not re-render its content visible.
    Triggers cycle_page() + one free retry in fetch_race_raw, with
    no increment to the manifest retry counter.
    """

    pass


class PlaywrightScraper:
    def __init__(
        self,
        headless: bool = True,
        proxy: str | None = None,
    ) -> None:
        self.headless = headless
        # Optional proxy URL, e.g. "http://user:pass@host:port".
        # Passed through to every new_context() call. Read from
        # SCRAPER_PROXY env var in main.py; None means no proxy.
        self._proxy = proxy
        self.playwright: Playwright | None = None
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None

        # Enhanced Telemetry
        self.network_anomalies: list[dict[str, Any]] = []
        self.page_errors: list[str] = []
        self.console_logs: list[str] = []

    def _make_proxy_settings(self) -> ProxySettings | None:
        """Returns a typed ProxySettings object, or None."""
        if not self._proxy:
            return None
        return ProxySettings(server=self._proxy)

    async def start(self) -> None:
        """Lifecycle: Start the browser and shared session context."""
        if self.browser:
            return
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
            ],
        )
        self.context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=USER_AGENT,
            proxy=self._make_proxy_settings(),
        )
        await self._init_new_page()

    async def _init_new_page(self) -> None:
        """Creates a fresh page, registers handlers, and loads the SPA."""
        if not self.context:
            raise RuntimeError("Context not initialized")

        self.page = await self.context.new_page()
        self.network_anomalies.clear()
        self.page_errors.clear()
        self.console_logs.clear()

        async def on_response(response: Response) -> None:
            if (  # noqa: SIM102
                response.status in [403, 406, 429, 503]
                and "greyhoundbet" in response.url
            ):
                if response.request.resource_type in [
                    "fetch",
                    "xhr",
                    "document",
                ]:
                    self.network_anomalies.append(
                        {
                            "url": response.url,
                            "status": response.status,
                            "type": "response_error",
                            "time": datetime.now().isoformat(),
                        }
                    )

        async def on_requestfailed(request: Request) -> None:
            res_type = request.resource_type
            if "greyhoundbet" in request.url and res_type in [
                "fetch",
                "xhr",
                "document",
            ]:
                self.network_anomalies.append(
                    {
                        "url": request.url,
                        "error": str(request.failure),
                        "type": "request_failed",
                        "time": datetime.now().isoformat(),
                    }
                )

        self.page.on("response", on_response)
        self.page.on("requestfailed", on_requestfailed)

        self.page.on("crash", lambda _: logger.error("Page crashed!"))
        self.page.on(
            "pageerror",
            lambda exc: self.page_errors.append(str(exc)),
        )
        self.page.on(
            "console",
            lambda msg: self.console_logs.append(msg.text),
        )

        async def dismiss_overlay(locator: Locator) -> None:
            await locator.hover()
            await self._human_pause(0.3, 0.1)
            await locator.click()

        overlay_selector = (
            "button:has-text('Accept'), button:has-text('Agree'), "
            "#onetrust-accept-btn-handler"
        )

        await self.page.add_locator_handler(
            self.page.locator(overlay_selector), dismiss_overlay
        )

        try:
            logger.info("Initializing SPA Session (Landing on Base URL)...")
            response = await self.page.goto(
                BASE_URL, wait_until="domcontentloaded", timeout=30000
            )
            if response and response.status in [403, 406, 429]:
                raise ScraperBlockedError(
                    f"HTTP {response.status} on Base load"
                )

            initial_overlay = self.page.locator(overlay_selector)
            try:
                await initial_overlay.first.wait_for(
                    state="visible", timeout=2000
                )
                await initial_overlay.first.hover()
                await self._human_pause(0.4, 0.1)
                await initial_overlay.first.click()
            except PlaywrightTimeoutError:
                pass

            # SPA hydration gate: wait for the first-load animation to
            # disappear before returning. CLAUDE.md mandates this as the
            # correct signal that the SPA bundle has executed and the
            # hashchange router is bound. Tolerate a missing element or
            # short timeout — the element is not always present, and we
            # fall back to the human-pause below in that case.
            try:
                anim_loc = self.page.locator("#firstLoadingAnimation").first
                await anim_loc.wait_for(state="hidden", timeout=8000)
            except PlaywrightTimeoutError:
                logger.debug(
                    "firstLoadingAnimation hydration gate timed out; "
                    "proceeding anyway."
                )
            except Exception:
                pass

            await self._human_pause(2.0, 0.5)
        except Exception as e:
            await self._save_diagnostics(self.page, "init_failure")
            raise e

    async def _warmup_meeting_list(self) -> bool:
        """Prove the SPA router is alive by round-tripping to meeting-list.

        Navigates to today's meeting-list hash and waits for
        ``.appList.raceList`` to be visible. Returns True if the SPA
        responded, False otherwise. Used after ``cycle_page()`` and
        ``restart()`` to guarantee hashchange is bound before the next
        real race navigation is attempted.
        """
        if not self.page:
            return False
        date_str = str(date.today())
        try:
            await self.page.evaluate(
                "(hash) => { window.location.hash = hash; }",
                f"meeting-list/r_date={date_str}",
            )
            await self.page.wait_for_url(
                self._build_url_predicate("meeting-list", {"r_date": date_str}),
                timeout=15000,
            )
            await self.page.locator(".appList.raceList").first.wait_for(
                state="visible", timeout=15000
            )
            logger.info("SPA warmup OK (meeting-list responded).")
            return True
        except Exception as e:
            logger.warning(f"SPA warmup failed: {e}")
            return False

    async def cycle_page(self, warmup: bool = False) -> bool:
        """Closes current context, opens one to obliterate memory/state.

        When ``warmup=True``, performs a meeting-list round-trip after
        re-initialisation to confirm the hashchange router is bound.
        Returns True if the SPA is demonstrably alive (or warmup was
        not requested), False if the warmup navigation failed — the
        caller should then escalate to a full browser restart.
        """
        logger.info("Recycling SPA Context to clear accumulated DOM state...")
        if self.page:
            await self.page.close()
        if self.context:
            await self.context.close()

        # Deadlock Fix: We MUST rebuild the Context, not just the page
        if not self.browser:
            raise RuntimeError("Browser not initialized")
        self.context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=USER_AGENT,
            proxy=self._make_proxy_settings(),
        )
        await self._init_new_page()

        if warmup:
            return await self._warmup_meeting_list()
        return True

    async def stop(self) -> None:
        """Lifecycle: Cleanup."""
        if self.page:
            await self.page.close()
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        self.page = None
        self.context = None
        self.browser = None
        self.playwright = None

    async def restart(self) -> bool:
        """Full browser teardown + re-launch + warmup.

        Escalation beyond ``cycle_page()``: tears down the Chromium
        process entirely to clear leaked V8 state, service workers,
        disk cache locks, and anything else that survives a context
        swap. Returns True if the post-restart SPA warmup succeeds.
        """
        logger.warning("🔁 Full browser restart requested.")
        try:
            await self.stop()
        except Exception as e:
            logger.warning(f"Error during stop() on restart: {e}")
        await self.start()
        return await self._warmup_meeting_list()

    async def _human_pause(self, mu: float, sigma: float) -> None:
        """Centralized helper for Gaussian traffic-shaping pauses."""
        await asyncio.sleep(abs(random.gauss(mu, sigma)))

    def _build_url_predicate(
        self,
        expected_route: str,
        expected_hash_params: dict[str, str],
    ) -> Callable[[str], bool]:
        """Creates a strict URL predicate for exact SPA route matches."""

        def predicate(url_str: str) -> bool:
            if "#" not in url_str:
                return False
            fragment = url_str.split("#", 1)[1]

            # EXACT ROUTE CHECK: Uses partition to isolate the route token
            route, _, rest = fragment.partition("/")
            if route != expected_route:
                return False

            parsed = parse_qs(rest)
            for k, v in expected_hash_params.items():
                if str(v) not in parsed.get(k, []):
                    return False
            return True

        return predicate

    def _to_24h_format(self, time_str: str) -> str:
        """Convert 12-h time string to 24-h HH:MM. Returns input on failure."""
        try:
            parts = time_str.strip().split(":")
            if len(parts) != 2:
                return time_str
            h = int(parts[0])
            m = int(parts[1])
            if h < 11:
                h += 12
            return f"{h:02d}:{m:02d}"
        except Exception:
            return time_str

    async def get_races_metadata(self, target_date: date) -> list[RaceTarget]:
        """Discover today's race targets from the meeting-list SPA page.

        [D] Uses a single page.evaluate() to extract both href and
        link text in one CDP roundtrip, then parses r_time directly
        from the link text (e.g. "19:45 Race 3"). This replaces the
        old approach of reading #pagerCardTime, which showed the next
        *global* race time rather than the race being viewed.
        """
        if not self.page:
            raise RuntimeError("Scraper not started.")

        self.network_anomalies.clear()
        self.page_errors.clear()
        self.console_logs.clear()

        date_str = str(target_date)

        try:
            hash_path = f"meeting-list/r_date={date_str}"
            await self.page.evaluate(
                "(hash) => { window.location.hash = hash; }",
                hash_path,
            )

            await self.page.wait_for_url(
                self._build_url_predicate("meeting-list", {"r_date": date_str}),
                timeout=15000,
            )

            try:
                list_locator = self.page.locator(".appList.raceList")
                await list_locator.first.wait_for(
                    state="visible", timeout=20000
                )
            except Exception as e:
                is_blocked = await self._check_soft_block(
                    self.page, "Discovery"
                )
                if is_blocked:
                    raise ScraperBlockedError(
                        "Soft Block / Captcha detected during discovery"
                    ) from e
                raise RuntimeError(
                    "Failed to find race list. "
                    "DOM structure may have changed."
                ) from e

            # [D] Single evaluate call: href + link text for r_time.
            links_data: list[dict[str, str]] = await self.page.evaluate(
                """
                () => {
                    const links = document.querySelectorAll(
                        'ul.appList.raceList a'
                    );
                    return Array.from(links).map(a => ({
                        href: a.getAttribute('href') || '',
                        text: a.innerText.trim()
                    }));
                }
                """
            )

            races_to_scrape: list[RaceTarget] = []
            time_pattern = re.compile(r"\b(\d{1,2}:\d{2})\b")

            for link in links_data:
                href = link.get("href", "")
                text = link.get("text", "")
                if not href:
                    continue
                try:
                    query_str = href.split("/")[-1] if "/" in href else href
                    params = parse_qs(query_str)
                    track_id = params.get("track_id", [""])[0]
                    r_date_val = params.get("r_date", [""])[0]
                    races_ids_str = params.get("races_ids", [""])[0]

                    if track_id and r_date_val and races_ids_str:
                        # Parse r_time from link text; normalize to 24-h.
                        m = time_pattern.search(text)
                        r_time = (
                            self._to_24h_format(m.group(1)) if m else "00:00"
                        )
                        ids = races_ids_str.split(",")
                        for rid in ids:
                            if rid:
                                races_to_scrape.append(
                                    {
                                        "race_id": rid,
                                        "track_id": track_id,
                                        "r_date": r_date_val,
                                        "races_ids": races_ids_str,
                                        "r_time": r_time,
                                    }
                                )
                except Exception:
                    continue

            return races_to_scrape
        except Exception as e:
            await self._save_diagnostics(self.page, "discovery_failure")
            raise e

    async def fetch_race_raw(self, target: RaceTarget) -> dict[str, Any]:
        """Fetch race data. Implements targeted retries for SPA failures.

        Three recovery paths, in priority order:
          StaleSPAError   — URL navigation stalled → cycle + retry
          HiddenDOMError  — tab rendered but content hidden → cycle + retry
          General failure → return error dict (counted as FAILED)

        cycle_page() + retry never increments the manifest retry counter
        for StaleSPAError or HiddenDOMError — these are session-level
        issues, not data-level failures.
        """
        if not self.page:
            raise RuntimeError("Scraper not started")

        self.network_anomalies.clear()
        self.page_errors.clear()
        self.console_logs.clear()

        try:
            return await self._scrape_race_robust(self.page, target)
        except ScraperBlockedError as e:
            await self._save_diagnostics(
                self.page, f"block_{target['race_id']}", target
            )
            raise e
        except StaleSPAError:
            logger.warning(
                f"Stale SPA DOM detected for {target['race_id']}. "
                "Recycling and retrying once."
            )
            await self._save_diagnostics(
                self.page,
                f"stale_spa_{target['race_id']}",
                target,
            )
            try:
                await self.cycle_page(warmup=True)
                return await self._scrape_race_robust(self.page, target)
            except Exception as e2:
                await self._save_diagnostics(
                    self.page,
                    f"crash_retry_{target['race_id']}",
                    target,
                )
                return {
                    "race_id": target["race_id"],
                    "timestamp": datetime.now().isoformat(),
                    "error": f"CRITICAL_FAILURE: {str(e2)}",
                    "scan_report": {"status": "FAILED"},
                }
        except HiddenDOMError as e:
            # [C] Hidden DOM: tab container present but CSS-hidden.
            # Recycle SPA context and retry once. No retry counter hit.
            logger.warning(
                f"Hidden DOM for {target['race_id']}: {e}. "
                "Cycling page and retrying once."
            )
            await self._save_diagnostics(
                self.page,
                f"hidden_dom_{target['race_id']}",
                target,
            )
            try:
                await self.cycle_page(warmup=True)
                return await self._scrape_race_robust(self.page, target)
            except Exception as e2:
                await self._save_diagnostics(
                    self.page,
                    f"crash_hidden_{target['race_id']}",
                    target,
                )
                return {
                    "race_id": target["race_id"],
                    "timestamp": datetime.now().isoformat(),
                    "error": f"CRITICAL_FAILURE_AFTER_HIDDEN_DOM: {e2}",
                    "scan_report": {"status": "FAILED"},
                }
        except Exception as e:
            await self._save_diagnostics(
                self.page, f"crash_{target['race_id']}", target
            )
            return {
                "race_id": target["race_id"],
                "timestamp": datetime.now().isoformat(),
                "error": f"CRITICAL_FAILURE: {str(e)}",
                "scan_report": {"status": "FAILED"},
            }

    async def _scrape_race_robust(
        self, page: Page, target: RaceTarget
    ) -> dict[str, Any]:
        race_id = target["race_id"]
        query_params = {
            "track_id": target["track_id"],
            "race_id": race_id,
            "r_date": target["r_date"],
            "races_ids": target["races_ids"],
            "tab": "card",
        }
        hash_path = f"card/{urlencode(query_params, safe=',')}"
        base_race_url = f"{BASE_URL}/#{hash_path}"

        try:
            await page.evaluate(
                "(hash) => { window.location.hash = hash; }",
                hash_path,
            )
            await page.wait_for_url(
                self._build_url_predicate(
                    "card", {"race_id": race_id, "tab": "card"}
                ),
                timeout=15000,
            )
            await page.locator("#raceTitleBox").first.wait_for(
                state="attached", timeout=15000
            )
            await page.locator(".runnerBlock").first.wait_for(
                state="visible", timeout=15000
            )
            await self._simulate_human_reading(page)

        except PlaywrightTimeoutError as e:
            raise StaleSPAError(f"Stale SPA DOM Timeout: {e}") from e
        except Exception as e:
            is_blocked = await self._check_soft_block(page, "Race Card Load")
            if is_blocked:
                raise ScraperBlockedError(
                    "Soft Block detected on Race Card"
                ) from e
            raise RuntimeError(f"Race Card Load Failure: {str(e)}") from e

        snapshots: dict[str, str] = {}
        scan_report: dict[str, str] = {}

        try:
            try:
                snapshots["header"] = await page.locator(
                    "#raceTitleBox"
                ).first.inner_html(timeout=5000)
            except PlaywrightTimeoutError:
                snapshots["header"] = ""

            # [D] r_time comes from discovery (target dict); no longer
            # extracted from #pagerCardTime which showed the next global
            # race time rather than this race's actual time.
            race_time = target.get("r_time", "00:00")

        except Exception as e:
            scan_report["meta_error"] = str(e)
            race_time = target.get("r_time", "00:00")

        snapshots["card"], scan_report["card"] = await self._switch_tab(
            page, base_race_url, "card", "#cardTab-card", race_id
        )
        snapshots["form"], scan_report["form"] = await self._switch_tab(
            page, base_race_url, "form", "#cardTab-form", race_id
        )
        snapshots["stats"], scan_report["stats"] = await self._switch_tab(
            page, base_race_url, "stats", "#cardTab-stats", race_id
        )
        snapshots["tips"], scan_report["tips"] = await self._switch_tab(
            page, base_race_url, "tips", "#cardTab-tips", race_id
        )

        # [C] Detect hidden DOM before harvesting dogs. If any quality
        # tab returned HIDDEN_DOM, raise HiddenDOMError so fetch_race_raw
        # can cycle and retry the whole race for free.
        hidden_tabs = [
            t
            for t in ("card", "form", "stats")
            if scan_report.get(t) == "HIDDEN_DOM"
        ]
        if hidden_tabs:
            raise HiddenDOMError(
                f"Hidden DOM on tabs {hidden_tabs} for race {race_id}"
            )

        # Return to card tab before harvesting dogs
        await self._switch_tab(
            page, base_race_url, "card", "#cardTab-card", race_id
        )

        dogs_data = await self._harvest_dogs(page, base_race_url, race_id)
        scan_report["dogs_count"] = str(len(dogs_data))

        return {
            "race_id": race_id,
            "track_id": target["track_id"],
            "r_date": target["r_date"],
            "race_time": race_time,
            "timestamp": datetime.now().isoformat(),
            "url": base_race_url,
            "html_snapshots": snapshots,
            "dogs": dogs_data,
            "scan_report": scan_report,
        }

    async def _check_soft_block(self, page: Page, context_msg: str) -> bool:
        """Returns True only when there is strong evidence of a block."""
        if any(a.get("status") in [403, 406] for a in self.network_anomalies):
            logger.error(
                "%s: Hard block (403/406) detected via network events.",
                context_msg,
            )
            return True

        if page.is_closed():
            return False

        try:
            content = await page.inner_text("body")
        except Exception:
            return False

        content_lower = content.lower()
        if any(
            kw in content_lower
            for kw in [
                "access denied",
                "captcha",
                "security check",
                "verify you are human",
            ]
        ):
            logger.error("%s: Soft block markers detected in DOM.", context_msg)
            return True

        return False

    async def _save_diagnostics(
        self,
        page: Page,
        prefix: str,
        target: RaceTarget | None = None,
    ) -> None:
        """Captures diagnostics, JS errors, and console logs on failure."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            base_name = f"{prefix}_{timestamp}"

            if not page.is_closed():
                await page.screenshot(path=f"{base_name}.png", full_page=True)
                html_content = await page.content()
                with open(f"{base_name}.html", "w", encoding="utf-8") as f:
                    f.write(html_content)

            safe_target = (
                {k: str(v) for k, v in target.items()} if target else None
            )
            meta_data = {
                "url": page.url if not page.is_closed() else "closed",
                "timestamp": datetime.now().isoformat(),
                "target": safe_target,
                "network_anomalies": self.network_anomalies,
                "page_errors": self.page_errors,
                "console_logs": self.console_logs[-50:],
            }
            with open(f"{base_name}.meta.json", "w", encoding="utf-8") as f:
                json.dump(meta_data, f, indent=2)

            logger.info(f"Saved diagnostics: {base_name}")
        except Exception as e:
            logger.error(f"Failed to save diagnostics: {e}")

    async def _simulate_human_reading(self, page: Page) -> None:
        """Simulates human presence: one scroll and a single variable pause.

        A WAF cares that pauses exist and vary — not that they're long.
        Reduced from two pauses (mu=1.2 + mu=0.4) to one (mu=0.6).
        """
        try:
            scroll_y = int(random.gauss(300, 80))
            await page.evaluate("(y) => { window.scrollBy(0, y); }", scroll_y)

            runners = page.locator(".runnerBlock")
            try:
                count = await runners.count()
                if count > 0:
                    await runners.nth(random.randint(0, count - 1)).hover(
                        timeout=2000
                    )
            except Exception:
                pass

            await self._human_pause(0.6, 0.15)

            await page.evaluate(
                "(y) => { window.scrollBy(0, y); }", -(scroll_y // 2)
            )
        except Exception:
            pass

    async def _snapshot_safe(
        self, page: Page, selector: str
    ) -> tuple[str, str]:
        """Extracts inner HTML. Uses try/except to avoid count() guards."""
        try:
            html = await page.locator(
                f"{selector}:not(.printContainer)"
            ).first.inner_html(timeout=3000)
            if len(html) > 50:
                return html, "OK"
        except Exception:
            pass

        try:
            return (
                await page.locator(selector).first.inner_html(timeout=3000),
                "OK_FALLBACK_1",
            )
        except Exception:
            pass

        try:
            return (
                await page.locator("#card-scroll").first.inner_html(
                    timeout=3000
                ),
                "OK_FALLBACK_WRAPPER",
            )
        except Exception:
            pass

        return "", "NOT_FOUND"

    async def _switch_tab(
        self,
        page: Page,
        base_url: str,
        tab_name: str,
        tab_selector: str,
        race_id: str,
    ) -> tuple[str, str]:
        """Navigate to a race card tab and snapshot its content.

        [C] Returns ``("", "HIDDEN_DOM")`` when Playwright confirms the
        container element exists in the DOM but is CSS-hidden (the log
        line ``locator resolved to hidden`` appears in the timeout
        message). This is distinct from ``FAILED``, which covers cases
        where the element is missing or the navigation itself failed.
        The caller (_scrape_race_robust) raises HiddenDOMError when any
        quality tab returns HIDDEN_DOM, triggering cycle + free retry.
        """
        try:
            tab_loc = page.locator(tab_selector).first
            await tab_loc.wait_for(state="attached", timeout=3000)
        except PlaywrightTimeoutError:
            return "SKIPPED_NO_TAB", "MISSING_BTN"

        unique_tab_selectors: dict[str, str] = {
            "card": ".runnerBlock",
            "form": ".formGrid",
            "stats": ".statsList",
            "tips": ".tipsGrid",
        }

        invariant_selector = unique_tab_selectors.get(
            tab_name, "#sortContainer"
        )
        snapshot_container = (
            "#sortContainer"
            if tab_name in ["card", "form"]
            else invariant_selector
        )

        try:
            await tab_loc.hover()
            await self._human_pause(0.12, 0.03)
            await tab_loc.click()

            try:
                await page.wait_for_url(
                    self._build_url_predicate(
                        "card",
                        {"race_id": race_id, "tab": tab_name},
                    ),
                    timeout=10000,
                )
            except Exception:
                new_hash = f"tab={tab_name}"
                full_hash = base_url.replace("tab=card", new_hash).split("#")[1]
                await page.evaluate(
                    "(hash) => { window.location.hash = hash; }",
                    full_hash,
                )
                await page.wait_for_url(
                    self._build_url_predicate(
                        "card",
                        {"race_id": race_id, "tab": tab_name},
                    ),
                    timeout=10000,
                )

            await page.locator(f"{tab_selector}.active").first.wait_for(
                state="visible", timeout=10000
            )

            # [C] wait_for(visible) on the invariant selector.
            # If it times out with "locator resolved to hidden",
            # the SPA rendered the shell but not the content —
            # signal HIDDEN_DOM for cycle + free retry.
            try:
                await page.locator(invariant_selector).first.wait_for(
                    state="visible", timeout=10000
                )
            except PlaywrightTimeoutError as te:
                if "locator resolved to hidden" in str(te):
                    logger.warning(
                        f"Hidden DOM on {tab_name} tab for race "
                        f"{race_id}: container present but invisible"
                    )
                    return "", "HIDDEN_DOM"
                raise

            return await self._snapshot_safe(page, snapshot_container)

        except PlaywrightTimeoutError as e:
            if "locator resolved to hidden" in str(e):
                logger.warning(
                    f"Hidden DOM (outer) on {tab_name} for {race_id}"
                )
                return "", "HIDDEN_DOM"
            return f"TAB_FAILED: {str(e)}", "FAILED"
        except Exception as e:
            return f"TAB_FAILED: {str(e)}", "FAILED"

    async def _build_runner_manifest(self, page: Page) -> list[dict[str, str]]:
        """Extracts runner data in a single JS evaluate call."""
        result: list[dict[str, str]] = await page.evaluate(
            """
            () => {
                const runners = [];
                document.querySelectorAll('.runnerBlock').forEach(el => {
                    const link = el.querySelector('a.gh');
                    if (!link) return;
                    const nEl = link.querySelector('strong');
                    const name = nEl ? nEl.innerText.trim() : '';
                    const href = link.getAttribute('href') || '';
                    let dog_id = '';
                    const m = href.match(/dog_id=([^&]+)/);
                    if (m) dog_id = m[1];
                    let trap = '0';
                    const tIcon = link.querySelector('i[class*="trap"]');
                    if (tIcon) {
                        const cls = (
                            tIcon.getAttribute('class') || ''
                        ).split(' ');
                        for (const c of cls) {
                            if (c.startsWith('trap') &&
                                /^trap\\d+$/.test(c)) {
                                trap = c.slice(4);
                                break;
                            }
                        }
                    }
                    if (name) runners.push({ trap, name, dog_id });
                });
                return runners;
            }
            """
        )
        return result

    def _is_valid_dog_html(self, html: str) -> bool:
        if not html or len(html) < 50:
            return False
        s = html.lstrip().lower()
        if not s.startswith("<div"):
            return False
        return not s.startswith("<!doctype")

    async def _harvest_dogs(
        self, page: Page, reset_url: str, race_id: str
    ) -> list[dict[str, Any]]:
        manifest = await self._build_runner_manifest(page)
        dogs_result: list[dict[str, Any]] = []

        for runner in manifest:
            trap = runner["trap"]
            dog_id = runner["dog_id"]
            name = runner["name"]

            dog_data: dict[str, Any] = {
                "trap": int(trap) if trap.isdigit() else 0,
                "dog_id": dog_id,
                "name": name,
            }

            success = False
            error_log = ""

            for attempt in range(3):
                try:
                    runner_selector = f"a.gh[href*='dog_id={dog_id}']"
                    locator = page.locator(runner_selector).first

                    try:
                        await locator.wait_for(state="attached", timeout=5000)
                        await locator.scroll_into_view_if_needed(timeout=5000)
                    except Exception as err:
                        raise Exception(
                            f"Link dog {dog_id} not found {attempt}"
                        ) from err

                    await locator.hover()
                    await self._human_pause(0.12, 0.03)
                    await locator.click()

                    try:
                        await page.wait_for_url(
                            self._build_url_predicate(
                                "dog", {"dog_id": dog_id}
                            ),
                            timeout=15000,
                        )
                    except Exception:
                        clean_name = name.split("(")[0].strip()
                        await page.locator(
                            f".ghName:has-text('{clean_name}')"
                        ).first.wait_for(state="visible", timeout=15000)

                    await page.locator(".ghName").first.wait_for(
                        state="visible", timeout=15000
                    )
                    await self._simulate_human_reading(page)

                    html = ""
                    try:
                        html = await page.locator(
                            "#dog-scroll"
                        ).first.inner_html(timeout=5000)
                    except PlaywrightTimeoutError:
                        html = await page.content()

                    if not self._is_valid_dog_html(html):
                        raise ValueError("HTML failed validation")

                    dog_data["html"] = html

                    navigated_back = False
                    try:
                        back_btn = page.locator(
                            "a[data-eventid='cards_back_to_card']"
                        ).first
                        await back_btn.wait_for(state="visible", timeout=2000)
                        await back_btn.hover()
                        await self._human_pause(0.12, 0.03)
                        await back_btn.click()
                        navigated_back = True
                    except PlaywrightTimeoutError:
                        pass

                    if not navigated_back:
                        hash_path = (
                            reset_url.split("#", 1)[1]
                            if "#" in reset_url
                            else reset_url
                        )
                        await page.evaluate(
                            "(hash) => { window.location.hash = hash; }",
                            hash_path,
                        )

                    await page.wait_for_url(
                        self._build_url_predicate(
                            "card",
                            {"race_id": race_id, "tab": "card"},
                        ),
                        timeout=15000,
                    )
                    await page.locator(".runnerBlock").first.wait_for(
                        state="visible", timeout=15000
                    )

                    success = True
                    break

                except Exception as e:
                    error_log = str(e)
                    logger.warning(
                        f"Retry {attempt + 1}/3 Dog {dog_id}: {error_log}"
                    )

                    is_blocked = await self._check_soft_block(
                        page, "Dog Parsing"
                    )
                    if is_blocked:
                        raise ScraperBlockedError(  # noqa: B904
                            "Soft Block detected"
                        )

                    try:
                        hash_path = (
                            reset_url.split("#", 1)[1]
                            if "#" in reset_url
                            else reset_url
                        )
                        await page.evaluate(
                            "(hash) => { window.location.hash = hash; }",
                            hash_path,
                        )
                        await page.wait_for_url(
                            self._build_url_predicate(
                                "card",
                                {"race_id": race_id, "tab": "card"},
                            ),
                            timeout=15000,
                        )
                        await page.locator(".runnerBlock").first.wait_for(
                            state="visible", timeout=20000
                        )
                        await self._human_pause(2.0, 0.0)
                    except Exception as err2:
                        logger.debug(f"Dog recovery flow failed: {err2}")

            if not success:
                dog_data["error"] = f"Failed after 3 attempts: {error_log}"

            dogs_result.append(dog_data)

        return dogs_result
