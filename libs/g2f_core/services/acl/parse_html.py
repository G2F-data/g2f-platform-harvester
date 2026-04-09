# filepath: libs/g2f_core/services/acl/parse_html.py
"""
Purpose: HTML parsers for the Bronze V2 ACL layer.
Usage:   Called by BronzeV2Adapter to extract structured data from
         raw HTML snapshots stored in Bronze JSON files.

Contract

All functions return plain dicts of raw strings. Type coercion
(int, float, date) is the responsibility of Pydantic domain models,
not these parsers. This keeps the ACL boundary clean and testable.
"""

from __future__ import annotations

import re
from typing import Any

from bs4 import BeautifulSoup, Tag

# ── Helpers ──


def _strip_label(text: str, label: str) -> str:
    """Remove a leading label word from a text string.

    Handles both "Sire BallymacAnton" (no space) and
    "Sire Ballymac Anton" (with space) cases.
    """
    return re.sub(
        rf"^{re.escape(label)}\s*", "", text, flags=re.IGNORECASE
    ).strip()


def _parse_distance(raw: str) -> int:
    """Convert a distance string to metres. Returns 0 on failure."""
    if not raw:
        return 0
    m = re.search(r"(\d+)", raw)
    if not m:
        return 0
    d = int(m.group(1))
    return round(d * 0.9144) if "y" in raw.lower() else d


def _parse_race_meta(title_text: str) -> dict[str, Any]:
    """Extract grade, distance_meters, race_type from title string.

    Example input: "A4 - 503m Flat"
    """
    meta: dict[str, Any] = {
        "grade": "",
        "distance_meters": 0,
        "race_type": "Flat",
    }
    parts = title_text.split("-")
    if parts:
        meta["grade"] = parts[0].strip()
    if len(parts) >= 2:
        rest = parts[1].strip()
        dist_m = re.search(r"(\d+[my]?)", rest, re.IGNORECASE)
        if dist_m:
            meta["distance_meters"] = _parse_distance(dist_m.group(1))
        for rt in ("Hurdle", "Chase", "Flat"):
            if rt.lower() in rest.lower():
                meta["race_type"] = rt
                break
    return meta


# ── Header HTML ──


def parse_header_html(html: str) -> dict[str, Any]:
    """Parse the header HTML snapshot for top-level race metadata.

    This targets the #title-circle-container holding distance and
    grade, which resides in html_snapshots["header"], not in the
    card snapshot.
    """
    if not html:
        return {}

    soup = BeautifulSoup(html, "lxml")
    title_tag = soup.find(class_="titleColumn2")
    if title_tag:
        return _parse_race_meta(title_tag.get_text(strip=True))

    return {}


# ── Card HTML ──


def parse_card_html(html: str) -> dict[str, Any]:
    """Parse the race card HTML snapshot.

    Returns:
        {
            "entries": [
                {
                    "trap": int,
                    "dog_id": str,
                    "dog_name": str,
                    "trainer": str,
                    "form_string": str,
                    "sp_forecast": str,
                    "topspeed": str,
                    "comment": str,
                }
            ],
            "race_meta": { ... fallback ... },
        }
    """
    if not html:
        return {"entries": [], "race_meta": {}}

    soup = BeautifulSoup(html, "lxml")
    result: dict[str, Any] = {"entries": [], "race_meta": {}}

    # Fallback in case the header wasn't provided but is embedded
    title_tag = soup.find(class_="titleColumn2")
    if title_tag:
        result["race_meta"] = _parse_race_meta(title_tag.get_text(strip=True))

    entries: list[dict[str, Any]] = []
    for block in soup.find_all(class_="runnerBlock"):
        entry = _parse_runner_block(block)
        if entry:
            entries.append(entry)
    result["entries"] = entries

    return result


def _parse_runner_block(block: Tag) -> dict[str, Any] | None:
    """Extract one runner's pre-race fields from a runnerBlock div."""
    trap = 0
    dog_id = ""
    dog_name = ""

    link = block.find("a", class_="gh")
    if link:
        href_raw = link.get("href")
        if isinstance(href_raw, str):
            m = re.search(r"dog_id=(\d+)", href_raw)
            if m:
                dog_id = m.group(1)

        trap_icon = block.find(class_=re.compile(r"^bigTrap trap\d+$"))
        if trap_icon:
            raw_classes = trap_icon.get("class")
            if isinstance(raw_classes, list):
                for cls in raw_classes:
                    m2 = re.match(r"trap(\d+)", cls)
                    if m2:
                        trap = int(m2.group(1))
                        break

        strong = link.find("strong")
        if strong:
            dog_name = strong.get_text(strip=True)

    if trap == 0 or not dog_id:
        return None

    trainer = ""
    form_string = ""
    sp_forecast = ""
    topspeed = ""
    comment = ""

    info_div = block.find(class_="info")
    if info_div:
        comment_tag = info_div.find(class_="comment")
        if comment_tag:
            comment = comment_tag.get_text(strip=True)

        for row in info_div.find_all("tr"):
            for cell in row.find_all("td"):
                em = cell.find("em")
                if not em:
                    continue
                label = em.get_text(strip=True).rstrip(":").lower()
                val = cell.get_text(strip=True)[
                    len(em.get_text(strip=True)) :
                ].strip()
                if label == "form":
                    form_string = val
                elif label == "tnr":
                    trainer = val
                elif label == "sp forecast":
                    sp_forecast = val
                elif label == "topspeed":
                    topspeed = val if val != "\u2014" else ""

    return {
        "trap": trap,
        "dog_id": dog_id,
        "dog_name": dog_name,
        "trainer": trainer,
        "form_string": form_string,
        "sp_forecast": sp_forecast,
        "topspeed": topspeed,
        "comment": comment,
    }


# ── Dog HTML ──


def parse_dog_html(html: str) -> dict[str, Any]:
    """Parse a dog's profile HTML snapshot.

    Returns:
        {
            "identity": {
                "name": str, "sire": str, "dam": str,
                "sex": str, "color": str, "dob": str,
            },
            "past_runs": [ { ... PastRun-compatible keys ... } ],
        }
    """
    if not html:
        return {"identity": {}, "past_runs": []}

    soup = BeautifulSoup(html, "lxml")
    result: dict[str, Any] = {"identity": {}, "past_runs": []}

    name_tag = soup.find("h1", class_="ghName")
    if name_tag:
        result["identity"]["name"] = name_tag.get_text(strip=True)

    pedigree = soup.find("table", class_="pedigree")
    if pedigree:
        rows = pedigree.find_all("tr")
        if rows:
            cells_0 = rows[0].find_all("td")
            if len(cells_0) >= 1:
                raw_sire = cells_0[0].get_text(strip=True)
                result["identity"]["sire"] = _strip_label(raw_sire, "Sire")
            if len(cells_0) >= 2:
                raw_dam = cells_0[1].get_text(strip=True)
                result["identity"]["dam"] = _strip_label(raw_dam, "Dam")
        if len(rows) >= 2:
            cells_1 = rows[1].find_all("td")
            if len(cells_1) >= 1:
                sex_str = cells_1[0].get_text(strip=True)
                result["identity"]["sex"] = sex_str
                parts = sex_str.split()
                if len(parts) >= 2:
                    result["identity"]["color"] = parts[0]
            if len(cells_1) >= 2:
                result["identity"]["dob"] = cells_1[1].get_text(strip=True)

    table = soup.find("table", id="sortableTable")
    if not table:
        table = soup.find("table", class_="pastRaces")
    if table:
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        for row in table.find_all("tr")[1:]:
            cells = row.find_all("td")
            if not cells:
                continue
            raw_row: dict[str, str] = {}
            for i, cell in enumerate(cells):
                if i < len(headers):
                    raw_row[headers[i]] = cell.get_text(strip=True)
                    if headers[i] == "Date":
                        lnk = cell.find("a")
                        if lnk:
                            href_raw = lnk.get("href")
                            if isinstance(href_raw, str):
                                m = re.search(r"race_id=(\d+)", href_raw)
                                if m:
                                    raw_row["date_race_id"] = m.group(1)
                    if headers[i] == "Win/Sec":
                        lnk = cell.find("a")
                        if lnk:
                            href_raw = lnk.get("href")
                            if isinstance(href_raw, str):
                                m2 = re.search(r"dog_id=(\d+)", href_raw)
                                if m2:
                                    raw_row["Win/Sec_id"] = m2.group(1)
            normalised = _normalise_past_run(raw_row)
            if normalised:
                result["past_runs"].append(normalised)

    return result


def _normalise_past_run(raw: dict[str, str]) -> dict[str, Any]:
    """Remap raw HTML column names to PastRun-compatible field names."""
    fin = raw.get("Fin", "")
    return {
        "run_date": raw.get("Date", ""),
        "track_short": raw.get("Track", ""),
        "grade": raw.get("Grade", ""),
        "distance_meters": raw.get("Dis", ""),
        "trap": raw.get("Trp", ""),
        "finish_pos": fin,
        "finish_pos_raw": fin,
        "distance_beaten": raw.get("By", ""),
        "win_time": raw.get("WnTm", ""),
        "calc_time": raw.get("CalTm", ""),
        "split_time": raw.get("Split", ""),
        "going": raw.get("Gng", ""),
        "remarks": raw.get("Remarks", ""),
        "competitor_name": raw.get("Win/Sec", ""),
        "competitor_id": raw.get("Win/Sec_id", ""),
        "date_race_id": raw.get("date_race_id", ""),
        "sp": raw.get("SP", ""),
        "weight_kg": raw.get("Wght", ""),
        "bends": raw.get("Bends", ""),
    }


# ── Stats HTML ──


def parse_stats_html(html: str) -> dict[str, Any]:
    """Parse the stats tab HTML snapshot."""
    if not html:
        return {}

    soup = BeautifulSoup(html, "lxml")
    result: dict[str, Any] = {}

    for item in soup.find_all("li"):
        h4 = item.find("h4")
        if not h4:
            continue
        title = h4.get_text(strip=True)
        p_tag = item.find("p")
        comment = p_tag.get_text(strip=True) if p_tag else ""
        stat: dict[str, Any] = {"comment": comment}
        for li in item.find_all("li"):
            trap_icon = li.find("i", class_=re.compile(r"trap\d+"))
            strong = li.find("strong")
            if trap_icon and strong:
                raw_classes = trap_icon.get("class")
                if isinstance(raw_classes, list):
                    for cls in raw_classes:
                        m = re.match(r"trap(\d+)", cls)
                        if m:
                            stat[f"trap{m.group(1)}"] = strong.get_text(
                                strip=True
                            )
        result[title] = stat

    return result


# ── Tips HTML ──


def parse_tips_html(html: str) -> dict[str, Any]:
    """Parse the tips tab HTML snapshot."""
    if not html:
        return {}

    soup = BeautifulSoup(html, "lxml")
    result: dict[str, Any] = {}

    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        selection_text = cells[1].get_text(strip=True)
        if not selection_text or selection_text.lower() == "selection":
            continue
        if "other races" in selection_text.lower():
            continue

        result["SELECTION"] = selection_text

        if len(cells) >= 3:
            strength_div = cells[2].find("div", class_=re.compile(r"star"))
            if strength_div:
                raw_classes = strength_div.get("class")
                if isinstance(raw_classes, list):
                    for cls in raw_classes:
                        if cls.startswith("star"):
                            result["STRENGTH"] = cls
                            break

        if len(cells) >= 4:
            traps: list[str] = []
            for icon in cells[3].find_all("i", class_=re.compile(r"trap\d+")):
                raw_classes = icon.get("class")
                if isinstance(raw_classes, list):
                    for cls in raw_classes:
                        if re.match(r"trap\d+", cls):
                            traps.append(cls)
                            break
            if traps:
                result["1st,2nd,3rd"] = traps

        break

    return result
