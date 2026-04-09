# filepath: libs/g2f_core/services/acl/parse_result_html.py
"""
Purpose: HTML parser for Bronze V2 result pages.
Usage:   Called by BronzeResultAdapter to extract structured
         data from raw result HTML snapshots.

Contract: Returns plain dicts of raw strings. Type coercion
is the responsibility of Pydantic domain models and the adapter.
"""

from __future__ import annotations

import re
from typing import Any

from bs4 import BeautifulSoup, Tag


def parse_result_page(
    result_html: str,
    header_html: str = "",
) -> dict[str, Any]:
    """Parse a complete result page into structured data.

    Args:
        result_html: The ``.meetingResultsList`` outer HTML.
        header_html: The header area HTML (contains raceTitle
            with grade, distance, going, and track name).

    Returns:
        {
            "race_meta": { ... },
            "placements": [ { ... }, ... ],
        }
    """
    data: dict[str, Any] = {
        "race_meta": {},
        "placements": [],
    }

    if header_html:
        data["race_meta"] = _parse_race_meta(header_html)

    if not result_html:
        return data

    soup = BeautifulSoup(result_html, "lxml")

    for container in soup.find_all("div", class_="container"):
        placement = _parse_placement(container)
        if placement:
            data["placements"].append(placement)

    comments_div = soup.find("div", class_="commentsContainer")
    if comments_div:
        _parse_comments_container(comments_div, data["race_meta"])

    return data


def _parse_race_meta(
    header_html: str,
) -> dict[str, Any]:
    """Extract race metadata from the header area."""
    meta: dict[str, Any] = {}
    soup = BeautifulSoup(header_html, "lxml")

    rtitle = soup.find("span", class_="rTitle")
    if rtitle:
        title_text = rtitle.get_text(strip=True)
        # Strip trailing " DD/MM/YY" date suffix to get the track name.
        # Splitting on the last space is unreliable for multi-word names
        # like "Dunstall Park" — the date suffix is always \d{2}/\d{2}/\d{2}.
        meta["track_name"] = re.sub(
            r"\s+\d{2}/\d{2}/\d{2}$", "", title_text
        ).strip()

    status_span = soup.find(
        "span",
        attrs={"data-eventid": "results_title_toggle"},
    )
    if not status_span:
        status_box = soup.find("div", class_="statusBox")
        if status_box:
            status_span = status_box.find("span")

    if status_span:
        race_head = status_span.get_text(strip=True)
        meta["race_head"] = race_head
        _parse_race_head_fields(race_head, meta)

    return meta


def _parse_race_head_fields(race_head: str, meta: dict[str, Any]) -> None:
    """Parse structured fields from race head string.

    Example: "Race 1 £125 (A8) 450m Going: N"
    """
    grade_m = re.search(r"\(([A-Z0-9a-z]+)\)", race_head)
    if grade_m:
        meta["grade"] = grade_m.group(1)

    dist_m = re.search(r"(\d+)m", race_head)
    if dist_m:
        meta["distance_meters"] = int(dist_m.group(1))

    prize_m = re.search(r"£([\d,]+)", race_head)
    if prize_m:
        meta["prize"] = f"£{prize_m.group(1)}"

    going_m = re.search(r"Going:\s*(.+)$", race_head)
    if going_m:
        going_raw = going_m.group(1).strip()
        meta["going_raw"] = going_raw
        meta["going_allowance"] = _parse_going(going_raw)


def _parse_going(going_raw: str) -> int | None:
    """Convert going string to integer allowance.

    "+15" → 15, "-10" → -10, "N" → 0
    """
    if not going_raw:
        return None
    going_raw = going_raw.strip()

    num_m = re.match(r"^([+-]?\d+)$", going_raw)
    if num_m:
        return int(num_m.group(1))

    cat_map = {"n": 0, "normal": 0, "std": 0}
    if going_raw.lower() in cat_map:
        return cat_map[going_raw.lower()]

    return None


def _parse_placement(
    container: Tag,
) -> dict[str, Any] | None:
    """Parse one dog's result from a .container div."""
    link = container.find("a", class_="details")
    if not link:
        return None

    href = link.get("href", "")
    if isinstance(href, list):
        href = href[0]
    dog_id = ""
    dog_id_m = re.search(r"dog_id=(\d+)", str(href))
    if dog_id_m:
        dog_id = dog_id_m.group(1)
    if not dog_id:
        return None

    result_div = container.find("div", class_="result")
    if not result_div:
        return None

    # Finish position
    place_div = result_div.find("div", class_="place")
    finish_pos = 0
    if place_div:
        place_text = place_div.get_text(strip=True)
        pos_m = re.match(r"(\d+)", place_text)
        if pos_m:
            finish_pos = int(pos_m.group(1))

    # Trap — the element carries two separate CSS classes: "bigTrap" and
    # "trap4". BeautifulSoup's class_ regex matches individual tokens, so
    # we search for the "trapN" token, not the combined class string.
    trap = 0
    trap_div = result_div.find("div", class_=re.compile(r"^trap\d$"))
    if trap_div:
        classes = trap_div.get("class") or []  # type: ignore
        for cls in classes:
            trap_m = re.match(r"^trap(\d)$", cls)
            if trap_m:
                trap = int(trap_m.group(1))
                break

    # Dog name
    name_div = result_div.find("div", class_="name")
    dog_name = name_div.get_text(strip=True) if name_div else ""

    # Dog identity
    details_div = result_div.find("div", class_="dog-result-details")
    dog_color = ""
    dog_sex = ""
    dog_sire_dam = ""
    dog_dob = ""
    if details_div:
        color_span = details_div.find("span", class_="dog-color")
        if color_span:
            dog_color = color_span.get_text(strip=True)
        sex_span = details_div.find("span", class_="dog-sex")
        if sex_span:
            dog_sex = sex_span.get_text(strip=True)
        sd_span = details_div.find("span", class_="dog-sire-dam")
        if sd_span:
            dog_sire_dam = sd_span.get_text(strip=True)
        dob_span = details_div.find("span", class_="dog-date-of-birth")
        if dob_span:
            dog_dob = dob_span.get_text(strip=True)

    # Time (col1), odds (col2), trainer (col3)
    time_raw = ""
    starting_price = ""
    trainer_name = ""

    cols = result_div.find_all("div", class_="col")
    for col in cols:
        classes = col.get("class")  # type: ignore
        if not isinstance(classes, list):
            continue
        if "col1" in classes:
            time_raw = col.get_text(strip=True).replace("\xa0", "")
        elif "col2" in classes:
            starting_price = col.get_text(strip=True)
        elif "col3" in classes:
            # DOM has two spans: "Trainer:" and "T:"
            # get_text produces "Trainer:T: Y Bell"
            # Strip both labels via replace chain
            raw = col.get_text(strip=True)
            raw = raw.replace("Trainer:", "").replace("T:", "").strip()
            trainer_name = raw

    # Comment and sectional time
    result_comment = ""
    sectional_time = None
    comment_p = result_div.find("p", class_="comment")
    if comment_p:
        raw_comment = comment_p.get_text(strip=True)
        sec_m = re.match(r"\((\d+\.\d+)\)\s*(.*)", raw_comment)
        if sec_m:
            sectional_time = float(sec_m.group(1))
            result_comment = sec_m.group(2).strip()
        else:
            result_comment = raw_comment

    return {
        "finish_position": finish_pos,
        "dog_id": dog_id,
        "dog_name": dog_name,
        "trap": trap,
        "time_raw": time_raw,
        "starting_price": starting_price,
        "trainer_name": trainer_name,
        "result_comment": result_comment,
        "sectional_time": sectional_time,
        "dog_color": dog_color,
        "dog_sex": dog_sex,
        "dog_sire_dam": dog_sire_dam,
        "dog_dob": dog_dob,
    }


def _parse_comments_container(div: Tag, meta: dict[str, Any]) -> None:
    """Extract F/C, T/C, Total SP% from comments div."""
    for child in div.find_all("div"):
        text = child.get_text(strip=True)
        if text.startswith("F/C:"):
            meta["forecast"] = text[4:].strip()
        elif text.startswith("T/C:"):
            meta["tricast"] = text[4:].strip()

    sp_div = div.find("div", class_="col-sp")
    if sp_div:
        sp_text = sp_div.get_text(strip=True)
        sp_m = re.search(r"([\d.]+)", sp_text)
        if sp_m:
            meta["total_sp_pct"] = sp_m.group(1)


def parse_results_list(html: str) -> list[dict[str, str]]:
    """Parse the results-list page for race discovery.

    Returns list of race dicts with race_id, track_id,
    r_date, r_time.
    """
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    races: list[dict[str, str]] = []
    seen: set[str] = set()

    for link in soup.find_all("a", href=True):
        href = str(link.get("href", ""))
        if "result-meeting-result/" not in href:
            continue

        race_id_m = re.search(r"race_id=(\d+)", href)
        if not race_id_m:
            continue
        race_id = race_id_m.group(1)
        if race_id in seen:
            continue
        seen.add(race_id)

        track_id = ""
        tid_m = re.search(r"track_id=(\d+)", href)
        if tid_m:
            track_id = tid_m.group(1)

        r_date = ""
        date_m = re.search(r"r_date=([\d-]+)", href)
        if date_m:
            r_date = date_m.group(1)

        r_time = ""
        time_m = re.search(r"r_time=([\d:]+)", href)
        if time_m:
            r_time = time_m.group(1)

        races.append(
            {
                "race_id": race_id,
                "track_id": track_id,
                "r_date": r_date,
                "r_time": r_time,
            }
        )

    return races
