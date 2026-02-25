from __future__ import annotations

from typing import Dict, Any, List, Tuple, Optional
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup


USER_AGENT = "Mozilla/5.0 (compatible; GitHubActionsScraper/1.0)"
REQUEST_TIMEOUT = 30
SLEEP_SECONDS = 1.0  # be kind


def _extract_crew_name(center_cell: Any) -> str:
    name_tag = center_cell.find("font", attrs={"size": "+2"})
    if not name_tag:
        raise ValueError("Crew name font tag not found.")
    b = name_tag.find("b")
    if not b:
        raise ValueError("Crew name <b> not found.")
    return b.get_text(strip=True)


def _extract_public_statement(center_cell: Any) -> str:
    """
    Your original logic: find <p align="left">, take stripped lines,
    and skip the first line if it looks like a label.
    """
    p_tag = center_cell.find("p", attrs={"align": "left"})
    if not p_tag:
        return ""

    lines = list(p_tag.stripped_strings)
    if not lines:
        return ""

    # Often first line is a label like "Public statement:" — skip it if multiple lines exist.
    if len(lines) >= 2:
        return " ".join(lines[1:])
    return lines[0]


def _extract_captain(soup: BeautifulSoup) -> str:
    """
    Your original approach: look for a table that contains the captain icon,
    then follow to the next row and grab the first <a>.
    """
    for table in soup.find_all("table"):
        if table.find("img", {"src": "/yoweb/images/crew-captain.png"}):
            parent_tr = table.find_parent("tr")
            if not parent_tr:
                continue
            next_tr = parent_tr.find_next_sibling("tr")
            if not next_tr:
                continue
            captain_link = next_tr.find("a")
            if captain_link:
                return captain_link.get_text(strip=True)
    return ""


def _scrape_one(crew_url: str, session: requests.Session) -> Dict[str, str]:
    r = session.get(
        crew_url,
        timeout=REQUEST_TIMEOUT,
        headers={"User-Agent": USER_AGENT},
    )
    if r.status_code != 200:
        raise ValueError(f"HTTP Error: {r.status_code}")

    soup = BeautifulSoup(r.text, "html.parser")

    tables = soup.find_all("table")
    if len(tables) < 2:
        raise ValueError("Expected at least 2 tables on the page.")

    info_table = tables[1]
    center_cell = info_table.find("td", attrs={"align": "center"})
    if not center_cell:
        raise ValueError("Center cell not found in second table.")

    crew_name = _extract_crew_name(center_cell)
    public_statement = _extract_public_statement(center_cell)
    captain = _extract_captain(soup)

    return {
        "Crew Name": crew_name,
        "Public Statement": public_statement,
        "Captain": captain,
        "Crew URL": crew_url,
    }


def run(ctx) -> Dict[str, Any]:
    # Pull Crew URLs from previous stage instead of Google Sheets
    crews_df: pd.DataFrame = ctx.data["crews"]["crews_df"]
    if "Crew URL" not in crews_df.columns:
        raise RuntimeError("crews_df missing required column: 'Crew URL'")

    crew_urls = (
        crews_df["Crew URL"]
        .dropna()
        .astype(str)
        .map(str.strip)
    )
    crew_urls = crew_urls[crew_urls != ""].unique().tolist()

    crew_data: List[Dict[str, str]] = []
    failures: List[Dict[str, str]] = []

    session = requests.Session()

    for i, crew_url in enumerate(crew_urls, start=1):
        try:
            row = _scrape_one(crew_url, session)
            crew_data.append(row)
            print(f"✅ ({i}/{len(crew_urls)}) Scraped: {row['Crew Name']}", flush=True)
        except Exception as e:
            failures.append({
                "Crew URL": crew_url,
                "Error Type": type(e).__name__,
                "Message": str(e),
            })
            print(f"❌ ({i}/{len(crew_urls)}) Failed: {crew_url} - {type(e).__name__}: {e}", flush=True)

        time.sleep(SLEEP_SECONDS)

    crew_details_df = pd.DataFrame(
        crew_data,
        columns=["Crew Name", "Public Statement", "Captain", "Crew URL"]
    )

    failures_df = pd.DataFrame(
        failures,
        columns=["Crew URL", "Error Type", "Message"]
    )

    return {
        "crew_details_df": crew_details_df,
        "crew_failures_df": failures_df,
        "meta": {
            "input_urls": int(len(crew_urls)),
            "success": int(len(crew_details_df)),
            "failures": int(len(failures_df)),
            "sleep_seconds": SLEEP_SECONDS,
        }
    }