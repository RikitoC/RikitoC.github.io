from __future__ import annotations

from typing import Dict, Any, Optional
import pandas as pd
import requests
from bs4 import BeautifulSoup

FLAG_URL = "https://emerald.puzzlepirates.com/yoweb/flag/info.wm?flagid=10007105"
BASE = "https://emerald.puzzlepirates.com"

def _find_crews_table(soup: BeautifulSoup) -> Optional[Any]:
    """
    Try to locate the crews table by looking for a header row
    that contains the expected columns.
    """
    expected = ["Crew", "Rank", "Members", "Fame"]
    for table in soup.find_all("table"):
        # grab first row headers-ish text
        header_cells = table.find_all("th")
        if not header_cells:
            # sometimes headers are in td
            first_tr = table.find("tr")
            header_cells = first_tr.find_all("td") if first_tr else []
        header_text = " ".join(c.get_text(" ", strip=True) for c in header_cells)

        # weak-but-effective match
        if all(word.lower() in header_text.lower() for word in expected):
            return table
    return None

def run(ctx) -> Dict[str, Any]:
    r = requests.get(
        FLAG_URL,
        timeout=30,
        headers={"User-Agent": "Mozilla/5.0 (compatible; GitHubActionsScraper/1.0)"}
    )
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    table = _find_crews_table(soup)

    if table is None:
        # Fallback: keep your old behavior if we can’t detect headers
        tables = soup.find_all("table")
        if len(tables) > 10:
            table = tables[10]
        else:
            raise RuntimeError("Could not find crews table (not enough <table> elements).")

    rows = []
    for tr in table.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if len(tds) < 4:
            continue

        crew_link = tds[0].find("a")
        if not crew_link or not crew_link.get("href"):
            continue

        crew_name = crew_link.get_text(strip=True)
        crew_url = crew_link["href"]
        if crew_url.startswith("/"):
            crew_url = BASE + crew_url

        rank = tds[1].get_text(strip=True)
        members = tds[2].get_text(strip=True)
        fame = tds[3].get_text(strip=True)

        rows.append({
            "Crew Name": crew_name,
            "Crew URL": crew_url,
            "Rank": rank,
            "Members": members,
            "Fame": fame,
        })

    df = pd.DataFrame(rows, columns=["Crew Name", "Crew URL", "Rank", "Members", "Fame"])

    return {
        "crews_df": df,
        "meta": {
            "flag_url": FLAG_URL,
            "rows": int(len(df)),
        }
    }