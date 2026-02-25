from __future__ import annotations

from typing import Dict, Any, List, Tuple
import time
import urllib.parse

import pandas as pd
import requests
from bs4 import BeautifulSoup


BASE = "https://emerald.puzzlepirates.com"
USER_AGENT = "Mozilla/5.0 (compatible; GitHubActionsScraper/1.0)"
REQUEST_TIMEOUT = 30
SLEEP_SECONDS = 1.0  # be kind


def _get_crew_name(soup: BeautifulSoup) -> str:
    tag = soup.select_one('font[size="+2"] > b')
    return tag.get_text(strip=True) if tag else "Unknown"


def _is_pirate_link(href: str) -> bool:
    return "/yoweb/pirate.wm" in href and "target=" in href


def _make_absolute(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return BASE + href
    return BASE + "/" + href


def _scrape_one_crew(crew_url: str, session: requests.Session) -> Tuple[str, List[Dict[str, str]]]:
    r = session.get(
        crew_url,
        timeout=REQUEST_TIMEOUT,
        headers={"User-Agent": USER_AGENT},
    )
    if r.status_code != 200:
        raise ValueError(f"HTTP Error: {r.status_code}")

    soup = BeautifulSoup(r.text, "html.parser")
    crew_name = _get_crew_name(soup)

    # Find "jobbing pirates" marker image (used as a cutoff)
    jobbing_img = soup.find("img", {"src": "/yoweb/images/crew-jobbing.png"})

    pirate_rows: List[Dict[str, str]] = []
    jobbing_reached = False

    # Strategy:
    # Walk through the document in order, but only consider pirate links.
    # Once we hit the jobbing marker, stop collecting.
    for el in soup.body.descendants if soup.body else []:
        if jobbing_img and el == jobbing_img:
            jobbing_reached = True

        if jobbing_reached:
            continue

        if getattr(el, "name", None) == "a" and el.has_attr("href"):
            href = el["href"]
            if _is_pirate_link(href):
                pirate_name = el.get_text(strip=True)
                pirate_url = _make_absolute(href)

                # (Optional) normalize URL (keeps it stable)
                # You can remove this if you want the exact href.
                parsed = urllib.parse.urlsplit(pirate_url)
                pirate_url = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))

                pirate_rows.append({
                    "Pirate URL": pirate_url,
                    "Pirate Name": pirate_name,
                    "Crew Name": crew_name,
                    "Crew URL": crew_url,
                })

    # Deduplicate (crew pages can contain repeats)
    if pirate_rows:
        seen = set()
        deduped = []
        for row in pirate_rows:
            key = (row["Pirate URL"], row["Pirate Name"], row["Crew Name"])
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        pirate_rows = deduped

    return crew_name, pirate_rows


def run(ctx) -> Dict[str, Any]:
    # Prefer the crew URLs we already scraped from the flag page
    crews_df: pd.DataFrame = ctx.data["crews"]["crews_df"]
    if "Crew URL" not in crews_df.columns:
        raise RuntimeError("crews_df missing required column: 'Crew URL'")

    crew_urls = (
        crews_df["Crew URL"].dropna().astype(str).map(str.strip)
    )
    crew_urls = crew_urls[crew_urls != ""].unique().tolist()

    session = requests.Session()

    all_rows: List[Dict[str, str]] = []
    failures: List[Dict[str, str]] = []

    for i, crew_url in enumerate(crew_urls, start=1):
        try:
            crew_name, rows = _scrape_one_crew(crew_url, session)
            all_rows.extend(rows)
            print(f"✅ ({i}/{len(crew_urls)}) Crew {crew_name}: +{len(rows)} pirates", flush=True)
        except Exception as e:
            failures.append({
                "Crew URL": crew_url,
                "Error Type": type(e).__name__,
                "Message": str(e),
            })
            print(f"❌ ({i}/{len(crew_urls)}) Failed: {crew_url} - {type(e).__name__}: {e}", flush=True)

        time.sleep(SLEEP_SECONDS)

    pirate_urls_df = pd.DataFrame(
        all_rows,
        columns=["Pirate URL", "Pirate Name", "Crew Name", "Crew URL"]
    )

    failures_df = pd.DataFrame(
        failures,
        columns=["Crew URL", "Error Type", "Message"]
    )

    return {
        "pirate_urls_df": pirate_urls_df,
        "pirate_urls_failures_df": failures_df,
        "meta": {
            "input_crews": int(len(crew_urls)),
            "pirates_found": int(len(pirate_urls_df)),
            "failures": int(len(failures_df)),
            "sleep_seconds": SLEEP_SECONDS,
        }
    }