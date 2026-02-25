from __future__ import annotations

from typing import Dict, Any, List
import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup


USER_AGENT = "Mozilla/5.0 (compatible; GitHubActionsScraper/1.0)"
REQUEST_TIMEOUT = 30
SLEEP_SECONDS = 1.0  # be kind

ALL_SKILLS = [
    "Sailing", "Rigging", "Carpentry", "Patching", "Bilging", "Gunning", "Treasure Haul", "Navigating",
    "Battle Navigation", "Swordfighting", "Rumble", "Drinking", "Spades", "Hearts", "Treasure Drop",
    "Poker", "Distilling", "Alchemistry", "Shipwrightery", "Blacksmithing", "Foraging", "Weaving"
]

CREW_RE = re.compile(r"(\w+)\s+of the crew\s+(.+)", re.IGNORECASE)
FLAG_RE = re.compile(r"(\w+)\s+of the flag\s+(.+)", re.IGNORECASE)


def parse_skills(skill_str: str) -> str:
    parts = skill_str.split("/")
    exp = parts[0].strip() if len(parts) > 0 else ""
    standing = parts[1].strip() if len(parts) > 1 else ""
    return f"{exp} / {standing}" if (exp or standing) else ""


def _safe_text(node) -> str:
    return node.get_text(strip=True) if node else ""


def _scrape_one(url: str, session: requests.Session) -> Dict[str, Any]:
    r = session.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT})
    if r.status_code != 200:
        raise ValueError(f"HTTP Error: {r.status_code}")

    soup = BeautifulSoup(r.text, "html.parser")

    # Pirate name
    pirate_name = ""
    name_tag = soup.find("font", attrs={"size": "+1"})
    if name_tag:
        pirate_name = name_tag.get_text(strip=True)
    if not pirate_name:
        # fallback: title or first bold
        pirate_name = soup.title.get_text(strip=True) if soup.title else ""

    crew_rank, crew_name, flag_role, flag_name = "", "", "", ""

    # Left column (your original selector)
    left_column = soup.find("td", attrs={"width": "190"})
    if left_column:
        tables = left_column.find_all("table")
        if tables:
            for row in tables[0].find_all("tr"):
                text = row.get_text(" ", strip=True)

                if "of the crew" in text.lower():
                    m = CREW_RE.search(text)
                    if m:
                        crew_rank = m.group(1)
                        a = row.find("a")
                        crew_name = _safe_text(a)

                if "of the flag" in text.lower():
                    m = FLAG_RE.search(text)
                    if m:
                        flag_role = m.group(1)
                        a = row.find("a")
                        flag_name = _safe_text(a)

    # Skills
    skills = {skill: "" for skill in ALL_SKILLS}
    for img in soup.find_all("img", alt=True):
        skill = img.get("alt", "").strip()
        if skill in skills:
            td = img.find_parent("td")
            if not td:
                continue
            next_td = td.find_next_sibling("td")
            if not next_td:
                continue
            skill_str = next_td.get_text(separator=" ", strip=True)
            skills[skill] = parse_skills(skill_str)

    return {
        "Pirate URL": url,
        "Pirate Name": pirate_name,
        "Crew Rank": crew_rank,
        "Crew Name": crew_name,
        "Flag Role": flag_role,
        "Flag Name": flag_name,
        **skills
    }


def run(ctx) -> Dict[str, Any]:
    pirate_urls_df: pd.DataFrame = ctx.data["pirate_urls"]["pirate_urls_df"]
    if "Pirate URL" not in pirate_urls_df.columns:
        raise RuntimeError("pirate_urls_df missing required column: 'Pirate URL'")

    urls = (
        pirate_urls_df["Pirate URL"]
        .dropna()
        .astype(str)
        .map(str.strip)
    )
    urls = urls[urls != ""].unique().tolist()

    session = requests.Session()

    rows: List[Dict[str, Any]] = []
    failures: List[Dict[str, str]] = []

    for i, url in enumerate(urls, start=1):
        try:
            row = _scrape_one(url, session)
            rows.append(row)
            print(f"✅ ({i}/{len(urls)}) {row.get('Pirate Name','(unknown)')}", flush=True)
        except Exception as e:
            failures.append({
                "Pirate URL": url,
                "Error Type": type(e).__name__,
                "Message": str(e),
            })
            print(f"❌ ({i}/{len(urls)}) Failed: {url} - {type(e).__name__}: {e}", flush=True)

        time.sleep(SLEEP_SECONDS)

    cols = ["Pirate URL", "Pirate Name", "Crew Rank", "Crew Name", "Flag Role", "Flag Name"] + ALL_SKILLS
    pirates_df = pd.DataFrame(rows, columns=cols)
    failures_df = pd.DataFrame(failures, columns=["Pirate URL", "Error Type", "Message"])

    return {
        "pirates_df": pirates_df,
        "pirates_failures_df": failures_df,
        "meta": {
            "input_urls": int(len(urls)),
            "success": int(len(pirates_df)),
            "failures": int(len(failures_df)),
            "sleep_seconds": SLEEP_SECONDS,
        }
    }