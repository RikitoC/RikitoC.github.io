from __future__ import annotations

from typing import Dict, Any, List
import time
import urllib.parse
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup


BASE = "https://emerald.puzzlepirates.com"
USER_AGENT = "Mozilla/5.0 (compatible; ExternalPirateWatcher/1.0)"
REQUEST_TIMEOUT = 30
SLEEP_SECONDS = 1.0

INPUT_CSV = "data/xoutflag.csv"
OUTPUT_LATEST_CSV = "data/external_pirates_latest.csv"
OUTPUT_HISTORY_CSV = "data/external_pirates_history.csv"


def _make_absolute(href: str) -> str:
    if not href:
        return ""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return BASE + href
    return BASE + "/" + href


def _normalize_url(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    url = _make_absolute(url)
    parsed = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.query, "")
    )


def _pirate_name_to_url(name: str) -> str:
    encoded = urllib.parse.quote((name or "").strip())
    return f"{BASE}/yoweb/pirate.wm?classic=false&target={encoded}"


def _extract_name_from_url(pirate_url: str) -> str:
    parsed = urllib.parse.urlsplit(pirate_url)
    qs = urllib.parse.parse_qs(parsed.query)
    return qs.get("target", [""])[0].strip()


def _clean(text: str) -> str:
    return " ".join(str(text or "").split()).strip()


def _load_targets(csv_path: str) -> pd.DataFrame:
    path = Path(csv_path)
    if not path.exists():
        raise RuntimeError(f"Input CSV not found: {csv_path}")

    df = pd.read_csv(path).fillna("")
    df.columns = [str(c).strip() for c in df.columns]

    has_name = "Pirate Name" in df.columns
    has_url = "Pirate URL" in df.columns

    if not has_name and not has_url:
        raise RuntimeError(
            f"{csv_path} must contain at least 'Pirate Name' or 'Pirate URL'"
        )

    rows = []
    for _, rec in df.iterrows():
        pirate_name = str(rec.get("Pirate Name", "")).strip() if has_name else ""
        pirate_url = str(rec.get("Pirate URL", "")).strip() if has_url else ""

        if not pirate_name and not pirate_url:
            continue

        if not pirate_url and pirate_name:
            pirate_url = _pirate_name_to_url(pirate_name)

        pirate_url = _normalize_url(pirate_url)

        if not pirate_name:
            pirate_name = _extract_name_from_url(pirate_url)

        if pirate_url:
            rows.append({
                "Pirate Name": pirate_name,
                "Pirate URL": pirate_url,
            })

    out = pd.DataFrame(rows, columns=["Pirate Name", "Pirate URL"])
    if out.empty:
        return out

    return out.drop_duplicates(subset=["Pirate URL"]).reset_index(drop=True)


def _extract_main_name(soup: BeautifulSoup, pirate_url: str) -> str:
    tag = soup.select_one('td[align="center"][height="32"] font[size="+1"] > b')
    if tag:
        return _clean(tag.get_text())
    return _extract_name_from_url(pirate_url)


def _extract_portrait_url(soup: BeautifulSoup) -> str:
    img = soup.select_one('a[href*="/yoweb/gallery?pirate="] img')
    if img and img.has_attr("src"):
        return _make_absolute(img["src"])
    return ""


def _extract_identity_block(soup: BeautifulSoup) -> Dict[str, str]:
    row: Dict[str, str] = {
        "Crew Rank": "",
        "Crew Job": "",
        "Crew Name": "",
        "Flag Role": "",
        "Flag Name": "",
        "Navy Rank": "",
        "Navy Name": "",
        "Navy Archipelago": "",
    }

    # Crew row
    crew_img = soup.find("img", src=lambda s: s and s.startswith("/yoweb/images/crew-"))
    if crew_img:
        tr = crew_img.find_parent("tr")
        if tr:
            text = _clean(tr.get_text(" ", strip=True))
            crew_link = tr.find("a", href=lambda h: h and "/yoweb/crew/info.wm" in h)

            if crew_link:
                row["Crew Name"] = _clean(crew_link.get_text())

            if " of the crew " in text:
                left = text.split(" of the crew ", 1)[0].strip()
                parts = [p.strip() for p in left.split(" and ", 1)]
                if len(parts) == 2:
                    row["Crew Rank"] = parts[0]
                    row["Crew Job"] = parts[1]
                elif len(parts) == 1:
                    row["Crew Rank"] = parts[0]

    # Flag row
    flag_img = soup.find("img", src=lambda s: s and s.startswith("/yoweb/images/flag-"))
    if flag_img:
        tr = flag_img.find_parent("tr")
        if tr:
            text = _clean(tr.get_text(" ", strip=True))
            flag_link = tr.find("a", href=lambda h: h and "/yoweb/flag/info.wm" in h)

            if flag_link:
                row["Flag Name"] = _clean(flag_link.get_text())

            if " of the flag " in text:
                row["Flag Role"] = text.split(" of the flag ", 1)[0].strip()

    # Navy row
    for font in soup.find_all("font", size="-1"):
        text = _clean(font.get_text(" ", strip=True))
        if " Navy in the " in text and text.endswith(" Archipelago"):
            left, arch = text.rsplit(" Navy in the ", 1)
            row["Navy Archipelago"] = arch.replace(" Archipelago", "").strip()

            if " in the " in left:
                rank, navy_name = left.split(" in the ", 1)
                row["Navy Rank"] = rank.strip()
                row["Navy Name"] = (navy_name + " Navy").strip()
            break

    return row


def _extract_reputation(soup: BeautifulSoup) -> Dict[str, str]:
    out: Dict[str, str] = {}

    rep_header = soup.find("b", string=lambda s: s and _clean(s) == "Reputation")
    if not rep_header:
        return out

    outer_td = rep_header.find_parent("td")
    if not outer_td:
        return out

    table = outer_td.find("table")
    if not table:
        return out

    for tr in table.find_all("tr", recursive=False):
        img = tr.find("img")
        tds = tr.find_all("td", recursive=False)
        if not img or len(tds) < 2:
            continue

        rep_type = _clean(img.get("alt", ""))
        rep_value = _clean(tds[1].get_text(" ", strip=True))

        if rep_type and rep_value:
            out[f"Reputation {rep_type}"] = rep_value

    return out


def _extract_property_rows(soup: BeautifulSoup) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "Owns List": "",
        "Owns Count": 0,
        "Manages List": "",
        "Manages Count": 0,
        "Stalls List": "",
        "Stalls Count": 0,
        "Houses List": "",
        "Houses Count": 0,
    }

    owns_items: List[str] = []
    manages_items: List[str] = []

    for tr in soup.find_all("tr", valign="middle"):
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 2:
            continue

        text = _clean(tds[1].get_text(" ", strip=True))
        if text.startswith("Owns:"):
            value = text.replace("Owns:", "", 1).strip()
            parts = [p.strip(" ,") for p in value.split(",") if p.strip(" ,")]
            owns_items.extend(parts)

        elif text.startswith("Manages:"):
            value = text.replace("Manages:", "", 1).strip()
            parts = [p.strip(" ,") for p in value.split(",") if p.strip(" ,")]
            manages_items.extend(parts)

    row["Owns List"] = " | ".join(owns_items)
    row["Owns Count"] = len(owns_items)
    row["Manages List"] = " | ".join(manages_items)
    row["Manages Count"] = len(manages_items)

    stalls_header = soup.find("b", string=lambda s: s and _clean(s) == "Stalls")
    if stalls_header:
        p = stalls_header.find_parent("p")
        if p:
            next_p = p.find_next_sibling("p")
            if next_p:
                stalls = []
                for img in next_p.find_all("img"):
                    title = _clean(img.get("title", "") or img.get("alt", ""))
                    if title:
                        stalls.append(title)
                row["Stalls List"] = " | ".join(stalls)
                row["Stalls Count"] = len(stalls)

    houses_header = soup.find("b", string=lambda s: s and _clean(s) == "Houses")
    if houses_header:
        p = houses_header.find_parent("p")
        if p:
            next_p = p.find_next_sibling("p")
            if next_p:
                houses = []
                for img in next_p.find_all("img"):
                    title = _clean(img.get("title", "") or img.get("alt", ""))
                    if title:
                        houses.append(title)
                row["Houses List"] = " | ".join(houses)
                row["Houses Count"] = len(houses)

    return row


def _extract_hearties(soup: BeautifulSoup) -> Dict[str, Any]:
    names: List[str] = []

    hearties_header = soup.find("b", string=lambda s: s and _clean(s) == "Hearties")
    if not hearties_header:
        return {"Hearties List": "", "Hearties Count": 0}

    table = hearties_header.find_parent("table")
    if not table:
        return {"Hearties List": "", "Hearties Count": 0}

    for a in table.find_all("a", href=True):
        href = a["href"]
        if "pirate.wm?target=" in href:
            name = _clean(a.get_text())
            if name:
                names.append(name)

    return {
        "Hearties List": " | ".join(names),
        "Hearties Count": len(names),
    }


def _extract_skills(soup: BeautifulSoup) -> Dict[str, str]:
    out: Dict[str, str] = {}

    section_names = {"Piracy Skills", "Carousing Skills", "Crafting Skills"}

    for header_b in soup.find_all("b"):
        header = _clean(header_b.get_text())
        if header not in section_names:
            continue

        header_td = header_b.find_parent("td")
        if not header_td:
            continue

        table = header_td.find("table")
        if not table:
            continue

        for tr in table.find_all("tr", recursive=False):
            img = tr.find("img")
            tds = tr.find_all("td", recursive=False)
            if not img or len(tds) < 2:
                continue

            skill_name = _clean(img.get("alt", ""))
            if not skill_name:
                continue

            value_td = tds[1]
            font_main = value_td.find("font", size="-1")
            if not font_main:
                continue

            main_text = _clean(font_main.get_text(" ", strip=True))
            if not main_text or "/" not in main_text:
                continue

            experience, reputation = [part.strip() for part in main_text.split("/", 1)]

            out[f"Skill Experience {skill_name}"] = experience
            out[f"Skill Reputation {skill_name}"] = reputation
            out[f"Skill Category {skill_name}"] = header.replace(" Skills", "")

    return out


def _scrape_one_pirate(pirate_url: str, session: requests.Session) -> Dict[str, Any]:
    r = session.get(
        pirate_url,
        timeout=REQUEST_TIMEOUT,
        headers={"User-Agent": USER_AGENT},
    )
    if r.status_code != 200:
        raise ValueError(f"HTTP Error: {r.status_code}")

    soup = BeautifulSoup(r.text, "html.parser")

    row: Dict[str, Any] = {
        "Pirate Name": _extract_main_name(soup, pirate_url),
        "Pirate URL": pirate_url,
        "Portrait URL": _extract_portrait_url(soup),
    }

    row.update(_extract_identity_block(soup))
    row.update(_extract_reputation(soup))
    row.update(_extract_property_rows(soup))
    row.update(_extract_hearties(soup))
    row.update(_extract_skills(soup))

    return row


def _write_latest(df: pd.DataFrame, latest_csv: str) -> None:
    out = Path(latest_csv)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)


def _append_history(df: pd.DataFrame, history_csv: str) -> None:
    out = Path(history_csv)
    out.parent.mkdir(parents=True, exist_ok=True)

    now = datetime.utcnow()
    df = df.copy()
    df["Scrape Date"] = now.strftime("%Y-%m-%d")
    df["Scraped At UTC"] = now.strftime("%Y-%m-%d %H:%M:%S")

    if out.exists():
        try:
            old = pd.read_csv(out).fillna("")
            combined = pd.concat([old, df], ignore_index=True, sort=False)
        except pd.errors.EmptyDataError:
            combined = df
        except Exception as e:
            print(f"⚠️ Could not read existing history file {history_csv}: {e}")
            combined = df
    else:
        combined = df

    combined.to_csv(out, index=False)


def run(ctx=None) -> Dict[str, Any]:
    targets_df = _load_targets(INPUT_CSV)
    session = requests.Session()

    rows: List[Dict[str, Any]] = []
    failures: List[Dict[str, str]] = []

    for i, rec in enumerate(targets_df.to_dict("records"), start=1):
        pirate_url = rec["Pirate URL"]

        try:
            row = _scrape_one_pirate(pirate_url, session)
            rows.append(row)
            print(
                f"✅ ({i}/{len(targets_df)}) {row.get('Pirate Name', '')} | "
                f"Crew: {row.get('Crew Name', '')} | "
                f"Flag: {row.get('Flag Name', '')}",
                flush=True,
            )
        except Exception as e:
            failures.append({
                "Pirate URL": pirate_url,
                "Error Type": type(e).__name__,
                "Message": str(e),
            })
            print(
                f"❌ ({i}/{len(targets_df)}) Failed: {pirate_url} - {type(e).__name__}: {e}",
                flush=True,
            )

        time.sleep(SLEEP_SECONDS)

    pirates_df = pd.DataFrame(rows)
    failures_df = pd.DataFrame(failures, columns=["Pirate URL", "Error Type", "Message"])

    if not pirates_df.empty:
        first_cols = [
            "Pirate Name",
            "Pirate URL",
            "Portrait URL",
            "Crew Rank",
            "Crew Job",
            "Crew Name",
            "Flag Role",
            "Flag Name",
            "Navy Rank",
            "Navy Name",
            "Navy Archipelago",
            "Owns Count",
            "Owns List",
            "Manages Count",
            "Manages List",
            "Stalls Count",
            "Stalls List",
            "Houses Count",
            "Houses List",
            "Hearties Count",
            "Hearties List",
        ]
        other_cols = [c for c in pirates_df.columns if c not in first_cols]
        pirates_df = pirates_df[
            [c for c in first_cols if c in pirates_df.columns] + sorted(other_cols)
        ]

    _write_latest(pirates_df, OUTPUT_LATEST_CSV)
    _append_history(pirates_df, OUTPUT_HISTORY_CSV)

    return {
        "external_pirates_df": pirates_df,
        "external_pirates_failures_df": failures_df,
        "meta": {
            "targets": int(len(targets_df)),
            "scraped": int(len(pirates_df)),
            "failures": int(len(failures_df)),
            "input_csv": INPUT_CSV,
            "latest_csv": OUTPUT_LATEST_CSV,
            "history_csv": OUTPUT_HISTORY_CSV,
            "sleep_seconds": SLEEP_SECONDS,
        }
    }


if __name__ == "__main__":
    result = run()
    print(result["meta"])
