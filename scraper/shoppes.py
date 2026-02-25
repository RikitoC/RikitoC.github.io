from __future__ import annotations

import re
import time
from typing import Dict, Any, List, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup


BASE = "https://emerald.puzzlepirates.com"
USER_AGENT = "Mozilla/5.0 (compatible; GitHubActionsScraper/1.0)"
REQUEST_TIMEOUT = 30
SLEEP_SECONDS = 1.0  # be kind

SHOP_TYPE_CANON = {
    "apothecary": "Apothecary",
    "distillery": "Distillery",
    "furnisher": "Furnisher",
    "ironmonger": "Iron Monger",
    "shipyard": "Shipyard",
    "tailor": "Tailor",
    "weavery": "Weavery",
    "fort": "Fort",
    "estateagent": "Estate Agent",
}

# icon: /yoweb/images/shop-<slug>.png  OR  /yoweb/images/shop-managed-<slug>.png
TYPE_AND_ROLE_RE = re.compile(r"/yoweb/images/shop(-managed)?-([a-z\-]+)\.png", re.I)

# finds each "Name on Location" pair; handles commas between pairs
NAME_LOC_PAIRS_RE = re.compile(r"\s*(?P<name>.+?)\s+on\s+(?P<loc>[^,]+?)(?=,|$)", re.I)


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def _humanize_slug(slug: str) -> str:
    if not slug:
        return ""
    label = slug.replace("-", " ").strip().title()
    label = label.replace("Ironmonger", "Iron Monger")
    return label

def _parse_icon_src(src: str) -> Tuple[str, str]:
    """
    Returns (shop_type_label, ownership_role) from the icon src.
    ownership_role: 'Owns' or 'Manages'
    """
    m = TYPE_AND_ROLE_RE.search(src or "")
    if not m:
        return "", ""
    is_managed = bool(m.group(1))
    slug = (m.group(2) or "").lower()
    label = SHOP_TYPE_CANON.get(slug) or _humanize_slug(slug)
    role = "Manages" if is_managed else "Owns"
    return label, role

def _extract_name_loc_pairs(text: str) -> List[Tuple[str, str]]:
    txt = _clean(text)
    txt = re.sub(r"^(Owns:|Manages:)\s*", "", txt, flags=re.I)
    pairs: List[Tuple[str, str]] = []
    for m in NAME_LOC_PAIRS_RE.finditer(txt):
        name = _clean(m.group("name"))
        loc = _clean(m.group("loc"))
        if not loc:
            loc = "Error"
        pairs.append((name, loc))
    return pairs


def extract_shop_rows(soup: BeautifulSoup, pirate_name: str, crew_name: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    left_col = soup.find("td", attrs={"width": "190"})
    if not left_col:
        return rows

    # 1) Shoppes (rows with icon + descriptive text)
    for tr in left_col.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        img = tds[0].find("img")
        if not img:
            continue

        shop_type, role = _parse_icon_src(img.get("src", ""))
        if not shop_type:
            continue  # not a shop icon

        text_cell = tds[1]
        txt = _clean(text_cell.get_text(" ", strip=True))

        pairs = _extract_name_loc_pairs(txt)
        if not pairs:
            pairs = [("", "Error")]

        for shop_name, location in pairs:
            rows.append({
                "Pirate Name": pirate_name,
                "Crew Name": crew_name,
                "Shop Type": shop_type,
                "Shop size": "Shoppe",
                "Shop Name": shop_name,
                "Location": location,
                "Ownership Role": role
            })

    # 2) Stalls (icons inside the Stalls block; each icon = one stall)
    stalls_header = left_col.find(string=re.compile(r"^\s*Stalls\s*$", re.I))
    if stalls_header:
        header_parent = stalls_header.find_parent()
        stalls_block = header_parent.find_next("p") if header_parent else None
        if stalls_block:
            for icon in stalls_block.find_all("img"):
                shop_type, role = _parse_icon_src(icon.get("src", ""))
                if not shop_type:
                    continue

                title = icon.get("title") or icon.get("alt") or ""
                title_clean = _clean(title)

                m = re.search(r"^(?P<name>.+?)\s+on\s+(?P<loc>.+)$", title_clean, flags=re.I)
                if m:
                    shop_name = _clean(m.group("name"))
                    location = _clean(m.group("loc"))
                else:
                    shop_name = ""
                    location = "Error"

                rows.append({
                    "Pirate Name": pirate_name,
                    "Crew Name": crew_name,
                    "Shop Type": shop_type,
                    "Shop size": "Stall",
                    "Shop Name": shop_name,
                    "Location": location,
                    "Ownership Role": role
                })

    return rows


def _scrape_one(url: str, session: requests.Session) -> List[Dict[str, str]]:
    r = session.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT})
    if r.status_code != 200:
        raise ValueError(f"HTTP Error: {r.status_code}")

    soup = BeautifulSoup(r.text, "html.parser")

    name_el = soup.find("font", attrs={"size": "+1"})
    pirate_name = name_el.get_text(strip=True) if name_el else ""

    crew_name = ""
    left_column = soup.find("td", attrs={"width": "190"})
    if left_column:
        tables = left_column.find_all("table")
        if tables:
            for row in tables[0].find_all("tr"):
                text = row.get_text(" ", strip=True)
                if "of the crew" in text.lower():
                    a = row.find("a")
                    if a:
                        crew_name = a.get_text(" ", strip=True)
                    break

    return extract_shop_rows(soup, pirate_name, crew_name)


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

    all_rows: List[Dict[str, str]] = []
    failures: List[Dict[str, str]] = []

    for i, url in enumerate(urls, start=1):
        try:
            rows = _scrape_one(url, session)
            all_rows.extend(rows)
            print(f"✅ ({i}/{len(urls)}) Shoppes scraped for {url} (+{len(rows)})", flush=True)
        except Exception as e:
            failures.append({
                "Pirate URL": url,
                "Error Type": type(e).__name__,
                "Message": str(e),
            })
            print(f"❌ ({i}/{len(urls)}) Failed: {url} - {type(e).__name__}: {e}", flush=True)

        time.sleep(SLEEP_SECONDS)

    columns = ["Pirate Name", "Crew Name", "Shop Type", "Shop size", "Shop Name", "Location", "Ownership Role"]
    shoppes_df = pd.DataFrame(all_rows, columns=columns)
    failures_df = pd.DataFrame(failures, columns=["Pirate URL", "Error Type", "Message"])

    return {
        "shoppes_df": shoppes_df,
        "shoppes_failures_df": failures_df,
        "meta": {
            "input_urls": int(len(urls)),
            "rows": int(len(shoppes_df)),
            "failures": int(len(failures_df)),
            "sleep_seconds": SLEEP_SECONDS,
        }
    }