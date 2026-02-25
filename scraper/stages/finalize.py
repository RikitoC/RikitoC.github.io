from __future__ import annotations

from typing import Dict
from datetime import datetime, timezone

import pandas as pd


VALID_TITLES = {"King", "Queen", "Prince", "Princess", "Lord", "Lady"}

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def _title_clean(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    # normalize casing: "queen" -> "Queen"
    return s[:1].upper() + s[1:].lower()

def _only_valid_title(s: str) -> str:
    t = _title_clean(s)
    return t if t in VALID_TITLES else ""

def run(ctx) -> Dict[str, pd.DataFrame]:
    # Pull stage outputs
    crews_df = ctx.data["crews"]["crews_df"]

    crew_details_df = ctx.data.get("crew_details", {}).get("crew_details_df", pd.DataFrame())
    crew_failures_df = ctx.data.get("crew_details", {}).get("crew_failures_df", pd.DataFrame())

    pirate_urls_df = ctx.data["pirate_urls"]["pirate_urls_df"]
    pirate_urls_failures_df = ctx.data["pirate_urls"]["pirate_urls_failures_df"]

    pirates_df = ctx.data["pirates"]["pirates_df"]
    pirates_failures_df = ctx.data["pirates"]["pirates_failures_df"]

    shoppes_df = ctx.data["shoppes"]["shoppes_df"]
    shoppes_failures_df = ctx.data["shoppes"]["shoppes_failures_df"]

    # --- Royals derived from pirates_df ---
    p = pirates_df.copy()

    # Clean title + enforce allowed list
    if "Flag Role" not in p.columns:
        p["Flag Role"] = ""
    if "Flag Name" not in p.columns:
        p["Flag Name"] = ""

    p["Flag Role"] = p["Flag Role"].astype(str).map(_only_valid_title)
    p["Flag Name"] = p["Flag Name"].astype(str).fillna("").map(str.strip)

    royals_df = p.loc[p["Flag Role"].isin(VALID_TITLES)].copy()

    # Keep royals output small + useful
    keep_cols = []
    for c in ["Pirate Name", "Flag Role", "Flag Name", "Crew Name", "Crew Rank", "Pirate URL"]:
        if c in royals_df.columns:
            keep_cols.append(c)

    royals_df = royals_df[keep_cols].copy() if keep_cols else royals_df

    # Sort royals: title rank then pirate name
    title_order = {"King": 0, "Queen": 1, "Prince": 2, "Princess": 3, "Lord": 4, "Lady": 5}
    if "Flag Role" in royals_df.columns:
        royals_df["_role_sort"] = royals_df["Flag Role"].map(title_order).fillna(999).astype(int)
        sort_cols = ["_role_sort"]
        if "Pirate Name" in royals_df.columns:
            sort_cols.append("Pirate Name")
        royals_df = royals_df.sort_values(sort_cols, ascending=True).drop(columns=["_role_sort"])

    # Optional: drop duplicates (same pirate can appear multiple times if URLs repeated)
    if "Pirate URL" in royals_df.columns:
        royals_df = royals_df.drop_duplicates(subset=["Pirate URL"])
    elif "Pirate Name" in royals_df.columns:
        royals_df = royals_df.drop_duplicates(subset=["Pirate Name"])

    # Add an update marker column (handy for the site)
    stamp = _utc_now_iso()
    for df in [crews_df, crew_details_df, pirate_urls_df, pirates_df, shoppes_df, royals_df]:
        if isinstance(df, pd.DataFrame) and not df.empty:
            df["Last Updated (UTC)"] = stamp

    return {
        # core datasets
        "crews.csv": crews_df,
        "crew_details.csv": crew_details_df,
        "pirate_urls.csv": pirate_urls_df,
        "pirates.csv": pirates_df,
        "shoppes.csv": shoppes_df,
        "royals.csv": royals_df,

        # failure logs
        "crew_failures.csv": crew_failures_df,
        "pirate_urls_failures.csv": pirate_urls_failures_df,
        "pirates_failures.csv": pirates_failures_df,
        "shoppes_failures.csv": shoppes_failures_df,
    }