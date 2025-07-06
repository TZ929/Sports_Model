"""Collect MLB ballpark factors from Baseball Savant Statcast leaderboard.

This script fetches the main park factors table from Baseball Savant and, for each venue, optionally
scrapes the advanced breakdown page to gather additional splits.  The resulting data is saved to
CSV files under `data/raw/` so it can be merged into the feature-engineering pipeline.

Example usage (CLI):

    python collect_park_factors_mlb.py --year 2024 --save-dir data/raw

If you omit --year the script will default to the current season.  Use --all-years to iterate
from 2008-present.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

import json
import html as ihtml

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

BASE_URL = "https://baseballsavant.mlb.com"
PARK_FACTORS_URL = f"{BASE_URL}/leaderboard/statcast-park-factors"
HEADERS = {"User-Agent": "Mozilla/5.0"}


def fetch_park_factors_page(year: int) -> str:
    """Return raw HTML text for the park factors leaderboard for the given year."""
    params = {"year": year}
    logger.info("Fetching park factors for %s", year)
    resp = requests.get(PARK_FACTORS_URL, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def _standardise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return DataFrame with normalised column names (snake_case)."""
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df


def _parse_via_html_table(html_text: str) -> Optional[pd.DataFrame]:
    """Try the old approach using <table> tags rendered server-side."""
    try:
        tables = pd.read_html(html_text)
    except ValueError:
        tables = []
    if not tables:
        return None
    return _standardise_columns(tables[0])


def _parse_via_nextjs_payload(html_text: str) -> Optional[pd.DataFrame]:
    """If no <table> exists, Baseball Savant (Next.js) embeds JSON in __NEXT_DATA__."""
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html_text, re.S)
    if not match:
        return None
    try:
        # Unescape HTML entities then load JSON
        raw_json = ihtml.unescape(match.group(1))
        data = json.loads(raw_json)
        page_props = data["props"]["pageProps"]
        # Depending on site build, the key may differ. Search for the first list[dict] value.
        table_like = None
        for key, value in page_props.items():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                table_like = value
                break
        if table_like is None:
            return None
        df = pd.DataFrame(table_like)
        return _standardise_columns(df)
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Failed to parse __NEXT_DATA__: %s", exc)
        return None


def parse_park_factors_table(html_text: str) -> pd.DataFrame:
    """Parse Baseball Savant park factors page into a DataFrame.

    The site recently migrated to Next.js, embedding data in a JSON blob instead of server-
    rendered tables.  We attempt both strategies for robustness.
    """
    df = _parse_via_html_table(html_text)
    if df is not None and not df.empty:
        return df

    df = _parse_via_nextjs_payload(html_text)
    if df is not None and not df.empty:
        return df

    raise ValueError("Failed to parse park factors – page structure unrecognised.")


def extract_venue_links(html: str) -> Dict[str, str]:
    """Return mapping of venue name -> detail page URL fragment (/park-factors?id=xx)."""
    soup = BeautifulSoup(html, "html.parser")
    link_map: Dict[str, str] = {}
    for a in soup.select("table a"):
        href = a.get("href")
        if href and "park-factors?" in href:
            venue_name = a.text.strip()
            # `href` can be either str or list[str]; convert to str to avoid type error
            href_str = href if isinstance(href, str) else href[0]
            # If the link is already absolute, leave it; otherwise prepend BASE_URL
            if href_str.startswith("http"):
                full_url = href_str
            else:
                full_url = BASE_URL + href_str
            link_map[venue_name] = full_url
    logger.info("Found %d venue links", len(link_map))
    return link_map


def fetch_venue_breakdown(url: str) -> Optional[pd.DataFrame]:
    """Fetch a venue breakdown table from the given Baseball Savant URL.

    Returns a DataFrame of the first table on the page, or None on failure.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        tables = pd.read_html(resp.text)
        if not tables:
            return None
        df = tables[0]
        df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
        return df
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("Failed to fetch breakdown for %s: %s", url, exc)
        return None


def enrich_with_breakdowns(df: pd.DataFrame, link_map: Dict[str, str]) -> pd.DataFrame:
    """Append selected breakdown metrics (if any) to the main DataFrame."""
    breakdown_rows: List[pd.Series] = []

    for venue, url in link_map.items():
        breakdown_df = fetch_venue_breakdown(url)
        if breakdown_df is None or breakdown_df.empty:
            continue
        # Example expectation: breakdown_df has columns like 'metric', 'factor', etc.
        # We'll pivot so each metric becomes its own column suffixed with _detail.
        breakdown_df = breakdown_df[[breakdown_df.columns[0], breakdown_df.columns[1]]]
        breakdown_df.columns = ["metric", "factor"]
        pivoted = breakdown_df.set_index("metric").T
        pivoted.index = [venue]
        breakdown_rows.append(pivoted.iloc[0])

    if not breakdown_rows:
        logger.warning("No venue breakdowns parsed; returning main DataFrame unchanged.")
        return df

    breakdown_full = pd.DataFrame(breakdown_rows)
    breakdown_full.reset_index(inplace=True)
    breakdown_full.rename(columns={"index": "venue"}, inplace=True)

    merged = df.merge(breakdown_full, how="left", left_on="venue", right_on="venue")
    return merged


def save_dataframe(df: pd.DataFrame, save_path: Path) -> None:
    save_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(save_path, index=False)
    logger.info("Saved %s", save_path)


def main(year: int, all_years: bool, save_dir: Path, include_details: bool = True) -> None:
    years = [year]
    if all_years:
        current = datetime.now().year
        years = list(range(current, 2007, -1))  # Statcast data is reliable from 2008 onwards

    for yr in years:
        html = fetch_park_factors_page(yr)
        df = parse_park_factors_table(html)
        # Add season column for clarity (some tables already have year column)
        if "year" not in df.columns:
            df.insert(0, "year", yr)

        if include_details:
            venue_links = extract_venue_links(html)
            df = enrich_with_breakdowns(df, venue_links)

        save_path = save_dir / f"park_factors_savant_{yr}.csv"
        save_dataframe(df, save_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Collect MLB park factors from Baseball Savant.")
    parser.add_argument("--year", type=int, default=datetime.now().year, help="Season year to fetch (default: current year)")
    parser.add_argument("--all-years", action="store_true", help="Fetch all seasons 2008-current")
    parser.add_argument("--save-dir", type=Path, default=Path("data/raw"), help="Directory to save CSV files")
    parser.add_argument("--skip-details", action="store_true", help="Skip scraping venue-level breakdown pages")
    args = parser.parse_args()

    main(
        year=args.year,
        all_years=args.all_years,
        save_dir=args.save_dir,
        include_details=not args.skip_details,
    ) 