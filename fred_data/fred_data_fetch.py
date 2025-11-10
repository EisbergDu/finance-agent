#!/usr/bin/env python3
"""
Template for downloading FRED data.

Usage:
 1. pip install requests
 2. Replace the API_KEY placeholder below.
 3. Run `python fred_data_fetch.py` (or pass start/end via args).

By default this script grabs:
 - DTWEXM: Trade-weighted dollar index (broad)
 - FEDFUNDS: Effective Federal Funds rate (overnight policy rate)
 - VIXCLS: VIX closing level (implied volatility)
 Results are saved under fred_data/.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date
from typing import Any, Dict, Optional

import requests

API_KEY = "c1fc7556af1da1e3bedf5089ecb865c6"  # <-- 替换为真实的 API key
OUTPUT_DIR = "fred_data"

SERIES_IDS = {
    "dollar_index": "DTWEXBGS",
    "overnight_rate": "FEDFUNDS",
    "market_volatility": "VIXCLS",
}


def fetch_series(
    series_id: str,
    start_date: str,
    end_date: str,
    realtime_start: Optional[str],
    realtime_end: Optional[str],
) -> Dict[str, Any]:
    """Call FRED observations endpoint for a single series."""
    url = "https://api.stlouisfed.org/fred/series/observations"
    params: Dict[str, Any] = {
        "series_id": series_id,
        "api_key": API_KEY,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date,
    }
    if realtime_start:
        params["realtime_start"] = realtime_start
    if realtime_end:
        params["realtime_end"] = realtime_end
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def save_dataset(name: str, data: Dict[str, Any], start: str, end: str) -> None:
    """Persist the fetched payload to disk for later analysis."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    safe_name = name.replace(" ", "_")
    filename = f"{safe_name}_{start}_{end}.json"
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)
    print(f"  saved {name} ({len(data.get('observations', []))} rows) -> {path}")


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for flexible date ranges."""
    parser = argparse.ArgumentParser(description="Fetch FRED series observations.")
    parser.add_argument("--start", default="2024-01-01", help="起始日期（含）")
    parser.add_argument("--end", default=date.today().isoformat(), help="结束日期（含）")
    parser.add_argument("--realtime-start", help="如需读取特定发布版本，请指定实时起始日期")
    parser.add_argument("--realtime-end", help="如需读取特定发布版本，请指定实时结束日期")
    return parser.parse_args()


def main() -> None:
    if "your_api_key_here" in API_KEY:
        raise SystemExit("请先将脚本顶部的 API_KEY 替换为真实的 key。")
    args = parse_args()
    print(f"Fetching data from {args.start} to {args.end} for {len(SERIES_IDS)} series.")
    for name, series_id in SERIES_IDS.items():
        print(f"- {name} ({series_id})")
        payload = fetch_series(
            series_id,
            args.start,
            args.end,
            realtime_start=args.realtime_start,
            realtime_end=args.realtime_end,
        )
        save_dataset(name, payload, args.start, args.end)


if __name__ == "__main__":
    main()
