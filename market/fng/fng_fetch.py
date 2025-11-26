#!/usr/bin/env python3
"""
Fetch and save the Alternative.me Fear and Greed Index for a given date range.

Data source and attribution:
- Source: "Fear and Greed Index" by Alternative.me
- API: https://api.alternative.me/fng/
- Website: https://alternative.me/crypto/fear-and-greed-index/

Attribution requirement:
You must acknowledge the source alongside any display of the data. This script
adds 'source' and 'source_url' columns to the CSV to keep attribution next to the data.

Usage examples:
  python OHLCV/fng_fetch.py
  python OHLCV/fng_fetch.py --start 2024-01-01 --end 2025-10-31
  python OHLCV/fng_fetch.py --out /path/to/FNG_daily_2024-01-01_2025-10-31.csv
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


API_URL = "https://api.alternative.me/fng/?limit=0&format=json"
SOURCE_NAME = "Alternative.me Fear and Greed Index"
SOURCE_URL = "https://api.alternative.me/fng/"


def _http_get_json(url: str, timeout_seconds: int = 30, retries: int = 3, backoff: float = 1.5) -> Dict[str, Any]:
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "fng-fetch-script/1.0 (+https://alternative.me/)"})
            with urlopen(req, timeout=timeout_seconds) as resp:
                if resp.status != 200:
                    raise HTTPError(url, resp.status, f"HTTP {resp.status}", resp.headers, None)
                raw = resp.read()
                return json.loads(raw.decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as e:
            last_err = e
            if attempt < retries - 1:
                sleep_seconds = backoff ** attempt
                time.sleep(sleep_seconds)
            else:
                break
    assert last_err is not None
    raise RuntimeError(f"Failed to fetch {url}: {last_err}") from last_err


def _parse_date_yyyy_mm_dd(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def _to_utc_date_from_unix_seconds(ts_seconds: int) -> dt.date:
    return dt.datetime.utcfromtimestamp(ts_seconds).date()


def fetch_all_fng() -> Dict[str, Any]:
    """
    Fetch all available Fear and Greed Index entries as JSON from Alternative.me.
    Returns the parsed JSON dictionary.
    """
    data = _http_get_json(API_URL, timeout_seconds=30, retries=3, backoff=1.6)
    if not isinstance(data, dict) or "data" not in data:
        raise ValueError("Unexpected API response structure.")
    return data


def filter_and_normalize_records(
    records: Iterable[Dict[str, Any]], start_date: dt.date, end_date: dt.date
) -> List[Dict[str, Any]]:
    """
    Filter API records to the inclusive [start_date, end_date] range and normalize fields.
    Ensures one row per day (keeps the last record per date if duplicates exist).
    """
    by_date: Dict[dt.date, Tuple[int, Dict[str, Any]]] = {}
    for rec in records:
        # timestamp is a string in seconds
        ts_raw = rec.get("timestamp")
        if ts_raw is None:
            continue
        try:
            ts = int(str(ts_raw))
        except ValueError:
            continue
        day = _to_utc_date_from_unix_seconds(ts)
        if day < start_date or day > end_date:
            continue
        value_str = rec.get("value")
        classification = rec.get("value_classification")
        # Normalize and store; if multiple per day, keep the one with the latest timestamp
        normalized = {
            "date": day.isoformat(),
            "value": int(value_str) if isinstance(value_str, (int, str)) and str(value_str).isdigit() else None,
            "value_classification": str(classification) if classification is not None else None,
            "timestamp": ts,
            "source": SOURCE_NAME,
            "source_url": SOURCE_URL,
        }
        prev = by_date.get(day)
        if prev is None or ts >= prev[0]:
            by_date[day] = (ts, normalized)

    # Emit rows sorted by date ascending
    rows: List[Dict[str, Any]] = [item for _, item in sorted(by_date.values(), key=lambda x: _to_utc_date_from_unix_seconds(x[0]))]
    # Alternatively sort by the 'date' field to be explicit
    rows.sort(key=lambda r: r["date"])
    return rows


def save_csv(rows: List[Dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["date", "value", "value_classification", "timestamp", "source", "source_url"]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k) for k in fieldnames})


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch Alternative.me Fear and Greed Index for a date range and save CSV.")
    parser.add_argument("--start", type=str, default="2024-01-01", help="Start date (YYYY-MM-DD), inclusive. Default: 2024-01-01")
    parser.add_argument("--end", type=str, default="2025-10-31", help="End date (YYYY-MM-DD), inclusive. Default: 2025-10-31")
    parser.add_argument("--out", type=str, default="", help="Output CSV path. Default: OHLCV/FNG_daily_{start}_{end}.csv next to this script.")
    args = parser.parse_args(argv)

    try:
        start_date = _parse_date_yyyy_mm_dd(args.start)
        end_date = _parse_date_yyyy_mm_dd(args.end)
    except ValueError as e:
        print(f"Invalid date format: {e}", file=sys.stderr)
        return 2

    if end_date < start_date:
        print("End date must be on or after start date.", file=sys.stderr)
        return 2

    script_dir = Path(__file__).resolve().parent
    default_out = script_dir / f"FNG_daily_{start_date.isoformat()}_{end_date.isoformat()}.csv"
    out_path = Path(args.out) if args.out else default_out

    print(f"Fetching Fear and Greed Index from {API_URL}")
    print("Source: Alternative.me Fear and Greed Index (https://alternative.me/crypto/fear-and-greed-index/)")
    data = fetch_all_fng()

    records = data.get("data", [])
    rows = filter_and_normalize_records(records, start_date=start_date, end_date=end_date)

    save_csv(rows, out_path)
    print(f"Saved {len(rows)} rows to {out_path}")
    print("Please display attribution next to any use of this data: 'Source: Alternative.me Fear and Greed Index'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


