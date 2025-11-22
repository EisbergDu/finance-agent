import os
import sys
import csv
import time
import json
from datetime import datetime, date
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen, Request
import ssl
from typing import Optional, Tuple


API_KEY = "ZP30FX94JVIC7GZR"
BASE_URL = "https://www.alphavantage.co/query"

# Output directory (place script and data in the same folder as requested)
OUTPUT_DIR = Path(__file__).resolve().parent

# Symbols to fetch
SYMBOLS = ["NVDA", "KO"]

# Start date filter
START_DATE = date(2023, 1, 1)


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def fetch_quarterly_earnings(symbol: str) -> list[dict]:
    params = {"function": "EARNINGS", "symbol": symbol, "apikey": API_KEY}
    query = f"{BASE_URL}?{urlencode(params)}"
    req = Request(query, headers={"User-Agent": "python-urllib"})
    # Some environments require a permissive SSL context for simple API calls.
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with urlopen(req, timeout=30, context=ctx) as resp:
        body = resp.read().decode("utf-8")
    data = json.loads(body)

    if "quarterlyEarnings" not in data:
        # surface helpful message if rate limited or error
        message = data.get("Note") or data.get("Information") or data.get("Error Message")
        raise RuntimeError(f"Unexpected response for {symbol}. Message: {message or json.dumps(data)[:500]}")

    rows = []
    for item in data["quarterlyEarnings"]:
        try:
            fiscal_date = parse_date(item["fiscalDateEnding"])
        except Exception:
            # skip malformed rows
            continue
        if fiscal_date < START_DATE:
            continue

        rows.append(
            {
                "symbol": symbol,
                "fiscalDateEnding": item.get("fiscalDateEnding"),
                "reportedDate": item.get("reportedDate"),
                "reportedEPS": item.get("reportedEPS"),
                "estimatedEPS": item.get("estimatedEPS"),
                "surprise": item.get("surprise"),
                "surprisePercentage": item.get("surprisePercentage"),
            }
        )
    return rows


def write_csv(symbol: str, rows: list[dict]) -> Path:
    today_str = date.today().strftime("%Y-%m-%d")
    filename = f"{symbol}_earnings_quarterly_2023-01-01_{today_str}.csv"
    out_path = OUTPUT_DIR / filename

    fieldnames = [
        "symbol",
        "fiscalDateEnding",
        "reportedDate",
        "reportedEPS",
        "estimatedEPS",
        "surprise",
        "surprisePercentage",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return out_path


def fetch_estimates(symbol: str) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Returns:
      - quarterly EPS estimate rows
      - quarterly Revenue estimate rows
      - trending estimates rows (new API shape with mixed horizons)
    """
    params = {"function": "EARNINGS_ESTIMATES", "symbol": symbol, "apikey": API_KEY}
    query = f"{BASE_URL}?{urlencode(params)}"
    req = Request(query, headers={"User-Agent": "python-urllib"})
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    with urlopen(req, timeout=30, context=ctx) as resp:
        body = resp.read().decode("utf-8")
    data = json.loads(body)

    eps_rows: list[dict] = []
    for item in data.get("quarterlyEarningsEstimates", []) or []:
        try:
            fiscal_date = parse_date(item.get("fiscalDateEnding"))
        except Exception:
            continue
        if fiscal_date < START_DATE:
            continue
        eps_rows.append(
            {
                "symbol": symbol,
                "fiscalDateEnding": item.get("fiscalDateEnding"),
                # key names per API; keep robust fallbacks
                "estimatedEPS": item.get("estimatedEPS") or item.get("estimated_eps"),
                "numberAnalystsEstimated": item.get("numberAnalystsEstimated")
                or item.get("numberAnalystEstimated")
                or item.get("numberOfAnalysts"),
            }
        )

    rev_rows: list[dict] = []
    for item in data.get("quarterlyRevenueEstimates", []) or []:
        try:
            fiscal_date = parse_date(item.get("fiscalDateEnding"))
        except Exception:
            continue
        if fiscal_date < START_DATE:
            continue
        rev_rows.append(
            {
                "symbol": symbol,
                "fiscalDateEnding": item.get("fiscalDateEnding"),
                "revenueEstimate": item.get("revenueEstimate") or item.get("revenue_estimate"),
                "numberAnalystsEstimated": item.get("numberAnalystsEstimated")
                or item.get("numberAnalystEstimated")
                or item.get("numberOfAnalysts"),
            }
        )

    # Support new "Trending" shape that returns an 'estimates' array
    trending_rows: list[dict] = []
    for item in data.get("estimates", []) or []:
        # The field may be 'date' (end of fiscal), keep if >= START_DATE when parseable
        keep = True
        try:
            if item.get("date"):
                d = parse_date(item["date"])
                keep = d >= START_DATE
        except Exception:
            # keep row if date missing
            pass
        if not keep:
            continue
        row = {"symbol": symbol}
        # copy all primitive fields through to be future-proof
        for k, v in item.items():
            if isinstance(v, (str, int, float)) or v is None:
                row[k] = v
        trending_rows.append(row)

    # If none of the known shapes appeared, surface a helpful message
    if not eps_rows and not rev_rows and "estimates" not in data:
        message = data.get("Note") or data.get("Information") or data.get("Error Message")
        raise RuntimeError(f"Unexpected estimates response for {symbol}. Message: {message or json.dumps(data)[:500]}")

    return eps_rows, rev_rows, trending_rows


def write_estimates_csv(
    symbol: str, eps_rows: list[dict], rev_rows: list[dict], trending_rows: list[dict]
) -> Tuple[Optional[Path], Optional[Path], Optional[Path]]:
    today_str = date.today().strftime("%Y-%m-%d")
    eps_path: Optional[Path] = None
    rev_path: Optional[Path] = None
    trending_path: Optional[Path] = None

    if eps_rows:
        eps_path = OUTPUT_DIR / f"{symbol}_earnings_estimates_quarterly_2023-01-01_{today_str}.csv"
        with eps_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["symbol", "fiscalDateEnding", "estimatedEPS", "numberAnalystsEstimated"],
            )
            writer.writeheader()
            for row in eps_rows:
                writer.writerow(row)

    if rev_rows:
        rev_path = OUTPUT_DIR / f"{symbol}_revenue_estimates_quarterly_2023-01-01_{today_str}.csv"
        with rev_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["symbol", "fiscalDateEnding", "revenueEstimate", "numberAnalystsEstimated"],
            )
            writer.writeheader()
            for row in rev_rows:
                writer.writerow(row)

    if trending_rows:
        # Build a stable union of keys across rows
        keys = {"symbol"}
        for r in trending_rows:
            keys.update(r.keys())
        # Sort for determinism: symbol first, then date/horizon if present, then others
        ordered = ["symbol"]
        for k in ("date", "horizon"):
            if k in keys and k not in ordered:
                ordered.append(k)
        for k in sorted(keys - set(ordered)):
            ordered.append(k)
        trending_path = OUTPUT_DIR / f"{symbol}_earnings_estimates_trending_2023-01-01_{today_str}.csv"
        with trending_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=ordered)
            writer.writeheader()
            for row in trending_rows:
                writer.writerow(row)

    return eps_path, rev_path, trending_path


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    all_ok = True
    for idx, symbol in enumerate(SYMBOLS):
        try:
            rows = fetch_quarterly_earnings(symbol)
            out_file = write_csv(symbol, rows)
            print(f"Wrote {len(rows)} rows to {out_file}")
            eps_rows, rev_rows, trending_rows = fetch_estimates(symbol)
            eps_path, rev_path, trending_path = write_estimates_csv(symbol, eps_rows, rev_rows, trending_rows)
            if eps_path:
                print(f"Wrote {len(eps_rows)} rows to {eps_path}")
            if rev_path:
                print(f"Wrote {len(rev_rows)} rows to {rev_path}")
            if trending_path:
                print(f"Wrote {len(trending_rows)} rows to {trending_path}")
        except Exception as exc:
            all_ok = False
            print(f"Failed for {symbol}: {exc}", file=sys.stderr)

        # be polite to API (free tier has tight limits)
        if idx < len(SYMBOLS) - 1:
            time.sleep(12)

    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())


