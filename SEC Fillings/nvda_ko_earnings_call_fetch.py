"""
Fetch earnings call transcripts for NVDA and KO from Seeking Alpha
between 2024-01-01 and 2025-10-31, and save each call as a .txt file.

Notes
-----
- This script scrapes public web pages on Seeking Alpha. Make sure this
  usage is consistent with their terms of service and be gentle with
  request frequency.
- Output directory is created in the same folder as this script.
"""

import time
from datetime import datetime
from pathlib import Path
from random import choice
from typing import List, Dict, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# As of late 2025, Seeking Alpha company earnings call transcripts are listed
# under the /earnings/transcripts path.
# We filter for titles containing \"Earnings Call Transcript\".
BASE_LIST_URL = "https://seekingalpha.com/symbol/{ticker}/earnings/transcripts"
BASE_ARTICLE_URL = "https://seekingalpha.com"

# Global rate limiting configuration (be gentle to the site)
MIN_SECONDS_BETWEEN_REQUESTS = 2.0
MAX_RETRIES = 3
BACKOFF_FACTOR = 2.0

# Reuse a single session for all requests
SESSION = requests.Session()

# Track last request timestamp (monotonic clock)
_LAST_REQUEST_TIME: float = 0.0

# Rotate over the same user agents used in the original notebook
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
    "Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201",
    "Mozilla/5.0 (Macintosh; Intel MaPc OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246",
]

START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 10, 31)

TICKERS: List[str] = ["NVDA", "KO"]

OUT_DIR = Path(__file__).resolve().parent / "nvda_ko_earnings_call_transcripts_2024_2025"


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _make_headers() -> Dict[str, str]:
    """
    Build headers with a randomly chosen User-Agent from the configured pool.
    """
    return {"User-Agent": choice(USER_AGENTS)}


def _rate_limited_get(
    url: str,
    *,
    params: Optional[Dict] = None,
    timeout: int = 30,
) -> requests.Response:
    """
    Perform a GET request with:
    - Global rate limiting via MIN_SECONDS_BETWEEN_REQUESTS
    - Simple retry with exponential backoff on transient errors
    """
    global _LAST_REQUEST_TIME

    for attempt in range(1, MAX_RETRIES + 1):
        # Enforce minimum spacing between all HTTP requests
        now = time.monotonic()
        wait_for = MIN_SECONDS_BETWEEN_REQUESTS - (now - _LAST_REQUEST_TIME)
        if wait_for > 0:
            time.sleep(wait_for)

        headers = _make_headers()

        try:
            resp = SESSION.get(
                url,
                headers=headers,
                params=params,
                timeout=timeout,
            )
            resp.raise_for_status()
            _LAST_REQUEST_TIME = time.monotonic()
            return resp
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                # Give up after the last retry
                raise

            # Exponential backoff before the next attempt
            backoff_seconds = BACKOFF_FACTOR * (2 ** (attempt - 1))
            print(
                f"[WARN] Request error on {url} (attempt {attempt}/{MAX_RETRIES}): "
                f"{exc}. Backing off for {backoff_seconds:.1f}s."
            )
            time.sleep(backoff_seconds)


def _get_soup(url: str, params: Optional[Dict] = None) -> BeautifulSoup:
    resp = _rate_limited_get(url, params=params, timeout=30)
    return BeautifulSoup(resp.text, "html.parser")


def list_earnings_call_transcripts_for_ticker(ticker: str, max_pages: int = 10) -> pd.DataFrame:
    """
    Crawl the transcripts listing pages for a single ticker and return
    all earnings-call transcripts within the configured date range.
    """
    records: List[Dict] = []

    for page in range(1, max_pages + 1):
        params = {"page": page} if page > 1 else None
        url = BASE_LIST_URL.format(ticker=ticker)
        print(f"[{ticker}] Fetching list page {page} ...")
        soup = _get_soup(url, params=params)

        articles = soup.find_all("article")
        if not articles:
            break

        page_had_any_in_range = False
        oldest_on_page: Optional[datetime] = None

        for art in articles:
            h3 = art.find("h3")
            if not h3:
                continue
            title = h3.get_text(strip=True)
            # Only keep earnings call transcripts
            if "Earnings Call Transcript" not in title:
                continue

            a = art.find("a")
            href = a["href"] if a and a.has_attr("href") else None
            if not href:
                continue

            span = art.find("span")
            date_text = span.get_text(strip=True) if span else ""
            try:
                d = pd.to_datetime(date_text, infer_datetime_format=True)
            except Exception:
                continue

            d_py = d.to_pydatetime()
            if oldest_on_page is None or d_py < oldest_on_page:
                oldest_on_page = d_py

            if START_DATE <= d_py <= END_DATE:
                page_had_any_in_range = True
                records.append(
                    {
                        "ticker": ticker,
                        "title": title,
                        "url": href,
                        "date": d,
                    }
                )

        # If the oldest transcript on this page is before our start date
        # and this page already has nothing in range, we can stop.
        if oldest_on_page is not None and oldest_on_page < START_DATE and not page_had_any_in_range:
            break

        # Be polite between pages
        time.sleep(1.0)

    return pd.DataFrame(records)


def fetch_transcript_text(relative_url: str) -> str:
    """
    Download the full transcript text from a Seeking Alpha article URL.
    """
    url = BASE_ARTICLE_URL + relative_url
    resp = _rate_limited_get(url, timeout=60)

    soup = BeautifulSoup(resp.text, "html.parser")
    ps = soup.find_all("p")
    lines = [p.get_text(strip=True) for p in ps]
    return "\n".join(lines)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    all_frames: List[pd.DataFrame] = []

    for ticker in TICKERS:
        df_t = list_earnings_call_transcripts_for_ticker(ticker)
        print(f"[{ticker}] Found {len(df_t)} earnings call transcripts in range.")
        if not df_t.empty:
            all_frames.append(df_t)

    if not all_frames:
        print("No transcripts found in the specified date range.")
        return

    df_all = pd.concat(all_frames, ignore_index=True)
    # Save metadata CSV for reference
    meta_path = OUT_DIR / "nvda_ko_earnings_call_metadata_2024_2025.csv"
    df_all.to_csv(meta_path, index=False)
    print(f"Saved metadata to {meta_path}")

    # Download each transcript as a .txt file
    for _, row in df_all.iterrows():
        rel_url = row["url"]
        date_str = row["date"].strftime("%Y-%m-%d")
        ticker = row["ticker"]
        fname = f"{ticker}_{date_str}_earnings_call.txt"
        out_path = OUT_DIR / fname

        if out_path.exists() and out_path.stat().st_size > 0:
            continue

        print(f"[{ticker}] Downloading transcript {rel_url} -> {fname}")
        try:
            text = fetch_transcript_text(rel_url)
        except Exception as exc:
            print(f"[WARN] Failed to fetch {rel_url}: {exc}")
            continue

        out_path.write_text(text, encoding="utf-8")
        time.sleep(1.0)  # be gentle to the site

    print(f"Done. Transcripts saved under {OUT_DIR}")


if __name__ == "__main__":
    main()


