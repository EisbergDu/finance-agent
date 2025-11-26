import csv
import time
import datetime as dt
from typing import Dict, List, Optional
import requests
from pathlib import Path


API_KEY = "ZP30FX94JVIC7GZR"
BASE_URL = "https://www.alphavantage.co/query"

START_DATE = dt.date(2024, 1, 1)
END_DATE = dt.date(2025, 10, 31)


class AlphaVantageError(Exception):
    pass


def _request_with_retries(params: Dict[str, str], max_retries: int = 6) -> Dict:
    """
    Request helper with simple exponential backoff for API notes/rate limits.
    """
    backoff_seconds = 15
    for attempt in range(1, max_retries + 1):
        response = requests.get(BASE_URL, params=params, timeout=60)
        try:
            payload = response.json()
        except ValueError as exc:
            raise AlphaVantageError(f"Non-JSON response for params={params}: {exc}") from exc

        # Handle Alpha Vantage's rate-limit or informative notes
        note_msg = payload.get("Note") or payload.get("Information")
        if note_msg:
            if attempt == max_retries:
                raise AlphaVantageError(f"API rate/info message after {attempt} attempts: {note_msg}")
            time.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 120)
            continue

        if "Error Message" in payload:
            raise AlphaVantageError(f"API error for params={params}: {payload['Error Message']}")

        return payload

    raise AlphaVantageError("Exhausted retries without success.")


def _within_range(date_str: str) -> bool:
    d = dt.date.fromisoformat(date_str)
    return START_DATE <= d <= END_DATE


def fetch_stock_daily(symbol: str) -> List[Dict]:
    """
    Fetch TIME_SERIES_DAILY (non-adjusted) for an equity symbol (e.g., NVDA, KO).
    """
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "full",
        "datatype": "json",
        "apikey": API_KEY,
    }
    payload = _request_with_retries(params)
    if "Time Series (Daily)" not in payload:
        raise AlphaVantageError(f"Missing 'Time Series (Daily)' for {symbol}. Response keys: {list(payload.keys())}")

    series = payload["Time Series (Daily)"]
    records: List[Dict] = []
    for date_str, row in series.items():
        if not _within_range(date_str):
            continue
        records.append(
            {
                "date": date_str,
                "symbol": symbol,
                "asset_type": "equity",
                "open": float(row["1. open"]),
                "high": float(row["2. high"]),
                "low": float(row["3. low"]),
                "close": float(row["4. close"]),
                "volume": float(row["5. volume"]),
            }
        )

    # Ensure sorted by date ascending
    records.sort(key=lambda r: r["date"])
    return records


def fetch_crypto_daily(symbol: str, market: str = "USD") -> List[Dict]:
    """
    Fetch DIGITAL_CURRENCY_DAILY for a crypto (e.g., BTC, DOGE) in a given market (USD).
    """
    params = {
        "function": "DIGITAL_CURRENCY_DAILY",
        "symbol": symbol,
        "market": market,
        "apikey": API_KEY,
    }
    payload = _request_with_retries(params)
    key = "Time Series (Digital Currency Daily)"
    if key not in payload:
        raise AlphaVantageError(f"Missing '{key}' for {symbol}. Response keys: {list(payload.keys())}")

    series = payload[key]
    records: List[Dict] = []
    for date_str, row in series.items():
        if not _within_range(date_str):
            continue
        # Alpha Vantage provides USD-specific OHLC keys like "1a. open (USD)"
        open_key = "1a. open (USD)"
        high_key = "2a. high (USD)"
        low_key = "3a. low (USD)"
        close_key = "4a. close (USD)"
        volume_key = "5. volume"  # native units of the crypto

        # Some libraries mirror with "1b/2b/3b/4b" keys; prefer the "a" USD keys if present
        try:
            open_px = float(row[open_key])
            high_px = float(row[high_key])
            low_px = float(row[low_key])
            close_px = float(row[close_key])
        except KeyError:
            # Fallback in case only non-USD keys appear (unlikely with market=USD)
            open_px = float(row.get("1. open", row.get("1b. open (USD)")))
            high_px = float(row.get("2. high", row.get("2b. high (USD)")))
            low_px = float(row.get("3. low", row.get("3b. low (USD)")))
            close_px = float(row.get("4. close", row.get("4b. close (USD)")))

        vol_val = row.get(volume_key)
        volume: Optional[float] = float(vol_val) if vol_val is not None else None

        records.append(
            {
                "date": date_str,
                "symbol": f"{symbol}-{market}",
                "asset_type": "crypto",
                "open": open_px,
                "high": high_px,
                "low": low_px,
                "close": close_px,
                "volume": volume,
            }
        )

    records.sort(key=lambda r: r["date"])
    return records


def fetch_fx_daily(from_symbol: str, to_symbol: str) -> List[Dict]:
    """
    Fetch FX_DAILY for a currency pair (e.g., XAU/USD for gold).
    Volume is not provided for FX; set to None.
    """
    params = {
        "function": "FX_DAILY",
        "from_symbol": from_symbol,
        "to_symbol": to_symbol,
        "outputsize": "full",
        "apikey": API_KEY,
    }
    payload = _request_with_retries(params)
    key = "Time Series FX (Daily)"
    if key not in payload:
        raise AlphaVantageError(f"Missing '{key}' for {from_symbol}{to_symbol}. Response keys: {list(payload.keys())}")

    series = payload[key]
    pair_symbol = f"{from_symbol}{to_symbol}"
    records: List[Dict] = []
    for date_str, row in series.items():
        if not _within_range(date_str):
            continue
        records.append(
            {
                "date": date_str,
                "symbol": pair_symbol,
                "asset_type": "fx",
                "open": float(row["1. open"]),
                "high": float(row["2. high"]),
                "low": float(row["3. low"]),
                "close": float(row["4. close"]),
                "volume": None,
            }
        )

    records.sort(key=lambda r: r["date"])
    return records


def write_csv(path: str, rows: List[Dict]) -> None:
    fieldnames = ["date", "symbol", "asset_type", "open", "high", "low", "close", "volume"]
    path_obj = Path(path)
    # Ensure parent directory exists
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    with path_obj.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main() -> None:
    # Fetch each asset
    all_records: List[Dict] = []
    out_dir = Path(__file__).resolve().parent

    # Equities
    nvda = fetch_stock_daily("NVDA")
    write_csv(str(out_dir / "NVDA_daily_2024-01-01_2025-10-31.csv"), nvda)
    all_records.extend(nvda)
    time.sleep(15)  # be polite to 5 req/min limit

    ko = fetch_stock_daily("KO")
    write_csv(str(out_dir / "KO_daily_2024-01-01_2025-10-31.csv"), ko)
    all_records.extend(ko)
    time.sleep(15)

    # SPY (S&P 500 ETF)
    spy = fetch_stock_daily("SPY")
    write_csv(str(out_dir / "SPY_daily_2024-01-01_2025-10-31.csv"), spy)
    all_records.extend(spy)
    time.sleep(15)

    # Crypto (market USD)
    btc = fetch_crypto_daily("BTC", market="USD")
    write_csv(str(out_dir / "BTC-USD_daily_2024-01-01_2025-10-31.csv"), btc)
    all_records.extend(btc)
    time.sleep(15)

    doge = fetch_crypto_daily("DOGE", market="USD")
    write_csv(str(out_dir / "DOGE-USD_daily_2024-01-01_2025-10-31.csv"), doge)
    all_records.extend(doge)
    time.sleep(15)

    # Gold: try FX_DAILY XAUUSD; if unsupported, fall back to GLD ETF as proxy
    try:
        xauusd = fetch_fx_daily("XAU", "USD")
        write_csv(str(out_dir / "XAUUSD_daily_2024-01-01_2025-10-31.csv"), xauusd)
        all_records.extend(xauusd)
    except AlphaVantageError as exc:
        print(f"[Info] XAU/USD via FX_DAILY unavailable on Alpha Vantage: {exc}. Falling back to GLD (ETF) daily.")
        time.sleep(15)
        gld = fetch_stock_daily("GLD")
        write_csv(str(out_dir / "GLD_daily_2024-01-01_2025-10-31.csv"), gld)
        all_records.extend(gld)

    # Combined
    write_csv(str(out_dir / "all_assets_daily_2024-01-01_2025-10-31.csv"), all_records)


if __name__ == "__main__":
    main()


