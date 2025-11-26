import csv
import time
import datetime as dt
from typing import Dict, List
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
    通用请求函数：带简单指数退避，用于处理 Alpha Vantage 的限频 Note/Information。
    """
    backoff_seconds = 15
    for attempt in range(1, max_retries + 1):
        response = requests.get(BASE_URL, params=params, timeout=60)
        try:
            payload = response.json()
        except ValueError as exc:
            raise AlphaVantageError(f"Non-JSON response for params={params}: {exc}") from exc

        # Alpha Vantage 的限频或信息提示
        note_msg = payload.get("Note") or payload.get("Information")
        if note_msg:
            if attempt == max_retries:
                raise AlphaVantageError(
                    f"API rate/info message after {attempt} attempts: {note_msg}"
                )
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


def fetch_indicator(function_name: str, indicator_code: str, interval: str = "daily") -> List[Dict]:
    """
    通用经济指标抓取：INFLATION / UNEMPLOYMENT / FEDERAL_FUNDS_RATE 等。
    interval 可选 'daily' 或 'monthly'（视具体接口支持情况而定）。
    """
    params = {
        "function": function_name,
        "apikey": API_KEY,
        "interval": interval,
    }
    payload = _request_with_retries(params)

    if "data" not in payload:
        raise AlphaVantageError(
            f"Missing 'data' for function={function_name}. "
            f"Response keys: {list(payload.keys())}"
        )

    rows: List[Dict] = []
    for item in payload["data"]:
        date_str = item.get("date")
        value_str = item.get("value")
        if not date_str or value_str is None:
            continue
        if not _within_range(date_str):
            continue

        try:
            value = float(value_str)
        except (TypeError, ValueError):
            continue

        rows.append(
            {
                "date": date_str,
                "indicator": indicator_code,
                "value": value,
            }
        )

    # 按日期升序
    rows.sort(key=lambda r: r["date"])
    return rows


def write_macro_csv(path: Path, rows: List[Dict]) -> None:
    fieldnames = ["date", "indicator", "value"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def fetch_treasury_yield(maturity: str = "10year", interval: str = "daily") -> List[Dict]:
    """
    单独封装美国国债收益率 (TREASURY_YIELD) 抓取逻辑。

    maturity 取值示例：'3month', '2year', '5year', '7year', '10year', '30year'
    interval 可选 'daily' 或 'monthly'。
    """
    params = {
        "function": "TREASURY_YIELD",
        "apikey": API_KEY,
        "interval": interval,
        "maturity": maturity,
    }
    payload = _request_with_retries(params)

    if "data" not in payload:
        raise AlphaVantageError(
            f"Missing 'data' for TREASURY_YIELD. Response keys: {list(payload.keys())}"
        )

    rows: List[Dict] = []
    for item in payload["data"]:
        date_str = item.get("date")
        value_str = item.get("value")
        if not date_str or value_str is None:
            continue
        if not _within_range(date_str):
            continue

        try:
            value = float(value_str)
        except (TypeError, ValueError):
            continue

        rows.append(
            {
                "date": date_str,
                "indicator": f"TREASURY_YIELD_{maturity}",
                "value": value,
            }
        )

    rows.sort(key=lambda r: r["date"])
    return rows


def main() -> None:
    out_dir = Path(__file__).resolve().parent

    # 通胀 (INFLATION, daily)
    inflation = fetch_indicator("INFLATION", "INFLATION", interval="daily")
    write_macro_csv(
        out_dir / "INFLATION_daily_2024-01-01_2025-10-31.csv",
        inflation,
    )
    print(f"Wrote {len(inflation)} rows of INFLATION.")
    time.sleep(15)  # 遵守 5 次/分钟的限频

    # 失业率 (UNEMPLOYMENT, daily)
    unemployment = fetch_indicator("UNEMPLOYMENT", "UNEMPLOYMENT", interval="daily")
    write_macro_csv(
        out_dir / "UNEMPLOYMENT_daily_2024-01-01_2025-10-31.csv",
        unemployment,
    )
    print(f"Wrote {len(unemployment)} rows of UNEMPLOYMENT.")
    time.sleep(15)

    # 利率：联邦基金目标利率 (FEDERAL_FUNDS_RATE, daily)
    fed_funds = fetch_indicator("FEDERAL_FUNDS_RATE", "FEDERAL_FUNDS_RATE", interval="daily")
    write_macro_csv(
        out_dir / "FEDERAL_FUNDS_RATE_daily_2024-01-01_2025-10-31.csv",
        fed_funds,
    )
    print(f"Wrote {len(fed_funds)} rows of FEDERAL_FUNDS_RATE.")

    time.sleep(15)

    # 国债收益率：例如 10 年期国债 (TREASURY_YIELD, daily)
    tsy_10y = fetch_treasury_yield(maturity="10year", interval="daily")
    write_macro_csv(
        out_dir / "TREASURY_YIELD_10year_daily_2024-01-01_2025-10-31.csv",
        tsy_10y,
    )
    print(f"Wrote {len(tsy_10y)} rows of TREASURY_YIELD_10year.")


if __name__ == "__main__":
    main()