import datetime as dt
from pathlib import Path

import yfinance as yf
import pandas as pd


START_DATE = dt.date(2024, 1, 1)
END_DATE = dt.date(2025, 10, 31)

# Yahoo Finance 上常见的 VIX 相关代码：
# - "^VIX"    : CBOE Volatility Index
# - "^VIXCLS" : CBOE Volatility Index (historical close)
VIX_TICKERS = ["^VIX", "^VIXCLS"]


def fetch_vix_daily(start: dt.date, end: dt.date) -> pd.DataFrame:
    """
    使用 yfinance 抓取 VIX 指数的日频数据（包含 OHLCV）。
    """
    # 逐个尝试常见 VIX 代码，以提高在不同地区/镜像下的成功率
    last_error: Exception | None = None
    data = None

    for ticker in VIX_TICKERS:
        try:
            # yfinance 终止日期是包含的，这里把 end+1 天作为上界
            data = yf.download(
                ticker,
                start=start.isoformat(),
                end=(end + dt.timedelta(days=1)).isoformat(),
                interval="1d",
                auto_adjust=False,
                progress=False,
            )
            # 某些情况下 yfinance 会打印错误但仍返回空 DataFrame，这里显式检查
            if data is not None and not data.empty:
                symbol_label = "VIX" if ticker == "^VIX" else ticker.lstrip("^")
                break
        except Exception as exc:  # 捕获网络/解析等异常，尝试下一个代码
            last_error = exc
            data = None

    if data is None or data.empty:
        msg = "未获取到任何 VIX 数据，请检查网络环境或 yfinance / Yahoo Finance 可用性。"
        if last_error is not None:
            msg += f" 最后一次错误信息：{last_error}"
        raise RuntimeError(msg)

    # 标准化列名并添加字段
    data = data.reset_index()  # Date 变成一列
    data["date"] = data["Date"].dt.strftime("%Y-%m-%d")
    # 统一使用 "VIX" 作为 symbol，便于后续分析；如需区分，可改为 symbol_label
    data["symbol"] = "VIX"
    data["asset_type"] = "index"

    data.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        },
        inplace=True,
    )

    # 按日期过滤一次，防止多余日期
    data = data[(data["date"] >= start.isoformat()) & (data["date"] <= end.isoformat())]

    # 排序并选择输出列
    data = data.sort_values("date")
    cols = ["date", "symbol", "asset_type", "open", "high", "low", "close", "adj_close", "volume"]
    return data[cols]


def main() -> None:
    out_dir = Path(__file__).resolve().parent
    df = fetch_vix_daily(START_DATE, END_DATE)

    out_path = out_dir / "VIX_daily_2024-01-01_2025-10-31.csv"
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"VIX data saved to: {out_path}")


if __name__ == "__main__":
    main()