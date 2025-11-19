import os
import json
import requests
from urllib.parse import urlencode
from datetime import datetime
from pathlib import Path

# CryptoNews API 基址
API_BASE = "https://cryptonews-api.com/api/v1"

# 支持环境变量覆盖：CRYPTO_NEWS_TOKEN；否则使用默认占位
TOKEN = os.getenv("CRYPTO_NEWS_TOKEN", "mw7d3y7slvfvimpfrhbumewtxpxly1z9f3m6y9ko")  # ← 替换为你的真实加密新闻 API token

# 保存目录（Windows 路径）
SAVE_DIR = Path(r"I:\finance-agent\news_data\data\crypto")

def build_url(
    tickers="DOGE",               # 狗狗币
    items=50,
    page=1,
    date_range="11042024-11102024",  # 2024 大选周（ET）：11/04 ~ 11/10
    time_range="000000-235959",      # 全天（ET）
    search=None,
    source=None
):
    params = {
        "tickers": tickers,
        "items": str(items),
        "page": str(page),
        "date": date_range,      # CryptoNews 也支持与 StockNews 相同格式
        "time": time_range,      # 时区均为美国东部时间（ET）
        "token": TOKEN
    }
    if search:
        params["search"] = search
    if source:
        params["source"] = source
    return f"{API_BASE}?{urlencode(params)}"

def fetch_news(url, timeout=20):
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def print_brief(data, limit=10):
    items = data.get("data") or []
    print(f"Total returned: {len(items)}")
    for i, it in enumerate(items[:limit], 1):
        title = it.get("title")
        source = it.get("source")
        date = it.get("date")       # 已为 ET 展示
        link = it.get("news_url")
        print(f"{i}. [{source}] {title} | {date}")
        if link:
            print(f"   {link}")

def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)

def save_json(payload: dict, base_dir: Path, filename: str = None) -> Path:
    ensure_dir(base_dir)
    if not filename:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        filename = f"doge_news_2024_election_week_{ts}.json"
    fp = base_dir / filename
    with fp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return fp

if __name__ == "__main__":
    # 构建请求 URL（DOGE，2024 大选周，ET 全天）
    url = build_url(
        tickers="DOGE",
        items=100,
        page=3,
        # 可选过滤，按需启用：
        # search="ETF,Elon",
        # source="Reuters,CoinDesk"
    )
    print("=== 2024 大选周（ET） DOGE 新闻 ===")
    print("Request URL:", url)

    data = fetch_news(url)
    print_brief(data, limit=10)

    saved_path = save_json(data, SAVE_DIR)
    print(f"\nJSON 已保存到: {saved_path}")
