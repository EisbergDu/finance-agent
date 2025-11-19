import os
import json
import requests
from urllib.parse import urlencode
from datetime import datetime
from pathlib import Path

API_BASE = "https://stocknewsapi.com/api/v1"
# 你已经内置了 Token；也可以用环境变量覆盖：STOCKNEWS_TOKEN
TOKEN = os.getenv("STOCKNEWS_TOKEN", "ea8w3dkncuzby3rot9oreutb5no33phj3uxzci9m")

# 目标保存目录（Windows 路径）
SAVE_DIR = Path(r"I:\\finance-agent\\news_data\\data\\stock")

def build_url(
    tickers="NVDA",
    items=50,
    page=1,
    date_range="11042024-11102024",  # 2024 大选周（ET）：11/04 ~ 11/10
    time_range="000000-235959",       # 全天（ET）
    search=None,
    source=None
):
    params = {
        "tickers": tickers,
        "items": str(items),
        "page": str(page),
        "date": date_range,
        "time": time_range,
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
        date = it.get("date")        # 已为 ET 展示
        link = it.get("news_url")
        print(f"{i}. [{source}] {title} | {date}")
        if link:
            print(f"   {link}")

def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)

def save_json(payload: dict, base_dir: Path, filename: str = None) -> Path:
    ensure_dir(base_dir)
    if not filename:
        # 生成一个带时间戳的文件名，避免覆盖
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        filename = f"nvda_news_2024_election_week_{ts}.json"
    fp = base_dir / filename
    with fp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return fp

if __name__ == "__main__":
    # 构建请求 URL
    url = build_url(
        tickers="NVDA",
        items=100,
        page=2,
        # 可选过滤（按需启用）：
        # search="AI,datacenter",
        # source="Reuters,Bloomberg"
    )
    print("=== 2024 大选周（ET） NVDA 新闻 ===")
    print("Request URL:", url)

    # 拉取数据
    data = fetch_news(url)
    print_brief(data, limit=10)

    # 保存到 I:\finance-agent\news_data\data\stock
    saved_path = save_json(data, SAVE_DIR)
    print(f"\nJSON 已保存到: {saved_path}")
