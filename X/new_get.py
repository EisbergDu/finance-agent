# -*- coding: utf-8 -*-
"""
使用 /sapi/UserTweets + “耐艹版”逻辑
对 a_kol.xlsx 里的所有 KOL：
- 每个账号用 UserTweets 一页一页往前翻
- 每页内部多次重试
- 统计每个账号：
    - 本次抓到的总 tweets 数
    - 最早一条 tweet 时间
    - 最晚一条 tweet 时间
    - 是否已经翻到 2024-01-01 及以前
- 输出一个 Excel：kol_usertweets_window.xlsx

另外：
- SAVE_TWEETS = True       -> 保存每个账号的 tweets 为一个 JSON 文件
- SAVE_EVERY_PAGE = True   -> 每翻完一页就覆盖写入一次（中途挂了也有部分数据）
"""

import json
import http.client
import time
import logging
import random
from datetime import datetime
from typing import Optional, Dict, Any, List

import pandas as pd
import os
import traceback

# ==================== 全局配置 ====================

API_HOST = "api.apidance.pro"
API_KEY = "q4fa83ok43io70najdgmijdt2s6fkl"

MAX_PAGES_HARD = 5000            # 每个用户最多翻 500 页（上限，防止死循环）
MAX_RETRIES_PER_PAGE = 10        # 每一页最多重试次数
BASE_SLEEP_BETWEEN_PAGES = 3  # 页与页之间基础延迟（秒）
EXTRA_JITTER = 0.6              # 额外随机抖动（秒）

COVER_TARGET_DATE = datetime(2024, 1, 1)  # 判断是否覆盖到 2024-01-01

# 是否保存 tweets
SAVE_TWEETS = True          # 抓完后保存每个用户的 tweets
SAVE_EVERY_PAGE = True      # 每翻完一页就覆盖写一次（增量持久化）

# 路径配置（按你之前的结构来）
KOL_EXCEL_PATH = r"I:\finance-agent\X\a_kol.xlsx"
OUT_SUMMARY_PATH = r"I:\finance-agent\X\kol_usertweets_window.xlsx"
TWEETS_DIR = r"I:\finance-agent\X\tweets_json"
LOG_DIR = r"I:\finance-agent\X\logs"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(TWEETS_DIR, exist_ok=True)

log_filename = os.path.join(
    LOG_DIR, f"kol_usertweets_window_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ==================== 工具函数 ====================

def get_user_rest_id(user_name: str, max_retries: int = 3) -> Optional[str]:
    """通过 screen_name 获取用户 rest_id，带简单重试。"""
    for attempt in range(1, max_retries + 1):
        try:
            conn = http.client.HTTPSConnection(API_HOST, timeout=30)
            headers = {'apikey': API_KEY}
            url = (
                "/graphql/UserByScreenName?"
                "variables=%7B%22screen_name%22:%22" + user_name +
                "%22,%22withSafetyModeUserFields%22:true,%22withHighlightedLabel%22:true%7D"
            )
            conn.request("GET", url, '', headers)
            res = conn.getresponse()
            data = res.read()
            json_data = json.loads(data.decode("utf-8"))
            rest_id = json_data['data']['user']['result']['rest_id']
            logger.info(f"✓ 获取用户 @{user_name} 的 rest_id: {rest_id}")
            return rest_id
        except Exception as e:
            logger.error(f"✗ 获取用户 @{user_name} 的 rest_id 失败 (第 {attempt} 次): {e}")
            logger.debug(traceback.format_exc())
            if attempt < max_retries:
                wait = 2 ** attempt + random.uniform(0.5, 2.0)
                logger.info(f"等待 {wait:.2f} 秒后重试获取 rest_id ...")
                time.sleep(wait)
            else:
                logger.error(f"多次尝试仍然无法获取 @{user_name} 的 rest_id")
                return None
    return None


def get_user_tweets_page(user_id: str, cursor: Optional[str] = None) -> Dict[str, Any]:
    """获取单页用户推文（按 API 原样返回，一次请求）"""
    conn = http.client.HTTPSConnection(API_HOST, timeout=30)
    headers = {'apikey': API_KEY}
    url = f"/sapi/UserTweets?user_id={user_id}"
    if cursor:
        url += f"&cursor={cursor}"
    else:
        url += "&cursor=null"
    conn.request("GET", url, '', headers)
    res = conn.getresponse()
    data = res.read()
    text = data.decode("utf-8")
    try:
        return json.loads(text)
    except Exception as e:
        # 如果 JSON 解析失败，也打印一点原始内容帮助排查
        logger.error(f"解析 JSON 失败: {e}, 原始响应前 200 字符: {text[:200]}")
        raise


def parse_tweet_datetime(tweet: Dict[str, Any]) -> Optional[datetime]:
    """
    按你给的样例：
    "created_at": "Sat Dec 06 11:50:19 +0000 2025"
    """
    created_at_str = tweet.get("created_at")
    if not created_at_str:
        return None
    try:
        dt = datetime.strptime(created_at_str, "%a %b %d %H:%M:%S %z %Y")
        return dt.replace(tzinfo=None)
    except Exception as e:
        logger.warning(f"解析 created_at 失败: {created_at_str} ({e})")
        return None


def save_user_tweets(user_name: str, tweets: List[Dict[str, Any]]):
    """把某个用户当前已抓到的 tweets 全量保存（覆盖写）"""
    try:
        filename = os.path.join(TWEETS_DIR, f"{user_name}.json")
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(tweets, f, ensure_ascii=False, indent=2)
        logger.info(f"[@{user_name}] 当前已抓到 {len(tweets)} 条 tweets，已保存到 {filename}")
    except Exception as e:
        logger.error(f"[@{user_name}] 保存 tweets 到文件失败: {e}")
        logger.debug(traceback.format_exc())


# ==================== 核心：单个用户的窗口测试 ====================

def analyze_single_user_window(user_name: str) -> Dict[str, Any]:
    """
    用“耐艹版”逻辑，对单个用户做 UserTweets 翻页：
    - 每页内部多次重试
    - 记录总 tweets 数、最早时间、最晚时间
    - 每页结束后可选择保存当前已抓到的所有 tweets
    返回一个 summary dict
    """
    summary = {
        "user_name": user_name,
        "user_id": None,
        "total_tweets": 0,            # 本次抓到的条数
        "earliest_dt": None,          # datetime
        "latest_dt": None,            # datetime
        "earliest_dt_str": None,
        "latest_dt_str": None,
        "covers_2024_01_or_earlier": False,
        "pages_reached": 0,
        "remark": ""
    }

    logger.info("=" * 60)
    logger.info(f"开始处理用户: @{user_name}")
    logger.info("=" * 60)

    user_id = get_user_rest_id(user_name)
    if not user_id:
        summary["remark"] = "rest_id 获取失败"
        return summary

    summary["user_id"] = user_id

    all_tweets: List[Dict[str, Any]] = []
    cursor = None
    page = 1
    min_dt = None
    max_dt = None

    while page <= MAX_PAGES_HARD:
        logger.info(f"=== @{user_name} 第 {page} 页 ===")

        tweets = None
        resp = None

        # ---------- 这一页内部重试 ----------
        for attempt in range(1, MAX_RETRIES_PER_PAGE + 1):
            try:
                logger.info(f"请求第 {page} 页，第 {attempt} 次尝试...")
                resp = get_user_tweets_page(user_id, cursor)
                tweets = resp.get("tweets")

                if tweets is None:
                    logger.warning(
                        f"第 {page} 页，第 {attempt} 次：响应中没有 'tweets' 字段，准备重试..."
                    )
                elif len(tweets) == 0:
                    logger.warning(
                        f"第 {page} 页，第 {attempt} 次：tweets 数量为 0，"
                        "可能触发限流 / 窗口尽头，准备重试..."
                    )
                else:
                    logger.info(
                        f"第 {page} 页，第 {attempt} 次：成功拿到 {len(tweets)} 条 tweets"
                    )
                    break

            except Exception as e:
                logger.error(
                    f"第 {page} 页，第 {attempt} 次请求出错：{e}，准备稍后重试..."
                )
                logger.debug(traceback.format_exc())

            # 只有失败才 sleep
            sleep_sec = BASE_SLEEP_BETWEEN_PAGES * attempt + random.uniform(0, EXTRA_JITTER)
            logger.info(f"第 {page} 页，第 {attempt} 次失败后，睡眠 {sleep_sec:.2f} 秒再试")
            time.sleep(sleep_sec)

        # ---------- 重试结束后的判断 ----------
        if not tweets:
            logger.info(
                f"第 {page} 页在重试 {MAX_RETRIES_PER_PAGE} 次后仍然没有有效 tweets，"
                "认为已经达到该用户在当前状态下的可见历史尽头 / 或严重限流，停止。"
            )
            break

        # 正常处理这一页
        logger.info(f"第 {page} 页最终拿到 tweets 数量：{len(tweets)}")
        summary["pages_reached"] = page

        for tw in tweets:
            dt = parse_tweet_datetime(tw)
            if dt is None:
                continue
            if (min_dt is None) or (dt < min_dt):
                min_dt = dt
            if (max_dt is None) or (dt > max_dt):
                max_dt = dt

        all_tweets.extend(tweets)
        total_now = len(all_tweets)
        logger.info(f"当前已累计 tweets 数：{total_now}")
        logger.info(f"当前时间范围大致为：[{min_dt}  ~  {max_dt}]")

        # ✅ 每页之后就保存一次（增量覆盖写）
        if SAVE_TWEETS and SAVE_EVERY_PAGE:
            save_user_tweets(user_name, all_tweets)

        # 处理 next_cursor
        next_cursor = resp.get("next_cursor_str") if isinstance(resp, dict) else None
        if not next_cursor:
            logger.info("@%s 没有 next_cursor 了，API 不再给更多历史。" % user_name)
            break

        cursor = next_cursor
        page += 1

        # 页与页之间也加一点延迟 + 抖动
        sleep_sec = BASE_SLEEP_BETWEEN_PAGES + random.uniform(0, EXTRA_JITTER)
        logger.info(f"页与页之间睡眠 {sleep_sec:.2f} 秒，防止请求过于频繁")
        time.sleep(sleep_sec)

    # ---------- 汇总结果 ----------
    summary["total_tweets"] = len(all_tweets)
    summary["earliest_dt"] = min_dt
    summary["latest_dt"] = max_dt

    if min_dt:
        summary["earliest_dt_str"] = min_dt.strftime("%Y-%m-%d %H:%M:%S")
        summary["covers_2024_01_or_earlier"] = (min_dt <= COVER_TARGET_DATE)
    if max_dt:
        summary["latest_dt_str"] = max_dt.strftime("%Y-%m-%d %H:%M:%S")

    if len(all_tweets) == 0:
        summary["remark"] = "未获取到任何 tweet（多次重试后仍为空）"
    else:
        if summary["covers_2024_01_or_earlier"]:
            summary["remark"] = "最早时间 ≤ 2024-01-01 ✅"
        else:
            summary["remark"] = "最早时间 > 2024-01-01 ❌"

    # ✅ 最终再保存一次“完整版”
    if SAVE_TWEETS and len(all_tweets) > 0:
        save_user_tweets(user_name, all_tweets)

    logger.info(
        f"@{user_name} 统计完成：total={summary['total_tweets']}, "
        f"earliest={summary['earliest_dt_str']}, latest={summary['latest_dt_str']}, "
        f"covers_2024_01={summary['covers_2024_01_or_earlier']}, pages={summary['pages_reached']}"
    )
    logger.info("=" * 60 + "\n")

    return summary


# ==================== 主函数：批量跑 a_kol ====================

def main():
    logger.info("读取 a_kol.xlsx 文件...")
    df_kol = pd.read_excel(KOL_EXCEL_PATH)
    handles_raw = df_kol["Twitter Handle"].tolist()
    # 去掉 @ 并 strip
    handles = [str(h).lstrip("@").strip() for h in handles_raw]

    logger.info(f"共读取 {len(handles)} 个账号")

    summaries: List[Dict[str, Any]] = []
    start_all = time.time()

    # 你可以先只测前几个账号
    # handles_to_test = handles[:5]
    handles_to_test = handles

    for i, name in enumerate(handles_to_test, 1):
        logger.info(f"整体进度：{i}/{len(handles_to_test)} (@{name})")
        try:
            summary = analyze_single_user_window(name)
        except Exception as e:
            logger.error(f"处理 @{name} 时发生未捕获异常: {e}")
            logger.error(traceback.format_exc())
            summary = {
                "user_name": name,
                "user_id": None,
                "total_tweets": 0,
                "earliest_dt": None,
                "latest_dt": None,
                "earliest_dt_str": None,
                "latest_dt_str": None,
                "covers_2024_01_or_earlier": False,
                "pages_reached": 0,
                "remark": f"未捕获异常: {e}"
            }
        summaries.append(summary)

        # 每个用户之间再稍微休息一下，避免太猛
        time.sleep(1.0)

    elapsed = time.time() - start_all
    logger.info(f"所有账号处理完成，总耗时 {elapsed:.2f} 秒")

    # 汇总成 DataFrame 输出
    df_summary = pd.DataFrame(summaries)
    cols_order = [
        "user_name", "user_id", "total_tweets",
        "earliest_dt_str", "latest_dt_str",
        "covers_2024_01_or_earlier", "pages_reached", "remark"
    ]
    df_summary = df_summary[cols_order]

    df_summary.to_excel(OUT_SUMMARY_PATH, index=False)
    logger.info(f"汇总结果已保存到：{OUT_SUMMARY_PATH}")


if __name__ == "__main__":
    main()
