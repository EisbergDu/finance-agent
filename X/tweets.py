import pandas as pd
import numpy as np
import requests
import json
import os
import time
import random
import re
import http.client
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# 配置日志
log_dir = r'I:\finance-agent\X\logs'
os.makedirs(log_dir, exist_ok=True)

log_filename = os.path.join(log_dir, f'tweets_fetch_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()  # 同时输出到控制台
    ]
)

logger = logging.getLogger(__name__)


def get_user_rest_id(user_name):
    """获取用户的rest_id"""
    try:
        conn = http.client.HTTPSConnection("api.apidance.pro")
        headers = {'apikey': 'q4fa83ok43io70najdgmijdt2s6fkl'}
        
        url = f"/graphql/UserByScreenName?variables=%7B%22screen_name%22:%22{user_name}%22,%22withSafetyModeUserFields%22:true,%22withHighlightedLabel%22:true%7D"
        conn.request("GET", url, '', headers)
        
        res = conn.getresponse()
        data = res.read()
        json_data = json.loads(data.decode("utf-8"))
        rest_id = json_data['data']['user']['result']['rest_id']
        
        logger.info(f"✓ 获取用户 @{user_name} 的 rest_id: {rest_id}")
        return rest_id
        
    except Exception as e:
        logger.error(f"✗ 获取用户 @{user_name} 的 rest_id 失败: {str(e)}")
        return None


def get_user_tweets_page(user_id, cursor=None):
    """获取单页用户推文"""
    conn = http.client.HTTPSConnection("api.apidance.pro")
    headers = {'apikey': 'q4fa83ok43io70najdgmijdt2s6fkl'}
    
    url = f"/sapi/UserTweets?user_id={user_id}"
    if cursor:
        url += f"&cursor={cursor}"
    else:
        url += "&cursor=null"
    
    conn.request("GET", url, '', headers)
    res = conn.getresponse()
    data = res.read()
    return json.loads(data.decode("utf-8"))


def fetch_user_tweets(user_id, user_name, max_pages=20):
    """
    获取用户的多页推文
    
    参数:
        user_id (str): Twitter用户ID
        user_name (str): Twitter用户名
        max_pages (int): 最多获取的页数，默认20页
    
    返回:
        list: 所有推文的列表
    """
    all_tweets = []
    cursor = None
    page = 1
    
    logger.info(f"开始获取用户 @{user_name} (ID: {user_id}) 的推文，最多 {max_pages} 页")
    
    while page <= max_pages:
        try:
            response = get_user_tweets_page(user_id, cursor)
            tweets = response.get('tweets', [])
            all_tweets.extend(tweets)
            
            logger.info(f"@{user_name} 第 {page} 页: 获取 {len(tweets)} 条，累计 {len(all_tweets)} 条")
            
            next_cursor = response.get('next_cursor_str')
            
            if not next_cursor:
                logger.info(f"@{user_name} 没有更多推文了")
                break
            
            cursor = next_cursor
            page += 1
            
            # 减少延迟，提高速度
            time.sleep(0.3)
                
        except Exception as e:
            logger.error(f"@{user_name} 获取第 {page} 页时出错: {str(e)}")
            break
    
    logger.info(f"@{user_name} 总共获取 {len(all_tweets)} 条推文")
    return all_tweets


def save_tweets(user_name, tweets, save_dir=r'I:\finance-agent\X\tweets_json'):
    """保存推文到文件"""
    try:
        os.makedirs(save_dir, exist_ok=True)
        filename = os.path.join(save_dir, f'{user_name}.json')
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(tweets, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✓ @{user_name} 推文已保存到 {filename}")
        return True
        
    except Exception as e:
        logger.error(f"✗ @{user_name} 保存失败: {str(e)}")
        return False


def process_single_user(user_name, max_pages=20):
    """处理单个用户（获取+保存）"""
    try:
        logger.info(f"{'='*60}")
        logger.info(f"开始处理用户: @{user_name}")
        logger.info(f"{'='*60}")
        
        # 获取 rest_id
        user_id = get_user_rest_id(user_name)
        if not user_id:
            logger.error(f"@{user_name} 无法获取 rest_id，跳过")
            return False
        
        # 获取推文
        tweets = fetch_user_tweets(user_id, user_name, max_pages)
        
        if not tweets:
            logger.warning(f"@{user_name} 没有获取到推文")
            return False
        
        # 保存推文
        success = save_tweets(user_name, tweets)
        
        logger.info(f"{'='*60}")
        logger.info(f"@{user_name} 处理完成")
        logger.info(f"{'='*60}\n")
        
        return success
        
    except Exception as e:
        logger.error(f"@{user_name} 处理失败: {str(e)}")
        return False


def batch_fetch_tweets_sequential(user_list, max_pages=20):
    """顺序批量获取（原始方式）"""
    logger.info(f"开始顺序处理 {len(user_list)} 个用户")
    start_time = time.time()
    
    success_count = 0
    fail_count = 0
    
    for i, user in enumerate(user_list, 1):
        logger.info(f"\n进度: {i}/{len(user_list)}")
        
        if process_single_user(user, max_pages):
            success_count += 1
        else:
            fail_count += 1
    
    elapsed_time = time.time() - start_time
    
    logger.info(f"\n{'='*60}")
    logger.info(f"批量处理完成！")
    logger.info(f"成功: {success_count}, 失败: {fail_count}")
    logger.info(f"总耗时: {elapsed_time:.2f} 秒")
    logger.info(f"{'='*60}")


def batch_fetch_tweets_parallel(user_list, max_pages=20, max_workers=5):
    """并行批量获取（加速版）"""
    logger.info(f"开始并行处理 {len(user_list)} 个用户，线程数: {max_workers}")
    start_time = time.time()
    
    success_count = 0
    fail_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_user = {
            executor.submit(process_single_user, user, max_pages): user 
            for user in user_list
        }
        
        # 处理完成的任务
        for i, future in enumerate(as_completed(future_to_user), 1):
            user = future_to_user[future]
            try:
                if future.result():
                    success_count += 1
                else:
                    fail_count += 1
                    
                logger.info(f"进度: {i}/{len(user_list)} (@{user} 完成)")
                
            except Exception as e:
                fail_count += 1
                logger.error(f"@{user} 处理异常: {str(e)}")
    
    elapsed_time = time.time() - start_time
    
    logger.info(f"\n{'='*60}")
    logger.info(f"并行处理完成！")
    logger.info(f"成功: {success_count}, 失败: {fail_count}")
    logger.info(f"总耗时: {elapsed_time:.2f} 秒")
    logger.info(f"平均每个用户: {elapsed_time/len(user_list):.2f} 秒")
    logger.info(f"{'='*60}")


# ==================== 主程序 ====================

if __name__ == "__main__":
    # 读取用户列表
    logger.info("读取 a_kol.xlsx 文件...")
    a_kol_df = pd.read_excel('I:\\finance-agent\\X\\a_kol.xlsx')
    a_kol_list = a_kol_df['Twitter Handle'].tolist()
    a_kol_list = [account.lstrip('@') for account in a_kol_list]
    
    logger.info(f"共读取 {len(a_kol_list)} 个用户")
    
    # 选择处理方式
    test_users = a_kol_list[:10]  # 测试前10个用户
    
    # 方式1: 顺序处理（慢但稳定）
    # batch_fetch_tweets_sequential(test_users, max_pages=20)
    
    # 方式2: 并行处理（快但可能触发限流）
    # max_workers=3: 同时处理3个用户（推荐）
    # max_workers=5: 同时处理5个用户（较快，但可能触发限流）
    batch_fetch_tweets_parallel(test_users, max_pages=20, max_workers=3)
    
    logger.info("所有任务完成！")
