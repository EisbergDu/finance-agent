import requests
import json
import time
from datetime import datetime

apikey = "q4fa83ok43io70najdgmijdt2s6fkl"
community_id = "1502929685792976898"

def extract_tweets(data):
    """从响应数据中提取推文信息"""
    tweets = []
    
    try:
        instructions = data['data']['communityResults']['result']['ranked_community_timeline']['timeline']['instructions']
        
        for instruction in instructions:
            if 'entries' in instruction:
                for entry in instruction['entries']:
                    # 跳过 cursor 条目
                    if entry.get('content', {}).get('cursorType'):
                        continue
                    
                    # 提取推文内容
                    content = entry.get('content', {})
                    
                    # 处理 TimelineTimelineItem
                    if content.get('__typename') == 'TimelineTimelineItem':
                        item_content = content.get('itemContent', {})
                        tweet_results = item_content.get('tweet_results', {}).get('result', {})
                        
                        # 处理 TweetWithVisibilityResults
                        if tweet_results.get('__typename') == 'TweetWithVisibilityResults':
                            tweet = tweet_results.get('tweet', {})
                        else:
                            tweet = tweet_results
                        
                        # 提取推文信息
                        if 'legacy' in tweet:
                            legacy = tweet['legacy']
                            full_text = legacy.get('full_text', '')
                            created_at = legacy.get('created_at', '')
                            
                            # 提取作者信息
                            author_name = ''
                            author_screen_name = ''
                            if 'core' in tweet and 'user_results' in tweet['core']:
                                user = tweet['core']['user_results'].get('result', {})
                                if 'legacy' in user:
                                    author_name = user['legacy'].get('name', '')
                                    author_screen_name = user['legacy'].get('screen_name', '')
                            
                            if full_text:  # 只添加有文本的推文
                                tweets.append({
                                    'full_text': full_text,
                                    'author_name': author_name,
                                    'author_screen_name': author_screen_name,
                                    'created_at': created_at
                                })
    
    except Exception as e:
        print(f"提取推文时出错: {e}")
    
    return tweets

def get_bottom_cursor(data):
    """提取 Bottom cursor"""
    try:
        instructions = data['data']['communityResults']['result']['ranked_community_timeline']['timeline']['instructions']
        for instruction in instructions:
            if 'entries' in instruction:
                for entry in instruction['entries']:
                    if entry.get('content', {}).get('cursorType') == 'Bottom':
                        return entry['content']['value']
    except Exception as e:
        print(f"提取 cursor 时出错: {e}")
    
    return None

def save_tweets(tweets, filename='community_tweets.json'):
    """保存推文到JSON文件"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(tweets, f, ensure_ascii=False, indent=2)
        print(f"  💾 已保存 {len(tweets)} 条推文到 {filename}")
        return True
    except Exception as e:
        print(f"  ❌ 保存失败: {e}")
        return False

def fetch_community_tweets(target_count=1000, delay=2, save_interval=100):
    """获取指定数量的社区推文
    
    Args:
        target_count: 目标推文数量
        delay: 每次请求之间的延迟秒数（默认2秒）
        save_interval: 每获取多少条推文自动保存一次（默认100条）
    """
    all_tweets = []
    cursor = None
    page = 1
    last_save_count = 0  # 记录上次保存时的数量
    
    url = "https://api.apidance.pro/graphql/CommunityTweetsTimeline"
    headers = {'apikey': apikey}
    output_file = 'community_tweets.json'
    
    while len(all_tweets) < target_count:
        # 构建请求参数
        variables = {
            "communityId": community_id,
            "count": 20,
            "displayLocation": "Community",
            "rankingMode": "Recency",
            "withCommunity": True
        }
        
        if cursor:
            variables['cursor'] = cursor
        
        params = {"variables": json.dumps(variables)}
        
        print(f"正在获取第 {page} 页...")
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            # 提取推文
            tweets = extract_tweets(data)
            all_tweets.extend(tweets)
            
            print(f"  ✓ 第 {page} 页获取 {len(tweets)} 条推文 (总计: {len(all_tweets)})")
            
            # ⭐ 每隔 save_interval 条自动保存
            if len(all_tweets) - last_save_count >= save_interval:
                save_tweets(all_tweets, output_file)
                last_save_count = len(all_tweets)
            
            # 获取下一页的 cursor
            cursor = get_bottom_cursor(data)
            
            if not cursor:
                print("没有更多数据了")
                # ⭐ 最后保存一次
                save_tweets(all_tweets, output_file)
                break
            
            page += 1
            
            # 如果已经达到目标数量,停止
            if len(all_tweets) >= target_count:
                # ⭐ 达到目标后保存
                save_tweets(all_tweets[:target_count], output_file)
                break
            
            # 添加延迟，避免触发速率限制
            if len(all_tweets) < target_count:
                print(f"  ⏳ 等待 {delay} 秒...")
                time.sleep(delay)
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"  ⚠️  触发速率限制 (429)，等待 60 秒后重试...")
                # ⭐ 遇到429错误时保存
                save_tweets(all_tweets, output_file)
                time.sleep(60)
                continue  # 重试当前请求
            else:
                print(f"请求出错: {e}")
                # ⭐ 遇到其他错误时保存
                save_tweets(all_tweets, output_file)
                break
        except KeyboardInterrupt:
            print("\n\n⚠️  用户中断程序")
            # ⭐ 用户中断时保存
            save_tweets(all_tweets, output_file)
            break
        except Exception as e:
            print(f"请求出错: {e}")
            # ⭐ 遇到任何错误时保存
            save_tweets(all_tweets, output_file)
            break
    
    # 只返回目标数量的推文
    return all_tweets[:target_count]

# 执行获取
print("开始获取推文数据...\n")
print("💡 提示: 按 Ctrl+C 可以随时中断并保存当前数据\n")

tweets = fetch_community_tweets(
    target_count=1000, 
    delay=10,           # 每次请求间隔5秒
    save_interval=100  # 每获取100条自动保存一次
)

print(f"\n" + "="*50)
print(f"✅ 完成！总共获取了 {len(tweets)} 条推文")
print(f"📁 数据已保存到 community_tweets.json")
print("="*50)

# 显示前几条示例
if tweets:
    print("\n前 3 条推文示例:")
    for i, tweet in enumerate(tweets[:3], 1):
        print(f"\n--- 推文 {i} ---")
        print(f"作者: {tweet['author_name']} (@{tweet['author_screen_name']})")
        print(f"时间: {tweet['created_at']}")
        print(f"内容: {tweet['full_text'][:100]}...")
