import os
import json
import re
from atproto import Client as BlueskyClient
from mastodon import Mastodon
import tweepy  # X (Twitter) 用

def parse_issue_body(body: str):
    # 前のバージョンと同じ（#1 [HN] 形式対応）
    print("=== Raw Issue Body Start ===")
    print(body)
    print("=== Raw Issue Body End ===")
    
    drafts = []
    blocks = re.split(r'(?=\n?#\d+\s*\[)', body.strip())
    print(f"Split into {len(blocks)} potential blocks")
    
    for i, block in enumerate(blocks):
        block = block.strip()
        if not block or not block.startswith('#'):
            continue
        print(f"\n--- Processing Block {i+1} ---")
        print(block)
        
        platform_match = re.search(r'#\d+\s*\[([^\]]+)\]', block, re.IGNORECASE)
        if platform_match:
            platform = platform_match.group(1).strip().upper()
            print(f"Found platform: {platform}")
        else:
            continue
        
        url_match = re.search(r'https?://[^\s\n]+', block)
        if url_match:
            target_url = url_match.group(0).rstrip('.').strip()
            print(f"Found URL: {target_url}")
        else:
            continue
        
        reply_match = re.search(r'返信文:\s*([\s\S]*?)(?=\n?#\d+|$)', block, re.IGNORECASE)
        if reply_match:
            reply_text = reply_match.group(1).strip()
            if reply_text:
                print(f"Found reply: {reply_text[:100]}...")
                drafts.append({
                    "platform": platform,
                    "target_url": target_url,
                    "reply": reply_text
                })
    
    print(f"\nParsed {len(drafts)} valid drafts")
    return drafts

def post_to_x(target_url: str, reply_text: str):
    consumer_key = os.environ.get('X_API_KEY')
    consumer_secret = os.environ.get('X_API_SECRET')
    access_token = os.environ.get('X_ACCESS_TOKEN')
    access_token_secret = os.environ.get('X_ACCESS_SECRET')
    print(f"X creds: consumer_key={'set' if consumer_key else 'missing'}, access_token={'set' if access_token else 'missing'}")
    if not all([consumer_key, consumer_secret, access_token, access_token_secret]):
        print("X credentials missing → skip")
        return False
    
    try:
        auth = tweepy.OAuth1UserHandler(consumer_key, consumer_secret, access_token, access_token_secret)
        api = tweepy.API(auth)
        print("X auth success")
        
        # URLからstatus ID抽出（例: https://x.com/user/status/123456）
        match = re.search(r'/status/(\d+)', target_url)
        if not match:
            print(f"Invalid X URL: {target_url}")
            return False
        status_id = match.group(1)
        
        api.update_status(status=reply_text, in_reply_to_status_id=int(status_id))
        print(f"X reply SUCCESS: {target_url}")
        return True
    except Exception as e:
        print(f"X ERROR: {str(e)}")
        return False

# Bluesky と Mastodon は前のまま

# main() でX対応追加
def main():
    # ... (前のmainと同じ)
    for i, d in enumerate(drafts, 1):
        print(f"\nProcessing {i}/{len(drafts)}: {d['platform']} - {d['target_url']}")
        platform = d['platform'].upper()
        url = d['target_url']
        text = d['reply']
        
        if platform in ['BLUESKY', 'BSKY']:
            if post_to_bluesky(url, text):
                success_count += 1
        elif platform in ['MASTODON', 'MSTD', 'MASTO']:
            if post_to_mastodon(url, text):
                success_count += 1
        elif platform in ['X', 'TWITTER']:
            if post_to_x(url, text):
                success_count += 1
        elif platform == 'HN':
            print(f"Skipping HN (no write API)")
        else:
            print(f"Unsupported: {platform}")
    
    print(f"\n=== FINAL: {success_count}/{len(drafts)} sent ===")

if __name__ == '__main__':
    main()
