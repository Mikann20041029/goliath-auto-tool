import os
import json
import re
from urllib.parse import urlparse
from atproto import Client as BlueskyClient
from mastodon import Mastodon

def parse_issue_body(body: str):
    """Issue body をパースしてドラフトリストを返す"""
    drafts = []
    blocks = re.split(r'-{3,}', body)  # "---" 以上で区切る
    current = {}
    
    for block in blocks:
        block = block.strip()
        if not block:
            continue
            
        # プラットフォーム行を探す (例: 1) [BLUESKY])
        platform_match = re.search(r'\d+\)\s*\[([^\]]+)\]', block)
        if platform_match:
            current = {"platform": platform_match.group(1).strip().upper()}
        
        # TARGET_URL
        url_match = re.search(r'TARGET_URL:\s*(https?://[^\s]+)', block, re.IGNORECASE)
        if url_match and current:
            current["target_url"] = url_match.group(1).strip()
        
        # REPLY_DRAFT（複数行対応）
        reply_match = re.search(r'REPLY_DRAFT:\s*([\s\S]*?)(?=\n\d+\)|\n---|$)', block, re.IGNORECASE)
        if reply_match and current.get("target_url"):
            reply_text = reply_match.group(1).strip()
            if reply_text:
                current["reply"] = reply_text
                drafts.append(current.copy())
                current = {}  # リセット
    
    return drafts

def post_to_bluesky(target_url: str, reply_text: str):
    handle = os.environ.get('BLUESKY_HANDLE')
    app_password = os.environ.get('BLUESKY_APP_PASSWORD')
    if not handle or not app_password:
        print("Bluesky credentials missing → skip")
        return False
    
    try:
        client = BlueskyClient()
        client.login(handle, app_password)
        
        # URLからpost情報を抽出
        match = re.search(r'/profile/([^/]+)/post/([^/]+)', target_url)
        if not match:
            print(f"Invalid Bluesky URL: {target_url}")
            return False
        
        author_handle = match.group(1)
        rkey = match.group(2)
        
        # 対象ポストのURIを構築
        root_uri = f"at://{author_handle}/app.bsky.feed.post/{rkey}"
        
        # リプライ投稿
        client.send_post(
            text=reply_text,
            reply_to={'root': {'uri': root_uri, 'cid': None}, 'parent': {'uri': root_uri, 'cid': None}}
        )
        print(f"Bluesky reply sent: {target_url}")
        return True
    except Exception as e:
        print(f"Bluesky error ({target_url}): {e}")
        return False

def post_to_mastodon(target_url: str, reply_text: str):
    access_token = os.environ.get('MASTODON_ACCESS_TOKEN')
    instance_url = os.environ.get('MASTODON_INSTANCE_URL')
    if not access_token or not instance_url:
        print("Mastodon credentials missing → skip")
        return False
    
    try:
        mastodon = Mastodon(
            access_token=access_token,
            api_base_url=instance_url.rstrip('/')
        )
        
        # URLからstatus ID抽出（最後がID）
        status_id = target_url.split('/')[-1]
        if not status_id.isdigit():
            print(f"Invalid Mastodon status ID: {target_url}")
            return False
        
        mastodon.status_post(status=reply_text, in_reply_to_id=status_id)
        print(f"Mastodon reply sent: {target_url}")
        return True
    except Exception as e:
        print(f"Mastodon error ({target_url}): {e}")
        return False

def main():
    # GitHub Actions のイベントペイロード取得
    event_path = os.environ.get('GITHUB_EVENT_PATH')
    if not event_path:
        print("No event path")
        return
    
    with open(event_path, 'r') as f:
        event = json.load(f)
    
    issue_body = event.get('issue', {}).get('body', '')
    if not issue_body:
        print("Empty issue body")
        return
    
    drafts = parse_issue_body(issue_body)
    if not drafts:
        print("No valid drafts found")
        return
    
    print(f"Found {len(drafts)} drafts")
    
    success_count = 0
    for d in drafts:
        platform = d.get('platform', '').upper()
        url = d.get('target_url', '')
        text = d.get('reply', '')
        
        if not url or not text:
            print("Missing url or text → skip")
            continue
        
        if platform == 'BLUESKY':
            if post_to_bluesky(url, text):
                success_count += 1
        elif platform in ['MASTODON', 'MSTD']:  # 表記揺れ対策
            if post_to_mastodon(url, text):
                success_count += 1
        else:
            print(f"Unsupported platform: {platform}")
    
    print(f"Completed: {success_count}/{len(drafts)} replies sent")

if __name__ == '__main__':
    main()
