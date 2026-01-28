#!/usr/bin/env python3
import os
import re
import json
import requests
import argparse
from urllib.parse import urlparse

INDEXNOW_DEFAULT_ENDPOINT = "https://api.indexnow.org/indexnow"  # IndexNow共通エンドポイント :contentReference[oaicite:4]{index=4}

def normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    # 末尾に付く括弧や句読点を軽く除去
    return u.rstrip(").,]}>\"'")

def extract_forward_last(issue_body: str) -> str:
    if not issue_body:
        return ""
    matches = re.findall(r"forward\s*:\s*(https?://\S+)", issue_body, flags=re.IGNORECASE)
    matches = [normalize_url(x) for x in matches if normalize_url(x)]
    return matches[-1] if matches else ""

def same_host(url: str, base: str) -> bool:
    try:
        a = urlparse(url)
        b = urlparse(base)
        return (a.scheme in ("http", "https")) and (a.netloc.lower() == b.netloc.lower())
    except Exception:
        return False

def gh_comment(repo: str, issue_number: int, body: str, token: str):
    if not token:
        return False, 0, "GH_TOKEN empty"
    api = f"https://api.github.com/repos/{repo}/issues/{issue_number}/comments"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "goliath-issue-router-indexnow",
    }
    r = requests.post(api, headers=headers, json={"body": body}, timeout=25)
    ok = 200 <= r.status_code < 300
    return ok, r.status_code, r.text

def indexnow_submit(endpoint: str, key: str, key_location: str, urls: list[str]):
    # POST JSON: urlList / key / keyLocation :contentReference[oaicite:5]{index=5}
    payload = {
        "host": urlparse(key_location).netloc,   # 任意だが入れておくと親切
        "key": key,
        "keyLocation": key_location,
        "urlList": urls,
    }
    headers = {"Content-Type": "application/json"}
    r = requests.post(endpoint, headers=headers, data=json.dumps(payload), timeout=25)
    return r.status_code, (r.text or "").strip()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--do-comment", action="store_true")
    args = ap.parse_args()

    title = os.getenv("ISSUE_TITLE", "")
    issue_body = os.getenv("ISSUE_BODY", "")
    issue_number = int(os.getenv("ISSUE_NUMBER", "0") or "0")
    repo = os.getenv("REPO", "")
    gh_token = os.getenv("GH_TOKEN", "")

    # 安全ゲート：候補Issue以外は触らない
    if not title.startswith("Goliath candidates"):
        print("Skip: not a Goliath candidates issue")
        return 0

    public_base = (os.getenv("PUBLIC_BASE_URL") or "").strip().rstrip("/")
    key = (os.getenv("INDEXNOW_KEY") or "").strip()
    endpoint = (os.getenv("INDEXNOW_ENDPOINT") or "").strip() or INDEXNOW_DEFAULT_ENDPOINT

    if not public_base:
        msg = "IndexNow: PUBLIC_BASE_URL が未設定なので停止"
        if args.do_comment:
            gh_comment(repo, issue_number, msg, gh_token)
        print(msg)
        return 0

    if not key:
        msg = "IndexNow: INDEXNOW_KEY が未設定（Secretsに追加して）"
        if args.do_comment:
            gh_comment(repo, issue_number, msg, gh_token)
        print(msg)
        return 0

    forward_url = extract_forward_last(issue_body)
    if not forward_url:
        msg = "IndexNow: forward: URL が見つからないので停止（Issue本文に forward: を入れて）"
        if args.do_comment:
            gh_comment(repo, issue_number, msg, gh_token)
        print(msg)
        return 0

    # IndexNowは「同一ホスト」縛りが強い。ホスト違いなら422/403になりやすい :contentReference[oaicite:6]{index=6}
    if not same_host(forward_url, public_base):
        msg = (
            "IndexNow: forward URL のホストが PUBLIC_BASE_URL と違うので停止\n"
            f"- forward: {forward_url}\n"
            f"- base: {public_base}\n"
            "（同一ドメインのURLだけ送って）"
        )
        if args.do_comment:
            gh_comment(repo, issue_number, msg, gh_token)
        print(msg)
        return 0

    # keyLocation は「公開で見える場所」を指す必要あり（{key}.txtを置く） :contentReference[oaicite:7]{index=7}
    key_location = f"{public_base}/{key}.txt"

    status, text = indexnow_submit(endpoint, key, key_location, [forward_url])

    ok = 200 <= status < 300
    lines = []
    lines.append("IndexNow (Layer1)")
    lines.append(f"- forward(last): {forward_url}")
    lines.append(f"- endpoint: {endpoint}")
    lines.append(f"- keyLocation: {key_location}")
    lines.append(f"- status: {status}")
    lines.append(f"- result: {'OK' if ok else 'FAILED'}")
    if text:
        lines.append(f"- response: {text[:300]}")
    if status == 429:
        lines.append("- note: 429は投げすぎ。間隔を空ける/バッチ送信にする :contentReference[oaicite:8]{index=8}")
    body = "\n".join(lines)

    if args.do_comment:
        gh_comment(repo, issue_number, body, gh_token)

    print(body)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
