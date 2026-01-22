import os
import re
import json
import time
import datetime
from typing import List, Dict, Any, Tuple, Optional

import requests
from openai import OpenAI

# ====== 重要 ======
# これは「返信の自動投稿」はしません。
# SNS上の投稿を拾って「手動返信用の下書き」を GitHub Issues に出すだけです。
# ==================

UA = "goliath-reply-drafter/1.0"
TIMEOUT = 20

ROOT = "goliath"
DB_PATH = f"{ROOT}/db.json"

# ---------------------------
# Utils
# ---------------------------

def now_utc_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def read_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def clip(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[:n].rstrip() + "…"

def safe_json_load(s: str) -> Optional[dict]:
    try:
        return json.loads(s)
    except Exception:
        # ```json ... ``` みたいなのが混じるケースを雑に救う
        s2 = re.sub(r"^```[a-zA-Z]*\s*", "", (s or "").strip())
        s2 = re.sub(r"\s*```$", "", s2.strip())
        try:
            return json.loads(s2)
        except Exception:
            return None

def create_github_issue(title: str, body: str):
    pat = os.getenv("GH_PAT", "").strip()
    repo = os.getenv("GITHUB_REPOSITORY", "").strip()
    if not pat or not repo:
        return
    url = f"https://api.github.com/repos/{repo}/issues"
    headers = {"Authorization": f"token {pat}", "Accept": "application/vnd.github+json", "User-Agent": UA}
    payload = {"title": title, "body": body}
    try:
        requests.post(url, headers=headers, json=payload, timeout=TIMEOUT)
    except Exception:
        pass

# ---------------------------
# Collectors (HN / Bluesky / Mastodon / X optional)
#   ※ ここは「投稿を拾う」だけ。返信は作らない。
# ---------------------------

DEFAULT_QUERIES = [
    "how do i", "how to", "error", "issue", "problem", "can't", "doesn't work",
    "convert", "calculator", "compare", "template", "timezone", "subscription",
]

def _days_ago_ts(days: int) -> int:
    dt = datetime.datetime.utcnow() - datetime.timedelta(days=days)
    return int(dt.timestamp())

def _dedup(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for it in items:
        u = (it.get("url") or "").strip()
        t = (it.get("text") or "").strip()
        if not u or not t:
            continue
        key = u + "|" + t[:160]
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out

def collect_hn(queries: List[str], days_back: int, limit_per_query: int) -> List[Dict[str, str]]:
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    min_ts = _days_ago_ts(days_back)
    out: List[Dict[str, str]] = []

    url = "https://hn.algolia.com/api/v1/search_by_date"
    for q in queries:
        params = {
            "query": q,
            "tags": "(story,comment)",
            "numericFilters": f"created_at_i>{min_ts}",
            "hitsPerPage": str(limit_per_query),
        }
        try:
            r = session.get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue

        for h in (data.get("hits") or []):
            created_at = h.get("created_at") or ""
            title = (h.get("title") or "").strip()
            story_title = (h.get("story_title") or "").strip()
            comment_text = (h.get("comment_text") or "").strip()
            text = title or story_title or comment_text
            if not text:
                continue

            object_id = h.get("objectID")
            if not object_id:
                continue
            hn_url = f"https://news.ycombinator.com/item?id={object_id}"
            out.append({"platform": "hn", "thread_url": hn_url, "post_text": text, "created_at": created_at})
        time.sleep(0.2)

    return _dedup([{"platform": x["platform"], "thread_url": x["thread_url"], "post_text": x["post_text"]} for x in out])

def collect_bluesky(queries: List[str], limit_per_query: int) -> List[Dict[str, str]]:
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    base = "https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts"
    out: List[Dict[str, str]] = []

    for q in queries:
        params = {"q": q, "limit": str(limit_per_query)}
        try:
            r = session.get(base, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue

        for p in (data.get("posts") or []):
            record = p.get("record") or {}
            text = str(record.get("text") or "").strip()
            if not text:
                continue
            uri = p.get("uri") or ""
            author = p.get("author") or {}
            handle = author.get("handle") or ""
            rkey = ""
            if uri and "/app.bsky.feed.post/" in uri:
                rkey = uri.split("/app.bsky.feed.post/")[-1]
            if handle and rkey:
                url = f"https://bsky.app/profile/{handle}/post/{rkey}"
            else:
                url = uri or "https://bsky.app/"
            out.append({"platform": "bluesky", "thread_url": url, "post_text": text})
        time.sleep(0.2)

    return _dedup(out)

def collect_mastodon(queries: List[str], limit_per_query: int) -> List[Dict[str, str]]:
    api_base = (os.getenv("MASTODON_API_BASE") or "").strip().rstrip("/")
    token = (os.getenv("MASTODON_ACCESS_TOKEN") or "").strip()
    if not api_base or not token:
        return []

    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Authorization": f"Bearer {token}"})
    out: List[Dict[str, str]] = []

    for q in queries:
        url = f"{api_base}/api/v2/search"
        params = {"q": q, "type": "statuses", "limit": str(limit_per_query), "resolve": "false"}
        try:
            r = session.get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue

        for s in (data.get("statuses") or []):
            content = (s.get("content") or "").strip()
            if not content:
                continue
            # contentはHTMLなので雑にタグ除去
            txt = content.replace("<br />", "\n").replace("<br/>", "\n").replace("<br>", "\n")
            txt = re.sub(r"<[^>]+>", "", txt).strip()
            if not txt:
                continue
            url2 = (s.get("url") or "").strip()
            if not url2:
                continue
            out.append({"platform": "mastodon", "thread_url": url2, "post_text": txt})
        time.sleep(0.2)

    return _dedup(out)

def collect_x(queries: List[str], limit_per_query: int) -> List[Dict[str, str]]:
    # Bearerが無ければ黙ってスキップ（= ここで落とさない）
    bearer = (os.getenv("X_BEARER_TOKEN") or "").strip()
    if not bearer:
        return []

    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Authorization": f"Bearer {bearer}"})

    out: List[Dict[str, str]] = []
    url = "https://api.x.com/2/tweets/search/recent"
    for q in queries:
        params = {"query": q, "max_results": str(min(limit_per_query, 100)), "tweet.fields": "created_at"}
        try:
            r = session.get(url, params=params, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue

        for t in (data.get("data") or []):
            tid = t.get("id")
            text = (t.get("text") or "").strip()
            if not tid or not text:
                continue
            out.append({"platform": "x", "thread_url": f"https://x.com/i/web/status/{tid}", "post_text": text})
        time.sleep(0.2)

    return _dedup(out)

def collect_threads(days_back: int, total_limit: int, per_query: int) -> List[Dict[str, str]]:
    srcs = (os.getenv("COLLECT_SOURCES") or "hn,bluesky,mastodon,x").lower().split(",")
    srcs = [s.strip() for s in srcs if s.strip()]

    qenv = (os.getenv("COLLECT_QUERIES") or "").strip()
    queries = [x.strip() for x in qenv.split(",") if x.strip()] if qenv else DEFAULT_QUERIES

    items: List[Dict[str, str]] = []
    if "hn" in srcs:
        items += collect_hn(queries, days_back=days_back, limit_per_query=per_query)
    if "bluesky" in srcs:
        items += collect_bluesky(queries, limit_per_query=per_query)
    if "mastodon" in srcs:
        items += collect_mastodon(queries, limit_per_query=per_query)
    if "x" in srcs:
        items += collect_x(queries, limit_per_query=per_query)

    items = _dedup(items)
    return items[:total_limit]

# ---------------------------
# Load tools (あなたのサイトの既存ツール一覧)
#   ここは goliath/db.json を “ツール棚” として使う想定
# ---------------------------

def load_tool_catalog(limit: int = 200) -> List[Dict[str, str]]:
    db = read_json(DB_PATH, [])
    out = []
    for e in db[:limit]:
        title = (e.get("title") or "").strip()
        url = (e.get("public_url") or "").strip()
        if title and url:
            out.append({"title": title, "url": url, "tags": e.get("tags", [])})
    return out

# ---------------------------
# OpenAI: match tool + draft reply (手動返信用)
# ---------------------------

def openai_match_and_draft(
    client: OpenAI,
    platform: str,
    thread_url: str,
    post_text: str,
    tools: List[Dict[str, str]],
) -> Optional[Dict[str, Any]]:
    """
    返り値は必ず target_url を含める（あなたの指定）
    """
    tools_compact = tools[:120]  # トークン節約
    prompt = f"""
You are drafting a helpful, natural reply for a person asking for help on social media.
This is NOT auto-posted. The user will manually send it.

STRICT OUTPUT:
Return ONLY valid JSON (no markdown, no commentary).

Inputs:
- platform: {platform}
- target_url: {thread_url}
- post_text: {post_text}

Available tools (title + url):
{json.dumps(tools_compact, ensure_ascii=False)}

Task:
1) Choose the SINGLE best matching tool from the list above (or choose none if truly irrelevant).
2) Draft a short, friendly reply in the same language as the post_text if possible.
3) The reply must NOT be spammy. One suggestion, one link, no aggressive marketing.
4) Put the chosen tool URL at the end of the reply on a new line.

Return JSON schema:
{{
  "tool_title": "string (empty if none)",
  "tool_url": "string (empty if none)",
  "reply": "string (empty if none)"
}}
""".strip()

    try:
        res = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4o"),
            messages=[{"role": "user", "content": prompt}],
        )
        raw = res.choices[0].message.content or ""
    except Exception:
        return None

    obj = safe_json_load(raw)
    if not obj:
        return None

    tool_title = (obj.get("tool_title") or "").strip()
    tool_url = (obj.get("tool_url") or "").strip()
    reply = (obj.get("reply") or "").strip()

    # tool_url が無い/短すぎるなら無効扱い
    if not tool_url or not reply:
        return None

    # ===== ここがあなたの指定(1): target_url にして必ず返す =====
    return {
        "platform": platform,
        "target_url": thread_url,  # ← 名前を明確化
        "post_excerpt": clip(post_text, 200),
        "tool_title": tool_title,
        "tool_url": tool_url,
        "reply": reply,
    }

# ---------------------------
# main
# ---------------------------

def main():
    # 収集設定（envで上書き可能）
    days_back = int(os.getenv("COLLECT_DAYS_BACK", "365"))
    total_limit = int(os.getenv("COLLECT_TOTAL_LIMIT", "30"))
    per_query = int(os.getenv("COLLECT_PER_QUERY", "8"))

    # 返信下書きの最大件数（暴走防止）
    max_drafts = int(os.getenv("DRAFT_MAX", "10"))

    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        create_github_issue(
            title="[Goliath] Missing OPENAI_API_KEY",
            body="OPENAI_API_KEY is not set in secrets/env."
        )
        return

    client = OpenAI(api_key=api_key)

    # 1) 投稿（悩み）を拾う
    threads = collect_threads(days_back=days_back, total_limit=total_limit, per_query=per_query)
    if not threads:
        create_github_issue(
            title="[Goliath] 0 threads collected",
            body="Collector returned 0 items. Check COLLECT_SOURCES / tokens / instance settings."
        )
        return

    # 2) あなたの既存ツール棚を読む
    tools = load_tool_catalog(limit=200)
    if not tools:
        create_github_issue(
            title="[Goliath] Tool catalog is empty",
            body=f"{DB_PATH} is empty or missing. Run your tool builder first so db.json has entries."
        )
        return

    # 3) 返信下書きを作る
    drafts: List[Dict[str, Any]] = []
    for t in threads:
        if len(drafts) >= max_drafts:
            break
        platform = t.get("platform", "").strip()
        thread_url = t.get("thread_url", "").strip()
        post_text = t.get("post_text", "").strip()
        if not platform or not thread_url or not post_text:
            continue

        d = openai_match_and_draft(client, platform, thread_url, post_text, tools)
        if d:
            drafts.append(d)
        time.sleep(0.3)

    if not drafts:
        create_github_issue(
            title="[Goliath] 0 reply drafts produced",
            body="OpenAI matching returned no usable drafts. Try adjusting COLLECT_QUERIES or increase COLLECT_TOTAL_LIMIT."
        )
        return

    # 4) Issues本文をあなたの指定どおりに出す
    lines = []
    lines.append(f"generated_at: {now_utc_iso()}")
    lines.append(f"collected_threads: {len(threads)}")
    lines.append(f"drafts: {len(drafts)}")
    lines.append("")
    lines.append("FORMAT:")
    lines.append("TARGET_URL: <url>")
    lines.append("REPLY_DRAFT:")
    lines.append("<text>")
    lines.append("")

    # ===== ここがあなたの指定(2): Issues本文フォーマット =====
    for i, d in enumerate(drafts, 1):
        lines.append("---")
        lines.append(f"{i}) [{d['platform']}]")
        lines.append(f"TARGET_URL: {d['target_url']}")
        lines.append("")
        lines.append("REPLY_DRAFT:")
        lines.append(d["reply"])
        lines.append("")
        # 補助情報（必要なら残す。不要なら消してOK）
        lines.append(f"TOOL: {d.get('tool_title','')}")
        lines.append(f"TOOL_URL: {d.get('tool_url','')}")
        lines.append("")
        lines.append(f"POST_EXCERPT: {d.get('post_excerpt','')}")
        lines.append("")

    create_github_issue(
        title="[Goliath] Reply drafts (manual) — target_url + reply + tool url",
        body="\n".join(lines).rstrip() + "\n"
    )

if __name__ == "__main__":
    main()

    main()
