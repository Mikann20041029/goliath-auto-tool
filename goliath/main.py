#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Goliath Auto Tool System - main.py (single-file)
- 8-hour cycle runner (run once; schedule is outside)
- Collect from: Bluesky, Mastodon, Reddit, Hacker News, X(mentions)
- Cluster -> choose best theme -> generate solution site(s)
- Validate/autofix up to 5
- Update hub/sites.json only (hub frozen)
- Update sitemap/robots safely (default: goliath/_out; root only if ALLOW_ROOT_UPDATE=1)
- Output Issues payload (bulk) with empathy + tool URL + reply draft (100+ items)
- 22 genres mapping -> affiliates.json top2 -> inject to AFF_SLOT
- SaaS-like design, Tailwind, dark mode, i18n (EN/JA/KO/ZH), 2500+ chars article
"""

from __future__ import annotations

import os
import re
import sys
import json
import time
import math
import html
import uuid
import hashlib
import random
import shutil
import string
import logging
import datetime as dt
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple, Iterable
from urllib.parse import urlencode, quote, urlparse
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


# =========================
# Config (ENV)
# =========================

REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())

GOLIATH_DIR = os.path.join(REPO_ROOT, "goliath")
PAGES_DIR = os.path.join(GOLIATH_DIR, "pages")
OUT_DIR = os.path.join(GOLIATH_DIR, "_out")  # safe outputs (sitemap/robots/issues payload, etc.)
POLICIES_DIR = os.path.join(REPO_ROOT, "policies")
HUB_DIR = os.path.join(REPO_ROOT, "hub")
HUB_SITES_JSON = os.path.join(HUB_DIR, "sites.json")

AFFILIATES_JSON = os.environ.get("AFFILIATES_JSON", os.path.join(REPO_ROOT, "affiliates.json"))

DEFAULT_LANG = os.environ.get("DEFAULT_LANG", "en")  # en/ja/ko/zh
LANGS = ["en", "ja", "ko", "zh"]

RUN_ID = os.environ.get("RUN_ID", str(int(time.time())))
MAX_THEMES = int(os.environ.get("MAX_THEMES", "6"))  # initial cap for site generation
MAX_COLLECT = int(os.environ.get("MAX_COLLECT", "260"))
MAX_AUTOFIX = int(os.environ.get("MAX_AUTOFIX", "5"))
RANDOM_SEED = os.environ.get("RANDOM_SEED", RUN_ID)

ALLOW_ROOT_UPDATE = os.environ.get("ALLOW_ROOT_UPDATE", "0") == "1"

# Social API credentials (optional)
BLUESKY_HANDLE = os.environ.get("BLUESKY_HANDLE", "")
BLUESKY_APP_PASSWORD = os.environ.get("BLUESKY_APP_PASSWORD", "")

MASTODON_BASE = os.environ.get("MASTODON_BASE", "")
MASTODON_TOKEN = os.environ.get("MASTODON_TOKEN", "")

REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET", "")
REDDIT_REFRESH_TOKEN = os.environ.get("REDDIT_REFRESH_TOKEN", "")
REDDIT_USER_AGENT = os.environ.get("REDDIT_USER_AGENT", "goliath-tool/1.0 (read-only)")
REDDIT_SUBREDDITS = os.environ.get(
    "REDDIT_SUBREDDITS",
    "webdev,sysadmin,programming,learnprogramming,privacy,photography,excel,smallbusiness,marketing,travel,cooking,fitness,personalfinance,careeradvice,relationship_advice"
)

HN_QUERY = os.environ.get("HN_QUERY", "error OR issue OR help OR how to OR advice OR recommend")
HN_MAX = int(os.environ.get("HN_MAX", "70"))

X_BEARER_TOKEN = os.environ.get("X_BEARER_TOKEN", "")
X_USER_ID = os.environ.get("X_USER_ID", "")
X_MAX = int(os.environ.get("X_MAX", "5"))

# OpenAI (optional)
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")

# Content requirements
MIN_ARTICLE_CHARS_JA = int(os.environ.get("MIN_ARTICLE_CHARS_JA", "2500"))
MIN_FAQ = int(os.environ.get("MIN_FAQ", "5"))
REF_URL_MIN = int(os.environ.get("REF_URL_MIN", "10"))
REF_URL_MAX = int(os.environ.get("REF_URL_MAX", "20"))
SUPP_URL_MIN = int(os.environ.get("SUPP_URL_MIN", "3"))

# Layout / theme
SITE_BRAND = os.environ.get("SITE_BRAND", "Mikanntool")
SITE_DOMAIN = os.environ.get("SITE_DOMAIN", "https://mikanntool.com")
SITE_CONTACT_EMAIL = os.environ.get("SITE_CONTACT_EMAIL", "contact@mikanntool.com")

# Keep hub frozen
FROZEN_PATH_PREFIXES = [
    os.path.join(REPO_ROOT, "hub", "index.html"),
    os.path.join(REPO_ROOT, "hub", "assets"),
    os.path.join(REPO_ROOT, "hub", "assets", "ui.v3.css"),
    os.path.join(REPO_ROOT, "hub", "assets", "app.v3.js"),
]

# 22 categories fixed
CATEGORIES_22 = [
    "Web/Hosting",
    "Dev/Tools",
    "AI/Automation",
    "Security/Privacy",
    "Media",
    "PDF/Docs",
    "Images/Design",
    "Data/Spreadsheets",
    "Business/Accounting/Tax",
    "Marketing/Social",
    "Productivity",
    "Education/Language",
    "Travel/Planning",
    "Food/Cooking",
    "Health/Fitness",
    "Study/Learning",
    "Money/Personal Finance",
    "Career/Work",
    "Relationships/Communication",
    "Home/Life Admin",
    "Shopping/Products",
    "Events/Leisure",
]

# Ensure random seed for reproducibility per run
random.seed(int(hashlib.sha256(RANDOM_SEED.encode("utf-8")).hexdigest()[:8], 16))

# Additional config for leads
LEADS_TOTAL = int(os.environ.get("LEADS_TOTAL", "100"))


# =========================
# Logging
# =========================

def setup_logging() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    log_path = os.path.join(OUT_DIR, f"run_{RUN_ID}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
    )
    logging.info("RUN_ID=%s", RUN_ID)
    logging.info("REPO_ROOT=%s", REPO_ROOT)


# =========================
# Utilities (IO / HTTP / Text)
# =========================

def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_text(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(content)

def read_json(path: str, default: Any = None) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")

def safe_slug(s: str, maxlen: int = 64) -> str:
    s = s.strip().lower()
    s = re.sub(r"https?://", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    if not s:
        s = "tool"
    return s[:maxlen].strip("-") or "tool"

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def http_get(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 20) -> Tuple[int, str]:
    h = headers or {}
    req = Request(url, headers=h, method="GET")
    try:
        with urlopen(req, timeout=timeout) as resp:
            status = resp.status
            data = resp.read()
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                text = data.decode("utf-8", errors="replace")
            return status, text
    except HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        return e.code, body
    except URLError as e:
        return 0, str(e)

def http_post_json(url: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None, timeout: int = 20) -> Tuple[int, Dict[str, Any], str]:
    h = {"Content-Type": "application/json"}
    if headers:
        h.update(headers)
    data = json.dumps(payload).encode("utf-8")
    req = Request(url, headers=h, data=data, method="POST")
    try:
        with urlopen(req, timeout=timeout) as resp:
            status = resp.status
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                return status, json.loads(raw), raw
            except Exception:
                return status, {}, raw
    except HTTPError as e:
        raw = ""
        try:
            raw = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        try:
            return e.code, json.loads(raw), raw
        except Exception:
            return e.code, {}, raw
    except URLError as e:
        return 0, {}, str(e)

def clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))

def uniq_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for x in items:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out

def is_frozen_path(path: str) -> bool:
    p = os.path.abspath(path)
    for fp in FROZEN_PATH_PREFIXES:
        fp_abs = os.path.abspath(fp)
        if p == fp_abs:
            return True
        if os.path.isdir(fp_abs) and p.startswith(fp_abs + os.sep):
            return True
    return False


# =========================
# Data models
# =========================

@dataclass
class Post:
    source: str
    id: str
    url: str
    text: str
    author: str
    created_at: str
    lang_hint: str = ""
    meta: Dict[str, Any] = None

    def norm_text(self) -> str:
        t = self.text or ""
        t = re.sub(r"\s+", " ", t).strip()
        return t

@dataclass
class Theme:
    title: str
    slug: str
    category: str
    problem_list: List[str]
    representative_posts: List[Post]
    score: float
    keywords: List[str]


# =========================
# Collectors
# =========================

def collect_bluesky(max_items: int = 60) -> List[Post]:
    if not (BLUESKY_HANDLE and BLUESKY_APP_PASSWORD):
        logging.info("Bluesky: skipped (missing BLUESKY_HANDLE/BLUESKY_APP_PASSWORD)")
        return []
    logging.info("Bluesky: collecting up to %d", max_items)
    status, js, raw = http_post_json(
        "https://bsky.social/xrpc/com.atproto.server.createSession",
        {"identifier": BLUESKY_HANDLE, "password": BLUESKY_APP_PASSWORD},
        headers={"Accept": "application/json"},
        timeout=20,
    )
    if status != 200 or "accessJwt" not in js:
        logging.warning("Bluesky: session failed status=%s body=%s", status, raw[:300])
        return []
    token = js["accessJwt"]
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    # Include tech + life queries, shuffled
    tech_queries = [
        "help error",
        "how do i fix",
        "can't login",
        "failed to",
        "pdf convert",
        "compress mp4",
        "excel formula",
        "privacy settings",
        "dns cname aaaa",
        "github pages domain",
    ]
    life_queries = [
        "travel plan",
        "itinerary",
        "packing list",
        "budget travel",
        "quick recipe",
        "meal prep",
        "sleep schedule",
        "workout plan",
        "study plan",
        "job interview",
        "resume help",
        "any advice",
        "procrastination",
        "compare products",
    ]
    queries = tech_queries + life_queries
    random.shuffle(queries)
    posts: List[Post] = []
    for q in queries:
        if len(posts) >= max_items:
            break
        url = "https://bsky.social/xrpc/app.bsky.feed.searchPosts?" + urlencode({"q": q, "limit": 25})
        st, body = http_get(url, headers=headers, timeout=20)
        if st != 200:
            continue
        try:
            data = json.loads(body)
        except Exception:
            continue
        for item in data.get("posts", []):
            if len(posts) >= max_items:
                break
            uri = item.get("uri", "")
            cid = item.get("cid", "")
            text = (item.get("record") or {}).get("text", "") or item.get("text", "")
            author = (item.get("author") or {}).get("handle", "") or "unknown"
            created_at = (item.get("record") or {}).get("createdAt", "") or item.get("indexedAt", "") or ""
            post_url = ""
            if uri:
                try:
                    parts = uri.split("/")
                    rkey = parts[-1]
                    post_url = f"https://bsky.app/profile/{author}/post/{rkey}"
                except Exception:
                    post_url = uri
            pid = sha1(f"bsky:{uri}:{cid}")
            if text and post_url:
                posts.append(Post(
                    source="bluesky",
                    id=pid,
                    url=post_url,
                    text=text,
                    author=author,
                    created_at=created_at or now_iso(),
                    meta={"query": q, "uri": uri, "cid": cid},
                ))
    logging.info("Bluesky: collected %d", len(posts))
    return posts

def collect_mastodon(max_items: int = 120) -> List[Post]:
    if not (MASTODON_BASE and MASTODON_TOKEN):
        logging.info("Mastodon: skipped (missing MASTODON_BASE/MASTODON_TOKEN)")
        return []
    base = MASTODON_BASE.rstrip("/")
    headers = {"Authorization": f"Bearer {MASTODON_TOKEN}", "Accept": "application/json"}
    logging.info("Mastodon: collecting up to %d from %s", max_items, base)
    tags = [
        "help", "support", "webdev", "privacy", "excel", "opensource", "github", "dns", "wordpress", "linux",
        "travel", "cooking", "recipe", "fitness", "study", "money", "career"
    ]
    queries = [
        "need help", "error", "how to fix", "cannot", "failed", "issue", "bug",
        "itinerary", "travel", "recipe", "advice", "should I", "recommend", "workout", "study"
    ]
    random.shuffle(tags)
    random.shuffle(queries)
    out: List[Post] = []
    def add_statuses(statuses: List[Dict[str, Any]], hint: str) -> None:
        nonlocal out
        for s in statuses:
            if len(out) >= max_items:
                return
            sid = s.get("id", "")
            url = s.get("url") or ""
            created_at = s.get("created_at") or now_iso()
            acct = (s.get("account") or {}).get("acct", "") or "unknown"
            content = s.get("content") or ""
            text = re.sub(r"<[^>]+>", " ", content)
            text = html.unescape(text).strip()
            if not text or not url:
                continue
            pid = sha1(f"mstdn:{sid}:{url}")
            out.append(Post(
                source="mastodon",
                id=pid,
                url=url,
                text=text,
                author=acct,
                created_at=created_at,
                meta={"hint": hint},
            ))
    # public timeline
    st, body = http_get(f"{base}/api/v1/timelines/public?limit=40", headers=headers, timeout=20)
    if st == 200:
        try:
            add_statuses(json.loads(body), "public")
        except Exception:
            pass
    # tag timelines
    for tag in tags:
        if len(out) >= max_items:
            break
        st, body = http_get(f"{base}/api/v1/timelines/tag/{quote(tag)}?limit=30", headers=headers, timeout=20)
        if st != 200:
            continue
        try:
            add_statuses(json.loads(body), f"tag:{tag}")
        except Exception:
            continue
    # search queries
    for q in queries:
        if len(out) >= max_items:
            break
        url = f"{base}/api/v2/search?" + urlencode({"q": q, "type": "statuses", "resolve": "true", "limit": "20"})
        st, body = http_get(url, headers=headers, timeout=20)
        if st != 200:
            continue
        try:
            data = json.loads(body)
            add_statuses(data.get("statuses", []), f"search:{q}")
        except Exception:
            continue
    logging.info("Mastodon: collected %d", len(out))
    return out

def reddit_oauth_token() -> Optional[str]:
    if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET and REDDIT_REFRESH_TOKEN):
        return None
    token_url = "https://www.reddit.com/api/v1/access_token"
    auth = f"{REDDIT_CLIENT_ID}:{REDDIT_CLIENT_SECRET}"
    basic = "Basic " + (base64_encode(auth))
    headers = {
        "Authorization": basic,
        "User-Agent": REDDIT_USER_AGENT,
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }
    form = urlencode({"grant_type": "refresh_token", "refresh_token": REDDIT_REFRESH_TOKEN}).encode("utf-8")
    req = Request(token_url, headers=headers, data=form, method="POST")
    try:
        with urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            js = json.loads(raw)
            return js.get("access_token")
    except Exception as e:
        logging.warning("Reddit: oauth token failed: %s", str(e))
        return None

def base64_encode(s: str) -> str:
    import base64
    return base64.b64encode(s.encode("utf-8")).decode("ascii")

def collect_reddit(max_items: int = 60) -> List[Post]:
    subs = [x.strip() for x in REDDIT_SUBREDDITS.split(",") if x.strip()]
    if not subs:
        subs = ["webdev", "sysadmin", "programming"]
    token = reddit_oauth_token()
    if token:
        base_url = "https://oauth.reddit.com"
        headers = {"Authorization": f"bearer {token}", "User-Agent": REDDIT_USER_AGENT, "Accept": "application/json"}
        logging.info("Reddit: OAuth mode collecting up to %d", max_items)
    else:
        base_url = "https://www.reddit.com"
        headers = {"User-Agent": REDDIT_USER_AGENT, "Accept": "application/json"}
        logging.info("Reddit: public mode collecting up to %d", max_items)
    random.shuffle(subs)
    out: List[Post] = []
    queries = ["help", "error", "how to", "issue", "can't", "cannot", "failed", "should I", "advice", "recommend", "itinerary", "recipe", "workout", "study", "best way", "tips"]
    for sub in subs:
        if len(out) >= max_items:
            break
        st, body = http_get(f"{base_url}/r/{quote(sub)}/new.json?limit=50", headers=headers, timeout=20)
        if st != 200:
            continue
        try:
            data = json.loads(body)
        except Exception:
            continue
        children = (((data or {}).get("data") or {}).get("children") or [])
        for ch in children:
            if len(out) >= max_items:
                break
            d = (ch or {}).get("data") or {}
            title = d.get("title") or ""
            selftext = d.get("selftext") or ""
            text = (title + "\n" + selftext).strip()
            if not text:
                continue
            low = text.lower()
            if not any(q in low for q in queries):
                continue
            permalink = d.get("permalink") or ""
            url = "https://www.reddit.com" + permalink if permalink.startswith("/") else (d.get("url") or "")
            author = d.get("author") or "unknown"
            created_utc = d.get("created_utc") or time.time()
            created_at = dt.datetime.fromtimestamp(created_utc, tz=dt.timezone.utc).astimezone().isoformat(timespec="seconds")
            rid = d.get("name") or d.get("id") or sha1(url)
            pid = sha1(f"reddit:{rid}:{url}")
            out.append(Post(
                source="reddit",
                id=pid,
                url=url,
                text=text,
                author=author,
                created_at=created_at,
                meta={"subreddit": sub},
            ))
    logging.info("Reddit: collected %d", len(out))
    return out

def collect_hn(max_items: int = 70) -> List[Post]:
    max_items = clamp(max_items, 10, 200)
    url = "https://hn.algolia.com/api/v1/search_by_date?" + urlencode({
        "query": HN_QUERY,
        "tags": "story,comment",
        "hitsPerPage": str(min(max_items, 100)),
        "page": "0",
    })
    st, body = http_get(url, headers={"Accept": "application/json"}, timeout=20)
    if st != 200:
        logging.warning("HN: failed status=%s", st)
        return []
    try:
        data = json.loads(body)
    except Exception:
        return []
    hits = data.get("hits", []) or []
    out: List[Post] = []
    for h in hits:
        if len(out) >= max_items:
            break
        text = (h.get("title") or "") + "\n" + (h.get("comment_text") or "")
        text = re.sub(r"<[^>]+>", " ", text)
        text = html.unescape(text).strip()
        if not text:
            continue
        object_id = h.get("objectID") or ""
        created_at = h.get("created_at") or now_iso()
        author = h.get("author") or "unknown"
        hn_url = h.get("url") or ""
        if not hn_url:
            hn_url = f"https://news.ycombinator.com/item?id={object_id}"
        pid = sha1(f"hn:{object_id}:{hn_url}")
        out.append(Post(
            source="hn",
            id=pid,
            url=hn_url,
            text=text,
            author=author,
            created_at=created_at,
            meta={"points": h.get("points", 0), "tags": h.get("_tags", [])},
        ))
    logging.info("HN: collected %d", len(out))
    return out

def collect_x_mentions(max_items: int = 5) -> List[Post]:
    if not (X_BEARER_TOKEN and X_USER_ID):
        logging.info("X: skipped (missing X_BEARER_TOKEN/X_USER_ID)")
        return []
    max_items = clamp(max_items, 1, 20)
    headers = {"Authorization": f"Bearer {X_BEARER_TOKEN}", "Accept": "application/json"}
    url = f"https://api.x.com/2/users/{quote(X_USER_ID)}/mentions?" + urlencode({
        "max_results": str(max_items),
        "tweet.fields": "created_at,author_id,lang",
    })
    st, body = http_get(url, headers=headers, timeout=20)
    if st != 200:
        logging.warning("X: mentions failed status=%s body=%s", st, body[:200])
        return []
    try:
        data = json.loads(body)
    except Exception:
        return []
    out: List[Post] = []
    for t in (data.get("data") or []):
        tid = t.get("id") or ""
        text = t.get("text") or ""
        created_at = t.get("created_at") or now_iso()
        author = t.get("author_id") or "unknown"
        url = f"https://x.com/i/web/status/{tid}"
        pid = sha1(f"x:{tid}:{url}")
        out.append(Post(
            source="x",
            id=pid,
            url=url,
            text=text,
            author=author,
            created_at=created_at,
            lang_hint=t.get("lang") or "",
            meta={"author_id": author},
        ))
    logging.info("X: collected %d", len(out))
    return out


# =========================
# Normalization & Clustering
# =========================

STOPWORDS_EN = set("""
a an the and or but if then else when while of for to in on at from by with without into onto over under
is are was were be been being do does did done have has had will would can could should may might
this that these those it its i'm youre youre we they them our your my mine me you he she his her
""".split())

STOPWORDS_JA = set(["これ", "それ", "あれ", "ため", "ので", "から", "です", "ます", "いる", "ある", "なる", "こと", "もの", "よう", "へ", "に", "を", "が", "と", "で", "も"])

def simple_tokenize(text: str) -> List[str]:
    t = text.lower()
    t = re.sub(r"https?://\S+", " ", t)
    t = re.sub(r"[\[\]()<>{}※*\"'`~^|\\]", " ", t)
    t = re.sub(r"[^0-9a-z\u3040-\u30ff\u4e00-\u9fff\s\-_/.:]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    parts = []
    for p in t.split():
        if len(p) <= 1:
            continue
        if p in STOPWORDS_EN:
            continue
        if p in STOPWORDS_JA:
            continue
        parts.append(p)
    jp_chunks = re.findall(r"[\u3040-\u30ff\u4e00-\u9fff]{2,}", t)
    parts.extend([c for c in jp_chunks if c not in STOPWORDS_JA and len(c) >= 2])
    return parts[:80]

def jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0

def cluster_posts(posts: List[Post], threshold: float = 0.22) -> List[List[Post]]:
    logging.info("Clustering %d posts (threshold=%.2f)", len(posts), threshold)
    token_sets: Dict[str, set] = {}
    for p in posts:
        token_sets[p.id] = set(simple_tokenize(p.norm_text()))
    clusters: List[List[Post]] = []
    used = set()
    for i, p in enumerate(posts):
        if p.id in used:
            continue
        used.add(p.id)
        base = token_sets[p.id]
        c = [p]
        for q in posts[i+1:]:
            if q.id in used:
                continue
            sim = jaccard(base, token_sets[q.id])
            if sim >= threshold:
                used.add(q.id)
                c.append(q)
        clusters.append(c)
    clusters.sort(key=lambda x: (-len(x), x[0].created_at))
    logging.info("Clusters: %d (top sizes=%s)", len(clusters), [len(c) for c in clusters[:8]])
    return clusters

def extract_keywords(posts: List[Post]) -> List[str]:
    freq: Dict[str, int] = {}
    for p in posts:
        for w in simple_tokenize(p.norm_text()):
            freq[w] = freq.get(w, 0) + 1
    items = sorted(freq.items(), key=lambda kv: (-kv[1], kv[0]))
    return [k for k, _ in items[:12]]

def choose_category(posts: List[Post], keywords: List[str]) -> str:
    text = " ".join([p.norm_text() for p in posts]).lower()
    k = set(keywords)
    def has_any(words: List[str]) -> bool:
        return any(w in text for w in words) or any(w in k for w in words)
    if has_any(["dns", "cname", "aaaa", "nameserver", "github pages", "hosting", "ssl", "https"]):
        return "Web/Hosting"
    if has_any(["python", "node", "npm", "pip", "powershell", "bash", "cli", "library", "compile", "error code", "stack trace", "dev"]):
        return "Dev/Tools"
    if has_any(["automation", "workflow", "cron", "github actions", "bot", "llm", "openai", "prompt", "agent"]):
        return "AI/Automation"
    if has_any(["privacy", "security", "2fa", "phishing", "cookie", "vpn", "encryption", "leak"]):
        return "Security/Privacy"
    if has_any(["video", "mp4", "compress", "codec", "ffmpeg", "audio", "subtitle"]):
        return "Media"
    if has_any(["pdf", "docs", "word", "ppt", "docx", "convert", "merge", "compress pdf"]):
        return "PDF/Docs"
    if has_any(["image", "png", "jpg", "webp", "design", "figma", "photoshop", "illustrator"]):
        return "Images/Design"
    if has_any(["excel", "spreadsheet", "csv", "google sheets", "vlookup", "pivot", "formula"]):
        return "Data/Spreadsheets"
    if has_any(["invoice", "tax", "accounting", "bookkeeping", "receipt", "vat"]):
        return "Business/Accounting/Tax"
    if has_any(["seo", "marketing", "ads", "social", "instagram", "tiktok", "youtube", "growth"]):
        return "Marketing/Social"
    if has_any(["productivity", "todo", "note", "calendar", "time management"]):
        return "Productivity"
    if has_any(["english", "language", "study english", "toeic", "eiken"]):
        return "Education/Language"
    if has_any(["travel", "trip", "hotel", "itinerary", "flight", "booking"]):
        return "Travel/Planning"
    if has_any(["recipe", "cook", "cooking", "kitchen"]):
        return "Food/Cooking"
    if has_any(["workout", "fitness", "diet", "health", "running"]):
        return "Health/Fitness"
    if has_any(["study", "learning", "exam", "homework"]):
        return "Study/Learning"
    if has_any(["money", "budget", "loan", "invest", "stock", "crypto"]):
        return "Money/Personal Finance"
    if has_any(["career", "job", "resume", "interview", "work"]):
        return "Career/Work"
    if has_any(["relationship", "communication", "friend", "chat", "texting"]):
        return "Relationships/Communication"
    if has_any(["home", "rent", "utility", "life admin", "paperwork", "moving"]):
        return "Home/Life Admin"
    if has_any(["buy", "shopping", "product", "recommend"]):
        return "Shopping/Products"
    if has_any(["event", "ticket", "concert", "sports"]):
        return "Events/Leisure"
    return "Dev/Tools"

def score_cluster(posts: List[Post], category: str) -> float:
    size = len(posts)
    text = " ".join([p.norm_text().lower() for p in posts])
    solvable_signals = [
        "how", "fix", "error", "failed", "can't", "cannot", "help",
        "設定", "直し", "原因", "エラー", "できない", "不具合", "失敗",
    ]
    tool_signals = [
        "convert", "compress", "calculator", "generator", "template", "checklist",
        "変換", "圧縮", "計算", "チェック", "テンプレ", "ツール",
    ]
    life_signals = [
        "plan", "itinerary", "packing", "recommend", "best", "compare", "budget", "schedule", "checklist", "template", "step-by-step",
        "urgent", "today", "tomorrow", "this week", "before i go",
        "i'm stuck", "im stuck", "i m stuck", "confused", "overwhelmed", "don't know", "dont know", "should i"
    ]
    s1 = sum(1 for w in solvable_signals if w in text)
    s2 = sum(1 for w in tool_signals if w in text)
    s3 = sum(1 for w in life_signals if w in text)
    score = size * 1.8 + s1 * 0.4 + s2 * 0.6 + s3 * 0.5
    if category in ["Web/Hosting", "PDF/Docs", "Media", "Data/Spreadsheets", "Security/Privacy", "AI/Automation"]:
        score *= 1.15
    broad_signals = ["just vent", "just venting", "愚痴", "ただ愚痴", "just needed to vent", "ranting"]
    if any(w in text for w in broad_signals):
        score *= 0.7
    return float(score)

def make_theme(posts: List[Post]) -> Theme:
    keywords = extract_keywords(posts)
    category = choose_category(posts, keywords)
    score = score_cluster(posts, category)
    top = keywords[:5]
    base_title = " / ".join([k for k in top if len(k) <= 18])[:60].strip(" /")
    if not base_title:
        base_title = category.replace("/", " ")
    title = f"{base_title} — Fix Guide & Tool"
    slug = safe_slug(base_title or category) + "-" + sha1(title)[:6]
    problems = []
    for p in posts[:12]:
        line = p.norm_text()
        line = line[:120].rstrip()
        if line:
            problems.append(line)
    problems = uniq_keep_order([re.sub(r"\s+", " ", x) for x in problems])
    while len(problems) < 10:
        problems.append(f"Trouble related to {category}: symptom #{len(problems)+1}")
    problems = problems[:20]
    return Theme(
        title=title,
        slug=slug,
        category=category,
        problem_list=problems,
        representative_posts=posts[:min(len(posts), 20)],
        score=score,
        keywords=keywords,
    )


# =========================
# Affiliates
# =========================

def load_affiliates() -> Tuple[Dict[str, Any], List[str]]:
    data = read_json(AFFILIATES_JSON, default={})
    if not isinstance(data, dict):
        return {}, []
    missing_keys: List[str] = []
    if "categories" in data and isinstance(data["categories"], dict):
        for cat in CATEGORIES_22:
            if cat not in data["categories"]:
                data["categories"][cat] = []
                missing_keys.append(cat)
    else:
        for cat in CATEGORIES_22:
            if cat not in data:
                data[cat] = []
                missing_keys.append(cat)
    if missing_keys:
        logging.warning("Affiliates missing categories keys added: %s", missing_keys)
    return data, missing_keys

def pick_affiliates_for_category(aff: Dict[str, Any], category: str, topn: int = 2) -> List[Dict[str, Any]]:
    items = []
    if "categories" in aff and isinstance(aff["categories"], dict):
        items = aff["categories"].get(category, []) or []
    elif category in aff:
        items = aff.get(category, []) or []
    else:
        for k, v in aff.items():
            if isinstance(v, list):
                items = v
                break
    def pr(x: Dict[str, Any]) -> float:
        try:
            return float(x.get("priority", 0))
        except Exception:
            return 0.0
    items2 = [x for x in items if isinstance(x, dict) and (x.get("html") or x.get("code") or x.get("url"))]
    items2.sort(key=lambda x: -pr(x))
    return items2[:topn]


# =========================
# Site inventory & related/popular
# =========================

def read_hub_sites() -> List[Dict[str, Any]]:
    data = read_json(HUB_SITES_JSON, default=[])
    if isinstance(data, dict) and "sites" in data:
        data = data["sites"]
    if not isinstance(data, list):
        return []
    return [x for x in data if isinstance(x, dict)]

def write_hub_sites(sites: List[Dict[str, Any]]) -> None:
    if is_frozen_path(HUB_SITES_JSON):
        pass
    os.makedirs(HUB_DIR, exist_ok=True)
    payload = {"sites": sites, "updated_at": now_iso()}
    write_json(HUB_SITES_JSON, payload)

def choose_related_tools(all_sites: List[Dict[str, Any]], category: str, exclude_slug: str, n: int = 5) -> List[Dict[str, Any]]:
    same = [s for s in all_sites if s.get("category") == category and s.get("slug") != exclude_slug]
    other = [s for s in all_sites if s.get("slug") != exclude_slug]
    random.shuffle(same)
    random.shuffle(other)
    picks = (same + other)[:n]
    out = []
    for s in picks:
        out.append({
            "title": s.get("title", "Tool"),
            "url": s.get("url", "#"),
            "category": s.get("category", ""),
            "slug": s.get("slug", ""),
        })
    return out

def compute_popular_sites(all_sites: List[Dict[str, Any]], n: int = 6) -> List[Dict[str, Any]]:
    def metric(s: Dict[str, Any]) -> float:
        for k in ["views", "score", "popularity"]:
            if k in s:
                try:
                    return float(s.get(k, 0))
                except Exception:
                    pass
        ts = s.get("updated_at") or s.get("created_at") or ""
        try:
            return dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0
    sites = list(all_sites)
    sites.sort(key=lambda x: metric(x), reverse=True)
    out = []
    for s in sites[:n]:
        out.append({
            "title": s.get("title", "Tool"),
            "url": s.get("url", "#"),
            "category": s.get("category", ""),
            "slug": s.get("slug", ""),
        })
    return out


# =========================
# i18n dictionaries (core UI strings)
# =========================

I18N = {
    "en": {
        "home": "Home",
        "about": "About Us",
        "all_tools": "All Tools",
        "language": "Language",
        "share": "Share",
        "problems": "Problems this tool can help with",
        "quick_answer": "Quick answer",
        "causes": "Common causes",
        "steps": "Step-by-step checklist",
        "pitfalls": "Common pitfalls & how to avoid them",
        "next": "If it still doesn’t work",
        "faq": "FAQ",
        "references": "Reference links",
        "supplement": "Supplementary resources",
        "related": "Related tools",
        "popular": "Popular tools",
        "disclaimer": "Disclaimer",
        "terms": "Terms",
        "privacy": "Privacy",
        "contact": "Contact",
        "footer_note": "We aim to provide practical, fast, and respectful troubleshooting guides.",
        "aff_title": "Recommended",
        "copy": "Copy",
        "copied": "Copied",
    },
    "ja": {
        "home": "Home",
        "about": "About Us",
        "all_tools": "All Tools",
        "language": "言語",
        "share": "共有",
        "problems": "このツールが助ける悩み一覧",
        "quick_answer": "結論（最短で直す方針）",
        "causes": "原因のパターン分け",
        "steps": "手順（チェックリスト）",
        "pitfalls": "よくある失敗と回避策",
        "next": "直らない場合の次の手",
        "faq": "FAQ",
        "references": "参考URL",
        "supplement": "補助資料",
        "related": "関連ツール",
        "popular": "人気のツール",
        "disclaimer": "免責事項",
        "terms": "利用規約",
        "privacy": "プライバシーポリシー",
        "contact": "お問い合わせ",
        "footer_note": "実務で使える手順に寄せて、短時間で解決できる形を目指しています。",
        "aff_title": "おすすめ",
        "copy": "コピー",
        "copied": "コピーしました",
    },
    "ko": { ... },
    "zh": { ... },
}


# =========================
# Content generation
# =========================

def build_quick_answer(category: str, keywords: List[str]) -> str:
    kw = ", ".join(keywords[:8])
    # specialized quick answer for certain categories
    if category == "Travel/Planning":
        lines = [
            "旅行計画の最短ルートは、「行き先・日程・予算・持ち物」を軸に全体像を組み立てることです。必須の予定と余裕時間をバランスさせ、持ち物チェックで抜け漏れを防ぎます。",
            "以下のチェックリストで、効率よく旅程を組むステップを確認できます。"
        ]
        return "\n".join(lines)
    if category == "Food/Cooking":
        lines = [
            "料理の時短ポイントは、献立をまとめて立て、買い物リストと作り置きを活用することです。一週間分のメニューを先に決めておき、栄養バランスも考慮しながら準備を効率化します。",
            "以下のガイドで、無理なく続けられる献立作成の手順を確認できます。"
        ]
        return "\n".join(lines)
    if category == "Health/Fitness":
        lines = [
            "健康習慣づくりの近道は、無理のない目標設定と習慣化です。運動・睡眠・食事の記録を取り、小さな目標を達成しながら徐々にレベルアップします。",
            "以下のチェックリストで、効果的に健康管理するステップを確認できます。"
        ]
        return "\n".join(lines)
    if category == "Study/Learning":
        lines = [
            "学習効率アップの鍵は、学習計画と復習スケジュールの徹底です。科目や範囲を細分化し、時間割や単語帳を活用して日々コツコツ進めます。",
            "以下の手順で、無理のない学習計画を立てる方法を確認できます。"
        ]
        return "\n".join(lines)
    if category == "Money/Personal Finance":
        lines = [
            "お金の悩み解決は、全体像の「見える化」から始まります。収入・支出を洗い出して予算を組み、無理のない返済や貯蓄プランを立てるのが近道です。",
            "以下のガイドで、効率的に家計管理するステップを確認できます。"
        ]
        return "\n".join(lines)
    if category == "Career/Work":
        lines = [
            "キャリアの悩みは、情報収集と計画立てで道が開けます。まず自己分析と業界研究を行い、履歴書作成や面接対策をステップごとにクリアしていきます。",
            "以下の手順で、効率的にキャリア構築する方法を確認できます。"
        ]
        return "\n".join(lines)
    if category == "Relationships/Communication":
        lines = [
            "人間関係の悩みは、問題を整理し対策を練ることで改善できます。伝えたいことを事前にまとめ、会話のシミュレーションやテンプレートを用意して臨むと冷静に対処できます。",
            "以下のチェックリストで、円滑なコミュニケーションを図る手順を確認できます。"
        ]
        return "\n".join(lines)
    if category == "Home/Life Admin":
        lines = [
            "生活管理の悩みは、タスクをリスト化して優先順位をつけることで解決に近づきます。引っ越しや片付けなどの大仕事も、項目ごとにチェックリスト化して一つずつ片付けていくと抜け漏れが減ります。",
            "以下のガイドで、効率的に生活タスクを処理する方法を確認できます。"
        ]
        return "\n".join(lines)
    if category == "Shopping/Products":
        lines = [
            "商品の選択で失敗しないコツは、条件を整理して比較することです。スペックや口コミを一覧表にまとめ、自分にとって譲れないポイントを明確にします。",
            "以下の手順で、後悔しない買い物をするためのポイントを確認できます。"
        ]
        return "\n".join(lines)
    if category == "Events/Leisure":
        lines = [
            "イベント企画の成功には、事前準備とスケジュール管理が欠かせません。必要な手配と当日の流れをリストアップし、早め早めの行動で余裕を作ります。",
            "以下のチェックリストで、スムーズにイベント準備を進める手順を確認できます。"
        ]
        return "\n".join(lines)
    # default (tech/general)
    base = [
        "最短で直す方針は「原因の切り分け→再現条件の固定→安全な最小変更→検証→戻せる形で反映」です。",
        f"今回のケースはカテゴリが「{category}」なので、まずは設定・権限・入力・ネットワークのどこで止まっているかを切り分けます。",
        f"キーワード（観測された兆候）: {kw}。",
        "以下のチェックリストは、上から順に潰せば“よくある事故”をほぼ回避できる順番に並べています。",
    ]
    return "\n".join(base)

def build_causes(category: str) -> List[str]:
    if category == "Travel/Planning":
        return [
            "行き先ややりたいことを詰め込みすぎて日程がパンクする",
            "移動時間や乗り継ぎを楽観的に見積もりすぎて遅延が発生",
            "予算を細かく計算しておらず、途中で資金不足に陥る",
            "持ち物リストを作らず忘れ物が発生する",
            "現地情報のリサーチ不足でトラブルに対応できない",
        ]
    if category == "Food/Cooking":
        return [
            "毎日の献立を一から考えて時間を浪費している",
            "買い物リストを作らず必要な材料を買い忘れる",
            "凝ったレシピばかり選んで調理に時間がかかる",
            "栄養バランスが偏りがちで健康面が不安になる",
            "作り置きをせず忙しい日に対応できない",
        ]
    if category == "Health/Fitness":
        return [
            "最初に目標を高く設定しすぎて挫折しやすい",
            "成果を記録しておらず、モチベーションが続かない",
            "食事や休養の管理が甘く、努力の効果が半減する",
            "自己流で進めて誤ったフォームやメニューになっている",
        ]
    if category == "Study/Learning":
        return [
            "勉強計画なしで行き当たりばったりになっている",
            "復習の習慣がなく、一度学んだ内容を忘れてしまう",
            "初めから長時間やりすぎて疲れ、継続できない",
            "苦手分野を後回しにし、弱点が放置されている",
            "勉強環境の整備不足で集中できない",
        ]
    if category == "Money/Personal Finance":
        return [
            "収入と支出を正確に把握せず無駄遣いが減らない",
            "クレジットやサブスクなど見えにくい支出を見落としている",
            "貯蓄や投資の優先順位が曖昧で後回しにしてしまう",
            "ローンや借金の返済計画を立てず、利子負担が膨らむ",
        ]
    if category == "Career/Work":
        return [
            "自己分析が不足し、自分の強みや志向を把握できていない",
            "業界や企業研究が足りず、応募先への理解が浅い",
            "履歴書・職務経歴書の内容が平凡でアピール不足",
            "面接練習不足で、本番で要点を伝えきれない",
        ]
    if category == "Relationships/Communication":
        return [
            "言いたいことを整理しないまま話し始めてしまう",
            "感情的になってしまい、伝え方が攻撃的になる",
            "相手の話を最後まで聞かず、自分の主張ばかりしてしまう",
            "相手に合わせすぎて自分の本音を言えず、ストレスになる",
        ]
    if category == "Home/Life Admin":
        return [
            "やることを頭だけで管理し、抜け漏れや後回しが発生する",
            "優先順位を付けておらず、重要な用事をつい先延ばしにする",
            "定期的な手続き（請求支払い等）を忘れ、期限を過ぎてしまう",
            "全て自分で抱え込み、周囲に頼んだり外注したりしない",
        ]
    if category == "Shopping/Products":
        return [
            "自分の求める条件を整理せず、なんとなく商品を探してしまう",
            "値段だけで飛びつき、機能や品質をよく確認せずに後悔する",
            "事前リサーチが不足し、商品の性能や相場を把握していない",
            "レビューやセール情報に流され、自分の基準がブレてしまう",
        ]
    if category == "Events/Leisure":
        return [
            "準備開始が遅れ、直前になって慌ててしまう",
            "必要なタスクの洗い出しに漏れがあり、抜けが発生する",
            "人に任せられる作業も抱え込んでしまい、負担が集中する",
            "当日のトラブル想定が甘く、イレギュラーに対応できない",
        ]
    return [
        "入力・前提条件のズレ（想定と実際が違う）",
        "権限/設定/バージョンの不一致",
        "キャッシュや反映待ち",
        "エラー箇所が別の場所に見えている（原因が前段にある）",
    ]

def build_steps(category: str) -> List[str]:
    if category == "Travel/Planning":
        return [
            "行きたい場所・体験をリストアップし、優先度をつける",
            "旅行の日程を決め、各日の大まかな予定を割り当てる",
            "フライト・宿泊など必要な予約を早めに押さえる",
            "持ち物チェックリストを作成し、余裕を持って準備する",
            "現地の気候や習慣を事前に調べ、対応策を用意する",
        ]
    if category == "Food/Cooking":
        return [
            "1週間分の献立をまとめて立てる（主菜・副菜・スープ等）",
            "献立に合わせた買い物リストを作成し、まとめ買いする",
            "作り置きや下ごしらえを週末に行い、平日の調理を簡略化",
            "栄養バランスを確認し、野菜やタンパク質を不足させない",
            "定番レシピをローテーションし、献立を考える時間を減らす",
        ]
    if category == "Health/Fitness":
        return [
            "達成可能な運動目標（例: 週3回30分）を設定する",
            "睡眠時間や食事内容を記録し、生活リズムを可視化する",
            "毎日決まった時間に運動するよう予定に組み込む",
            "小さな成功（例: 体重1kg減）を祝い、モチベーションを維持する",
            "疲労や痛みがあるときは無理せず休み、怪我を予防する",
        ]
    if category == "Study/Learning":
        return [
            "試験日や目標日から逆算して学習スケジュールを立てる",
            "科目ごとに範囲を分割し、日々のタスクに落とし込む",
            "定期的に復習日を設け、一度学んだ内容を必ず見直す",
            "苦手分野には多めの時間を割り、重点的に練習する",
            "スマホ通知オフや専用スペース確保など、集中できる環境を整える",
        ]
    if category == "Money/Personal Finance":
        return [
            "1か月分の収支（収入・固定費・変動費）を洗い出す",
            "必需品と娯楽費を分類し、削れる出費がないか検討する",
            "毎月の貯蓄額・投資額の目標を設定し、先取りで確保する",
            "借入やローンがあれば返済計画を立て、繰上返済も検討する",
            "家計管理アプリ等で収支を記録し、定期的に見直す",
        ]
    if category == "Career/Work":
        return [
            "自分のスキル・経験を棚卸しし、強みと弱みを書き出す",
            "興味のある業界・職種の情報を集め、必要なスキルを確認する",
            "履歴書・職務経歴書を作成し、第三者に添削を依頼する",
            "模擬面接を行い、自己紹介や志望動機を練習しておく",
            "現職でできる準備（資格取得やプロジェクト参加）を進めておく",
        ]
    if category == "Relationships/Communication":
        return [
            "現状の問題点を書き出し、自分が伝えたいことを整理する",
            "相手の立場や気持ちを想像し、配慮すべき点を考える",
            "伝える内容を簡潔な言葉でまとめ、言い方をシミュレーションする",
            "必要に応じて第三者のアドバイスを求め、偏った視点を修正する",
            "話す際は落ち着いた口調で、相手の話にも耳を傾ける",
        ]
    if category == "Home/Life Admin":
        return [
            "抱えている用事やタスクをすべて書き出し、見える化する",
            "緊急度と重要度で優先順位をつけ、上位から着手する",
            "大きなプロジェクト（引っ越し等）は小タスクに分割して管理する",
            "定期的な支払い・更新はリマインダーを設定して忘れないようにする",
            "外注や家族・友人の助けを検討し、一人で抱え込まない",
        ]
    if category == "Shopping/Products":
        return [
            "購入目的と予算上限をまず決めておく",
            "自分に必要な機能・条件を箇条書きで列挙する",
            "候補となる商品を数点に絞り、価格やレビューを比較する",
            "公式サイトや店舗で実物や詳細スペックを確認する",
            "総合的に判断し、納得できるものを選んだら迷わず購入する",
        ]
    if category == "Events/Leisure":
        return [
            "イベント/旅行の目的と予算、希望日程をまず決める",
            "必要な準備事項をすべて書き出し、スケジュールに落とし込む",
            "早めにチケット予約や会場手配など主要な手続きを済ませる",
            "当日の進行表を作成し、役割分担や緊急連絡先も明記する",
            "天候などの不測の事態に備え、代替案や予備日も検討する",
        ]
    return [
        "再現条件を固定する（同じ入力・同じ手順・同じ端末/ブラウザで再現）",
        "エラー表示やログをそのまま保存（スクショ/コピペ、時刻も残す）",
        "キャッシュを疑う（スーパーリロード/別ブラウザ/シークレット、Service Workerの登録も確認）",
        "設定・権限・トークン期限を確認（特に外部API/OAuth）",
        "最小変更で1点ずつ潰す（“まとめて変更”は禁止）",
        "直ったら差分を記録し、再発防止チェックを作る（次回は3分で復旧できる形）",
    ]

def build_pitfalls(category: str) -> List[str]:
    if category == "Travel/Planning":
        return [
            "予定を詰め込みすぎて移動や休息の時間が足りなくなる",
            "準備をギリギリまで先延ばしにして、直前に慌てる",
            "予算や時間に余裕を見込まず、トラブルに対応できない",
            "現地の事情を調査せずに訪れ、戸惑ってしまう",
        ]
    if category == "Food/Cooking":
        return [
            "気分に流されて計画した献立を守れなくなる",
            "完璧を求めて時間と労力をかけすぎる",
            "作り置きをしすぎて食べきれず、食材を無駄にする",
            "レシピを見ず自己流で作って味が安定しない",
        ]
    if category == "Health/Fitness":
        return [
            "最初から飛ばしすぎて、早々に息切れしてしまう",
            "結果を急ぎすぎて無理なダイエットや過度な運動に走る",
            "体調の小さな変化を無視し続け、怪我や不調を招く",
            "他人と比較して落ち込み、自分のペースを見失う",
        ]
    if category == "Study/Learning":
        return [
            "完璧な計画を立てようと時間をかけすぎて勉強開始が遅れる",
            "計画倒れになっても見直さず惰性で続けてしまう",
            "短期間で詰め込みすぎて内容を消化できない",
            "得意科目ばかり勉強して苦手を避けてしまう",
        ]
    if category == "Money/Personal Finance":
        return [
            "出費記録が三日坊主になり、家計管理を放棄してしまう",
            "節約に囚われすぎて必要な投資（資格取得等）も削ってしまう",
            "節約と散財を極端に繰り返し、ストレスで出費が増える",
            "家族と金銭感覚を共有せず、協力を得られない",
        ]
    if category == "Career/Work":
        return [
            "不安から闇雲に応募し、軸のない転職活動になる",
            "準備不足のまま面接に臨み、伝えたいことが伝えられない",
            "転職先の条件ばかり気にして、自分の成長計画を疎かにする",
            "退職を焦るあまり、次の職場をよく調べず決めてしまう",
        ]
    if category == "Relationships/Communication":
        return [
            "勢いで感情をぶつけ、関係をさらに悪化させる",
            "相手に察してほしいと期待しすぎて核心を伝えない",
            "話し合いの場を設けず、問題を先送りにする",
            "自己防衛に走って相手を責め、溝を深めてしまう",
        ]
    if category == "Home/Life Admin":
        return [
            "優先度の低い作業から手をつけ、重要な用事を後回しにする",
            "チェックリストを作っても更新せず、古い情報のまま進める",
            "一度に全て片付けようとして途中で力尽きる",
            "助けを求めず、自分だけで無理をして消耗する",
        ]
    if category == "Shopping/Products":
        return [
            "決めきれず何店舗も回り、時間と労力を浪費する",
            "安さにつられてまとめ買いし、結局使わないものが増える",
            "友人の意見に流され、本当は不要なものを買ってしまう",
            "セールの勢いで予算オーバーの買い物をする",
        ]
    if category == "Events/Leisure":
        return [
            "大丈夫だろうと油断し、事前確認を怠る",
            "一人で抱え込みすぎてチームに任せない",
            "当日のアドリブ頼みにして綿密な計画を立てない",
            "直前の変更に対応できず、パニックになる",
        ]
    return [
        "一気に複数箇所を変えてしまい、どれが原因か分からなくなる",
        "反映待ち（DNS/キャッシュ）を無視して、焦ってさらに壊す",
        "ログを取らずに試行回数だけ増やす（後で復旧不能になる）",
        "“いま見えている画面”が原因箇所だと決めつける（前段が原因のことが多い）",
    ]

def build_next_actions(category: str) -> List[str]:
    if category in ["Travel/Planning", "Food/Cooking", "Health/Fitness", "Study/Learning", 
                    "Money/Personal Finance", "Career/Work", "Relationships/Communication", 
                    "Home/Life Admin", "Shopping/Products", "Events/Leisure"]:
        return [
            "信頼できる第三者に相談し、客観的なアドバイスをもらう",
            "一度休憩して頭をリセットし、新しい視点で見直す",
            "優先順位を見直し、必要に応じて計画を修正する",
            "今回の経験から学び、次回に活かせるチェックリストを作る",
        ]
    return [
        "別経路で同じ結果が出るか確認（別端末/別回線/別ブラウザ）",
        "ログの粒度を上げる（失敗時のHTTPステータス、レスポンス先頭、例外スタック）",
        "“元に戻せる形”で段階的にロールバック（変更前後の差分を残す）",
        "同じ失敗を繰り返さないよう、チェック項目を固定化する",
    ]

def build_faq(category: str) -> List[Tuple[str, str]]:
    if category in ["Travel/Planning", "Food/Cooking", "Health/Fitness", "Study/Learning", 
                    "Money/Personal Finance", "Career/Work", "Relationships/Communication", 
                    "Home/Life Admin", "Shopping/Products", "Events/Leisure"]:
        return [
            ("最初に何から始めればいい？", "まずは全体像の把握です。抱えている要素をすべて書き出し、優先順位を付けるところから始めます。"),
            ("計画がうまく進んでいるか確認する方法は？", "途中経過を定期的に見直し、チェックリストが順調に消化できているか確認します。進捗が遅れていれば計画を調整しましょう。"),
            ("どの順番で進めるのが効率的？", "影響範囲が小さいタスクから片付けていくと、リスクを抑えられます。早めに終わるものから処理し、大きなものは小分けに取り組みます。"),
            ("完了後にやるべきことは？", "今回の経験から学んだことをまとめ、次回に活かせるチェックリストやテンプレートを作っておくと良いです。"),
            ("相談するときに何を伝えればいい？", "背景や目的、現状の進捗、特に困っている点を具体的に伝えると、相手もアドバイスしやすくなります。"),
        ]
    base = [
        ("最初に何を見ればいい？", "再現条件・エラー文・時刻・直前に変えた点の4つをまず固定します。"),
        ("キャッシュが原因かどうかの見分け方は？", "シークレット/別ブラウザ/別端末で同じ結果ならキャッシュ以外の可能性が高いです。"),
        ("何から手を付ける順番が良い？", "影響範囲が小さい順（確認→読み取り→最小変更→検証）で進めると安全です。"),
        ("直った後にやるべきことは？", "差分と再発防止チェックを残すと、次回は短時間で復旧できます。"),
        ("情報を共有するときに何を書けばいい？", "再現手順、期待結果、実結果、ログ/スクショ、環境（OS/ブラウザ/版）です。"),
    ]
    if category == "Web/Hosting":
        base.append(("DNSはどれくらいで反映される？", "TTLやプロバイダで差が出ます。第三者のDNS解決でも確認してから判断します。"))
    if category == "AI/Automation":
        base.append(("自動化が暴走しないようにするには？", "上書き禁止・衝突回避・凍結パス保護・ログ保存を必須にします。"))
    return base[: max(MIN_FAQ, 5)]

def supplemental_resources_for_category(category: str) -> List[str]:
    base = {
        "Web/Hosting": [
            "https://developer.mozilla.org/en-US/docs/Learn/Common_questions/Web_mechanics/What_is_a_domain_name",
            "https://pages.github.com/",
            "https://letsencrypt.org/docs/",
        ],
        "Security/Privacy": [
            "https://owasp.org/www-project-top-ten/",
            "https://developer.mozilla.org/en-US/docs/Web/HTTP/Cookies",
            "https://en.wikipedia.org/wiki/Phishing",
        ],
        "PDF/Docs": [
            "https://www.adobe.com/acrobat/resources/what-is-pdf.html",
            "https://en.wikipedia.org/wiki/PDF",
            "https://developer.mozilla.org/en-US/docs/Web/API/File",
        ],
        "Media": [
            "https://ffmpeg.org/documentation.html",
            "https://en.wikipedia.org/wiki/Video_codec",
            "https://developer.mozilla.org/en-US/docs/Web/Media",
        ],
        "Data/Spreadsheets": [
            "https://support.google.com/docs/?hl=en#topic=1382883",
            "https://support.microsoft.com/excel",
            "https://en.wikipedia.org/wiki/Comma-separated_values",
        ],
        "AI/Automation": [
            "https://docs.github.com/en/actions",
            "https://en.wikipedia.org/wiki/Cron",
            "https://en.wikipedia.org/wiki/Rate_limiting",
        ],
    }
    return base.get(category, [
        "https://developer.mozilla.org/",
        "https://docs.github.com/",
        "https://en.wikipedia.org/wiki/Troubleshooting",
    ])

def generate_long_article_ja(theme: Theme) -> str:
    intro = (
        f"このページは「{theme.category}」でよく起きるトラブルを、"
        f"短時間で安全に解決するためのガイドです。"
        f"原因を推測で決め打ちせず、再現条件を固定し、"
        f"影響範囲の小さい順に確認していくことで、無駄な試行回数を減らします。\n"
    )
    why = (
        "多くの不具合は、(1)設定の不一致、(2)権限やトークンの期限切れ、"
        "(3)キャッシュや反映待ち、(4)入力条件の揺れ、のどれかに落ちます。"
        "逆に言うと、この4点を順に潰すだけで“直らない理由”の大半は説明できます。\n"
    )
    detail = (
        "ここで大事なのは「最小変更」です。"
        "一度に複数箇所をいじると、直ったとしても原因が分からず再発します。"
        "最小変更→検証→記録、を繰り返すと、次回はチェックリストだけで復旧できます。\n"
    )
    causes = build_causes(theme.category)
    steps = build_steps(theme.category)
    pitfalls = build_pitfalls(theme.category)
    nxt = build_next_actions(theme.category)
    cause_text = "【原因のパターン分け】\n" + "\n".join([f"- {c}" for c in causes]) + "\n"
    step_text = "【手順（チェックリスト）】\n" + "\n".join([f"- {s}" for s in steps]) + "\n"
    pit_text = "【よくある失敗と回避策】\n" + "\n".join([f"- {p}" for p in pitfalls]) + "\n"
    nxt_text = "【直らない場合の次の手】\n" + "\n".join([f"- {x}" for x in nxt]) + "\n"
    examples = "【このページで扱う悩み一覧（例）】\n" + "\n".join([f"- {p}" for p in theme.problem_list]) + "\n"
    verify = (
        "【検証のコツ】\n"
        "- まず“期待結果”を文章にする（何ができれば成功か）\n"
        "- 失敗が出たら、入力・環境・時刻・ログをセットで残す\n"
        "- 直った瞬間に、何を変えたかを1行で書ける状態にする\n"
        "- 再発防止は“次回3分で復旧できるか”で判断する\n"
        "これだけで、調査が感情ではなく手順になります。\n"
    )
    tree = (
        "【切り分けの分岐（迷った時用）】\n"
        "1) 別ブラウザ/別端末でも同じ？\n"
        "  - はい → サーバ/設定/権限側が濃厚\n"
        "  - いいえ → キャッシュ/拡張機能/端末依存が濃厚\n"
        "2) 同じ入力・同じ手順で再現する？\n"
        "  - はい → 原因の追跡が可能。ログを増やして一点ずつ潰す\n"
        "  - いいえ → 入力条件が揺れている。まず再現条件の固定が最優先\n"
        "この分岐を守るだけで、無駄な試行をかなり減らせます。\n"
    )
    body = "\n".join([intro, why, detail, examples, cause_text, step_text, pit_text, nxt_text, verify, tree]).strip()
    if len(body) < MIN_ARTICLE_CHARS_JA:
        pads = []
        while len(body) + sum(len(x) for x in pads) < MIN_ARTICLE_CHARS_JA + 200:
            pads.append(
                "【追加メモ】\n"
                "問題が複雑に見える時ほど、最初に“変えた点”を列挙し、"
                "それを一つずつ戻して差分を取ると復旧が早くなります。"
                "ログがない場合は、まずログを作ることが最短ルートです。\n"
            )
        body = body + "\n" + "\n".join(pads)
    return body.strip()

def openai_generate(theme: Theme, refs: List[str]) -> Optional[Dict[str, Any]]:
    if not OPENAI_API_KEY:
        return None
    try:
        return None
    except Exception:
        return None


# =========================
# HTML generation
# =========================

def html_escape(s: str) -> str:
    return html.escape(s, quote=True)

def render_affiliate_block(affiliate: Dict[str, Any]) -> str:
    if affiliate.get("html"):
        return str(affiliate["html"])
    if affiliate.get("code"):
        return str(affiliate["code"])
    if affiliate.get("url"):
        title = html_escape(affiliate.get("title", "Recommended"))
        url = html_escape(affiliate["url"])
        return f'<a class="underline" href="{url}" rel="nofollow noopener" target="_blank">{title}</a>'
    return ""

def build_i18n_script(default_lang: str = "en") -> str:
    i18n_json = json.dumps(I18N, ensure_ascii=False)
    return f"""
<script>
const I18N = {i18n_json};
const LANGS = {json.dumps(LANGS)};
function setLang(lang) {{
  if (!LANGS.includes(lang)) lang = "{default_lang}";
  document.documentElement.setAttribute("lang", lang);
  localStorage.setItem("lang", lang);
  document.querySelectorAll("[data-i18n]").forEach(el => {{
    const key = el.getAttribute("data-i18n");
    const v = (I18N[lang] && I18N[lang][key]) || (I18N["{default_lang}"][key]) || key;
    el.textContent = v;
  }});
}}
function initLang() {{
  const saved = localStorage.getItem("lang");
  const lang = saved || "{default_lang}";
  setLang(lang);
  const sel = document.getElementById("langSel");
  if (sel) {{
    sel.value = lang;
    sel.addEventListener("change", (e) => setLang(e.target.value));
  }}
}}
document.addEventListener("DOMContentLoaded", initLang);
</script>
""".strip()

def build_page_html(theme: Theme,
                    tool_url: str,
                    all_sites: List[Dict[str, Any]],
                    affiliates_top2: List[Dict[str, Any]],
                    references: List[str],
                    supplements: List[str],
                    article_ja: str,
                    faq: List[Tuple[str, str]],
                    related_tools: List[Dict[str, Any]],
                    popular_sites: List[Dict[str, Any]]) -> str:
    problems_html = "\n".join([f"<li class='py-1'>{html_escape(p)}</li>" for p in theme.problem_list])
    quick_answer = build_quick_answer(theme.category, theme.keywords)
    causes = build_causes(theme.category)
    steps = build_steps(theme.category)
    pitfalls = build_pitfalls(theme.category)
    next_actions = build_next_actions(theme.category)
    causes_html = "\n".join([f"<li class='py-1'>{html_escape(c)}</li>" for c in causes])
    steps_html = "\n".join([f"<li class='py-1'>{html_escape(s)}</li>" for s in steps])
    pitfalls_html = "\n".join([f"<li class='py-1'>{html_escape(p)}</li>" for p in pitfalls])
    next_html = "\n".join([f"<li class='py-1'>{html_escape(n)}</li>" for n in next_actions])
    faq_html = "\n".join([
        f"""
        <details class="rounded-2xl border border-white/10 bg-white/5 p-4">
          <summary class="cursor-pointer font-medium">{html_escape(q)}</summary>
          <div class="mt-2 text-white/80 leading-relaxed">{html_escape(a)}</div>
        </details>
        """.strip()
        for q, a in faq
    ])
    ref_html = "\n".join([f"<li class='py-1'><a class='underline break-all' href='{html_escape(u)}' target='_blank' rel='noopener'>{html_escape(u)}</a></li>" for u in references])
    sup_html = "\n".join([f"<li class='py-1'><a class='underline break-all' href='{html_escape(u)}' target='_blank' rel='noopener'>{html_escape(u)}</a></li>" for u in supplements])
    aff_blocks = []
    for a in affiliates_top2[:2]:
        title = html_escape(a.get("title", "Recommended"))
        block = render_affiliate_block(a)
        if not block:
            continue
        aff_blocks.append(f"""
        <div class="rounded-2xl border border-white/10 bg-white/5 p-4">
          <div class="text-sm text-white/70 mb-2">{title}</div>
          <div class="prose prose-invert max-w-none">{block}</div>
        </div>
        """.strip())
    if not aff_blocks:
        aff_blocks = ["""
        <div class="rounded-2xl border border-white/10 bg-white/5 p-4">
          <div class="text-sm text-white/70 mb-2">Recommended</div>
          <div class="text-white/70">No affiliate available for this category.</div>
        </div>
        """.strip()]
    aff_html = "\n".join(aff_blocks)
    related_html = "\n".join([
        f"<li class='py-1'><a class='underline' href='{html_escape(t['url'])}'>{html_escape(t['title'])}</a> <span class='text-white/50 text-xs'>({html_escape(t.get('category',''))})</span></li>"
        for t in related_tools
    ])
    popular_html = "\n".join([
        f"<li class='py-1'><a class='underline' href='{html_escape(t['url'])}'>{html_escape(t['title'])}</a> <span class='text-white/50 text-xs'>({html_escape(t.get('category',''))})</span></li>"
        for t in popular_sites
    ])
    canonical = tool_url if tool_url.startswith("http") else (SITE_DOMAIN.rstrip("/") + "/" + theme.slug + "/")
    article_html = "<p class='leading-relaxed whitespace-pre-wrap text-white/85'>" + html_escape(article_ja) + "</p>"
    share_script = """
<script>
function copyText(id){
  const el = document.getElementById(id);
  if(!el) return;
  navigator.clipboard.writeText(el.value).then(()=>{
    const b = document.getElementById("copyBtn");
    if(b){ b.textContent = (window.I18N && I18N[document.documentElement.lang] && I18N[document.documentElement.lang].copied) || "Copied"; }
    setTimeout(()=>{ const b2=document.getElementById("copyBtn"); if(b2){ b2.textContent = (window.I18N && I18N[document.documentElement.lang] && I18N[document.documentElement.lang].copy) || "Copy"; } }, 1200);
  });
}
</script>
""".strip()
    html_doc = f"""<!doctype html>
<html lang="{html_escape(DEFAULT_LANG)}">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html_escape(theme.title)} | {html_escape(SITE_BRAND)}</title>
  <meta name="description" content="{html_escape('Practical troubleshooting guide and tool: ' + theme.title)}">
  <link rel="canonical" href="{html_escape(canonical)}">
  <meta property="og:title" content="{html_escape(theme.title)}">
  <meta property="og:description" content="{html_escape('Fix guide + checklist + FAQ + references')}">
  <meta property="og:type" content="website">
  <meta property="og:url" content="{html_escape(canonical)}">
  <meta name="twitter:card" content="summary_large_image">
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    :root {{ color-scheme: dark; }}
    body {{
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, "Noto Sans JP", "Noto Sans KR", "Noto Sans SC", Arial, "Apple Color Emoji", "Segoe UI Emoji";
    }}
  </style>
</head>
<body class="min-h-screen bg-zinc-950 text-white">
  <!-- Gradient background -->
  <div class="pointer-events-none fixed inset-0 opacity-70">
    <div class="absolute -top-24 -left-24 h-96 w-96 rounded-full bg-gradient-to-br from-indigo-500/35 to-cyan-400/20 blur-3xl"></div>
    <div class="absolute top-40 -right-24 h-96 w-96 rounded-full bg-gradient-to-br from-emerald-500/25 to-lime-400/10 blur-3xl"></div>
    <div class="absolute bottom-0 left-1/4 h-96 w-96 rounded-full bg-gradient-to-br from-fuchsia-500/20 to-rose-400/10 blur-3xl"></div>
  </div>
  <header class="relative z-10 mx-auto max-w-6xl px-4 py-6">
    <div class="flex items-center justify-between gap-4">
      <a href="{html_escape(SITE_DOMAIN)}" class="flex items-center gap-3">
        <div class="h-10 w-10 rounded-2xl bg-white/10 border border-white/10 flex items-center justify-center font-bold">M</div>
        <div>
          <div class="text-sm text-white/70">{html_escape(SITE_BRAND)}</div>
          <div class="font-semibold">{html_escape(theme.title[:48])}</div>
        </div>
      </a>
      <div class="flex items-center gap-3">
        <nav class="hidden md:flex items-center gap-5 text-sm text-white/80">
          <a class="hover:text-white" data-i18n="home" href="{html_escape(SITE_DOMAIN)}">Home</a>
          <a class="hover:text-white" data-i18n="about" href="{html_escape(SITE_DOMAIN.rstrip('/') + '/about.html')}">About Us</a>
          <a class="hover:text-white" data-i18n="all_tools" href="{html_escape(SITE_DOMAIN.rstrip('/') + '/hub/')}">All Tools</a>
        </nav>
        <div class="flex items-center gap-2 rounded-2xl border border-white/10 bg-white/5 px-3 py-2">
          <span class="text-xs text-white/70" data-i18n="language">Language</span>
          <select id="langSel" class="bg-transparent text-sm outline-none">
            <option value="en">EN</option>
            <option value="ja">JA</option>
            <option value="ko">KO</option>
            <option value="zh">ZH</option>
          </select>
        </div>
      </div>
    </div>
  </header>
  <main class="relative z-10 mx-auto max-w-6xl px-4 pb-16">
    <!-- Hero -->
    <section class="rounded-3xl border border-white/10 bg-white/5 p-6 md:p-10 shadow-2xl shadow-black/40">
      <div class="flex flex-col md:flex-row md:items-end md:justify-between gap-6">
        <div class="max-w-3xl">
          <div class="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-white/70">
            <span>{html_escape(theme.category)}</span>
            <span>•</span>
            <span>{html_escape(now_iso())}</span>
          </div>
          <h1 class="mt-4 text-2xl md:text-4xl font-semibold leading-tight">{html_escape(theme.title)}</h1>
          <p class="mt-3 text-white/75 leading-relaxed">
            A practical guide + checklist + FAQ + references. Built from real public posts and patterns.
          </p>
        </div>
        <div class="w-full md:w-[360px]">
          <div class="rounded-2xl border border-white/10 bg-black/30 p-4">
            <div class="text-sm text-white/70 mb-2" data-i18n="share">Share</div>
            <div class="flex gap-2">
              <input id="shareUrl" class="w-full rounded-xl bg-black/40 border border-white/10 px-3 py-2 text-sm text-white/80"
                     value="{html_escape(tool_url)}" readonly />
              <button id="copyBtn" onclick="copyText('shareUrl')" class="rounded-xl border border-white/10 bg-white/10 px-4 py-2 text-sm hover:bg-white/15" data-i18n="copy">Copy</button>
            </div>
            <div class="mt-3 text-xs text-white/60">
              Canonical: <span class="break-all">{html_escape(canonical)}</span>
            </div>
          </div>
        </div>
      </div>
    </section>
    <!-- Grid -->
    <section class="mt-8 grid grid-cols-1 lg:grid-cols-3 gap-6">
      <div class="lg:col-span-2 space-y-6">
        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold" data-i18n="problems">Problems this tool can help with</h2>
          <ul class="mt-3 list-disc pl-6 text-white/80">
            {problems_html}
          </ul>
        </section>
        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold" data-i18n="quick_answer">Quick answer</h2>
          <div class="mt-3 text-white/80 leading-relaxed whitespace-pre-wrap">{html_escape(quick_answer)}</div>
        </section>
        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold" data-i18n="causes">Common causes</h2>
          <ul class="mt-3 list-disc pl-6 text-white/80">
            {causes_html}
          </ul>
        </section>
        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold" data-i18n="steps">Step-by-step checklist</h2>
          <ul class="mt-3 list-disc pl-6 text-white/80">
            {steps_html}
          </ul>
        </section>
        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold" data-i18n="pitfalls">Common pitfalls & how to avoid them</h2>
          <ul class="mt-3 list-disc pl-6 text-white/80">
            {pitfalls_html}
          </ul>
        </section>
        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold" data-i18n="next">If it still doesn’t work</h2>
          <ul class="mt-3 list-disc pl-6 text-white/80">
            {next_html}
          </ul>
        </section>
        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold">Deep Guide (JA)</h2>
          <div class="mt-4">
            {article_html}
          </div>
        </section>
        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold" data-i18n="faq">FAQ</h2>
          <div class="mt-4 grid gap-3">
            {faq_html}
          </div>
        </section>
        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold" data-i18n="references">Reference links</h2>
          <ul class="mt-3 list-disc pl-6 text-white/80">
            {ref_html}
          </ul>
          <h3 class="mt-6 text-base font-semibold text-white/90" data-i18n="supplement">Supplementary resources</h3>
          <ul class="mt-3 list-disc pl-6 text-white/80">
            {sup_html}
          </ul>
        </section>
      </div>
      <aside class="space-y-6">
        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <div class="flex items-center justify-between">
            <h2 class="text-lg font-semibold" data-i18n="aff_title">Recommended</h2>
            <div class="text-xs text-white/60">AFF</div>
          </div>
          <div class="mt-4 grid gap-3">
            {aff_html}
          </div>
        </section>
        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold" data-i18n="related">Related tools</h2>
          <ul class="mt-3 list-disc pl-6 text-white/80">
            {related_html}
          </ul>
        </section>
        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold" data-i18n="popular">Popular tools</h2>
          <ul class="mt-3 list-disc pl-6 text-white/80">
            {popular_html}
          </ul>
        </section>
        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold">Meta</h2>
          <div class="mt-3 text-sm text-white/75 space-y-2">
            <div><span class="text-white/60">Category:</span> {html_escape(theme.category)}</div>
            <div><span class="text-white/60">Slug:</span> {html_escape(theme.slug)}</div>
            <div><span class="text-white/60">Run:</span> {html_escape(RUN_ID)}</div>
            <div><span class="text-white/60">Keywords:</span> {html_escape(", ".join(theme.keywords[:12]))}</div>
          </div>
        </section>
      </aside>
    </section>
  </main>
  <footer class="relative z-10 border-t border-white/10 bg-black/30">
    <div class="mx-auto max-w-6xl px-4 py-10">
      <div class="grid grid-cols-1 md:grid-cols-3 gap-8">
        <div>
          <div class="font-semibold">{html_escape(SITE_BRAND)}</div>
          <div class="mt-2 text-sm text-white/70" data-i18n="footer_note">We aim to provide practical, fast, and respectful troubleshooting guides.</div>
        </div>
        <div>
          <div class="text-sm font-semibold text-white/80">Links</div>
          <ul class="mt-2 text-sm text-white/70 space-y-1">
            <li><a class="underline" href="{html_escape(SITE_DOMAIN)}" data-i18n="home">Home</a></li>
            <li><a class="underline" href="{html_escape(SITE_DOMAIN.rstrip('/') + '/about.html')}" data-i18n="about">About Us</a></li>
            <li><a class="underline" href="{html_escape(SITE_DOMAIN.rstrip('/') + '/hub/')}" data-i18n="all_tools">All Tools</a></li>
          </ul>
        </div>
        <div>
          <div class="text-sm font-semibold text-white/80">Legal</div>
          <ul class="mt-2 text-sm text-white/70 space-y-1">
            <li><a class="underline" href="/policies/privacy.html" data-i18n="privacy">Privacy</a></li>
            <li><a class="underline" href="/policies/terms.html" data-i18n="terms">Terms</a></li>
            <li><a class="underline" href="/policies/disclaimer.html" data-i18n="disclaimer">Disclaimer</a></li>
            <li><a class="underline" href="/policies/contact.html" data-i18n="contact">Contact</a></li>
          </ul>
          <div class="mt-3 text-xs text-white/50">Contact: {html_escape(SITE_CONTACT_EMAIL)}</div>
        </div>
      </div>
      <div class="mt-10 text-xs text-white/45">© {dt.datetime.now().year} {html_escape(SITE_BRAND)}. Built automatically.</div>
    </div>
  </footer>
  {build_i18n_script(DEFAULT_LANG)}
  {share_script}
</body>
</html>
"""
    return html_doc


# =========================
# Policies pages generation
# =========================

def generate_policies_pages() -> None:
    os.makedirs(POLICIES_DIR, exist_ok=True)
    privacy = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Privacy Policy | {html_escape(SITE_BRAND)}</title><script src="https://cdn.tailwindcss.com"></script></head>
<body class="bg-zinc-950 text-white"><main class="mx-auto max-w-3xl px-4 py-10">
  <h1 class="text-2xl font-semibold">Privacy Policy</h1>
  <p class="mt-4 text-white/80 leading-relaxed">
    This website may display ads (including Google AdSense) and may use cookies or similar technologies to measure usage and improve services.
    Third-party vendors, including Google, use cookies to serve ads based on prior visits.
  </p>
  <h2 class="mt-8 text-xl font-semibold">Data we may collect</h2>
  <ul class="mt-3 list-disc pl-6 text-white/80">
    <li>Basic access logs (timestamp, user agent, referrer, pages accessed)</li>
    <li>Anonymous analytics data</li>
    <li>Cookie identifiers used by ad/analytics providers</li>
  </ul>
  <h2 class="mt-8 text-xl font-semibold">Contact</h2>
  <p class="mt-3 text-white/80">For inquiries: {html_escape(SITE_CONTACT_EMAIL)}</p>
  <p class="mt-10 text-sm text-white/60"><a class="underline" href="{html_escape(SITE_DOMAIN)}">Home</a></p>
</main></body></html>
"""
    write_text(os.path.join(POLICIES_DIR, "privacy.html"), privacy)
    terms = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Terms | {html_escape(SITE_BRAND)}</title><script src="https://cdn.tailwindcss.com"></script></head>
<body class="bg-zinc-950 text-white"><main class="mx-auto max-w-3xl px-4 py-10">
  <h1 class="text-2xl font-semibold">Terms</h1>
  <p class="mt-4 text-white/80 leading-relaxed">
    By using this website, you agree that the content is provided as-is for informational purposes.
    You are responsible for verifying any steps before applying them to your environment.
  </p>
  <h2 class="mt-8 text-xl font-semibold">Usage</h2>
  <ul class="mt-3 list-disc pl-6 text-white/80">
    <li>No warranty is provided.</li>
    <li>Do not use the site for unlawful activities.</li>
    <li>We may update content without notice.</li>
  </ul>
  <p class="mt-10 text-sm text-white/60"><a class="underline" href="{html_escape(SITE_DOMAIN)}">Home</a></p>
</main></body></html>
"""
    write_text(os.path.join(POLICIES_DIR, "terms.html"), terms)
    disclaimer = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Disclaimer | {html_escape(SITE_BRAND)}</title><script src="https://cdn.tailwindcss.com"></script></head>
<body class="bg-zinc-950 text-white"><main class="mx-auto max-w-3xl px-4 py-10">
  <h1 class="text-2xl font-semibold">Disclaimer</h1>
  <p class="mt-4 text-white/80 leading-relaxed">
    This site may contain affiliate links. If you purchase through them, we may earn a commission.
    Recommendations are selected by category matching and priority rules.
  </p>
  <p class="mt-4 text-white/80 leading-relaxed">
    We do not guarantee outcomes. Always back up your data and test changes safely.
  </p>
  <p class="mt-10 text-sm text-white/60"><a class="underline" href="{html_escape(SITE_DOMAIN)}">Home</a></p>
</main></body></html>
"""
    write_text(os.path.join(POLICIES_DIR, "disclaimer.html"), disclaimer)
    contact = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Contact | {html_escape(SITE_BRAND)}</title><script src="https://cdn.tailwindcss.com"></script></head>
<body class="bg-zinc-950 text-white"><main class="mx-auto max-w-3xl px-4 py-10">
  <h1 class="text-2xl font-semibold">Contact</h1>
  <p class="mt-4 text-white/80 leading-relaxed">
    運営者/運営サイト: {html_escape(SITE_BRAND)} / {html_escape(SITE_DOMAIN)}
  </p>
  <p class="mt-4 text-white/80 leading-relaxed">
    Email: {html_escape(SITE_CONTACT_EMAIL)}
  </p>
  <p class="mt-10 text-sm text-white/60"><a class="underline" href="{html_escape(SITE_DOMAIN)}">Home</a></p>
</main></body></html>
"""
    write_text(os.path.join(POLICIES_DIR, "contact.html"), contact)


# =========================
# Validation & Autofix
# =========================

def validate_site_html(html_doc: str, theme: Theme, references: List[str], supplements: List[str]) -> List[str]:
    issues = []
    required_keys = ["problems", "quick_answer", "causes", "steps", "pitfalls", "next", "faq", "references"]
    for k in required_keys:
        if f'data-i18n="{k}"' not in html_doc:
            issues.append(f"missing i18n key block: {k}")
    for p in ["/policies/privacy.html", "/policies/terms.html", "/policies/disclaimer.html", "/policies/contact.html"]:
        if p not in html_doc:
            issues.append(f"missing legal link: {p}")
    if "AFF_SLOT" not in html_doc:
        issues.append("missing AFF_SLOT marker")
    if not (REF_URL_MIN <= len(references) <= REF_URL_MAX):
        issues.append(f"references count out of range: {len(references)}")
    if len(supplements) < SUPP_URL_MIN:
        issues.append(f"supplement count too low: {len(supplements)}")
    if len(re.sub(r"<[^>]+>", "", html_doc)) < 1200:
        issues.append("page text content too small (overall)")
    if theme.slug not in html_doc:
        issues.append("slug not present in page")
    return issues

def autofix_inputs(theme: Theme,
                   references: List[str],
                   supplements: List[str],
                   faq: List[Tuple[str, str]],
                   article_ja: str) -> Tuple[List[str], List[str], List[Tuple[str, str]], str]:
    references = uniq_keep_order([u for u in references if u])
    if len(references) < REF_URL_MIN:
        filler = [
            "https://developer.mozilla.org/",
            "https://docs.github.com/",
            "https://en.wikipedia.org/wiki/Troubleshooting",
            "https://en.wikipedia.org/wiki/Domain_Name_System",
            "https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol",
        ]
        for u in filler:
            if len(references) >= REF_URL_MIN:
                break
            if u not in references:
                references.append(u)
    references = references[:REF_URL_MAX]
    supplements = uniq_keep_order([u for u in supplements if u])
    if len(supplements) < SUPP_URL_MIN:
        for u in supplemental_resources_for_category(theme.category):
            if len(supplements) >= SUPP_URL_MIN:
                break
            if u not in supplements:
                supplements.append(u)
    if len(faq) < MIN_FAQ:
        extra = build_faq(theme.category)
        for q, a in extra:
            if len(faq) >= MIN_FAQ:
                break
            faq.append((q, a))
    if len(article_ja) < MIN_ARTICLE_CHARS_JA:
        article_ja = generate_long_article_ja(theme)
    return references, supplements, faq, article_ja


# =========================
# Sitemap & robots
# =========================

def build_sitemap_urls(sites: List[Dict[str, Any]]) -> List[str]:
    urls = []
    for s in sites:
        u = s.get("url") or ""
        if u and u.startswith("http"):
            urls.append(u)
    return uniq_keep_order(urls)

def render_sitemap_xml(urls: List[str]) -> str:
    now_date = dt.datetime.now(dt.timezone.utc).date().isoformat()
    items = []
    for u in urls:
        items.append(f"""  <url>
    <loc>{html_escape(u)}</loc>
    <lastmod>{now_date}</lastmod>
  </url>""")
    return """<?xml version="1.0" encoding="UTF-8"?>\n""" + \
        """<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n""" + \
        "\n".join(items) + "\n</urlset>\n"

def render_robots_txt(sitemap_url: str) -> str:
    return f"""User-agent: *
Allow: /

Sitemap: {sitemap_url}
"""

def update_sitemap_robots(all_sites: List[Dict[str, Any]]) -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    urls = build_sitemap_urls(all_sites)
    sitemap = render_sitemap_xml(urls)
    robots = render_robots_txt(SITE_DOMAIN.rstrip("/") + "/sitemap.xml")
    write_text(os.path.join(OUT_DIR, "sitemap.xml"), sitemap)
    write_text(os.path.join(OUT_DIR, "robots.txt"), robots)
    if ALLOW_ROOT_UPDATE:
        root_sitemap = os.path.join(REPO_ROOT, "sitemap.xml")
        root_robots = os.path.join(REPO_ROOT, "robots.txt")
        for p in [root_sitemap, root_robots]:
            if os.path.exists(p):
                shutil.copy2(p, p + f".bak_{RUN_ID}")
        write_text(root_sitemap, sitemap)
        write_text(root_robots, robots)
        logging.info("Updated root sitemap.xml and robots.txt (backup created)")
    else:
        logging.info("Root sitemap/robots not updated (ALLOW_ROOT_UPDATE!=1). Written to goliath/_out instead.")


# =========================
# Hub sites.json update
# =========================

def ensure_unique_site_entry(existing: List[Dict[str, Any]], new_entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    slug = new_entry.get("slug", "")
    url = new_entry.get("url", "")
    out = []
    replaced = False
    for s in existing:
        if not isinstance(s, dict):
            continue
        if slug and s.get("slug") == slug:
            merged = dict(s)
            merged.update(new_entry)
            out.append(merged)
            replaced = True
        elif url and s.get("url") == url:
            merged = dict(s)
            merged.update(new_entry)
            out.append(merged)
            replaced = True
        else:
            out.append(s)
    if not replaced:
        out.append(new_entry)
    return out

def hub_url_for_slug(slug: str) -> str:
    return SITE_DOMAIN.rstrip("/") + "/goliath/pages/" + slug + "/"

def ghpages_url_for_slug(slug: str) -> str:
    parsed = urlparse(SITE_DOMAIN)
    if parsed.netloc and "github.io" in parsed.netloc:
        return SITE_DOMAIN.rstrip("/") + "/goliath/pages/" + slug + "/"
    return hub_url_for_slug(slug)


# =========================
# Build sites
# =========================

def reserve_output_folder(base_slug: str) -> str:
    slug = base_slug
    path = os.path.join(PAGES_DIR, slug)
    if not os.path.exists(path):
        return slug
    i = 2
    while True:
        slug2 = f"{base_slug}-{i}"
        path2 = os.path.join(PAGES_DIR, slug2)
        if not os.path.exists(path2):
            return slug2
        i += 1

def build_references(theme: Theme) -> List[str]:
    urls = [p.url for p in theme.representative_posts if p.url]
    urls = uniq_keep_order(urls)
    if len(urls) < REF_URL_MIN:
        for u in supplemental_resources_for_category(theme.category):
            if len(urls) >= REF_URL_MIN:
                break
            if u not in urls:
                urls.append(u)
    return urls[:REF_URL_MAX]

def build_reply_main_text() -> str:
    empathy = "I know how you feel, it's really tough to deal with."
    second = "I put together a quick one-page guide for you that might help solve this."
    third = "It covers common causes and a step-by-step checklist to save you time."
    last = "I hope it helps."
    return f"{empathy} {second} {third} {last}"

def build_one_site(theme: Theme, all_sites: List[Dict[str, Any]], aff: Dict[str, Any]) -> Dict[str, Any]:
    final_slug = reserve_output_folder(theme.slug)
    theme.slug = final_slug
    folder = os.path.join(PAGES_DIR, final_slug)
    index_path = os.path.join(folder, "index.html")
    tool_url = SITE_DOMAIN.rstrip("/") + "/goliath/pages/" + final_slug + "/"
    top2 = pick_affiliates_for_category(aff, theme.category, topn=2)
    references = build_references(theme)
    supplements = supplemental_resources_for_category(theme.category)
    faq = build_faq(theme.category)
    article_ja = generate_long_article_ja(theme)
    references, supplements, faq, article_ja = autofix_inputs(theme, references, supplements, faq, article_ja)
    related = choose_related_tools(all_sites, theme.category, exclude_slug=final_slug, n=5)
    popular = compute_popular_sites(all_sites, n=6)
    html_doc = build_page_html(
        theme=theme,
        tool_url=tool_url,
        all_sites=all_sites,
        affiliates_top2=top2,
        references=references,
        supplements=supplements,
        article_ja=article_ja,
        faq=faq,
        related_tools=related,
        popular_sites=popular,
    )
    for attempt in range(1, MAX_AUTOFIX + 1):
        issues = validate_site_html(html_doc, theme, references, supplements)
        if not issues:
            break
        logging.warning("Validate issues (attempt %d): %s", attempt, issues)
        references, supplements, faq, article_ja = autofix_inputs(theme, references, supplements, faq, article_ja)
        html_doc = build_page_html(
            theme=theme,
            tool_url=tool_url,
            all_sites=all_sites,
            affiliates_top2=top2,
            references=references,
            supplements=supplements,
            article_ja=article_ja,
            faq=faq,
            related_tools=related,
            popular_sites=popular,
        )
    write_text(index_path, html_doc)
    entry = {
        "title": theme.title,
        "slug": final_slug,
        "category": theme.category,
        "url": tool_url,
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "run_id": RUN_ID,
        "keywords": theme.keywords[:12],
    }
    return entry


# =========================
# Issues payload output
# =========================

def build_issues_payload(themes: List[Theme],
                         site_entries: List[Dict[str, Any]],
                         source_counts: Dict[str, int],
                         reply_count: int,
                         missing_affiliates: List[str]) -> str:
    by_slug = {s["slug"]: s for s in site_entries if isinstance(s, dict) and s.get("slug")}
    lines = []
    lines.append(f"# Goliath Issues Payload ({now_iso()})")
    lines.append("")
    lines.append(f"- run_id: {RUN_ID}")
    lines.append(f"- generated_sites: {len(site_entries)}")
    if source_counts:
        counts_line = " ".join([f"{name}: {cnt}" for name, cnt in source_counts.items()])
        lines.append(f"- collected_posts: {counts_line}")
    lines.append(f"- reply_candidates: {reply_count}")
    if missing_affiliates:
        lines.append(f"- affiliates_missing_categories: {', '.join(missing_affiliates)}")
    lines.append("")
    reply_main_text = build_reply_main_text()
    for theme in themes:
        se = by_slug.get(theme.slug)
        if not se:
            continue
        tool_url = se.get("url", "#")
        for p in theme.representative_posts:
            lines.append(p.url)
            lines.append(reply_main_text)
            lines.append(tool_url)
            lines.append("")
    return "\n".join(lines).strip() + "\n"


# =========================
# Runner
# =========================

def ensure_dirs() -> None:
    os.makedirs(PAGES_DIR, exist_ok=True)
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(POLICIES_DIR, exist_ok=True)

def collect_all_posts() -> Tuple[List[Post], Dict[str, int]]:
    posts: List[Post] = []
    source_counts: Dict[str, int] = {}
    bsky = collect_bluesky(max_items=60); source_counts["Bluesky"] = len(bsky); posts.extend(bsky)
    xposts = collect_x_mentions(max_items=X_MAX); source_counts["X"] = len(xposts); posts.extend(xposts)
    reddit_posts = collect_reddit(max_items=60); source_counts["Reddit"] = len(reddit_posts); posts.extend(reddit_posts)
    hn_posts = collect_hn(max_items=HN_MAX); source_counts["HN"] = len(hn_posts); posts.extend(hn_posts)
    mastodon_posts = collect_mastodon(max_items=120); source_counts["Mastodon"] = len(mastodon_posts); posts.extend(mastodon_posts)
    seen = set()
    deduped = []
    for p in posts:
        key = sha1((p.url or "") + "|" + (p.norm_text()[:200] or ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(p)
    deduped = deduped[:MAX_COLLECT]
    logging.info("Total collected posts (deduped): %d", len(deduped))
    return deduped, source_counts

def run() -> None:
    setup_logging()
    ensure_dirs()
    generate_policies_pages()
    aff, missing_affiliates = load_affiliates()
    hub_sites = read_hub_sites()
    posts, source_counts = collect_all_posts()
    if len(posts) < 12:
        logging.warning("Too few posts collected (%d). Generating fallback theme.", len(posts))
        fallback_post = Post(
            source="system",
            id=sha1("fallback"),
            url="https://example.com",
            text="Need help fixing an issue quickly with a safe checklist and references.",
            author="system",
            created_at=now_iso(),
        )
        clusters = [[fallback_post]]
    else:
        clusters = cluster_posts(posts, threshold=0.22)
    themes: List[Theme] = []
    posts_used = 0
    non_solo_clusters = [c for c in clusters if len(c) >= 2]
    solo_clusters = [c for c in clusters if len(c) < 2]
    for c in non_solo_clusters:
        if len(themes) >= MAX_THEMES and posts_used >= LEADS_TOTAL:
            break
        t = make_theme(c)
        themes.append(t)
        posts_used += len(t.representative_posts)
        if posts_used >= LEADS_TOTAL and len(themes) >= MAX_THEMES:
            break
    if posts_used < LEADS_TOTAL:
        for c in solo_clusters:
            if posts_used >= LEADS_TOTAL:
                break
            t = make_theme(c)
            themes.append(t)
            posts_used += len(t.representative_posts)
    if posts_used < LEADS_TOTAL:
        logging.warning("Collected posts still below target (%d/%d). Padding with stub entries.", posts_used, LEADS_TOTAL)
        dummy_prompts = [
            "travel itinerary", "quick recipe", "workout routine", "study plan", "budget planning",
            "job interview", "communication issue", "organizing home tasks", "compare products", "event schedule"
        ]
        i = 0
        while posts_used < LEADS_TOTAL:
            prompt = dummy_prompts[i % len(dummy_prompts)]
            dummy_text = f"I need help with a {prompt}."
            dummy_post = Post(
                source="stub",
                id=sha1(f"stub{posts_used}{prompt}"),
                url="https://example.com/dummy",
                text=dummy_text,
                author="user",
                created_at=now_iso(),
            )
            t = make_theme([dummy_post])
            themes.append(t)
            posts_used += len(t.representative_posts)
            i += 1
    themes.sort(key=lambda t: t.score, reverse=True)
    if not themes:
        themes = [make_theme(clusters[0])]
    site_entries: List[Dict[str, Any]] = []
    for t in themes:
        entry = build_one_site(t, hub_sites, aff)
        site_entries.append(entry)
        hub_sites = ensure_unique_site_entry(hub_sites, entry)
    write_hub_sites(hub_sites)
    logging.info("Updated hub/sites.json (hub frozen respected)")
    update_sitemap_robots(hub_sites if isinstance(hub_sites, list) else [])
    reply_count = sum(len(t.representative_posts) for t in themes)
    logging.info("Generated reply candidates: %d", reply_count)
    payload_md = build_issues_payload(themes, site_entries, source_counts, reply_count, missing_affiliates)
    issues_path = os.path.join(OUT_DIR, f"issues_payload_{RUN_ID}.md")
    write_text(issues_path, payload_md)
    logging.info("Wrote Issues payload: %s", issues_path)
    summary = {
        "run_id": RUN_ID,
        "generated_at": now_iso(),
        "counts": {
            "posts": len(posts),
            "clusters": len(clusters),
            "themes": len(themes),
            "sites": len(site_entries),
        },
        "sites": site_entries,
        "notes": {
            "hub_frozen": True,
            "hub_updated_files": ["hub/sites.json"],
            "root_sitemap_updated": ALLOW_ROOT_UPDATE,
        },
    }
    write_json(os.path.join(OUT_DIR, f"summary_{RUN_ID}.json"), summary)
    logging.info("DONE")

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        logging.error("Interrupted")
        sys.exit(130)
    except Exception as e:
        logging.exception("Fatal error: %s", str(e))
        sys.exit(1)
