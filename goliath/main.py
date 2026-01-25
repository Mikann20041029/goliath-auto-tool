#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Goliath Auto Tool System - main.py (single-file)

RUN ONCE per execution (schedule outside).
Collect from: Bluesky, Mastodon, Reddit, Hacker News, X (mentions + optional search)
Cluster -> choose best themes -> generate solution site(s)
Validate/autofix up to N (lightweight)
Update hub/sites.json only (hub frozen)
Generate sitemap.xml + robots.txt safely (default: goliath/_out; root only if ALLOW_ROOT_UPDATE=1)
Output Issues payload (bulk) with: pain URL + empathy reply + generated page URL
22 genres mapping -> affiliates.json top2 -> inject to AFF_SLOT
SaaS-like design, Tailwind, dark mode, i18n (EN/JA/KO/ZH), 2500+ chars (JA) article
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
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, quote
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError


# =========================
# Config (ENV)
# =========================

REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())

GOLIATH_DIR = os.path.join(REPO_ROOT, "goliath")
PAGES_DIR = os.path.join(GOLIATH_DIR, "pages")
OUT_DIR = os.path.join(GOLIATH_DIR, "_out")          # safe outputs (sitemap/robots/issues payload, etc.)
POLICIES_DIR = os.path.join(REPO_ROOT, "policies")   # allowed new folder (your rule)
HUB_DIR = os.path.join(REPO_ROOT, "hub")
HUB_SITES_JSON = os.path.join(HUB_DIR, "sites.json")

AFFILIATES_JSON = os.environ.get("AFFILIATES_JSON", os.path.join(REPO_ROOT, "affiliates.json"))

DEFAULT_LANG = os.environ.get("DEFAULT_LANG", "en")  # en/ja/ko/zh
LANGS = ["en", "ja", "ko", "zh"]

RUN_ID = os.environ.get("RUN_ID", str(int(time.time())))
MAX_THEMES = int(os.environ.get("MAX_THEMES", "6"))          # how many sites to build per run
MAX_COLLECT = int(os.environ.get("MAX_COLLECT", "260"))      # overall target (spec 173+; we overshoot)
MAX_AUTOFIX = int(os.environ.get("MAX_AUTOFIX", "5"))
RANDOM_SEED = os.environ.get("RANDOM_SEED", RUN_ID)

ALLOW_ROOT_UPDATE = os.environ.get("ALLOW_ROOT_UPDATE", "0") == "1"

# Social API credentials (optional)
BLUESKY_HANDLE = os.environ.get("BLUESKY_HANDLE", "")
BLUESKY_APP_PASSWORD = os.environ.get("BLUESKY_APP_PASSWORD", "")

MASTODON_BASE = os.environ.get("MASTODON_BASE", "")          # e.g. https://mastodon.social
MASTODON_TOKEN = os.environ.get("MASTODON_TOKEN", "")

REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET", "")
REDDIT_REFRESH_TOKEN = os.environ.get("REDDIT_REFRESH_TOKEN", "")
REDDIT_USER_AGENT = os.environ.get("REDDIT_USER_AGENT", "goliath-tool/1.0 (read-only)")
REDDIT_SUBREDDITS = os.environ.get(
    "REDDIT_SUBREDDITS",
    # mixed tech + life
    "webdev,sysadmin,programming,learnprogramming,privacy,photography,excel,smallbusiness,marketing,"
    "travel,solotravel,cooking,mealprep,fitness,loseit,productivity,studytips,personalfinance,careerguidance,relationships"
)

HN_QUERY = os.environ.get("HN_QUERY", "help OR error OR issue OR how to OR guide OR checklist")
HN_MAX = int(os.environ.get("HN_MAX", "70"))

# X/Twitter
X_BEARER_TOKEN = os.environ.get("X_BEARER_TOKEN", "")
X_USER_ID = os.environ.get("X_USER_ID", "")          # numeric user id for mentions lookup
X_MAX = int(os.environ.get("X_MAX", "5"))            # spec mentions-limited default
X_SEARCH_MAX = int(os.environ.get("X_SEARCH_MAX", "20"))
X_ENABLE_SEARCH = os.environ.get("X_ENABLE_SEARCH", "0") == "1"
X_SEARCH_QUERY = os.environ.get(
    "X_SEARCH_QUERY",
    # safe broad mix; you can override
    "(help OR stuck OR confused OR checklist OR template OR itinerary OR packing OR esim OR refund OR cancellation OR recipe OR meal prep OR calories OR sleep OR workout OR study plan OR procrastination OR resume OR interview OR budget OR compare) -is:retweet lang:en"
)

# OpenAI (optional)
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")  # you can change

# Content requirements
MIN_ARTICLE_CHARS_JA = int(os.environ.get("MIN_ARTICLE_CHARS_JA", "2500"))  # requirement
MIN_FAQ = int(os.environ.get("MIN_FAQ", "5"))
REF_URL_MIN = int(os.environ.get("REF_URL_MIN", "10"))
REF_URL_MAX = int(os.environ.get("REF_URL_MAX", "20"))
SUPP_URL_MIN = int(os.environ.get("SUPP_URL_MIN", "3"))

# Lead / Issue requirements
LEADS_TOTAL = int(os.environ.get("LEADS_TOTAL", "100"))  # MUST be >=100 default
ISSUE_CHUNK_SIZE = int(os.environ.get("ISSUE_CHUNK_SIZE", "35"))  # markdown chunking

# Layout / branding
SITE_BRAND = os.environ.get("SITE_BRAND", "Mikanntool")
SITE_DOMAIN = os.environ.get("SITE_DOMAIN", "https://mikanntool.com")  # canonical/og base
SITE_CONTACT_EMAIL = os.environ.get("SITE_CONTACT_EMAIL", "contact@mikanntool.com")

# Keep hub frozen: do not touch these (ONLY sites.json is allowed)
FROZEN_PATH_PREFIXES = [
    os.path.join(REPO_ROOT, "hub", "index.html"),
    os.path.join(REPO_ROOT, "hub", "assets"),
    os.path.join(REPO_ROOT, "hub", "assets", "ui.v3.css"),
    os.path.join(REPO_ROOT, "hub", "assets", "app.v3.js"),
]

# 22 categories fixed (spec)
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

# Keywords (tech + life) for filtering/scoring
KEYWORDS = [
    # Tech
    "help", "error", "issue", "bug", "fix", "failed", "can't", "cannot", "doesn't work", "broken",
    "dns", "cname", "aaaa", "ssl", "https", "github pages", "domain", "redirect",
    "pdf", "docx", "pptx", "convert", "merge", "compress", "mp4", "ffmpeg",
    "excel", "sheets", "vlookup", "pivot", "formula", "csv",
    "privacy", "cookie", "2fa", "phishing", "vpn",
    "automation", "workflow", "cron", "github actions", "api", "rate limit",
    # Life
    "itinerary", "travel plan", "packing list", "layover", "esim", "refund", "cancellation", "budget",
    "recipe", "meal prep", "calories", "protein", "macro", "grocery list",
    "sleep", "workout", "routine", "habit", "weight loss",
    "study plan", "memorize", "revision", "procrastination", "focus",
    "resume", "cv", "interview", "job", "career",
    "anxiety", "awkward", "conversation", "template",
    "checklist", "step-by-step", "compare", "best", "recommend",
]

# Content safety exclusions (keep strict)
BAN_WORDS = [
    # violence/hate/illegal (light screen)
    "kill yourself", "suicide", "how to make a bomb", "buy drugs", "credit card dump",
]
ADULT_WORDS = [
    # adult explicit (exclude)
    "porn", "sex", "nude", "onlyfans", "hookup", "fetish",
]
SENSITIVE_WORDS = [
    # self-harm intent
    "i want to die", "end my life", "self harm",
]

random.seed(int(hashlib.sha256(RANDOM_SEED.encode("utf-8")).hexdigest()[:8], 16))


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
    logging.info("SITE_DOMAIN=%s", SITE_DOMAIN)


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
    s = (s or "").strip().lower()
    s = re.sub(r"https?://", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    if not s:
        s = "tool"
    return s[:maxlen].strip("-") or "tool"

def sha1(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

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

def base64_encode(s: str) -> str:
    import base64
    return base64.b64encode(s.encode("utf-8")).decode("ascii")


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
# Content filters
# =========================

def is_adult_or_sensitive(text: str) -> bool:
    low = (text or "").lower()
    if any(w in low for w in BAN_WORDS):
        return True
    if any(w in low for w in ADULT_WORDS):
        return True
    if any(w in low for w in SENSITIVE_WORDS):
        return True
    return False

def looks_like_spam(text: str) -> bool:
    low = (text or "").lower()
    if "airdrop" in low and "crypto" in low:
        return True
    if low.count("http://") + low.count("https://") >= 5:
        return True
    if len(low) < 20:
        return True
    return False

def keep_post(p: Post) -> bool:
    t = p.norm_text()
    if not t or not p.url:
        return False
    if is_adult_or_sensitive(t):
        return False
    if looks_like_spam(t):
        return False
    return True


# =========================
# Collectors
# =========================

def collect_bluesky(max_items: int = 60) -> List[Post]:
    """
    ATProto:
      - createSession: https://bsky.social/xrpc/com.atproto.server.createSession
      - searchPosts:   https://bsky.social/xrpc/app.bsky.feed.searchPosts?q=...
    """
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

    queries = [
        # Tech
        "help error", "how do i fix", "can't login", "failed to",
        "pdf convert", "compress mp4", "excel formula", "privacy settings",
        "dns cname aaaa", "github pages domain", "redirect loop", "ssl certificate",
        # Life
        "itinerary template", "packing list", "layover advice", "esim not working",
        "refund cancellation", "budget plan", "meal prep plan", "recipe ideas",
        "calorie deficit", "sleep schedule", "workout routine",
        "study plan", "procrastination", "resume advice", "interview tips",
        "conversation template", "awkward silence",
        "moving checklist", "declutter checklist", "compare products",
        "weekend plan rain",
    ]

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
            p = Post(
                source="bluesky",
                id=pid,
                url=post_url,
                text=text or "",
                author=author,
                created_at=created_at or now_iso(),
                meta={"query": q, "uri": uri, "cid": cid},
            )
            if keep_post(p):
                posts.append(p)

    logging.info("Bluesky: collected %d", len(posts))
    return posts

def collect_mastodon(max_items: int = 120) -> List[Post]:
    """
    Mastodon:
      - public timeline: /api/v1/timelines/public?limit=
      - tag timeline: /api/v1/timelines/tag/{tag}?limit=
      - search: /api/v2/search?q=...&type=statuses&resolve=true
    """
    if not (MASTODON_BASE and MASTODON_TOKEN):
        logging.info("Mastodon: skipped (missing MASTODON_BASE/MASTODON_TOKEN)")
        return []

    base = MASTODON_BASE.rstrip("/")
    headers = {"Authorization": f"Bearer {MASTODON_TOKEN}", "Accept": "application/json"}
    logging.info("Mastodon: collecting up to %d from %s", max_items, base)

    tags = [
        # Tech
        "help", "support", "webdev", "privacy", "excel", "opensource", "github", "dns", "wordpress", "linux",
        # Life
        "travel", "solotravel", "cooking", "mealprep", "fitness", "studytips", "personalfinance", "career",
        "productivity", "relationships", "lifehacks",
    ]
    queries = [
        # Tech
        "need help", "error", "how to fix", "cannot", "failed", "issue", "bug",
        # Life
        "itinerary", "packing list", "eSIM", "refund", "budget",
        "meal prep", "recipe", "calories", "sleep", "workout",
        "study plan", "procrastination",
        "resume", "interview",
        "conversation template", "awkward",
        "moving checklist", "declutter",
        "compare products", "weekend plan",
    ]

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
            p = Post(
                source="mastodon",
                id=pid,
                url=url,
                text=text,
                author=acct,
                created_at=created_at,
                meta={"hint": hint},
            )
            if keep_post(p):
                out.append(p)

    st, body = http_get(f"{base}/api/v1/timelines/public?limit=40", headers=headers, timeout=20)
    if st == 200:
        try:
            add_statuses(json.loads(body), "public")
        except Exception:
            pass

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
    basic = "Basic " + base64_encode(auth)
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

def collect_reddit(max_items: int = 60) -> List[Post]:
    """
    Reddit:
      - OAuth preferred; else public JSON endpoints (rate-limited)
      - Pull /new and filter by broad triggers (tech + life)
    """
    subs = [x.strip() for x in REDDIT_SUBREDDITS.split(",") if x.strip()]
    if not subs:
        subs = ["webdev", "sysadmin", "travel", "cooking"]

    token = reddit_oauth_token()
    if token:
        base = "https://oauth.reddit.com"
        headers = {"Authorization": f"bearer {token}", "User-Agent": REDDIT_USER_AGENT, "Accept": "application/json"}
        logging.info("Reddit: OAuth mode collecting up to %d", max_items)
    else:
        base = "https://www.reddit.com"
        headers = {"User-Agent": REDDIT_USER_AGENT, "Accept": "application/json"}
        logging.info("Reddit: public mode collecting up to %d", max_items)

    triggers = [
        "help", "how to", "issue", "can't", "cannot", "failed", "fix",
        "itinerary", "packing", "esim", "refund", "budget",
        "meal prep", "recipe", "calories", "sleep", "workout",
        "study plan", "procrastination",
        "resume", "interview",
        "template", "checklist", "compare", "recommend",
    ]

    out: List[Post] = []
    per_sub_limit = max(10, int(math.ceil(max_items / max(1, min(len(subs), 10)))))
    for sub in subs:
        if len(out) >= max_items:
            break

        st, body = http_get(f"{base}/r/{quote(sub)}/new.json?limit=50", headers=headers, timeout=20)
        if st != 200:
            continue
        try:
            data = json.loads(body)
        except Exception:
            continue
        children = (((data or {}).get("data") or {}).get("children") or [])
        cnt = 0

        for ch in children:
            if len(out) >= max_items or cnt >= per_sub_limit:
                break
            d = (ch or {}).get("data") or {}
            title = d.get("title") or ""
            selftext = d.get("selftext") or ""
            text = (title + "\n" + selftext).strip()
            if not text:
                continue
            low = text.lower()
            if not any(t in low for t in triggers):
                continue
            permalink = d.get("permalink") or ""
            url = "https://www.reddit.com" + permalink if permalink.startswith("/") else (d.get("url") or "")
            author = d.get("author") or "unknown"
            created_utc = d.get("created_utc") or time.time()
            created_at = dt.datetime.fromtimestamp(created_utc, tz=dt.timezone.utc).astimezone().isoformat(timespec="seconds")
            rid = d.get("name") or d.get("id") or sha1(url)
            pid = sha1(f"reddit:{rid}:{url}")

            p = Post(
                source="reddit",
                id=pid,
                url=url,
                text=text,
                author=author,
                created_at=created_at,
                meta={"subreddit": sub},
            )
            if keep_post(p):
                out.append(p)
                cnt += 1

    logging.info("Reddit: collected %d", len(out))
    return out

def collect_hn(max_items: int = 70) -> List[Post]:
    """
    Hacker News (Algolia search_by_date for help-like content)
    """
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
        p = Post(
            source="hn",
            id=pid,
            url=hn_url,
            text=text,
            author=author,
            created_at=created_at,
            meta={"points": h.get("points", 0), "tags": h.get("_tags", [])},
        )
        if keep_post(p):
            out.append(p)

    logging.info("HN: collected %d", len(out))
    return out

def collect_x_mentions(max_items: int = 5) -> List[Post]:
    """
    X v2 mentions timeline requires:
      - X_BEARER_TOKEN
      - X_USER_ID
    """
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
        p = Post(
            source="x",
            id=pid,
            url=url,
            text=text,
            author=author,
            created_at=created_at,
            lang_hint=t.get("lang") or "",
            meta={"author_id": author, "mode": "mentions"},
        )
        if keep_post(p):
            out.append(p)

    logging.info("X: collected %d (mentions)", len(out))
    return out

def collect_x_search_recent(max_items: int = 20) -> List[Post]:
    """
    Optional: X recent search endpoint (requires proper access on X project)
    Enable with X_ENABLE_SEARCH=1.
    """
    if not X_BEARER_TOKEN:
        return []
    max_items = clamp(max_items, 5, 100)
    headers = {"Authorization": f"Bearer {X_BEARER_TOKEN}", "Accept": "application/json"}

    url = "https://api.x.com/2/tweets/search/recent?" + urlencode({
        "query": X_SEARCH_QUERY,
        "max_results": str(min(max_items, 100)),
        "tweet.fields": "created_at,author_id,lang",
    })
    st, body = http_get(url, headers=headers, timeout=20)
    if st != 200:
        logging.warning("X: search failed status=%s body=%s", st, body[:220])
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
        pid = sha1(f"xsearch:{tid}:{url}")
        p = Post(
            source="x",
            id=pid,
            url=url,
            text=text,
            author=author,
            created_at=created_at,
            lang_hint=t.get("lang") or "",
            meta={"author_id": author, "mode": "search"},
        )
        if keep_post(p):
            out.append(p)

    logging.info("X: collected %d (search)", len(out))
    return out


# =========================
# Normalization & Clustering
# =========================

STOPWORDS_EN = set("""
a an the and or but if then else when while of for to in on at from by with without into onto over under
is are was were be been being do does did done have has had will would can could should may might
this that these those it its i'm youre you're we they them our your my mine me you he she his her
""".split())

STOPWORDS_JA = set(["これ", "それ", "あれ", "ため", "ので", "から", "です", "ます", "いる", "ある", "なる", "こと", "もの", "よう", "へ", "に", "を", "が", "と", "で", "も"])

def simple_tokenize(text: str) -> List[str]:
    t = (text or "").lower()
    t = re.sub(r"https?://\S+", " ", t)
    t = re.sub(r"[\[\]()<>{}※*\"'`~^|\\]", " ", t)
    t = re.sub(r"[^0-9a-z\u3040-\u30ff\u4e00-\u9fff\s\-_/.:]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    parts: List[str] = []
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
    return parts[:100]

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
        for q in posts[i + 1:]:
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

def extract_keywords(posts: List[Post], topk: int = 14) -> List[str]:
    freq: Dict[str, int] = {}
    for p in posts:
        for w in simple_tokenize(p.norm_text()):
            freq[w] = freq.get(w, 0) + 1
    items = sorted(freq.items(), key=lambda kv: (-kv[1], kv[0]))
    return [k for k, _ in items[:topk]]

def choose_category(posts: List[Post], keywords: List[str]) -> str:
    text = " ".join([p.norm_text() for p in posts]).lower()
    k = set(keywords)

    def has_any(words: List[str]) -> bool:
        return any(w in text for w in words) or any(w in k for w in words)

    # Tech mapping
    if has_any(["dns", "cname", "aaaa", "a record", "nameserver", "github pages", "hosting", "ssl", "https", "domain", "redirect"]):
        return "Web/Hosting"
    if has_any(["python", "node", "npm", "pip", "powershell", "bash", "cli", "library", "compile", "stack trace", "dev"]):
        return "Dev/Tools"
    if has_any(["automation", "workflow", "cron", "github actions", "llm", "openai", "prompt", "agent"]):
        return "AI/Automation"
    if has_any(["privacy", "security", "2fa", "phishing", "cookie", "vpn", "encryption", "leak"]):
        return "Security/Privacy"
    if has_any(["video", "mp4", "compress", "codec", "ffmpeg", "audio", "subtitle"]):
        return "Media"
    if has_any(["pdf", "docs", "word", "ppt", "docx", "convert", "merge"]):
        return "PDF/Docs"
    if has_any(["image", "png", "jpg", "webp", "design", "figma", "photoshop", "illustrator"]):
        return "Images/Design"
    if has_any(["excel", "spreadsheet", "csv", "google sheets", "vlookup", "pivot", "formula"]):
        return "Data/Spreadsheets"
    if has_any(["invoice", "tax", "accounting", "bookkeeping", "receipt", "vat"]):
        return "Business/Accounting/Tax"
    if has_any(["seo", "marketing", "ads", "social", "instagram", "tiktok", "youtube", "growth"]):
        return "Marketing/Social"
    if has_any(["productivity", "todo", "note", "calendar", "time management", "habit"]):
        return "Productivity"
    if has_any(["english", "language", "toeic", "eiken", "grammar"]):
        return "Education/Language"

    # Life mapping
    if has_any(["travel", "trip", "hotel", "itinerary", "flight", "booking", "packing", "layover", "esim"]):
        return "Travel/Planning"
    if has_any(["recipe", "cook", "cooking", "meal prep", "grocery", "nutrition"]):
        return "Food/Cooking"
    if has_any(["workout", "fitness", "diet", "health", "running", "sleep", "calories"]):
        return "Health/Fitness"
    if has_any(["study", "learning", "exam", "homework", "memorize", "procrastination"]):
        return "Study/Learning"
    if has_any(["money", "budget", "loan", "invest", "stock", "fee", "refund"]):
        return "Money/Personal Finance"
    if has_any(["career", "job", "resume", "cv", "interview", "salary"]):
        return "Career/Work"
    if has_any(["relationship", "communication", "friend", "texting", "awkward", "conversation", "template"]):
        return "Relationships/Communication"
    if has_any(["home", "rent", "utility", "life admin", "paperwork", "moving", "declutter"]):
        return "Home/Life Admin"
    if has_any(["buy", "shopping", "product", "recommend", "compare", "best"]):
        return "Shopping/Products"
    if has_any(["event", "ticket", "concert", "sports", "weekend", "leisure", "date plan", "rainy day"]):
        return "Events/Leisure"

    return "Dev/Tools"

def score_text_for_lead(text: str) -> float:
    low = (text or "").lower()

    # Base pain / solvable signals
    solvable = [
        "how", "fix", "error", "failed", "can't", "cannot", "help", "issue", "bug", "broken",
        "confused", "overwhelmed", "stuck", "don’t know", "don't know", "which one", "what should i do",
        "設定", "直し", "原因", "エラー", "できない", "不具合", "失敗", "困る",
    ]
    toolish = [
        "convert", "compress", "calculator", "generator", "template", "checklist", "compare", "recommend", "best",
        "itinerary", "packing", "schedule", "plan", "budget", "meal prep", "shopping list", "workout plan", "study plan",
        "変換", "圧縮", "計算", "チェック", "テンプレ", "比較", "おすすめ",
    ]
    urgency = ["today", "tomorrow", "this week", "before i go", "urgent", "asap", "now", "期限", "明日", "今日", "今週", "急ぎ"]
    too_broad = ["life sucks", "i hate", "why is everything", "rant", "just venting"]

    s = 0.0
    s += sum(1 for w in solvable if w in low) * 0.9
    s += sum(1 for w in toolish if w in low) * 1.1
    s += sum(1 for w in urgency if w in low) * 1.2
    s -= sum(1 for w in too_broad if w in low) * 1.5

    # Slight length preference (not too long, not too short)
    L = len(low)
    if 80 <= L <= 600:
        s += 1.0
    elif L < 40:
        s -= 1.0

    return s

def score_cluster(posts: List[Post], category: str) -> float:
    size = len(posts)
    joined = " ".join([p.norm_text() for p in posts])
    stext = score_text_for_lead(joined)

    # Category balance (life also should win)
    cat_boost = 1.0
    if category in ["Web/Hosting", "PDF/Docs", "Media", "Data/Spreadsheets", "Security/Privacy", "AI/Automation"]:
        cat_boost = 1.10
    if category in ["Travel/Planning", "Food/Cooking", "Health/Fitness", "Study/Learning", "Money/Personal Finance", "Career/Work"]:
        cat_boost = 1.08

    return float((size * 1.6 + stext * 1.2) * cat_boost)

def make_theme(posts: List[Post]) -> Theme:
    keywords = extract_keywords(posts)
    category = choose_category(posts, keywords)
    score = score_cluster(posts, category)

    # SEO-ish title: include human search pattern + category hints
    core = " ".join([k for k in keywords[:4] if len(k) <= 18]).strip()
    if not core:
        core = category.replace("/", " ")
    # include both: English core + readable suffix
    title = f"{core} guide + checklist + template"
    slug = safe_slug(core) + "-" + sha1(title)[:6]

    problems: List[str] = []
    for p in posts[:14]:
        line = p.norm_text()[:140].rstrip()
        if line:
            problems.append(line)
    problems = uniq_keep_order([re.sub(r"\s+", " ", x) for x in problems])

    while len(problems) < 10:
        problems.append(f"Common trouble in {category}: example #{len(problems)+1}")
    problems = problems[:20]

    return Theme(
        title=title,
        slug=slug,
        category=category,
        problem_list=problems,
        representative_posts=posts[: min(len(posts), 8)],
        score=score,
        keywords=keywords,
    )


# =========================
# Affiliates
# =========================

def load_affiliates() -> Dict[str, Any]:
    data = read_json(AFFILIATES_JSON, default={})
    if not isinstance(data, dict):
        return {}
    return data

def pick_affiliates_for_category(aff: Dict[str, Any], category: str, topn: int = 2) -> List[Dict[str, Any]]:
    """
    Expected affiliates.json format (flexible):
      - { "categories": { "Web/Hosting": [ {id,title,html,priority,tags}, ... ], ... } }
      - or { "Web/Hosting": [ ... ] }
    """
    items = []
    if "categories" in aff and isinstance(aff["categories"], dict):
        items = aff["categories"].get(category, []) or []
    elif category in aff:
        items = aff.get(category, []) or []

    def pr(x: Dict[str, Any]) -> float:
        try:
            return float(x.get("priority", 0))
        except Exception:
            return 0.0

    # keep only dict items with usable html/url
    items2 = [x for x in items if isinstance(x, dict) and (x.get("html") or x.get("url") or x.get("code"))]
    items2.sort(key=lambda x: -pr(x))
    return items2[:topn]

def sanitize_affiliate_html(h: str) -> str:
    """
    Very conservative: forbid <script and inline event handlers.
    """
    if not h:
        return ""
    low = h.lower()
    if "<script" in low:
        return ""
    # remove on* handlers
    h = re.sub(r"\son[a-z]+\s*=\s*\"[^\"]*\"", "", h, flags=re.IGNORECASE)
    h = re.sub(r"\son[a-z]+\s*=\s*'[^']*'", "", h, flags=re.IGNORECASE)
    return h.strip()

def validate_affiliates_keys(aff: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Check affiliates.json has keys for all categories (missing allowed but we report).
    Returns (ok, missing_keys)
    """
    cats = []
    if "categories" in aff and isinstance(aff["categories"], dict):
        cats = list(aff["categories"].keys())
        missing = [c for c in CATEGORIES_22 if c not in aff["categories"]]
        return (len(missing) == 0, missing)
    # flat format
    missing = [c for c in CATEGORIES_22 if c not in aff]
    return (len(missing) == 0, missing)


# =========================
# Site inventory (hub/sites.json) & related/popular
# =========================

def read_hub_sites() -> List[Dict[str, Any]]:
    data = read_json(HUB_SITES_JSON, default=[])
    if isinstance(data, dict) and "sites" in data:
        data = data["sites"]
    if not isinstance(data, list):
        return []
    return [x for x in data if isinstance(x, dict)]

def write_hub_sites(sites: List[Dict[str, Any]]) -> None:
    # hub frozen: ONLY update sites.json
    if is_frozen_path(HUB_SITES_JSON):
        # if this ever becomes frozen by mistake, hard stop
        raise RuntimeError("hub/sites.json is unexpectedly frozen; refusing to write")
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
        "problems": "Problems this page can help with",
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
        "footer_note": "Practical, fast, and respectful guides.",
        "aff_title": "Recommended",
        "copy": "Copy",
        "copied": "Copied",
        "tool": "Tool",
        "generate": "Generate",
        "reset": "Reset",
    },
    "ja": {
        "home": "Home",
        "about": "About Us",
        "all_tools": "All Tools",
        "language": "言語",
        "share": "共有",
        "problems": "このページが助ける悩み一覧",
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
        "tool": "ツール",
        "generate": "生成",
        "reset": "リセット",
    },
    "ko": {
        "home": "Home",
        "about": "About Us",
        "all_tools": "All Tools",
        "language": "언어",
        "share": "공유",
        "problems": "이 페이지가 해결할 수 있는 고민",
        "quick_answer": "결론(가장 빠른 해결 방향)",
        "causes": "원인 패턴",
        "steps": "체크리스트(단계별)",
        "pitfalls": "자주 하는 실수와 회피법",
        "next": "계속 안 될 때",
        "faq": "FAQ",
        "references": "참고 링크",
        "supplement": "추가 자료",
        "related": "관련 도구",
        "popular": "인기 도구",
        "disclaimer": "면책",
        "terms": "이용약관",
        "privacy": "개인정보 처리방침",
        "contact": "문의",
        "footer_note": "실무에 바로 쓰이는 해결 가이드를 목표로 합니다.",
        "aff_title": "추천",
        "copy": "복사",
        "copied": "복사됨",
        "tool": "도구",
        "generate": "생성",
        "reset": "초기화",
    },
    "zh": {
        "home": "Home",
        "about": "About Us",
        "all_tools": "All Tools",
        "language": "语言",
        "share": "分享",
        "problems": "本页可帮助解决的问题",
        "quick_answer": "结论（最快方向）",
        "causes": "常见原因分类",
        "steps": "步骤清单",
        "pitfalls": "常见坑与规避方法",
        "next": "仍无法解决时",
        "faq": "FAQ",
        "references": "参考链接",
        "supplement": "补充资料",
        "related": "相关工具",
        "popular": "热门工具",
        "disclaimer": "免责声明",
        "terms": "条款",
        "privacy": "隐私政策",
        "contact": "联系",
        "footer_note": "我们追求可落地、快速、尊重用户的指南。",
        "aff_title": "推荐",
        "copy": "复制",
        "copied": "已复制",
        "tool": "工具",
        "generate": "生成",
        "reset": "重置",
    },
}

def build_i18n_script(default_lang: str = "en") -> str:
    i18n_json = json.dumps(I18N, ensure_ascii=False)
    return f"""
<script>
window.I18N = {i18n_json};
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


# =========================
# Content generation (article, checklist, faq)
# =========================

def build_quick_answer(category: str, keywords: List[str]) -> str:
    kw = ", ".join(keywords[:8])
    base = [
        "最短で直す方針は「原因の切り分け → 最小変更 → 検証 → 記録」です。",
        f"今回のテーマはカテゴリ「{category}」なので、入力・設定・権限・反映待ち（キャッシュ/DNS）を順に潰します。",
        f"キーワード（観測された兆候）: {kw}",
        "チェックリストは上から順にやると、失敗率が一気に下がります。",
    ]
    return "\n".join(base)

def build_causes(category: str) -> List[str]:
    common = {
        "Web/Hosting": [
            "DNSの反映待ち（TTL）やレコード種別の誤り（A/CNAME/AAAAの混在）",
            "HTTPS/証明書の自動発行待ち、リダイレクトのループ",
            "カスタムドメイン・ベースURL・パスの不一致",
            "キャッシュ（CDN/ブラウザ/Service Worker）による古い表示",
        ],
        "PDF/Docs": [
            "サイズ/ページ数制限、暗号化・スキャンPDFの互換性",
            "変換先の選択ミス（テキスト化 vs 画像化）",
            "ブラウザのメモリ不足・拡張機能の干渉",
            "フォントや埋め込みの問題",
        ],
        "Media": [
            "コーデック不一致（H.264/H.265/AV1）、音声形式の不一致",
            "ビットレートや解像度の上限超え",
            "端末の性能/メモリ不足",
            "ファイル破損やコンテナ不整合",
        ],
        "Data/Spreadsheets": [
            "参照範囲ズレ・絶対参照/相対参照のミス",
            "CSVの区切り文字・文字コード・日付形式の差",
            "フィルタ/ピボットの更新漏れ",
            "共有権限や保護範囲の設定ミス",
        ],
        "AI/Automation": [
            "APIキー/権限不足、レート制限、環境変数名の不一致",
            "入力仕様が揺れて出力が安定しない",
            "パス上書き事故、衝突処理漏れ",
            "ログ不足で原因特定が遅れる",
        ],
        "Travel/Planning": [
            "目的（移動/観光/休息）が曖昧で旅程が破綻する",
            "移動時間の見積もり不足、乗り換え/待ち時間の未考慮",
            "持ち物が多すぎ or 重要品不足（パスポート/充電/保険/eSIM）",
            "予約条件（キャンセル/手数料）を見落とす",
        ],
        "Food/Cooking": [
            "献立が曖昧で買い物が増える",
            "作り置きの賞味期限/保存方法のミス",
            "栄養の偏り（タンパク質/食物繊維不足）",
            "手順が複雑で続かない",
        ],
        "Health/Fitness": [
            "睡眠/食事/運動が同時に崩れて原因が分からない",
            "目標が大きすぎて続かない",
            "記録がなく改善点が見えない",
            "疲労が抜けず逆にパフォーマンスが落ちる",
        ],
        "Study/Learning": [
            "復習の間隔がない（忘却曲線に負ける）",
            "タスクが大きすぎて先延ばしになる",
            "優先順位が曖昧で迷う",
            "アウトプット不足で定着しない",
        ],
        "Money/Personal Finance": [
            "固定費の把握不足、見えないサブスクが残る",
            "手数料/返金条件を見落とす",
            "予算の配分が雑で月末に詰む",
            "目標と現実の差が大きい",
        ],
        "Career/Work": [
            "職務要約が弱い（何ができるかが伝わらない）",
            "面接対策が抽象的で詰む",
            "応募先の選定軸がない",
            "実績の言語化ができない",
        ],
        "Relationships/Communication": [
            "目的が曖昧（距離を縮めたい/誤解を解きたい）が混在",
            "話し過ぎ・聞き過ぎの偏り",
            "相手の文脈（状況）を読まずに送ってしまう",
            "言い回しが強すぎて誤解される",
        ],
        "Home/Life Admin": [
            "作業が大きすぎて着手できない（分解不足）",
            "捨てる基準がない",
            "期限/手続きの抜け（住所変更など）",
            "必要物のリスト化不足",
        ],
        "Shopping/Products": [
            "比較軸がない（価格/耐久/保証/用途）",
            "レビューの読み方が偏る",
            "型番/世代差で地雷を踏む",
            "必要以上のスペックを買う",
        ],
        "Events/Leisure": [
            "天候/混雑を考慮していない",
            "移動/予算の制約に合ってない",
            "当日の持ち物/時間割がない",
            "候補が多すぎて決められない",
        ],
    }
    return common.get(category, [
        "入力・前提条件のズレ",
        "権限/設定/バージョンの不一致",
        "キャッシュや反映待ち",
        "原因が前段にある（見えている画面が原因とは限らない）",
    ])

def build_steps(category: str) -> List[str]:
    steps = [
        "再現条件を固定する（同じ入力・同じ手順・同じ環境）",
        "エラー文/ログ/スクショをそのまま保存（時刻も）",
        "キャッシュを疑う（別ブラウザ/シークレット/別端末）",
        "設定・権限・期限（トークン/予約条件/期限）を確認",
        "最小変更で1点ずつ潰す（まとめて変更しない）",
        "直ったら差分を記録し、再発防止チェックを作る",
    ]
    if category == "Web/Hosting":
        steps += [
            "DNSを確認（A/CNAME/AAAA、TTL）。nslookup/digで第三者視点でも検証",
            "HTTPS/リダイレクト/ベースパス（/ と /hub/ 境界）を確認",
        ]
    if category == "AI/Automation":
        steps += [
            "実行単位を小さく切って検証（collectだけ→buildだけ→validateだけ）",
            "上書き禁止/衝突回避（-2,-3）の挙動をログで確認",
        ]
    if category == "Travel/Planning":
        steps += [
            "目的を1行で固定（例：移動優先/写真優先/疲れない優先）",
            "移動時間＋バッファ（15〜30分）を各所に入れる",
            "持ち物を『必須/あると便利/現地調達』で3分類する",
        ]
    if category == "Food/Cooking":
        steps += [
            "献立を『主菜/副菜/汁物』で枠組み化する",
            "買い物リストをカテゴリ分け（肉魚/野菜/調味料）で作る",
            "保存日数の上限を決めて回転させる",
        ]
    if category == "Study/Learning":
        steps += [
            "1日の最小単位（10〜20分）を決めて毎日回す",
            "復習の間隔（翌日/3日後/7日後）を先に予定に入れる",
            "アウトプット（小テスト/音読/英作文）を必ず入れる",
        ]
    return steps

def build_pitfalls(category: str) -> List[str]:
    pitfalls = [
        "一気に複数箇所を変えて原因が分からなくなる",
        "反映待ち（DNS/キャッシュ/予約反映）を無視して焦って壊す",
        "ログ/記録を取らずに試行回数だけ増やす",
        "“いま見えている画面”が原因だと決めつける（前段が原因のことが多い）",
    ]
    if category in ["Web/Hosting", "AI/Automation"]:
        pitfalls.append("既存URLや凍結領域（/hub/）を上書きして資産を壊す（絶対禁止）")
    return pitfalls

def build_next_actions(category: str) -> List[str]:
    nxt = [
        "別端末/別回線/別ブラウザで同じ結果か確認",
        "ログの粒度を上げる（HTTPステータス/例外/レスポンス先頭）",
        "元に戻せる形でロールバック（差分を残す）",
        "チェックリスト化して再発防止する",
    ]
    if category == "Security/Privacy":
        nxt.append("怪しいリンクは踏まず、公式ドメインと証明書を再確認する")
    return nxt

def build_faq(category: str) -> List[Tuple[str, str]]:
    base = [
        ("最初に何を見ればいい？", "再現条件・エラー文・時刻・直前に変えた点の4つを固定します。"),
        ("キャッシュが原因かどうかの見分け方は？", "別ブラウザ/別端末でも同じならキャッシュ以外の可能性が高いです。"),
        ("手を付ける順番は？", "影響範囲が小さい順（確認→読み取り→最小変更→検証）です。"),
        ("直った後にやるべきことは？", "差分と再発防止チェックを残すと、次回は短時間で復旧できます。"),
        ("共有するときに必要な情報は？", "再現手順、期待結果、実結果、ログ/スクショ、環境（OS/ブラウザ/版）です。"),
    ]
    if category == "Web/Hosting":
        base.append(("DNSはどれくらいで反映される？", "TTLやプロバイダで差があります。第三者の解決結果でも確認します。"))
    if category == "Travel/Planning":
        base.append(("旅程が詰みやすいポイントは？", "移動時間の見積もり不足と“やりたいこと”の詰め込み過ぎです。"))
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
        "Travel/Planning": [
            "https://en.wikipedia.org/wiki/Travel_insurance",
            "https://en.wikipedia.org/wiki/Time_zone",
            "https://en.wikipedia.org/wiki/Flight_connection",
        ],
        "Food/Cooking": [
            "https://en.wikipedia.org/wiki/Food_storage",
            "https://en.wikipedia.org/wiki/Meal_preparation",
            "https://en.wikipedia.org/wiki/Nutrition",
        ],
        "Health/Fitness": [
            "https://en.wikipedia.org/wiki/Sleep_hygiene",
            "https://en.wikipedia.org/wiki/Physical_exercise",
            "https://en.wikipedia.org/wiki/Body_mass_index",
        ],
        "Study/Learning": [
            "https://en.wikipedia.org/wiki/Spaced_repetition",
            "https://en.wikipedia.org/wiki/Active_recall",
            "https://en.wikipedia.org/wiki/Procrastination",
        ],
        "Money/Personal Finance": [
            "https://en.wikipedia.org/wiki/Personal_finance",
            "https://en.wikipedia.org/wiki/Budget",
            "https://en.wikipedia.org/wiki/Interest",
        ],
        "Career/Work": [
            "https://en.wikipedia.org/wiki/Job_interview",
            "https://en.wikipedia.org/wiki/R%C3%A9sum%C3%A9",
            "https://en.wikipedia.org/wiki/Cover_letter",
        ],
    }
    return base.get(category, [
        "https://developer.mozilla.org/",
        "https://docs.github.com/",
        "https://en.wikipedia.org/wiki/Troubleshooting",
    ])

def generate_long_article_ja(theme: Theme) -> str:
    intro = (
        f"このページは「{theme.category}」でよく起きる悩み・詰まりを、"
        f"短時間で安全に解決するためのガイドです。"
        f"推測で決め打ちせず、再現条件を固定し、影響範囲の小さい順に確認します。\n"
    )
    why = (
        "多くの問題は、(1)前提条件のズレ、(2)設定/権限/期限、"
        "(3)反映待ち（キャッシュ/DNS/予約条件）、(4)手順の揺れ、のどれかに落ちます。"
        "この4点を順番に潰すだけで、調査が“運”から“手順”に変わります。\n"
    )
    detail = (
        "重要なのは「最小変更」です。"
        "一度に複数箇所をいじると、直っても原因が分からず再発します。"
        "最小変更 → 検証 → 記録、を回すと、次回はチェックリストだけで復旧できます。\n"
    )

    causes = build_causes(theme.category)
    steps = build_steps(theme.category)
    pitfalls = build_pitfalls(theme.category)
    nxt = build_next_actions(theme.category)

    examples = "【このページで扱う悩み一覧（例）】\n" + "\n".join([f"- {p}" for p in theme.problem_list]) + "\n"
    cause_text = "【原因のパターン分け】\n" + "\n".join([f"- {c}" for c in causes]) + "\n"
    step_text = "【手順（チェックリスト）】\n" + "\n".join([f"- {s}" for s in steps]) + "\n"
    pit_text = "【よくある失敗と回避策】\n" + "\n".join([f"- {p}" for p in pitfalls]) + "\n"
    nxt_text = "【直らない場合の次の手】\n" + "\n".join([f"- {x}" for x in nxt]) + "\n"

    verify = (
        "【検証のコツ】\n"
        "- “期待結果”を文章にする（何ができれば成功か）\n"
        "- 失敗が出たら、入力・環境・時刻・ログをセットで残す\n"
        "- 直った瞬間に、何を変えたかを1行で書ける状態にする\n"
        "- 再発防止は“次回3分で復旧できるか”で判断する\n"
        "この型に沿うだけで、試行錯誤が減ります。\n"
    )

    tree = (
        "【切り分けの分岐（迷った時用）】\n"
        "1) 別ブラウザ/別端末でも同じ？\n"
        "  - はい → サーバ/設定/権限/期限側が濃厚\n"
        "  - いいえ → キャッシュ/拡張機能/端末依存が濃厚\n"
        "2) 同じ入力・同じ手順で再現する？\n"
        "  - はい → 原因追跡が可能。ログを増やして一点ずつ潰す\n"
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


# =========================
# Tool UI (client-side templates)
# =========================

def build_tool_block(theme: Theme) -> str:
    """
    No server, no AI. Provide useful templates: checklist/plan generator.
    """
    cat = theme.category
    default_input = ""
    placeholder = ""

    if cat == "Travel/Planning":
        placeholder = "Trip basics: dates, city, must-see, pace (slow/normal), budget, constraints..."
        default_input = "Dates: \nCities: \nMust-see: \nPace: \nBudget: \nConstraints: \n"
    elif cat == "Food/Cooking":
        placeholder = "Diet style, available time, dislikes/allergies, servings, budget..."
        default_input = "Diet style: \nTime per meal: \nDislikes/allergies: \nServings: \nBudget: \n"
    elif cat == "Study/Learning":
        placeholder = "Goal, deadline, daily minutes, weak areas, materials..."
        default_input = "Goal: \nDeadline: \nDaily minutes: \nWeak areas: \nMaterials: \n"
    elif cat == "Money/Personal Finance":
        placeholder = "Income, fixed costs, debt, savings goal, next payment dates..."
        default_input = "Income: \nFixed costs: \nDebt: \nSavings goal: \nUpcoming payments: \n"
    elif cat == "Career/Work":
        placeholder = "Role target, strengths, achievements, gaps, interview date..."
        default_input = "Target role: \nStrengths: \nAchievements: \nGaps: \nInterview date: \n"
    elif cat == "Relationships/Communication":
        placeholder = "Situation, what you want, tone, constraints, who is involved..."
        default_input = "Situation: \nGoal: \nTone: \nConstraints: \n"
    else:
        placeholder = "Paste the error / situation / constraints. Keep it short but specific."
        default_input = "Symptoms: \nWhat changed recently: \nEnvironment: \nWhat you already tried: \n"

    # Basic generator that outputs structured checklist / plan
    return f"""
<section class="rounded-3xl border border-white/10 bg-white/5 p-6">
  <div class="flex items-center justify-between gap-3">
    <h2 class="text-lg font-semibold" data-i18n="tool">Tool</h2>
    <div class="text-xs text-white/60">Local template generator (no server)</div>
  </div>

  <div class="mt-3 grid grid-cols-1 md:grid-cols-2 gap-4">
    <div>
      <label class="text-sm text-white/70">Input</label>
      <textarea id="toolIn" class="mt-2 w-full h-40 rounded-2xl bg-black/40 border border-white/10 p-3 text-sm text-white/80"
        placeholder="{html.escape(placeholder)}">{html.escape(default_input)}</textarea>
      <div class="mt-3 flex gap-2">
        <button id="genBtn" class="rounded-xl border border-white/10 bg-white/10 px-4 py-2 text-sm hover:bg-white/15" data-i18n="generate">Generate</button>
        <button id="resetBtn" class="rounded-xl border border-white/10 bg-black/20 px-4 py-2 text-sm hover:bg-white/10" data-i18n="reset">Reset</button>
      </div>
    </div>
    <div>
      <label class="text-sm text-white/70">Output</label>
      <textarea id="toolOut" class="mt-2 w-full h-40 rounded-2xl bg-black/40 border border-white/10 p-3 text-sm text-white/80"
        placeholder="Your structured plan/checklist will appear here." readonly></textarea>
      <div class="mt-2 text-xs text-white/60">Tip: copy this into Notes or your task app.</div>
    </div>
  </div>

  <script>
  function lines(s) {{
    return (s||"").split(/\\r?\\n/).map(x=>x.trim()).filter(Boolean);
  }}
  function genTemplate(cat, input) {{
    const L = lines(input);
    const head = "Category: " + cat + "\\nGenerated: " + new Date().toISOString() + "\\n\\n";
    if (cat === "Travel/Planning") {{
      return head +
`1) Goals (1 line)\\n- \\n\\n2) Day-by-day itinerary\\n- Day 1: \\n- Day 2: \\n\\n3) Budget split\\n- Transport: \\n- Stay: \\n- Food: \\n- Activities: \\n\\n4) Packing checklist\\n- Essentials (passport, card, charger)\\n- Clothes\\n- Health\\n- Tech\\n\\n5) Risk checks\\n- Insurance\\n- eSIM/roaming\\n- Cancellation rules\\n\\nInput notes:\\n- ${L.join("\\n- ")}`;
    }}
    if (cat === "Food/Cooking") {{
      return head +
`1) Weekly meal plan\\n- Mon: \\n- Tue: \\n- ...\\n\\n2) Batch prep list\\n- Protein\\n- Veg\\n- Sauce\\n\\n3) Grocery list\\n- Meat/Fish\\n- Veg\\n- Staples\\n- Seasoning\\n\\n4) Nutrition checks\\n- Protein target\\n- Fiber\\n- Water\\n\\nInput notes:\\n- ${L.join("\\n- ")}`;
    }}
    if (cat === "Study/Learning") {{
      return head +
`1) Goal & deadline\\n- \\n\\n2) Daily minimum (10-20 min)\\n- \\n\\n3) Weekly plan\\n- Mon: \\n- Tue: \\n- ...\\n\\n4) Review schedule\\n- Next day / 3 days / 7 days\\n\\n5) Output\\n- Mini test / speaking / writing\\n\\nInput notes:\\n- ${L.join("\\n- ")}`;
    }}
    if (cat === "Money/Personal Finance") {{
      return head +
`1) Monthly snapshot\\n- Income\\n- Fixed costs\\n- Variable budget\\n\\n2) Cut list\\n- Subscriptions\\n- Fees\\n\\n3) Debt plan\\n- Min payments\\n- Extra payments\\n\\n4) Next 7 days checklist\\n- Pay dates\\n- Confirm refunds\\n\\nInput notes:\\n- ${L.join("\\n- ")}`;
    }}
    if (cat === "Career/Work") {{
      return head +
`1) Resume bullets (STAR)\\n- Situation: \\n- Task: \\n- Action: \\n- Result: \\n\\n2) Interview prep\\n- 3 strengths\\n- 2 weaknesses (with fixes)\\n- Why this role\\n\\n3) Next actions\\n- Apply list\\n- Follow-up\\n\\nInput notes:\\n- ${L.join("\\n- ")}`;
    }}
    if (cat === "Relationships/Communication") {{
      return head +
`1) Goal (1 line)\\n- \\n\\n2) Message template\\n- Empathy: \\n- Context: \\n- Ask: \\n- Soft close: \\n\\n3) Boundaries\\n- What you won't do\\n\\nInput notes:\\n- ${L.join("\\n- ")}`;
    }}
    // default (tech & general)
    return head +
`1) Repro steps\\n- \\n\\n2) Environment\\n- OS/Browser/Version\\n\\n3) Checklist\\n- Check permissions/tokens\\n- Check cache\\n- Check minimal config\\n- Check logs\\n\\n4) Rollback plan\\n- What to revert\\n\\nInput notes:\\n- ${L.join("\\n- ")}`;
  }}
  document.addEventListener("DOMContentLoaded", () => {{
    const cat = {json.dumps(cat)};
    const inEl = document.getElementById("toolIn");
    const outEl = document.getElementById("toolOut");
    const def = inEl.value;
    document.getElementById("genBtn").addEventListener("click", () => {{
      outEl.value = genTemplate(cat, inEl.value);
    }});
    document.getElementById("resetBtn").addEventListener("click", () => {{
      inEl.value = def;
      outEl.value = "";
    }});
  }});
  </script>
</section>
""".strip()


# =========================
# HTML generation (Tailwind, dark mode, i18n)
# =========================

def html_escape(s: str) -> str:
    return html.escape(s or "", quote=True)

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

def build_page_html(
    theme: Theme,
    tool_url: str,
    all_sites: List[Dict[str, Any]],
    affiliates_top2: List[Dict[str, Any]],
    references: List[str],
    supplements: List[str],
    article_ja: str,
    faq: List[Tuple[str, str]],
    related_tools: List[Dict[str, Any]],
    popular_sites: List[Dict[str, Any]],
) -> str:

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

    # affiliates slot: top 2
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

    tool_block = build_tool_block(theme)
    i18n_script = build_i18n_script(DEFAULT_LANG)

    # NOTE: we never touch hub/index.html; just link to /hub/
    html_doc = f"""<!doctype html>
<html lang="{html_escape(DEFAULT_LANG)}">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html_escape(theme.title)} | {html_escape(SITE_BRAND)}</title>
  <meta name="description" content="{html_escape('Guide + checklist + templates: ' + theme.title)}">
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
          <div class="font-semibold">{html_escape(theme.title[:52])}</div>
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
            Guide + checklist + templates + references. Built from real public posts and common patterns.
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

    <section class="mt-8 grid grid-cols-1 lg:grid-cols-3 gap-6">
      <div class="lg:col-span-2 space-y-6">

        {tool_block}

        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold" data-i18n="problems">Problems this page can help with</h2>
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
          <div class="mt-3">{article_html}</div>
        </section>

        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold" data-i18n="faq">FAQ</h2>
          <div class="mt-3 space-y-3">{faq_html}</div>
        </section>

        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold" data-i18n="references">Reference links</h2>
          <ul class="mt-3 list-disc pl-6 text-white/80">{ref_html}</ul>
          <h3 class="mt-6 text-base font-semibold text-white/90" data-i18n="supplement">Supplementary resources</h3>
          <ul class="mt-3 list-disc pl-6 text-white/80">{sup_html}</ul>
        </section>

      </div>

      <aside class="space-y-6">
        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold" data-i18n="aff_title">Recommended</h2>
          <div class="mt-3 space-y-3" id="affSlot">
            {aff_html}
          </div>
          <!-- AFF_SLOT -->
        </section>

        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold" data-i18n="related">Related tools</h2>
          <ul class="mt-3 list-disc pl-6 text-white/80">{related_html}</ul>
        </section>

        <section class="rounded-3xl border border-white/10 bg-white/5 p-6">
          <h2 class="text-lg font-semibold" data-i18n="popular">Popular tools</h2>
          <ol class="mt-3 list-decimal pl-6 text-white/80">{popular_html}</ol>
        </section>
      </aside>
    </section>
  </main>

  <footer class="relative z-10 border-t border-white/10 bg-zinc-950/70">
    <div class="mx-auto max-w-6xl px-4 py-10">
      <div class="grid grid-cols-1 md:grid-cols-4 gap-6">
        <div class="md:col-span-2">
          <div class="text-sm text-white/70">{html_escape(SITE_BRAND)}</div>
          <div class="mt-2 text-white/70 text-sm leading-relaxed" data-i18n="footer_note">Practical, fast, and respectful guides.</div>
          <div class="mt-2 text-xs text-white/50">© {dt.datetime.now().year} {html_escape(SITE_BRAND)}</div>
        </div>
        <div>
          <div class="text-sm font-semibold mb-2">Legal</div>
          <ul class="text-sm text-white/70 space-y-1">
            <li><a class="underline" data-i18n="privacy" href="{html_escape(SITE_DOMAIN.rstrip('/') + '/policies/privacy.html')}">Privacy</a></li>
            <li><a class="underline" data-i18n="terms" href="{html_escape(SITE_DOMAIN.rstrip('/') + '/policies/terms.html')}">Terms</a></li>
            <li><a class="underline" data-i18n="disclaimer" href="{html_escape(SITE_DOMAIN.rstrip('/') + '/policies/disclaimer.html')}">Disclaimer</a></li>
          </ul>
        </div>
        <div>
          <div class="text-sm font-semibold mb-2" data-i18n="contact">Contact</div>
          <div class="text-sm text-white/70 break-all">{html_escape(SITE_CONTACT_EMAIL)}</div>
          <div class="mt-2 text-sm text-white/70">
            <a class="underline" href="{html_escape(SITE_DOMAIN.rstrip('/') + '/hub/')}">/hub/</a>
          </div>
       
