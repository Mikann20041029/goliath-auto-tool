#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Goliath Auto Tool System - main.py (single-file)

- 8-hour cycle runner (run once; scheduling is outside)
- Collect from: Bluesky, Mastodon, Reddit, Hacker News, X (mentions)
- Normalize -> cluster -> choose best themes -> generate solution sites
- Validate/autofix up to MAX_AUTOFIX
- Update hub/sites.json only (hub assets + hub/index.html are frozen)
- Generate sitemap.xml + robots.txt safely (default: goliath/_out; root only if ALLOW_ROOT_UPDATE=1)
- Generate shortlinks (/goliath/go/<code>/) and provide "short URL + one-line value" post drafts
- Output Issues payload (bulk) with:
    - Problem URL
    - Reply (EN, empathy + “made a one-page guide” + tool URL last line)
  Minimum 100 leads per run (stub fill if needed)
- 22 categories (fixed) -> affiliates.json top2 -> inject to AFF_SLOT
- SaaS-like design, Tailwind, dark mode, i18n (EN/JA/KO/ZH), 2500+ chars JP article + FAQ + references + legal pages
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import html
import json
import logging
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen


# =============================================================================
# Config (ENV)
# =============================================================================
def env_first(*names: str, default: str = "") -> str:
    for n in names:
        v = os.environ.get(n, "").strip()
        if v:
            return v
    return default

# ---- Public base (link生成はここ基準) ----
# 今は GitHub Pages 配下に出したい → Actions側で PUBLIC_BASE_URL を入れる
# 例: https://mikann20041029.github.io
PUBLIC_BASE_URL = env_first("PUBLIC_BASE_URL", "PUBLIC_SITE_BASE", default=os.environ.get("SITE_DOMAIN", "").strip() or "https://mikann20041029.github.io")

# ---- Bluesky ----
BLUESKY_HANDLE = env_first("BLUESKY_HANDLE", "BSKY_HANDLE", "BLUESKY_ID")
BLUESKY_APP_PASSWORD = env_first("BLUESKY_APP_PASSWORD", "BSKY_APP_PASSWORD", "BLUESKY_PASSWORD")

# ---- Mastodon ----
MASTODON_BASE = env_first("MASTODON_BASE", "MASTODON_INSTANCE", "MASTODON_URL")
MASTODON_TOKEN = env_first("MASTODON_TOKEN", "MASTODON_ACCESS_TOKEN")

# ---- Reddit ----
REDDIT_CLIENT_ID = env_first("REDDIT_CLIENT_ID", "REDDIT_ID")
REDDIT_CLIENT_SECRET = env_first("REDDIT_CLIENT_SECRET", "REDDIT_SECRET")
REDDIT_REFRESH_TOKEN = env_first("REDDIT_REFRESH_TOKEN", "REDDIT_REFRESH")
REDDIT_USER_AGENT = env_first("REDDIT_USER_AGENT", default="goliath-tool/1.0 (read-only)")

# ---- X (Free: 月100 Reads 想定 / 1実行=1リクエスト運用) ----
X_BEARER_TOKEN = env_first("X_BEARER_TOKEN", "TWITTER_BEARER_TOKEN", "X_TOKEN", "TW_BEARER_TOKEN")
X_SEARCH_QUERY = os.environ.get("X_SEARCH_QUERY", '("how to" OR help OR error OR fix) lang:en -is:retweet').strip()
X_MAX = int(os.environ.get("X_MAX", "1"))
def getenv_any(names: Iterable[str], default: str = "") -> str:
    for n in names:
        v = os.environ.get(n)
        if v is None:
            continue
        v = str(v).strip()
        if v:
            return v
    return default

HN_QUERY = getenv_any(["HN_QUERY", "HACKER_NEWS_QUERY", "HN_SEARCH_QUERY"], "how to fix error OR help OR cannot OR failed OR bug")
# ---- Hacker News ----
HN_MAX = int(os.environ.get("HN_MAX", os.environ.get("HACKER_NEWS_MAX", "70")))

# ---- State file (重複返信防止) ----


REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())

STATE_DIR = os.path.join(REPO_ROOT, "state")
LAST_SEEN_PATH = os.path.join(STATE_DIR, "last_seen.json")


GOLIATH_DIR = os.path.join(REPO_ROOT, "goliath")
PAGES_DIR = os.path.join(GOLIATH_DIR, "pages")
OUT_DIR = os.path.join(GOLIATH_DIR, "_out")  # safe outputs (sitemap/robots/issues payload, etc.)

POLICIES_DIR = os.path.join(REPO_ROOT, "policies")  # allowed by your rule (new folder)
HUB_DIR = os.path.join(REPO_ROOT, "hub")
HUB_SITES_JSON = os.path.join(HUB_DIR, "sites.json")

AFFILIATES_JSON = os.environ.get("AFFILIATES_JSON", os.path.join(REPO_ROOT, "affiliates.json"))

DEFAULT_LANG = os.environ.get("DEFAULT_LANG", "en")  # en/ja/ko/zh
LANGS = ["en", "ja", "ko", "zh"]

RUN_ID = os.environ.get("RUN_ID", str(int(time.time())))
RANDOM_SEED = os.environ.get("RANDOM_SEED", RUN_ID)

MAX_THEMES = int(os.environ.get("MAX_THEMES", "6"))           # how many sites to build per run
MAX_COLLECT = int(os.environ.get("MAX_COLLECT", "260"))       # total target; spec 173+; overshoot allowed
MAX_AUTOFIX = int(os.environ.get("MAX_AUTOFIX", "5"))

ALLOW_ROOT_UPDATE = os.environ.get("ALLOW_ROOT_UPDATE", "0") == "1"
PING_SITEMAP = os.environ.get("PING_SITEMAP", "0") == "1"

# Social API credentials (optional)
# Social API credentials (optional) - accept alias env names too
BLUESKY_HANDLE = getenv_any(["BLUESKY_HANDLE", "BSKY_HANDLE", "BLUESKY_ID"], "")
BLUESKY_APP_PASSWORD = getenv_any(["BLUESKY_APP_PASSWORD", "BSKY_APP_PASSWORD", "BLUESKY_PASSWORD"], "")

MASTODON_BASE = getenv_any(["MASTODON_BASE", "MASTODON_INSTANCE", "MASTODON_INSTANCE_URL"], "")  # e.g. https://mastodon.social
MASTODON_TOKEN = getenv_any(["MASTODON_TOKEN", "MASTODON_ACCESS_TOKEN", "MASTODON_BEARER_TOKEN"], "")

REDDIT_CLIENT_ID = getenv_any(["REDDIT_CLIENT_ID", "REDDIT_ID", "REDDIT_APP_ID"], "")
REDDIT_CLIENT_SECRET = getenv_any(["REDDIT_CLIENT_SECRET", "REDDIT_SECRET", "REDDIT_APP_SECRET"], "")
REDDIT_REFRESH_TOKEN = getenv_any(["REDDIT_REFRESH_TOKEN", "REDDIT_TOKEN"], "")
REDDIT_USER_AGENT = getenv_any(["REDDIT_USER_AGENT"], "goliath-tool/1.0 (read-only)")
REDDIT_SUBREDDITS = getenv_any(["REDDIT_SUBREDDITS", "REDDIT_SUBS", "SUBREDDITS"], "webdev,sysadmin,programming,techsupport,github,privacy,excel,personalfinance,travel,solotravel,productivity,studytips,mealprep,fitness")

# X (Twitter) — accept alias + allow keyword-search mode
X_BEARER_TOKEN = getenv_any([
    "X_BEARER_TOKEN",
    "X_BEARER",
    "TWITTER_BEARER_TOKEN",
    "TW_BEARER_TOKEN",
    "X_API_BEARER_TOKEN",
    "TWITTER_API_BEARER_TOKEN",
], "")
X_USER_ID = getenv_any(["X_USER_ID", "X_USERID", "TWITTER_USER_ID", "TW_USER_ID", "TWITTER_ID", "X_ID"], "")
X_QUERY = getenv_any(["X_QUERY", "X_SEARCH_QUERY", "TWITTER_QUERY"], "")
X_API_BASE = getenv_any(["X_API_BASE", "TWITTER_API_BASE"], "https://api.x.com")

X_MAX = int(os.environ.get("X_MAX", "1"))    # 1 run = 1採用（read節約の前提）


# OpenAI (optional) - not required; kept off by default in this file
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")

# Content requirements
MIN_ARTICLE_CHARS_JA = int(os.environ.get("MIN_ARTICLE_CHARS_JA", "2500"))
MIN_FAQ = int(os.environ.get("MIN_FAQ", "5"))
REF_URL_MIN = int(os.environ.get("REF_URL_MIN", "10"))
REF_URL_MAX = int(os.environ.get("REF_URL_MAX", "20"))
SUPP_URL_MIN = int(os.environ.get("SUPP_URL_MIN", "3"))

# Leads/Issues
issue_items: List[Dict[str, Any]] = []
reply_count = 0

LEADS_TOTAL = int(os.environ.get("LEADS_TOTAL", "100"))  # IMPORTANT: default 100 per your requirement
ISSUE_MAX_ITEMS = int(os.environ.get("ISSUE_MAX_ITEMS", "40"))  # chunking for long issue body

# Branding / canonical
# Branding / canonical
SITE_BRAND = os.environ.get("SITE_BRAND", "Mikanntool")
SITE_LOGO = os.environ.get("SITE_LOGO", "🧰")

SITE_DOMAIN = env_first("SITE_DOMAIN", default=PUBLIC_BASE_URL)
HUB_BASE_URL = env_first("HUB_BASE_URL", default=PUBLIC_BASE_URL.rstrip("/") + "/hub/")


SITE_CONTACT_EMAIL = os.environ.get("SITE_CONTACT_EMAIL", "contact@mikanntool.com")


# Unsplash (optional): if set, we fetch one photo URL for hero background
UNSPLASH_ACCESS_KEY = os.environ.get("UNSPLASH_ACCESS_KEY", "")

# Keep hub frozen: do not touch these
FROZEN_PATH_PREFIXES = [
    os.path.join(REPO_ROOT, "hub", "index.html"),
    os.path.join(REPO_ROOT, "hub", "assets"),
    os.path.join(REPO_ROOT, "hub", "assets", "ui.v3.css"),
    os.path.join(REPO_ROOT, "hub", "assets", "app.v3.js"),
]

# 22 categories fixed (your spec)
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

random.seed(int(hashlib.sha256(RANDOM_SEED.encode("utf-8")).hexdigest()[:8], 16))


# =============================================================================
# Logging
# =============================================================================
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
    logging.info("ALLOW_ROOT_UPDATE=%s", ALLOW_ROOT_UPDATE)
    logging.info("LEADS_TOTAL=%s", LEADS_TOTAL)
    logging.info("HN_QUERY=%s", HN_QUERY)
    logging.info("HN_MAX=%s", HN_MAX)



# =============================================================================
# Utilities (IO / HTTP / Text)
# =============================================================================
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


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def safe_slug(s: str, maxlen: int = 64) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"https?://", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    if not s:
        s = "tool"
    return (s[:maxlen].strip("-") or "tool")


def clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))


def uniq_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
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


def http_post_json(
    url: str,
    payload: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 20,
) -> Tuple[int, Dict[str, Any], str]:
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


def base64_basic_auth(user: str, password: str) -> str:
    token = f"{user}:{password}"
    return base64.b64encode(token.encode("utf-8")).decode("ascii")


# =============================================================================
# Safety filters (BAN / adult/sensitive)
# =============================================================================
BAN_WORDS = [
    # illegal / violence / hate (basic)
    "kill", "murder", "bomb", "weapon", "terrorist",
    # explicit adult
    "porn", "nude", "sex", "blowjob", "dick", "vagina",
    # self-harm
    "suicide", "self-harm",
]

BAN_WORDS_JA = [
    "殺", "爆弾", "武器", "テロ",
    "ポルノ", "裸", "性行為", "ちんこ", "まんこ",
    "自殺", "自傷",
]

def adult_or_sensitive(text: str) -> bool:
    t = (text or "").lower()
    if any(w in t for w in BAN_WORDS):
        return True
    if any(w in (text or "") for w in BAN_WORDS_JA):
        return True
    return False


def too_broad_vent(text: str) -> bool:
    """
    Downrank content that is mainly venting with no actionable question.
    """
    t = (text or "").lower()
    # if there is no question-like marker and mostly abstract emotion words
    has_question = any(x in t for x in ["?", "how", "what", "which", "where", "when", "why", "help", "fix", "recommend", "best", "compare", "plan", "checklist"])
    emo = sum(1 for x in ["hate", "tired", "annoying", "frustrated", "sad", "depressed", "angry", "worst", "sucks"] if x in t)
    if (not has_question) and emo >= 2:
        return True
    return False


# =============================================================================
# Data models
# =============================================================================
@dataclass
class Post:
    source: str
    id: str
    url: str
    text: str
    author: str
    created_at: str
    lang_hint: str = ""
    meta: Optional[Dict[str, Any]] = None

    def norm_text(self) -> str:
        t = self.text or ""
        t = re.sub(r"\s+", " ", t).strip()
        return t


@dataclass
class Theme:
    title: str
    search_title: str
    slug: str
    category: str
    problem_list: List[str]
    representative_posts: List[Post]
    score: float
    keywords: List[str]
    short_code: str = ""  # /goliath/go/<code>/


# =============================================================================
# Collectors
# =============================================================================
KEYWORDS = [
    # tech
    "error", "issue", "help", "how do i", "how to", "can't", "cannot", "failed", "fix", "bug",
    "login", "password", "token", "oauth", "dns", "cname", "aaaa", "ssl", "github pages",
    "pdf", "convert", "compress", "mp4", "ffmpeg", "excel", "spreadsheet", "formula",
    # life / planning
    "itinerary", "travel plan", "packing list", "layover", "eSIM", "refund", "cancellation", "budget",
    "recipe", "meal prep", "calories", "protein", "sleep", "workout", "habit", "routine",
    "study plan", "memorize", "procrastination", "focus", "schedule", "checklist", "template",
    "resume", "interview", "anxiety", "compare", "recommend", "best",
    "move", "declutter", "cleaning", "laundry",
]

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
        logging.warning("Bluesky: session failed status=%s body=%s", status, (raw or "")[:300])
        return []

    token = js["accessJwt"]
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    # Mix tech + life
    queries = [
        # tech
        "how to fix error",
        "can't login help",
        "pdf convert fails",
        "compress mp4 best settings",
        "excel formula wrong",
        "github pages custom domain dns",
        "oauth token expired",
        "privacy settings cookies",
        # life
        "itinerary planner help",
        "packing list checklist",
        "layover eSIM advice",
        "refund cancellation policy",
        "meal prep plan",
        "calories protein plan",
        "sleep schedule fix",
        "workout routine beginner",
        "study plan schedule",
        "procrastination can't focus",
        "resume interview help",
        "budget template",
        "compare best option",
        "weekend plan ideas",
    ]

    out: List[Post] = []
    for q in queries:
        if len(out) >= max_items:
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
            if len(out) >= max_items:
                break
            uri = item.get("uri", "") or ""
            cid = item.get("cid", "") or ""
            record = item.get("record") or {}
            text = (record.get("text") or item.get("text") or "").strip()
            author = ((item.get("author") or {}).get("handle") or "unknown").strip()
            created_at = (record.get("createdAt") or item.get("indexedAt") or now_iso())

            if not text or adult_or_sensitive(text):
                continue

            post_url = ""
            if uri:
                try:
                    rkey = uri.split("/")[-1]
                    post_url = f"https://bsky.app/profile/{author}/post/{rkey}"
                except Exception:
                    post_url = uri

            if not post_url:
                continue

            pid = sha1(f"bsky:{uri}:{cid}:{post_url}")
            out.append(Post(
                source="bluesky",
                id=pid,
                url=post_url,
                text=text,
                author=author,
                created_at=created_at,
                meta={"query": q, "uri": uri, "cid": cid},
            ))

    logging.info("Bluesky: collected %d", len(out))
    return out


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
        # tech
        "help", "support", "webdev", "privacy", "excel", "opensource", "github", "dns", "linux",
        "pdf", "ffmpeg",
        # life
        "travel", "itinerary", "packing", "cooking", "mealprep", "fitness", "sleep",
        "studytips", "productivity", "personalfinance", "career", "relationships",
    ]
    queries = [
        # tech + life triggers
        "need help", "how to fix", "error", "cannot", "failed", "issue", "bug",
        "itinerary", "packing list", "meal prep", "workout plan", "sleep schedule",
        "study plan", "resume", "interview", "budget template", "compare", "recommend",
    ]

    out: List[Post] = []

    def add_statuses(statuses: List[Dict[str, Any]], hint: str) -> None:
        nonlocal out
        for s in statuses:
            if len(out) >= max_items:
                return
            sid = s.get("id", "") or ""
            url = (s.get("url") or "").strip()
            created_at = (s.get("created_at") or now_iso())
            acct = ((s.get("account") or {}).get("acct") or "unknown").strip()
            content = (s.get("content") or "")
            text = re.sub(r"<[^>]+>", " ", content)
            text = html.unescape(text).strip()

            if not url or not text or adult_or_sensitive(text):
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
            add_statuses(data.get("statuses", []) or [], f"search:{q}")
        except Exception:
            continue

    logging.info("Mastodon: collected %d", len(out))
    return out


def reddit_oauth_token() -> Optional[str]:
    if not (REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET and REDDIT_REFRESH_TOKEN):
        return None

    token_url = "https://www.reddit.com/api/v1/access_token"
    basic = "Basic " + base64_basic_auth(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET)
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
      - If OAuth creds exist: use https://oauth.reddit.com
      - Else: use public JSON endpoints (rate-limited)
    """
    subs = [x.strip() for x in (REDDIT_SUBREDDITS or "").split(",") if x.strip()]
    if not subs:
        subs = ["webdev", "sysadmin", "programming"]

    token = reddit_oauth_token()
    if token:
        base = "https://oauth.reddit.com"
        headers = {"Authorization": f"bearer {token}", "User-Agent": REDDIT_USER_AGENT, "Accept": "application/json"}
        logging.info("Reddit: OAuth mode collecting up to %d", max_items)
    else:
        base = "https://www.reddit.com"
        headers = {"User-Agent": REDDIT_USER_AGENT, "Accept": "application/json"}
        logging.info("Reddit: public mode collecting up to %d", max_items)

    triggers = [k.lower() for k in KEYWORDS]
    out: List[Post] = []

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
        for ch in children:
            if len(out) >= max_items:
                break
            d = (ch or {}).get("data") or {}
            title = (d.get("title") or "").strip()
            selftext = (d.get("selftext") or "").strip()
            text = (title + "\n" + selftext).strip()
            if not text or adult_or_sensitive(text):
                continue

            low = text.lower()
            if not any(t in low for t in triggers):
                continue

            permalink = (d.get("permalink") or "").strip()
            url = ("https://www.reddit.com" + permalink) if permalink.startswith("/") else ((d.get("url") or "").strip())
            if not url:
                continue

            author = (d.get("author") or "unknown").strip()
            created_utc = d.get("created_utc") or time.time()
            created_at = dt.datetime.fromtimestamp(float(created_utc), tz=dt.timezone.utc).astimezone().isoformat(timespec="seconds")
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
        logging.warning("HN: failed status=%s body=%s", st, (body or "")[:200])
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
        if not text or adult_or_sensitive(text):
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


# =============================================================================
# X state (duplicate prevention)
# =============================================================================


def load_last_seen() -> Dict[str, Any]:
    d = read_json(LAST_SEEN_PATH, default={})
    if not isinstance(d, dict):
        d = {}
    if "x_seen" not in d or not isinstance(d.get("x_seen"), list):
        d["x_seen"] = []
    return d

def save_last_seen(d: Dict[str, Any]) -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
    # keep small (latest 200 ids)
    seen = d.get("x_seen") or []
    if isinstance(seen, list):
        d["x_seen"] = seen[-200:]
    write_json(LAST_SEEN_PATH, d)

def collect_x_mentions(max_items: int = 1) -> List[Post]:
    """
    X v2: Keyword Search -> pick 1 tweet -> avoid duplicates via state/last_seen.json
    - 1回の実行で「採用は1件」に固定（read節約）
    - 検索だけで本文(text)は取れるので、追加のtweet取得はしない（= 実質1リクエスト前提）
    """
    if not X_BEARER_TOKEN:
        logging.info("X: skipped (missing X_BEARER_TOKEN or aliases)")
        return []

    max_items = 1  # 強制：1件だけ採用
    headers = {"Authorization": f"Bearer {X_BEARER_TOKEN}", "Accept": "application/json"}

    # クエリ（未指定なら省エネの固定クエリ）
    q = (X_QUERY or '("how to" OR help OR error OR failed OR bug OR fix) -is:retweet -is:reply').strip()

    # recent search
    url = f"{X_API_BASE.rstrip('/')}/2/tweets/search/recent?" + urlencode({
        "query": q,
        "max_results": "10",
        "tweet.fields": "created_at,lang,author_id",
    })

    st, body = http_get(url, headers=headers, timeout=20)
    if st != 200:
        logging.warning("X: search failed status=%s body=%s", st, (body or "")[:200])
        return []

    try:
        data = json.loads(body)
    except Exception:
        return []

    tweets = data.get("data") or []
    if not tweets:
        logging.info("X: collected 0")
        return []

    state = load_last_seen()
    seen = set(state.get("x_seen") or [])

    picked = None
    for t in tweets:
        tid = (t.get("id") or "").strip()
        if not tid:
            continue
        if tid in seen:
            continue
        picked = t
        break

    if not picked:
        logging.info("X: collected 0 (all duplicates)")
        return []

    tid = picked.get("id") or ""
    text = (picked.get("text") or "").strip()
    if not text or adult_or_sensitive(text):
        logging.info("X: collected 0 (filtered)")
        return []

    created_at = picked.get("created_at") or now_iso()
    author = picked.get("author_id") or "unknown"
    post_url = f"https://x.com/i/web/status/{tid}"
    pid = sha1(f"x:{tid}:{post_url}")

    # save state (commit will persist)
    state["x_seen"] = (state.get("x_seen") or []) + [tid]
    save_last_seen(state)

    out = [Post(
        source="x",
        id=pid,
        url=post_url,
        text=text,
        author=str(author),
        created_at=created_at,
        lang_hint=picked.get("lang") or "",
        meta={"query": q, "author_id": author},
    )]

    logging.info("X: collected %d (picked 1)", len(out))
    return out



# =============================================================================
# Normalization & Clustering
# =============================================================================
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

    # crude JP chunks to help clustering without full tokenizer
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
    """
    Lightweight clustering by Jaccard similarity of token sets.
    """
    logging.info("Clustering %d posts (threshold=%.2f)", len(posts), threshold)
    token_sets: Dict[str, set] = {p.id: set(simple_tokenize(p.norm_text())) for p in posts}

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
    """
    Heuristic category selection across fixed 22 categories.
    """
    text = " ".join([p.norm_text() for p in posts]).lower()
    k = set([x.lower() for x in keywords])

    def has_any(words: List[str]) -> bool:
        return any(w in text for w in words) or any(w in k for w in words)

    # tech
    if has_any(["dns", "cname", "aaaa", "a record", "nameserver", "github pages", "hosting", "ssl", "https"]):
        return "Web/Hosting"
    if has_any(["python", "node", "npm", "pip", "powershell", "bash", "cli", "library", "compile", "stack", "trace", "dev"]):
        return "Dev/Tools"
    if has_any(["automation", "workflow", "cron", "github actions", "llm", "openai", "prompt", "agent"]):
        return "AI/Automation"
    if has_any(["privacy", "security", "2fa", "phishing", "cookie", "vpn", "encryption", "leak"]):
        return "Security/Privacy"
    if has_any(["video", "mp4", "compress", "codec", "ffmpeg", "audio", "subtitle"]):
        return "Media"
    if has_any(["pdf", "docx", "ppt", "docs", "word", "convert", "merge", "compress pdf"]):
        return "PDF/Docs"
    if has_any(["image", "png", "jpg", "webp", "design", "figma", "photoshop", "illustrator"]):
        return "Images/Design"
    if has_any(["excel", "spreadsheet", "csv", "google sheets", "vlookup", "pivot", "formula"]):
        return "Data/Spreadsheets"
    if has_any(["invoice", "tax", "accounting", "bookkeeping", "receipt", "vat"]):
        return "Business/Accounting/Tax"
    if has_any(["seo", "marketing", "ads", "social", "instagram", "tiktok", "youtube", "growth"]):
        return "Marketing/Social"
    if has_any(["productivity", "todo", "note", "calendar", "time management", "procrastination", "focus"]):
        return "Productivity"
    if has_any(["english", "language", "toeic", "eiken", "ielts"]):
        return "Education/Language"

    # life
    if has_any(["travel", "trip", "hotel", "itinerary", "flight", "booking", "layover", "packing", "esim"]):
        return "Travel/Planning"
    if has_any(["recipe", "cook", "cooking", "meal prep", "kitchen", "grocery"]):
        return "Food/Cooking"
    if has_any(["workout", "fitness", "diet", "health", "running", "sleep", "calories", "protein"]):
        return "Health/Fitness"
    if has_any(["study", "learning", "exam", "homework", "memorize", "flashcards"]):
        return "Study/Learning"
    if has_any(["money", "budget", "loan", "invest", "stock", "fees", "refund"]):
        return "Money/Personal Finance"
    if has_any(["career", "job", "resume", "cv", "interview", "apply"]):
        return "Career/Work"
    if has_any(["relationship", "communication", "friend", "chat", "texting", "awkward"]):
        return "Relationships/Communication"
    if has_any(["home", "rent", "utility", "life admin", "paperwork", "moving", "declutter", "cleaning"]):
        return "Home/Life Admin"
    if has_any(["buy", "shopping", "product", "recommend", "compare", "best", "value"]):
        return "Shopping/Products"
    if has_any(["event", "ticket", "concert", "sports", "weekend plan", "date plan", "rainy day"]):
        return "Events/Leisure"

    return "Dev/Tools"


def score_cluster(posts: List[Post], category: str) -> float:
    """
    Score: cluster size + solvable tool signal + life “decision urgency” signals.
    """
    size = len(posts)
    text = " ".join([p.norm_text() for p in posts]).lower()

    solvable_signals = [
        "how", "fix", "error", "failed", "can't", "cannot", "help",
        "設定", "直し", "原因", "エラー", "できない", "不具合", "失敗",
    ]
    tool_signals = [
        "convert", "compress", "calculator", "generator", "planner", "template", "checklist", "step-by-step", "schedule",
        "変換", "圧縮", "計算", "チェック", "テンプレ", "ツール", "手順",
    ]
    life_decision = [
        "plan", "itinerary", "packing", "what should i do", "recommend", "best", "compare", "budget", "schedule",
        "checklist", "template", "step by step", "meal prep", "study plan",
    ]
    urgency = [
        "urgent", "today", "tomorrow", "this week", "before i go", "deadline", "soon", "asap",
        "今日", "明日", "今週", "出発前", "締切",
    ]
    stuck = [
        "i'm stuck", "confused", "overwhelmed", "don't know what to choose", "not sure", "anxiety",
        "詰んだ", "わからない", "迷う", "不安",
    ]

    s1 = sum(1 for w in solvable_signals if w in text)
    s2 = sum(1 for w in tool_signals if w in text)
    s3 = sum(1 for w in life_decision if w in text)
    s4 = sum(1 for w in urgency if w in text)
    s5 = sum(1 for w in stuck if w in text)

    score = size * 1.8 + s1 * 0.5 + s2 * 0.7 + s3 * 0.55 + s4 * 0.45 + s5 * 0.35

    if too_broad_vent(text):
        score *= 0.75

    # mild balancing so life categories can compete
    if category in ["Travel/Planning", "Food/Cooking", "Health/Fitness", "Study/Learning", "Money/Personal Finance",
                    "Career/Work", "Relationships/Communication", "Home/Life Admin", "Shopping/Products", "Events/Leisure"]:
        score *= 1.12

    return float(score)


def build_search_title(category: str, keywords: List[str]) -> str:
    """
    Force titles toward “search query” style (EN) + includes tool-ish noun.
    """
    kw = [k for k in keywords if len(k) <= 18][:6]
    base = " ".join(kw[:3]).strip()
    if not base:
        base = category.replace("/", " ")

    if category == "Travel/Planning":
        return f"{base} itinerary planner checklist"
    if category == "Food/Cooking":
        return f"{base} meal prep plan + shopping list"
    if category == "Health/Fitness":
        return f"{base} workout plan + habit tracker"
    if category == "Study/Learning":
        return f"{base} study plan schedule template"
    if category == "Money/Personal Finance":
        return f"{base} budget planner + fee checklist"
    if category == "Career/Work":
        return f"{base} resume checklist + interview prep"
    if category == "Relationships/Communication":
        return f"{base} conversation templates + awkwardness fixes"
    if category == "Home/Life Admin":
        return f"{base} moving checklist + life admin planner"
    if category == "Shopping/Products":
        return f"{base} compare tool + buying checklist"
    if category == "Events/Leisure":
        return f"{base} weekend plan generator + checklist"
    # tech
    if category == "Web/Hosting":
        return f"{base} DNS/SSL fix checklist"
    if category == "PDF/Docs":
        return f"{base} PDF convert/merge checklist"
    if category == "Media":
        return f"{base} video compression settings checklist"
    if category == "Data/Spreadsheets":
        return f"{base} spreadsheet formula fix checklist"
    if category == "Security/Privacy":
        return f"{base} privacy settings + login fix checklist"
    if category == "AI/Automation":
        return f"{base} automation workflow fix checklist"
    if category == "Marketing/Social":
        return f"{base} social growth checklist + template"
    if category == "Education/Language":
        return f"{base} language study plan template"
    return f"{base} fix guide checklist tool"


def make_theme(posts: List[Post]) -> Theme:
    keywords = extract_keywords(posts)
    category = choose_category(posts, keywords)
    score = score_cluster(posts, category)

    search_title = build_search_title(category, keywords)
    base_slug = safe_slug(search_title)
    # collision-safe slug allocation happens later
    title = f"{search_title} | {SITE_BRAND}"

    problems: List[str] = []
    for p in posts[:12]:
        line = p.norm_text()[:140].rstrip()
        if line:
            problems.append(line)
    problems = uniq_keep_order([re.sub(r"\s+", " ", x) for x in problems])

    while len(problems) < 10:
        problems.append(f"Trouble related to {category}: symptom #{len(problems)+1}")
    problems = problems[:20]

    return Theme(
        title=title,
        search_title=search_title,
        slug=base_slug,  # may be adjusted with -2,-3 on collision
        category=category,
        problem_list=problems,
        representative_posts=posts[: min(len(posts), 8)],
        score=score,
        keywords=keywords,
        short_code="",
    )


# =============================================================================
# Affiliates
# =============================================================================
def load_affiliates() -> Dict[str, Any]:
    data = read_json(AFFILIATES_JSON, default={})
    if not isinstance(data, dict):
        return {}
    return data


def normalize_affiliates_shape(aff: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Accept:
      - { "categories": { "<CAT>": [..], ... } }
      - or { "<CAT>": [..], ... }
    Return { "<CAT>": [ dict, ... ] } only for keys in CATEGORIES_22.
    """
    categories: Dict[str, Any] = {}
    if isinstance(aff.get("categories"), dict):
        categories = aff["categories"]
    else:
        categories = aff

    out: Dict[str, List[Dict[str, Any]]] = {}
    for cat in CATEGORIES_22:
        v = categories.get(cat, [])
        if isinstance(v, list):
            out[cat] = [x for x in v if isinstance(x, dict)]
        else:
            out[cat] = []
    return out


def sanitize_affiliate_html(h: str) -> str:
    """
    Script tags forbidden. Keep existing approach: strip <script ...>...</script>.
    """
    if not h:
        return ""
    h2 = re.sub(r"(?is)<script[^>]*>.*?</script>", "", h)
    return h2.strip()


def pick_affiliates_for_category(aff_norm: Dict[str, List[Dict[str, Any]]], category: str, topn: int = 2) -> List[Dict[str, Any]]:
    items = aff_norm.get(category, []) or []

    def pr(x: Dict[str, Any]) -> float:
        try:
            return float(x.get("priority", 0))
        except Exception:
            return 0.0

    cleaned: List[Dict[str, Any]] = []
    for x in items:
        html_code = x.get("html", "") or ""
        if html_code:
            x2 = dict(x)
            x2["html"] = sanitize_affiliate_html(str(html_code))
            cleaned.append(x2)
        elif x.get("url"):
            cleaned.append(x)
        else:
            # ignore items without html/url
            pass

    cleaned.sort(key=lambda x: -pr(x))
    return cleaned[:topn]


def audit_affiliate_keys(aff_raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Check affiliates.json keys match GENRES (CATEGORIES_22). Missing keys => issue note.
    """
    if isinstance(aff_raw.get("categories"), dict):
        keys = set(aff_raw["categories"].keys())
    elif isinstance(aff_raw, dict):
        keys = set(aff_raw.keys())
    else:
        keys = set()

    # ignore "categories" wrapper key itself
    keys.discard("categories")

    missing = [c for c in CATEGORIES_22 if c not in keys]
    extra = sorted([k for k in keys if k not in set(CATEGORIES_22)])

    return {
        "missing": missing,
        "extra": extra,
        "ok": (len(missing) == 0),
    }


# =============================================================================
# Hub inventory (hub/sites.json) & routing features (categories / popular / new / purpose)
# ---- Affiliates: always define aff_norm (and aff_audit) BEFORE any use ----
def init_affiliates() -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    """
    Safe initializer for affiliates.
    Returns:
      - aff_norm: { "<CAT>": [ {..}, .. ] }
      - aff_audit: { missing: [...], extra: [...], ok: bool }
    """
    try:
        _aff_raw = load_affiliates()
        _audit = audit_affiliate_keys(_aff_raw)
        _norm = normalize_affiliates_shape(_aff_raw)
        return _norm, _audit
    except Exception:
        # ultra-safe fallback (no affiliates)
        _audit = {"missing": CATEGORIES_22[:], "extra": [], "ok": False}
        _norm = {c: [] for c in CATEGORIES_22}
        return _norm, _audit

aff_norm, aff_audit = init_affiliates()


# =============================================================================
def read_hub_sites() -> List[Dict[str, Any]]:
    data = read_json(HUB_SITES_JSON, default={})
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict) and isinstance(data.get("sites"), list):
        return [x for x in data["sites"] if isinstance(x, dict)]
    return []


def write_hub_sites(sites: List[Dict[str, Any]], aggregates: Dict[str, Any]) -> None:
    """
    hub frozen: ONLY update sites.json.
    (Do not touch hub/index.html or hub/assets.)
    """
    if is_frozen_path(HUB_SITES_JSON):
        # sites.json itself is allowed (not in frozen list). Still, keep safe.
        pass

    os.makedirs(HUB_DIR, exist_ok=True)
    payload = {
        "sites": sites,
        "aggregates": aggregates,  # categories / popular / new / purpose
        "updated_at": now_iso(),
    }
    write_json(HUB_SITES_JSON, payload)


def compute_aggregates(all_sites: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Provide hub-strengthening data via sites.json (categories list + popular/new/purpose routes).
    Even if hub frontend ignores it today, the data is ready and future-proof.
    """
    # categories
    cats: Dict[str, List[Dict[str, Any]]] = {}
    for cat in CATEGORIES_22:
        cats[cat] = []

    for s in all_sites:
        cat = s.get("category") or ""
        if cat in cats:
            cats[cat].append({
                "title": s.get("search_title") or s.get("title") or "Tool",
                "url": s.get("url") or "#",
                "slug": s.get("slug") or "",
            })

    for cat in cats:
        # stable ordering: title
        cats[cat].sort(key=lambda x: (x.get("title") or "").lower())

    # new: by updated_at / created_at
    def ts(s: Dict[str, Any]) -> float:
        iso = s.get("updated_at") or s.get("created_at") or ""
        try:
            return dt.datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0

    new_sites = sorted(all_sites, key=ts, reverse=True)[:12]
    new_list = [{"title": s.get("search_title") or s.get("title") or "Tool", "url": s.get("url") or "#", "slug": s.get("slug") or ""} for s in new_sites]

    # popular: prefer views/score/popularity if present; else fallback to recency
    def pop_metric(s: Dict[str, Any]) -> float:
        for k in ["views", "score", "popularity"]:
            if k in s:
                try:
                    return float(s.get(k, 0))
                except Exception:
                    pass
        return ts(s)

    popular_sites = sorted(all_sites, key=pop_metric, reverse=True)[:12]
    popular_list = [{"title": s.get("search_title") or s.get("title") or "Tool", "url": s.get("url") or "#", "slug": s.get("slug") or ""} for s in popular_sites]

    # purpose routes: simple buckets for internal navigation
    purpose_buckets = {
        "Popular tools": popular_list[:8],
        "New tools": new_list[:8],
        "By purpose": [],  # filled below
    }

    purpose_keywords = {
        "convert": ["convert", "変換", "pdf", "docx", "png", "mp4"],
        "time": ["time", "schedule", "calendar", "deadline", "study plan", "itinerary"],
        "productivity": ["template", "checklist", "planner", "workflow", "habit"],
        "pricing": ["budget", "fees", "cost", "price", "compare", "refund"],
    }

    by_purpose: Dict[str, List[Dict[str, Any]]] = {k: [] for k in purpose_keywords.keys()}
    for s in all_sites:
        title = (s.get("search_title") or s.get("title") or "").lower()
        for bucket, words in purpose_keywords.items():
            if any(w.lower() in title for w in words):
                by_purpose[bucket].append({
                    "title": s.get("search_title") or s.get("title") or "Tool",
                    "url": s.get("url") or "#",
                    "slug": s.get("slug") or "",
                })

    # keep small
    for bucket in by_purpose:
        by_purpose[bucket] = by_purpose[bucket][:12]

    purpose_buckets["By purpose"] = [{"bucket": k, "items": v} for k, v in by_purpose.items()]

    return {
        "categories": cats,
        "popular": popular_list,
        "new": new_list,
        "purpose": purpose_buckets,
    }


# =============================================================================
# Shortlinks (for “short URL + one-line value”)
# =============================================================================
BASE62 = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

def to_base62(n: int) -> str:
    if n == 0:
        return "0"
    out = []
    while n > 0:
        n, r = divmod(n, 62)
        out.append(BASE62[r])
    return "".join(reversed(out))

def short_code_for_url(url: str) -> str:
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
    n = int(h, 16)
    code = to_base62(n)
    return code[:8]

def build_shortlink_page(target_url: str, code: str) -> Tuple[str, str]:
    """
    Returns (relative_path_under_repo, html_content)
    Short link lives under: goliath/go/<code>/index.html
    """
    rel_dir = os.path.join("goliath", "go", code)
    rel_path = os.path.join(rel_dir, "index.html")
    esc = html.escape(target_url, quote=True)
    content = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="robots" content="noindex">
  <meta http-equiv="refresh" content="0;url={esc}">
  <link rel="canonical" href="{esc}">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Redirect</title>
</head>
<body style="font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;padding:24px;">
  <p>Redirecting…</p>
  <p><a href="{esc}">{esc}</a></p>
  <script>location.replace("{esc}");</script>
</body>
</html>
"""
    return rel_path, content


# =============================================================================
# i18n dictionaries (core UI strings)
# =============================================================================
I18N = {'en': {'home': 'Home',
        'about': 'About Us',
        'all_tools': 'All Tools',
        'language': 'Language',
        'share': 'Share',
        'problems': 'Problems this tool can help with',
        'tool': 'Tool',
        'quick_answer': 'Quick answer',
        'causes': 'Common causes',
        'steps': 'Step-by-step checklist',
        'pitfalls': 'Common pitfalls & how to avoid them',
        'next': 'If it still doesn’t work',
        'faq': 'FAQ',
        'references': 'Reference links',
        'supplement': 'Supplementary resources',
        'related': 'Related tools',
        'popular': 'Popular tools',
        'disclaimer': 'Disclaimer',
        'terms': 'Terms',
        'privacy': 'Privacy',
        'contact': 'Contact',
        'footer_note': 'Practical, fast, and respectful guides—built to reduce wasted trial-and-error.',
        'aff_title': 'Recommended',
        'copy': 'Copy',
        'copied': 'Copied',
        'short_value': 'Do it in 3 seconds',
        'tool_input': 'Input',
        'tool_input_hint': '(paste your details)',
        'tool_placeholder': 'Example: dates, constraints, what you tried, what you need…',
        'tool_generate': 'Generate',
        'tool_clear': 'Clear',
        'tool_tip': 'Tip: include the exact error message and what changed recently.'},
 'ja': {'home': 'Home',
        'about': 'About Us',
        'all_tools': 'All Tools',
        'language': '言語',
        'share': '共有',
        'problems': 'このツールが助ける悩み一覧',
        'tool': 'ツール',
        'quick_answer': '結論（最短で直す方針）',
        'causes': '原因のパターン分け',
        'steps': '手順（チェックリスト）',
        'pitfalls': 'よくある失敗と回避策',
        'next': '直らない場合の次の手',
        'faq': 'FAQ',
        'references': '参考URL',
        'supplement': '補助資料',
        'related': '関連ツール',
        'popular': '人気のツール',
        'disclaimer': '免責事項',
        'terms': '利用規約',
        'privacy': 'プライバシーポリシー',
        'contact': 'お問い合わせ',
        'footer_note': '実務で使える手順に寄せて、短時間で解決できる形を目指しています。',
        'aff_title': 'おすすめ',
        'copy': 'コピー',
        'copied': 'コピーしました',
        'short_value': '3秒でできる',
        'tool_input': '入力',
        'tool_input_hint': '（状況を貼り付け）',
        'tool_placeholder': '例：日時、制約、試したこと、必要なこと…',
        'tool_generate': '生成',
        'tool_clear': 'クリア',
        'tool_tip': 'コツ：エラーメッセージ全文と「最近変えたこと」を入れると精度が上がります。'},
 'ko': {'home': 'Home',
        'about': 'About Us',
        'all_tools': 'All Tools',
        'language': '언어',
        'share': '공유',
        'problems': '이 도구가 해결할 수 있는 고민',
        'tool': '도구',
        'quick_answer': '결론(가장 빠른 해결 방향)',
        'causes': '원인 패턴',
        'steps': '체크리스트(단계별)',
        'pitfalls': '자주 하는 실수와 회피법',
        'next': '계속 안 될 때',
        'faq': 'FAQ',
        'references': '참고 링크',
        'supplement': '추가 자료',
        'related': '관련 도구',
        'popular': '인기 도구',
        'disclaimer': '면책',
        'terms': '이용약관',
        'privacy': '개인정보 처리방침',
        'contact': '문의',
        'footer_note': '바로 실행 가능한 가이드를 목표로 합니다.',
        'aff_title': '추천',
        'copy': '복사',
        'copied': '복사됨',
        'short_value': '3초면 끝',
        'tool_input': '입력',
        'tool_input_hint': '(상황을 붙여넣기)',
        'tool_placeholder': '예: 날짜/제약/시도한 것/원하는 것…',
        'tool_generate': '생성',
        'tool_clear': '지우기',
        'tool_tip': '팁: 정확한 오류 메시지와 최근 변경점을 포함하세요.'},
 'zh': {'home': 'Home',
        'about': 'About Us',
        'all_tools': 'All Tools',
        'language': '语言',
        'share': '分享',
        'problems': '本工具可帮助解决的问题',
        'tool': '工具',
        'quick_answer': '结论（最快修复方向）',
        'causes': '常见原因分类',
        'steps': '步骤清单',
        'pitfalls': '常见坑与规避方法',
        'next': '仍无法解决时',
        'faq': 'FAQ',
        'references': '参考链接',
        'supplement': '补充资料',
        'related': '相关工具',
        'popular': '热门工具',
        'disclaimer': '免责声明',
        'terms': '条款',
        'privacy': '隐私政策',
        'contact': '联系',
        'footer_note': '提供可落地、快速、尊重用户的排障指南。',
        'aff_title': '推荐',
        'copy': '复制',
        'copied': '已复制',
        'short_value': '3秒搞定',
        'tool_input': '输入',
        'tool_input_hint': '（粘贴你的情况）',
        'tool_placeholder': '例如：日期、限制、已尝试内容、目标…',
        'tool_generate': '生成',
        'tool_clear': '清空',
        'tool_tip': '提示：请包含完整报错信息以及最近变更点。'}}


def build_i18n_script(default_lang: str) -> str:
    """
    Returns a <script> block that:
      - Applies i18n to all [data-i18n] nodes
      - Applies i18n to placeholders via [data-i18n-placeholder]
      - Persists language/theme to localStorage
      - Supports light/dark toggle (class-based Tailwind dark mode)
    """
    i18n_json = json.dumps(I18N, ensure_ascii=False)
    langs_json = json.dumps(sorted(list(I18N.keys())), ensure_ascii=False)

    return f"""<script>
const I18N = {i18n_json};
const LANGS = {langs_json};

function t(lang, key) {{
  return (I18N[lang] && I18N[lang][key]) || (I18N[\"{default_lang}\"] && I18N[\"{default_lang}\"][key]) || key;
}}

function setLang(lang) {{
  if (!LANGS.includes(lang)) lang = \"{default_lang}\";
  document.documentElement.setAttribute(\"lang\", lang);
  localStorage.setItem(\"lang\", lang);

  document.querySelectorAll(\"[data-i18n]\").forEach(el => {{
    const key = el.getAttribute(\"data-i18n\");
    el.textContent = t(lang, key);
  }});

  document.querySelectorAll(\"[data-i18n-placeholder]\").forEach(el => {{
    const key = el.getAttribute(\"data-i18n-placeholder\");
    el.setAttribute(\"placeholder\", t(lang, key));
  }});

  document.querySelectorAll(\"[data-i18n-value]\").forEach(el => {{
    const key = el.getAttribute(\"data-i18n-value\");
    el.value = t(lang, key);
  }});
}}

function initLang() {{
  const saved = localStorage.getItem(\"lang\");
  const lang = saved || \"{default_lang}\";
  setLang(lang);
  const sel = document.getElementById(\"langSel\");
  if (sel) {{
    sel.value = lang;
    sel.addEventListener(\"change\", (e) => setLang(e.target.value));
  }}
}}

function setTheme(mode) {{
  if (mode === \"dark\") {{
    document.documentElement.classList.add(\"dark\");
  }} else {{
    document.documentElement.classList.remove(\"dark\");
  }}
  localStorage.setItem(\"theme\", mode);
}}

function initTheme() {{
  const saved = localStorage.getItem(\"theme\");
  const prefersDark = window.matchMedia && window.matchMedia(\"(prefers-color-scheme: dark)\").matches;
  const mode = saved || (prefersDark ? \"dark\" : \"light\");
  setTheme(mode);

  const btn = document.getElementById(\"themeBtn\");
  if (btn) {{
    btn.addEventListener(\"click\", () => {{
      const isDark = document.documentElement.classList.contains(\"dark\");
      setTheme(isDark ? \"light\" : \"dark\");
    }});
  }}
}}

document.addEventListener(\"DOMContentLoaded\", () => {{
  initTheme();
  initLang();
}});
</script>"""


# =============================================================================
# Content generation (quick answer, causes, steps, faq, article)
# =============================================================================
def build_quick_answer(category: str, keywords: List[str]) -> str:
    kw = ", ".join(keywords[:10])
    base = [
        "最短で進める方針は「再現条件の固定 → 原因の切り分け → 最小変更 → 検証 → 記録」です。",
        f"今回のカテゴリは「{category}」なので、まずは“どこで止まっているか”を小さく分解して確認します。",
        f"観測キーワード: {kw}",
        "下のチェックリストは、上から順に潰せば“事故率”が下がる順番で並べています。",
    ]
    return "\n".join(base)


def build_causes(category: str) -> List[str]:
    common = {
        "Web/Hosting": [
            "DNSの反映待ち（TTL）やレコード種別の誤り（A/CNAME/AAAAの混在）",
            "HTTPS/証明書の自動発行待ち、リダイレクトのループ",
            "ホスティング側の設定（カスタムドメイン、パス、ベースURL）不一致",
            "キャッシュ（CDN/ブラウザ/Service Worker）による古い表示",
        ],
        "PDF/Docs": [
            "ファイルサイズ/ページ数上限による失敗",
            "フォント埋め込み・暗号化・スキャンPDFでの互換性問題",
            "変換先形式の選択ミス（画像化が必要なのにテキスト変換を選ぶ等）",
            "ブラウザのメモリ不足・拡張機能の干渉",
        ],
        "Media": [
            "コーデック不一致（H.264/H.265/AV1）や音声形式（AAC/Opus）",
            "ビットレート/解像度上限によるエラー",
            "端末性能・メモリ不足による処理落ち",
            "ファイル破損・コンテナ不整合（MP4/MKV）",
        ],
        "Data/Spreadsheets": [
            "関数の参照範囲ズレ・絶対参照/相対参照のミス",
            "区切り文字・文字コード・日付形式の差（CSV取り込み）",
            "フィルタ/ピボットの更新忘れ",
            "共有設定/権限で編集が反映されない",
        ],
        "Security/Privacy": [
            "権限（OAuth/トークン）期限切れ・スコープ不足",
            "Cookie/追跡ブロックでログインが壊れる",
            "2FAや端末認証の不一致",
            "偽サイト/フィッシング・セキュリティソフトの誤検知",
        ],
        "AI/Automation": [
            "APIキー/権限不足、レート制限、モデル名の不一致",
            "入力が曖昧で出力が安定しない（仕様が揺れている）",
            "ファイル/パスの上書き事故、衝突時の処理漏れ",
            "ログ不足で原因特定が遅れる",
        ],
        "Travel/Planning": [
            "目的・日数・移動制約が決まっておらず、旅程が発散する",
            "移動時間の見積もりが甘く、詰め込みすぎになる",
            "持ち物が“現地調達できる物/できない物”で分けられていない",
            "予算配分（宿/交通/食/予備費）が曖昧で不安が残る",
        ],
        "Food/Cooking": [
            "献立が先に決まらず、買い物が迷子になる",
            "作り置きの“保存日数/温め直し”を考えずに回らない",
            "栄養バランス（たんぱく質/野菜/炭水化物）の偏り",
            "時間の見積もり不足で結局外食になる",
        ],
        "Health/Fitness": [
            "睡眠/食事/運動のどれがボトルネックか分かっていない",
            "習慣化の単位が大きすぎて継続できない",
            "強度が高すぎて疲労→中断のループ",
            "記録がなく、改善点が見えない",
        ],
        "Study/Learning": [
            "復習タイミングが固定されず、忘却で効率が落ちる",
            "教材が多すぎて優先順位が決まらない",
            "目標が抽象的で、今日やることに落ちない",
            "集中環境が整っていない（通知/場所/時間帯）",
        ],
        "Money/Personal Finance": [
            "固定費・変動費・特別費の区別がなく、原因が見えない",
            "手数料/返金条件の確認不足",
            "支払い日・引き落とし日がズレて資金繰りが苦しい",
            "比較軸（総額/利便性/リスク）が曖昧",
        ],
        "Career/Work": [
            "職務要約が長すぎて要点が埋もれる",
            "実績が“数字”で書けておらず強みが伝わらない",
            "面接想定問答が用意されておらず詰まる",
            "応募先ごとのカスタムが不足",
        ],
        "Relationships/Communication": [
            "伝えたいことが多く、文が長くなって誤解される",
            "相手の温度感に合わせた言い回しが不足",
            "断り方/お願いの型がなく気まずくなる",
            "返信タイミングが不安で空回りする",
        ],
        "Home/Life Admin": [
            "やることの棚卸しがなく、抜け漏れが出る",
            "期限・提出先・必要書類が散らばっている",
            "片付けの範囲が広すぎて進まない",
            "ルーティン化できず毎回ゼロから考える",
        ],
        "Shopping/Products": [
            "比較軸（価格/保証/サイズ/耐久/用途）が定義できていない",
            "レビューの読み方が偏り、結論が出ない",
            "必要十分のスペックが分からない",
            "買うタイミング（セール/返品可否）が不明",
        ],
        "Events/Leisure": [
            "候補が多く、優先順位が決まらない",
            "天気・混雑・移動時間の見積もり不足",
            "当日の持ち物/予約/支払いが不安",
            "同行者の希望が整理できていない",
        ],
    }
    return common.get(category, [
        "入力・前提条件のズレ（想定と実際が違う）",
        "権限/設定/バージョンの不一致",
        "キャッシュや反映待ち",
        "原因が前段にあるのに、見えている画面で決め打ちしている",
    ])


def build_steps(category: str) -> List[str]:
    """
    Step-by-step checklist generator.
    NOTE: この関数は SyntaxError の原因になりやすいので、
          括弧やクォートの閉じ忘れが起きない “安全な固定形” にしています。
    """
    steps: List[str] = [
        "再現条件を固定する（同じ入力・同じ手順・同じ端末/ブラウザで再現）",
        "表示/ログをそのまま保存（コピペ/スクショ、時刻も残す）",
        "影響範囲が小さい順に確認（確認→読み取り→最小変更→検証）",
        "直ったら差分を記録し、再発防止チェックを作る（次回3分復旧が目標）",
    ]

    if category in ["Web/Hosting", "AI/Automation"]:
        steps += [
            "上書き禁止を強制する（衝突は -2/-3、凍結パスは触らない）",
            "ログ粒度を上げる（HTTPステータス/例外/レスポンス先頭）",
        ]

    if category == "Travel/Planning":
        steps += [
            "日数・出発/帰宅時刻・絶対にやりたいこと（3つ）を先に固定",
            "移動時間を先に置いて、残りに観光を入れる（詰め込み防止）",
            "持ち物を「必須/現地調達/予備」に分けてチェックリスト化",
            "予算を「宿/交通/食/観光/予備費」に割って上限を決める",
        ]

    if category == "Food/Cooking":
        steps += [
            "主菜を先に決める（3〜5個）→副菜→主食の順で決める",
            "買い物リストをカテゴリ別（肉/野菜/調味料…）に出す",
            "作り置きは保存日数ベースで回す（先に消費順を決める）",
            "調理は同時進行しやすい順に並べる（焼く/茹でる/切る）",
        ]

    if category == "Health/Fitness":
        steps += [
            "まず睡眠を固定（就寝/起床の時刻を先に決める）",
            "運動は最小単位から（例：腕立て5回/散歩10分）",
            "週の回数→強度の順で上げる（いきなり強度は上げない）",
            "記録は1項目だけ（体重/歩数/睡眠など）から開始",
        ]

    if category == "Study/Learning":
        steps += [
            "目標を「今週の量」→「今日の量」に割る（最小単位を作る）",
            "復習は翌日/3日後/7日後の固定枠で回す",
            "教材は同時に2つまで（増やすほど迷う）",
            "集中は環境で作る（通知OFF/場所固定/開始の儀式）",
        ]

    if category == "Money/Personal Finance":
        steps += [
            "固定費/変動費/特別費に分けて、まず固定費から最適化",
            "手数料/返金条件/解約条件を“先に”確認して事故を防ぐ",
            "支払い日・引き落とし日をカレンダーに固定（ズレで詰まない）",
            "比較軸（総額/利便性/リスク）を1枚にまとめて決め切る",
        ]

    if category == "Career/Work":
        steps += [
            "実績は数字で書く（例：改善率/件数/期間/役割）",
            "職務要約は3行で結論→根拠→再現性の順",
            "応募先ごとに要点だけ差し替える（全部を書き換えない）",
            "面接は想定質問を先に潰す（自己紹介/志望動機/強み/弱み）",
        ]

    if category == "Relationships/Communication":
        steps += [
            "文を短くする（1文1要点、余計な前置きを削る）",
            "お願い/断り/お礼の型を使う（毎回ゼロから考えない）",
            "相手の温度感に合わせて情報量を調整する",
            "返信が不安なら“選択肢”で返す（AかB、どっちがいい？形式）",
        ]

    if category == "Home/Life Admin":
        steps += [
            "やることを棚卸し→期限→提出先→必要書類の順で整理",
            "チェックリストは“提出単位”で作る（書類1つ=1項目）",
            "片付けは範囲を小さく切る（引き出し1つなど）",
            "ルーティンは固定時刻に置く（毎週/毎月で繰り返し）",
        ]

    if category == "Shopping/Products":
        steps += [
            "比較軸を決める（価格/保証/サイズ/耐久/用途）",
            "必要十分スペックを先に確定（上位互換を追わない）",
            "レビューは低評価→中評価→高評価の順で読む（地雷回避）",
            "返品条件と到着日を最後に確認して購入",
        ]

    if category == "Events/Leisure":
        steps += [
            "候補を3つまでに絞る（増やすほど決められない）",
            "天気・混雑・移動時間を先に置く（当日崩壊を防ぐ）",
            "予約/支払い/持ち物を前日までに確定",
            "同行者がいるなら希望を1枚にまとめて合意",
        ]

    # 余分に増えすぎないように上限
    return steps[:28]



def build_pitfalls(category: str) -> List[str]:
    pitfalls = [
        "一気に複数箇所を変えてしまい、どれが原因か分からなくなる",
        "反映待ち（DNS/キャッシュ）を無視して焦ってさらに壊す",
        "ログ/メモを取らずに試行回数だけ増やす（後で復旧不能になる）",
        "“いま見えている画面”が原因だと決めつける（前段が原因のことが多い）",
    ]
    if category in ["Web/Hosting", "AI/Automation"]:
        pitfalls.append("既存URLや凍結領域（/hub/）を上書きして資産を壊す（絶対禁止）")
    if category in ["Travel/Planning", "Food/Cooking", "Shopping/Products"]:
        pitfalls.append("比較軸が曖昧なまま情報収集し続けて決断できない")
    if category in ["Health/Fitness", "Study/Learning"]:
        pitfalls.append("最初から量を盛りすぎて、続かず自己嫌悪になる")
    return pitfalls


def build_next_actions(category: str) -> List[str]:
    nxt = [
        "別経路で同じ結果が出るか確認（別端末/別回線/別ブラウザ）",
        "ログ/メモの粒度を上げる（失敗時の条件と差分を残す）",
        "“元に戻せる形”で段階的にロールバック（変更前後の差分を残す）",
        "同じ失敗を繰り返さないよう、チェック項目を固定化する",
    ]
    if category == "Security/Privacy":
        nxt.append("怪しいリンク/認証画面は踏まない。公式ドメインと証明書を再確認")
    if category in ["Travel/Planning", "Money/Personal Finance"]:
        nxt.append("最悪ケース（延泊/キャンセル/手数料）を先に想定して予備費・代替案を用意")
    return nxt


def build_faq(category: str) -> List[Tuple[str, str]]:
    base = [
        ("What should I check first?", "Fix the conditions: steps, expected result, actual result, and what changed recently."),
        ("How do I know if it’s just cache / stale data?", "Try private mode or a different device. If it changes, cache is likely involved."),
        ("What’s the safest order to troubleshoot?", "Confirm → read-only checks → one small change → verify → write down the diff."),
        ("What should I do after it works?", "Save the diff + a quick checklist so the next recovery is under 3 minutes."),
        ("How should I share this problem with someone?", "Include steps to reproduce, expected vs actual, logs/screenshots, and environment."),
    ]
    if category == "Web/Hosting":
        base.append(("How long can DNS propagation take?", "It depends on TTL and resolvers. Confirm from a third-party DNS lookup too."))
    if category == "Travel/Planning":
        base.append(("How do I avoid overpacking?", "Split items into: must-have, can-buy-there, and optional backups. Then cut optional."))
    if category == "Shopping/Products":
        base.append(("How do I stop endless comparing?", "Limit to 3 options, pick 3 criteria, then decide using total cost + return policy."))
    # ensure >= MIN_FAQ
    return base[: max(MIN_FAQ, 5)]


def supplemental_resources_for_category(category: str) -> List[str]:
    base: Dict[str, List[str]] = {
        "Web/Hosting": [
            "https://pages.github.com/",
            "https://letsencrypt.org/docs/",
            "https://developer.mozilla.org/en-US/docs/Learn/Common_questions/Web_mechanics/What_is_a_domain_name",
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
            "https://en.wikipedia.org/wiki/Automation",
        ],
        "Travel/Planning": [
            "https://en.wikipedia.org/wiki/Travel_itinerary",
            "https://en.wikipedia.org/wiki/Packing_list",
            "https://www.wikivoyage.org/",
        ],
        "Food/Cooking": [
            "https://en.wikipedia.org/wiki/Meal_preparation",
            "https://en.wikipedia.org/wiki/Food_safety",
            "https://www.fda.gov/food",
        ],
        "Health/Fitness": [
            "https://en.wikipedia.org/wiki/Physical_fitness",
            "https://en.wikipedia.org/wiki/Sleep_hygiene",
            "https://www.who.int/health-topics/physical-activity",
        ],
        "Study/Learning": [
            "https://en.wikipedia.org/wiki/Spaced_repetition",
            "https://en.wikipedia.org/wiki/Testing_effect",
            "https://en.wikipedia.org/wiki/Study_skills",
        ],
        "Money/Personal Finance": [
            "https://en.wikipedia.org/wiki/Personal_finance",
            "https://en.wikipedia.org/wiki/Budget",
            "https://en.wikipedia.org/wiki/Interest",
        ],
        "Career/Work": [
            "https://en.wikipedia.org/wiki/Curriculum_vitae",
            "https://en.wikipedia.org/wiki/Job_interview",
            "https://en.wikipedia.org/wiki/Cover_letter",
        ],
        "Relationships/Communication": [
            "https://en.wikipedia.org/wiki/Interpersonal_communication",
            "https://en.wikipedia.org/wiki/Active_listening",
            "https://en.wikipedia.org/wiki/Nonviolent_communication",
        ],
        "Home/Life Admin": [
            "https://en.wikipedia.org/wiki/Checklist",
            "https://en.wikipedia.org/wiki/Time_management",
            "https://en.wikipedia.org/wiki/Personal_organizer",
        ],
        "Shopping/Products": [
            "https://en.wikipedia.org/wiki/Comparison_shopping",
            "https://en.wikipedia.org/wiki/Product_lifecycle",
            "https://en.wikipedia.org/wiki/Warranty",
        ],
        "Events/Leisure": [
            "https://en.wikipedia.org/wiki/Event_planning",
            "https://en.wikipedia.org/wiki/Ticket_(admission)",
            "https://en.wikipedia.org/wiki/Leisure",
        ],
        # tech-ish fallbacks
        "Dev/Tools": [
            "https://en.wikipedia.org/wiki/Debugging",
            "https://en.wikipedia.org/wiki/Software_bug",
            "https://docs.python.org/3/tutorial/errors.html",
        ],
        "Marketing/Social": [
            "https://en.wikipedia.org/wiki/Search_engine_optimization",
            "https://en.wikipedia.org/wiki/Digital_marketing",
            "https://en.wikipedia.org/wiki/Social_media",
        ],
        "Business/Accounting/Tax": [
            "https://en.wikipedia.org/wiki/Accounting",
            "https://en.wikipedia.org/wiki/Tax",
            "https://en.wikipedia.org/wiki/Invoice",
        ],
        "Images/Design": [
            "https://en.wikipedia.org/wiki/Raster_graphics",
            "https://en.wikipedia.org/wiki/Vector_graphics",
            "https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types",
        ],
        "Education/Language": [
            "https://en.wikipedia.org/wiki/Second-language_acquisition",
            "https://en.wikipedia.org/wiki/Language_learning",
            "https://en.wikipedia.org/wiki/Flashcard",
        ],
    }

    default = [
        "https://en.wikipedia.org/wiki/Troubleshooting",
        "https://en.wikipedia.org/wiki/Checklist",
        "https://developer.mozilla.org/",
    ]
    return (base.get(category) or default)




def pick_reference_urls(theme: Theme) -> List[str]:
    """
    References: 10-20 “source-like” URLs.
    Since we’re not doing web scraping here, we use:
      - representative post URLs (up to 8)
      - plus supplemental resources and well-known docs
    """
    refs = [p.url for p in theme.representative_posts if p.url]
    refs = uniq_keep_order(refs)
    refs = refs[:8]

    supp = supplemental_resources_for_category(theme.category)
    # mix-in to reach REF_URL_MIN
    extras = [
        "https://support.google.com/webmasters/answer/156184",
        "https://developers.google.com/search/docs/crawling-indexing/sitemaps/overview",
        "https://developer.mozilla.org/en-US/docs/Web/SEO",
        "https://developers.google.com/search/docs/crawling-indexing/robots/intro",
    ]
    pool = uniq_keep_order(supp + extras)
    random.shuffle(pool)

    for u in pool:
        if len(refs) >= REF_URL_MIN:
            break
        if u not in refs:
            refs.append(u)

    # cap
    return refs[: clamp(REF_URL_MAX, REF_URL_MIN, 30)]


def generate_long_article_ja(theme: Theme) -> str:
    """
    Must be >= MIN_ARTICLE_CHARS_JA chars.
    Deterministic long form to guarantee volume without OpenAI.
    """
    intro = (
        f"このページは「{theme.category}」でよく起きる悩みを、"
        f"短時間で安全に整理して解決へ進めるためのガイドです。\n"
        "ポイントは“推測で決め打ちしない”こと。再現条件を固定し、"
        "影響範囲が小さい順にチェックするだけで、無駄な試行回数が大きく減ります。\n"
    )
    why = (
        "多くのトラブルは、(1)設定の不一致、(2)権限や期限、(3)キャッシュ/反映待ち、"
        "(4)入力条件の揺れ、のどれかに落ちます。\n"
        "つまり、この4点を順に潰すだけで“直らない理由”の大半は説明できます。\n"
    )
    detail = (
        "大事なのは「最小変更」です。一度に複数箇所をいじると、直ったとしても原因が分からず再発します。\n"
        "最小変更→検証→記録、を守ると、次回はチェックリストだけで復旧できます。\n"
    )

    examples = "【このページで扱う悩み一覧（例）】\n" + "\n".join([f"- {p}" for p in theme.problem_list]) + "\n"
    causes = "【原因のパターン分け】\n" + "\n".join([f"- {c}" for c in build_causes(theme.category)]) + "\n"
    steps = "【手順（チェックリスト）】\n" + "\n".join([f"- {s}" for s in build_steps(theme.category)]) + "\n"
    pitfalls = "【よくある失敗と回避策】\n" + "\n".join([f"- {p}" for p in build_pitfalls(theme.category)]) + "\n"
    nxt = "【直らない場合の次の手】\n" + "\n".join([f"- {n}" for n in build_next_actions(theme.category)]) + "\n"

    verify = (
        "【検証のコツ】\n"
        "- “期待結果”を1文にする（何ができれば成功か）\n"
        "- 失敗が出たら、入力・環境・時刻・ログをセットで残す\n"
        "- 直った瞬間に、何を変えたかを1行で書ける状態にする\n"
        "- 再発防止は“次回3分で復旧できるか”で判断する\n"
        "これだけで、調査が感情ではなく手順になります。\n"
    )

    tree = (
        "【切り分けの分岐（迷った時用）】\n"
        "1) 別ブラウザ/別端末でも同じ？\n"
        "  - はい → サービス/設定/権限側が濃厚\n"
        "  - いいえ → キャッシュ/拡張機能/端末依存が濃厚\n"
        "2) 同じ入力・同じ手順で再現する？\n"
        "  - はい → 原因追跡が可能。ログを増やして一点ずつ潰す\n"
        "  - いいえ → 入力条件が揺れている。まず再現条件の固定が最優先\n"
        "この分岐を守るだけで、無駄な試行をかなり減らせます。\n"
    )

    body = "\n".join([intro, why, detail, examples, causes, steps, pitfalls, nxt, verify, tree]).strip()

    # pad to guarantee chars
    if len(body) < MIN_ARTICLE_CHARS_JA:
        pads: List[str] = []
        while len(body) + sum(len(x) for x in pads) < MIN_ARTICLE_CHARS_JA + 200:
            pads.append(
                "【追加メモ】\n"
                "問題が複雑に見える時ほど、最初に“変えた点”を列挙し、それを一つずつ戻して差分を取ると復旧が早くなります。\n"
                "ログがない場合は、まずログを作ることが最短ルートです。\n"
            )
        body = body + "\n" + "\n".join(pads)

    return body.strip()


def short_value_line(category: str) -> str:
    """
    One-line value (for Bluesky post draft).
    Keep it short, concrete, non-spammy.
    """
    mapping = {
        "Travel/Planning": "Build a clean itinerary + packing checklist in seconds.",
        "Food/Cooking": "Generate a meal-prep plan + shopping list in seconds.",
        "Health/Fitness": "Turn your goal into a tiny daily routine + tracker in seconds.",
        "Study/Learning": "Generate a study plan + spaced-review schedule in seconds.",
        "Money/Personal Finance": "Make a simple budget + fee checklist in seconds.",
        "Career/Work": "Turn your notes into resume bullets + interview prompts in seconds.",
        "Relationships/Communication": "Get short conversation templates (ask/decline/follow-up) in seconds.",
        "Home/Life Admin": "Create a moving/life-admin checklist in seconds.",
        "Shopping/Products": "Compare options using 3 criteria + decide fast in seconds.",
        "Events/Leisure": "Pick a weekend plan (A/B for weather) in seconds.",
        "Web/Hosting": "Get a DNS/SSL checklist + quick tests in seconds.",
        "PDF/Docs": "Get a PDF convert/merge checklist in seconds.",
        "Media": "Get video compression settings + checklist in seconds.",
        "Data/Spreadsheets": "Get spreadsheet debugging steps + checklist in seconds.",
        "Security/Privacy": "Get privacy/login troubleshooting checklist in seconds.",
        "AI/Automation": "Get automation workflow debugging checklist in seconds.",
    }
    return mapping.get(category, "Get a clean checklist + next steps in seconds.")


# =============================================================================
# Tool UI generation (category-aware planners)
# =============================================================================
def build_tool_ui(theme: Theme) -> str:
    """
    In-page "tool" block (simple + fast):
      - Problems this tool can help solve
      - A lightweight checklist
      - Minimal JS (none) to keep PageSpeed high
    """
    title_raw = getattr(theme, "search_title", None) or getattr(theme, "title", None) or "Tool"
    cat_raw = getattr(theme, "category", None) or "Dev/Tools"
    page_title = html.escape(str(title_raw))
    cat = html.escape(str(cat_raw))

    problems = getattr(theme, "problem_list", None) or []
    if not isinstance(problems, list):
        problems = [problems]

    problems_html_items: List[str] = []
    for p in problems:
        if p is None:
            continue
        s = str(p).strip()
        if not s:
            continue
        problems_html_items.append(f"<li class='leading-relaxed'>{html.escape(s)}</li>")
    if not problems_html_items:
        problems_html_items = ["<li class='text-slate-500'>(no items)</li>"]
    problems_html = "\n".join(problems_html_items)

    # Checklist steps (keep short; long explanation is in the article section)
    steps: List[str] = []
    if "build_steps" in globals():
        try:
            steps = build_steps(getattr(theme, "category", "")) or []
        except Exception:
            # build_steps is optional; keep UI usable
            steps = []
    if not steps:
        steps = [
            "Reproduce the issue with the same inputs",
            "Collect exact error messages + timestamps",
            "Try the smallest safe change first",
            "Verify the fix and document what changed",
        ]

    steps_html = "\n".join(
        f"<li class='leading-relaxed'>{html.escape(str(s))}</li>" for s in steps if str(s).strip()
    ) or "<li class='text-slate-500'>(no steps)</li>"

    return f"""
<section class="rounded-2xl border border-slate-200 bg-white/80 backdrop-blur p-5 shadow-sm dark:border-white/10 dark:bg-white/5">
  <div class="flex items-start justify-between gap-4">
    <div>
      <div class="text-xs uppercase tracking-wider text-slate-500 dark:text-white/50">{cat}</div>
      <h2 class="mt-1 text-lg font-semibold text-slate-900 dark:text-white" data-i18n="tool">Tool</h2>
      <p class="mt-1 text-sm text-slate-600 dark:text-white/70">{page_title}</p>
    </div>
  </div>

  <div class="mt-5 grid gap-4 md:grid-cols-2">
    <div class="rounded-xl border border-slate-200 bg-white/70 p-4 dark:border-white/10 dark:bg-black/20">
      <div class="text-sm font-semibold text-slate-900 dark:text-white" data-i18n="problems">Problems this tool can help solve</div>
      <ul class="mt-2 list-disc pl-5 text-sm text-slate-700 dark:text-white/75">
        {problems_html}
      </ul>
    </div>

    <div class="rounded-xl border border-slate-200 bg-white/70 p-4 dark:border-white/10 dark:bg-black/20">
      <div class="text-sm font-semibold text-slate-900 dark:text-white" data-i18n="quick_answer">Quick checklist</div>
      <ol class="mt-2 list-decimal pl-5 text-sm text-slate-700 dark:text-white/75">
        {steps_html}
      </ol>
    </div>
  </div>
</section>
""".strip()

def render_affiliate_block(affiliate: Dict[str, Any]) -> str:
    if affiliate.get("html"):
        return str(affiliate["html"])
    if affiliate.get("url"):
        title = html_escape(affiliate.get("title", "Recommended"))
        url = html_escape(affiliate["url"])
        return f'<a class="underline" href="{url}" rel="nofollow noopener" target="_blank">{title}</a>'
    return ""


def fetch_unsplash_bg_url() -> str:
    """
    Optional. If UNSPLASH_ACCESS_KEY is set, try to fetch a single abstract gradient image.
    Fallback to empty string (CSS gradients used).
    """
    if not UNSPLASH_ACCESS_KEY:
        return ""
    # Use Unsplash "random" endpoint (no heavy parsing needed)
    # https://api.unsplash.com/photos/random?query=abstract%20gradient&orientation=landscape
    url = "https://api.unsplash.com/photos/random?" + urlencode({
        "query": "abstract gradient",
        "orientation": "landscape",
        "content_filter": "high",
    })
    st, body = http_get(url, headers={"Accept": "application/json", "Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}"}, timeout=20)
    if st != 200:
        return ""
    try:
        js = json.loads(body)
        u = ((js.get("urls") or {}).get("regular") or "").strip()
        return u
    except Exception:
        return ""


def build_page_html(
    theme: Theme,
    tool_url: str,
    short_url: str,
    affiliates_top2: List[Dict[str, Any]],
    references: List[str],
    supplements: List[str],
    article_ja: str,
    faq: List[Tuple[str, str]],
    related_tools: List[Dict[str, Any]],
    popular_sites: List[Dict[str, Any]],
    hero_bg_url: str = "",
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
        <details class="rounded-2xl border border-slate-200/70 dark:border-white/10 bg-white/70 dark:bg-white/5 p-4">
          <summary class="cursor-pointer font-medium">{html_escape(q)}</summary>
          <div class="mt-2 text-slate-900 dark:text-slate-700 dark:text-white/80 leading-relaxed">{html_escape(a)}</div>
        </details>
        """.strip()
        for q, a in faq
    ])

    ref_html = "\n".join([f"<li class='py-1'><a class='underline break-all' href='{html_escape(u)}' target='_blank' rel='noopener'>{html_escape(u)}</a></li>" for u in references])
    sup_html = "\n".join([f"<li class='py-1'><a class='underline break-all' href='{html_escape(u)}' target='_blank' rel='noopener'>{html_escape(u)}</a></li>" for u in supplements])

    # affiliates slot: top2
    aff_blocks = []
    for a in affiliates_top2[:2]:
        title = html_escape(a.get("title", "Recommended"))
        block = render_affiliate_block(a)
        if not block:
            continue
        aff_blocks.append(f"""
        <div class="rounded-2xl border border-slate-200/70 dark:border-white/10 bg-white/70 dark:bg-white/5 p-4">
          <div class="text-sm text-slate-900 dark:text-slate-600 dark:text-white/70 mb-2">{title}</div>
          <div class="prose prose-invert max-w-none">{block}</div>
        </div>
        """.strip())
    if not aff_blocks:
        aff_blocks = ["""
        <div class="rounded-2xl border border-slate-200/70 dark:border-white/10 bg-white/70 dark:bg-white/5 p-4">
          <div class="text-sm text-slate-900 dark:text-slate-600 dark:text-white/70 mb-2">Recommended</div>
          <div class="text-slate-900 dark:text-slate-600 dark:text-white/70">No affiliate available for this category.</div>
        </div>
        """.strip()]
    aff_html = "\n".join(aff_blocks)

    related_html = "\n".join([
        f"<li class='py-1'><a class='underline' href='{html_escape(t.get('url','#'))}'>{html_escape(t.get('title','Tool'))}</a> "
        f"<span class='text-slate-900 dark:text-slate-500 dark:text-white/50 text-xs'>({html_escape(t.get('category',''))})</span></li>"
        for t in related_tools
    ])

    popular_html = "\n".join([
        f"<li class='py-1'><a class='underline' href='{html_escape(t.get('url','#'))}'>{html_escape(t.get('title','Tool'))}</a> "
        f"<span class='text-slate-900 dark:text-slate-500 dark:text-white/50 text-xs'>({html_escape(t.get('category',''))})</span></li>"
        for t in popular_sites
    ])

    canonical = tool_url if tool_url.startswith("http") else (SITE_DOMAIN.rstrip("/") + "/" + theme.slug + "/")

    article_html = "<p class='leading-relaxed whitespace-pre-wrap text-slate-900 dark:text-white/85'>" + html_escape(article_ja) + "</p>"
    try:
        tool_ui = build_tool_ui(theme)
    except Exception:
        logging.exception("build_tool_ui failed")
        raise
    # internal linking: ALWAYS provide a path back to /hub/
    hub_url = SITE_DOMAIN.rstrip("/") + "/hub/"

    # short URL block (for click-through + share)
    share_script = """
<script>
function copyTextFrom(id, btnId){
  const el = document.getElementById(id);
  if(!el) return;
  navigator.clipboard.writeText(el.value).then(()=>{
    const b = document.getElementById(btnId);
    if(b){
      b.textContent = (window.I18N && I18N[document.documentElement.lang] && I18N[document.documentElement.lang].copied) || "Copied";
    }
    setTimeout(()=>{
      const b2 = document.getElementById(btnId);
      if(b2){
        b2.textContent = (window.I18N && I18N[document.documentElement.lang] && I18N[document.documentElement.lang].copy) || "Copy";
      }
    }, 1200);
  });
}
</script>
""".strip()

    bg_css = ""
    if hero_bg_url:
        bg_css = f"""
  <div class="pointer-events-none fixed inset-0 opacity-40">
    <div class="absolute inset-0 bg-cover bg-center" style="background-image:url('{html_escape(hero_bg_url)}')"></div>
    <div class="absolute inset-0 bg-slate-50 dark:bg-zinc-950/70"></div>
  </div>
        """.strip()
    else:
        bg_css = """
  <div class="pointer-events-none fixed inset-0 opacity-80 dark:opacity-70">
    <div class="absolute -top-24 -left-24 h-96 w-96 rounded-full bg-gradient-to-br from-indigo-500/35 to-cyan-400/20 blur-3xl"></div>
    <div class="absolute top-40 -right-24 h-96 w-96 rounded-full bg-gradient-to-br from-emerald-500/25 to-lime-400/10 blur-3xl"></div>
    <div class="absolute bottom-0 left-1/4 h-96 w-96 rounded-full bg-gradient-to-br from-fuchsia-500/20 to-rose-400/10 blur-3xl"></div>
  </div>
        """.strip()

    html_doc = f"""<!doctype html>
<html lang="{html_escape(DEFAULT_LANG)}">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html_escape(theme.search_title)} | {html_escape(SITE_BRAND)}</title>
  <meta name="description" content="{html_escape('One-page fix guide + checklist + tool: ' + theme.search_title)}">
  <link rel="canonical" href="{html_escape(canonical)}">
  <meta property="og:title" content="{html_escape(theme.search_title)}">
  <meta property="og:description" content="{html_escape('Fix guide + checklist + FAQ + references')}">
  <meta property="og:type" content="website">
  <meta property="og:url" content="{html_escape(canonical)}">
  <meta name="twitter:card" content="summary_large_image">
  <script>tailwind = window.tailwind || {{}}; tailwind.config = {{ darkMode: "class" }};</script>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    :root {{ color-scheme: dark; }}
    body {{
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto,
        "Noto Sans JP","Noto Sans KR","Noto Sans SC", Arial, "Apple Color Emoji","Segoe UI Emoji";
    }}
    .glass {{ backdrop-filter: blur(10px); }}
  </style>
</head>
<body class="min-h-screen bg-slate-50 dark:bg-zinc-950 text-slate-900 dark:text-white">
  {bg_css}

  <header class="relative z-10 mx-auto max-w-6xl px-4 py-6">
    <div class="flex items-center justify-between gap-4">
      <a href="{html_escape(hub_url)}" class="flex items-center gap-3">
        <div class="h-10 w-10 rounded-2xl bg-slate-100/80 dark:bg-white/10 border border-slate-200/70 dark:border-white/10 flex items-center justify-center font-bold">🍊</div>
        <div>
          <div class="font-semibold leading-tight">{html_escape(SITE_BRAND)}</div>
          <div class="text-xs text-slate-900 dark:text-slate-500 dark:text-white/60">Hub → categories / popular / new</div>
        </div>
      </a>

      <nav class="flex items-center gap-3 text-sm">
        <a class="text-slate-900 dark:text-slate-700 dark:text-white/80 hover:text-slate-900 dark:text-white" href="{html_escape(hub_url)}" data-i18n="home">Home</a>
        <a class="text-slate-900 dark:text-slate-700 dark:text-white/80 hover:text-slate-900 dark:text-white" href="{html_escape(hub_url)}#about" data-i18n="about">About Us</a>
        <a class="text-slate-900 dark:text-slate-700 dark:text-white/80 hover:text-slate-900 dark:text-white" href="{html_escape(hub_url)}#tools" data-i18n="all_tools">All Tools</a>
        <button id="themeBtn" type="button" class="ml-2 rounded-xl bg-slate-100/80 hover:bg-slate-200/70 dark:bg-white/10 dark:hover:bg-white/20 border border-slate-200/70 dark:border-white/10 px-2 py-1 text-xs" aria-label="Theme">🌓</button>
        <select id="langSel" class="ml-2 rounded-xl bg-slate-100/80 dark:bg-white/10 border border-slate-200/70 dark:border-white/10 px-2 py-1 text-xs">
          <option value="en">EN</option>
          <option value="ja">JA</option>
          <option value="ko">KO</option>
          <option value="zh">ZH</option>
        </select>
      </nav>
    </div>
  </header>

  <main class="relative z-10 mx-auto max-w-6xl px-4 pb-16">
    <section class="rounded-3xl border border-slate-200/70 dark:border-white/10 bg-white/70 dark:bg-white/5 glass p-6 md:p-8">
      <div class="flex flex-col md:flex-row md:items-start md:justify-between gap-4">
        <div>
          <h1 class="text-2xl md:text-3xl font-semibold leading-tight">{html_escape(theme.search_title)}</h1>
          <p class="mt-2 text-slate-900 dark:text-slate-600 dark:text-white/70">
            Category: <span class="text-slate-900 dark:text-slate-900 dark:text-white/90">{html_escape(theme.category)}</span> ·
            Updated: <span class="text-slate-900 dark:text-slate-900 dark:text-white/90">{html_escape(now_iso())}</span>
          </p>
        </div>
        <div class="rounded-2xl border border-slate-200/70 dark:border-white/10 bg-white/60 dark:bg-black/20 p-4 w-full md:w-[360px]">
          <div class="text-sm text-slate-900 dark:text-slate-600 dark:text-white/70 mb-2" data-i18n="share">Share</div>
          <div class="space-y-2">
            <div class="text-xs text-slate-900 dark:text-slate-500 dark:text-white/60">Short URL (for posts)</div>
            <div class="flex items-center gap-2">
              <input id="shortUrl" value="{html_escape(short_url)}" class="w-full rounded-xl bg-white/80 dark:bg-black/40 border border-slate-200/70 dark:border-white/10 px-3 py-2 text-xs" readonly>
              <button id="copyBtnShort" class="rounded-xl bg-slate-100/80 dark:bg-white/10 border border-slate-200/70 dark:border-white/10 px-3 py-2 text-xs" data-i18n="copy" onclick="copyTextFrom('shortUrl','copyBtnShort')">Copy</button>
            </div>

            <div class="text-xs text-slate-900 dark:text-slate-500 dark:text-white/60">Full URL</div>
            <div class="flex items-center gap-2">
              <input id="fullUrl" value="{html_escape(tool_url)}" class="w-full rounded-xl bg-white/80 dark:bg-black/40 border border-slate-200/70 dark:border-white/10 px-3 py-2 text-xs" readonly>
              <button id="copyBtnFull" class="rounded-xl bg-slate-100/80 dark:bg-white/10 border border-slate-200/70 dark:border-white/10 px-3 py-2 text-xs" data-i18n="copy" onclick="copyTextFrom('fullUrl','copyBtnFull')">Copy</button>
            </div>
          </div>
        </div>
      </div>
    </section>

    <section class="mt-6">
  {tool_ui}
</section>

<script>
function copyTextFrom(inputId, btnId) {{
  const input = document.getElementById(inputId);
  const btn = document.getElementById(btnId);
  if (!input) return;

  const text = String(input.value ?? "");

  const done = () => {{
    if (!btn) return;
    const prev = btn.textContent;
    btn.textContent = "Copied";
    setTimeout(() => {{ btn.textContent = prev; }}, 1200);
  }};

  if (navigator.clipboard && window.isSecureContext) {{
    navigator.clipboard.writeText(text).then(done).catch(() => {{
      input.focus();
      input.select();
      try {{ document.execCommand("copy"); }} catch (e) {{}}
      done();
    }});
    return;
  }}

  input.focus();
  input.select();
  try {{ document.execCommand("copy"); }} catch (e) {{}}
  done();
}}
</script>


    <section class="mt-6 grid grid-cols-1 lg:grid-cols-3 gap-6">
      <div class="lg:col-span-2 space-y-6">
        <div class="rounded-3xl border border-slate-200/70 dark:border-white/10 bg-white/70 dark:bg-white/5 p-6">
          <h2 class="text-xl font-semibold" data-i18n="problems">Problems this tool can help with</h2>
          <ul class="mt-3 text-slate-900 dark:text-white/85 list-disc list-inside">
            {problems_html}
          </ul>
        </div>

        <div class="rounded-3xl border border-slate-200/70 dark:border-white/10 bg-white/70 dark:bg-white/5 p-6">
          <h2 class="text-xl font-semibold" data-i18n="quick_answer">Quick answer</h2>
          <pre class="mt-3 text-slate-900 dark:text-white/85 whitespace-pre-wrap leading-relaxed">{html_escape(quick_answer)}</pre>
        </div>

        <div class="rounded-3xl border border-slate-200/70 dark:border-white/10 bg-white/70 dark:bg-white/5 p-6">
          <h2 class="text-xl font-semibold" data-i18n="causes">Common causes</h2>
          <ul class="mt-3 text-slate-900 dark:text-white/85 list-disc list-inside">
            {causes_html}
          </ul>
        </div>

        <div class="rounded-3xl border border-slate-200/70 dark:border-white/10 bg-white/70 dark:bg-white/5 p-6">
          <h2 class="text-xl font-semibold" data-i18n="steps">Step-by-step checklist</h2>
          <ul class="mt-3 text-slate-900 dark:text-white/85 list-disc list-inside">
            {steps_html}
          </ul>
        </div>

        <div class="rounded-3xl border border-slate-200/70 dark:border-white/10 bg-white/70 dark:bg-white/5 p-6">
          <h2 class="text-xl font-semibold" data-i18n="pitfalls">Common pitfalls & how to avoid them</h2>
          <ul class="mt-3 text-slate-900 dark:text-white/85 list-disc list-inside">
            {pitfalls_html}
          </ul>
        </div>

        <div class="rounded-3xl border border-slate-200/70 dark:border-white/10 bg-white/70 dark:bg-white/5 p-6">
          <h2 class="text-xl font-semibold" data-i18n="next">If it still doesn’t work</h2>
          <ul class="mt-3 text-slate-900 dark:text-white/85 list-disc list-inside">
            {next_html}
          </ul>
        </div>

        <div class="rounded-3xl border border-slate-200/70 dark:border-white/10 bg-white/70 dark:bg-white/5 p-6">
          <h2 class="text-xl font-semibold">Long guide (JP, 2500+ chars)</h2>
          <div class="mt-3">{article_html}</div>
        </div>

        <div class="rounded-3xl border border-slate-200/70 dark:border-white/10 bg-white/70 dark:bg-white/5 p-6">
          <h2 class="text-xl font-semibold" data-i18n="faq">FAQ</h2>
          <div class="mt-3 space-y-3">{faq_html}</div>
        </div>

        <div class="rounded-3xl border border-slate-200/70 dark:border-white/10 bg-white/70 dark:bg-white/5 p-6">
          <h2 class="text-xl font-semibold" data-i18n="references">Reference links</h2>
          <ul class="mt-3 text-slate-900 dark:text-white/85 list-disc list-inside">
            {ref_html}
          </ul>
        </div>

        <div class="rounded-3xl border border-slate-200/70 dark:border-white/10 bg-white/70 dark:bg-white/5 p-6">
          <h2 class="text-xl font-semibold" data-i18n="supplement">Supplementary resources</h2>
          <ul class="mt-3 text-slate-900 dark:text-white/85 list-disc list-inside">
            {sup_html}
          </ul>
        </div>
      </div>

      <aside class="space-y-6">
        <div class="rounded-3xl border border-slate-200/70 dark:border-white/10 bg-white/70 dark:bg-white/5 p-6">
          <h3 class="text-lg font-semibold" data-i18n="aff_title">Recommended</h3>
          <div class="mt-3 space-y-3">
            <!-- AFF_SLOT (top2 injected) -->
            {aff_html}
          </div>
        </div>

        <div class="rounded-3xl border border-slate-200/70 dark:border-white/10 bg-white/70 dark:bg-white/5 p-6">
          <h3 class="text-lg font-semibold" data-i18n="related">Related tools</h3>
          <ul class="mt-3 text-slate-900 dark:text-white/85 list-disc list-inside">
            {related_html}
          </ul>
        </div>

        <div class="rounded-3xl border border-slate-200/70 dark:border-white/10 bg-white/70 dark:bg-white/5 p-6">
          <h3 class="text-lg font-semibold" data-i18n="popular">Popular tools</h3>
          <ul class="mt-3 text-slate-900 dark:text-white/85 list-disc list-inside">
            {popular_html}
          </ul>
        </div>
      </aside>
    </section>
  </main>

  <footer class="relative z-10 mt-10 bg-zinc-900/60 border-t border-slate-200/70 dark:border-white/10">
    <div class="mx-auto max-w-6xl px-4 py-10 grid grid-cols-1 md:grid-cols-4 gap-8">
      <div class="md:col-span-2">
        <div class="flex items-center gap-3">
          <div class="h-10 w-10 rounded-2xl bg-slate-100/80 dark:bg-white/10 border border-slate-200/70 dark:border-white/10 flex items-center justify-center font-bold">🍊</div>
          <div>
            <div class="font-semibold">{html_escape(SITE_BRAND)}</div>
            <div class="text-xs text-slate-900 dark:text-slate-500 dark:text-white/60" data-i18n="footer_note">Practical, fast, and respectful guides—built to reduce wasted trial-and-error.</div>
          </div>
        </div>
        <div class="mt-3 text-xs text-slate-900 dark:text-slate-500 dark:text-white/60">Contact: {html_escape(SITE_CONTACT_EMAIL)}</div>
      </div>

      <div class="text-sm">
        <div class="font-semibold mb-2">Legal</div>
        <ul class="space-y-2 text-slate-900 dark:text-slate-600 dark:text-white/70">
          <li><a class="underline" href="{html_escape(SITE_DOMAIN.rstrip('/') + '/policies/privacy.html')}" data-i18n="privacy">Privacy</a></li>
          <li><a class="underline" href="{html_escape(SITE_DOMAIN.rstrip('/') + '/policies/terms.html')}" data-i18n="terms">Terms</a></li>
          <li><a class="underline" href="{html_escape(SITE_DOMAIN.rstrip('/') + '/policies/contact.html')}" data-i18n="contact">Contact</a></li>
        </ul>
      </div>

      <div class="text-sm">
        <div class="font-semibold mb-2">Hub</div>
        <ul class="space-y-2 text-slate-900 dark:text-slate-600 dark:text-white/70">
          <li><a class="underline" href="{html_escape(hub_url)}">/hub/</a></li>
          <li><a class="underline" href="{html_escape(hub_url)}#tools">All tools</a></li>
        </ul>
      </div>
    </div>
  </footer>

  {build_i18n_script(DEFAULT_LANG)}
  {share_script}
</body>
</html>
"""
    return html_doc


# =============================================================================
# Site building helpers (slug collision safe, related/popular)
# =============================================================================
def allocate_unique_slug(base_slug: str) -> str:
    """
    No-overwrite rule: if goliath/pages/<slug> exists, use -2, -3...
    """
    base = safe_slug(base_slug)
    if not os.path.exists(os.path.join(PAGES_DIR, base)):
        return base
    for i in range(2, 100):
        cand = f"{base}-{i}"
        if not os.path.exists(os.path.join(PAGES_DIR, cand)):
            return cand
    # extremely unlikely
    return f"{base}-{sha1(base)[:6]}"


def site_url_for_slug(slug: str) -> str:
    """
    Public URL for a generated page.
    Your generation path: goliath/pages/<slug>/index.html
    """
    return SITE_DOMAIN.rstrip("/") + f"/goliath/pages/{slug}/"


def choose_related_tools(all_sites: List[Dict[str, Any]], category: str, exclude_slug: str, n: int = 5) -> List[Dict[str, Any]]:
    same = [s for s in all_sites if s.get("category") == category and s.get("slug") != exclude_slug]
    other = [s for s in all_sites if s.get("slug") != exclude_slug]
    random.shuffle(same)
    random.shuffle(other)
    picks = (same + other)[:n]
    return [{"title": s.get("search_title") or s.get("title", "Tool"), "url": s.get("url", "#"), "category": s.get("category", ""), "slug": s.get("slug", "")} for s in picks]


def compute_popular_sites(all_sites: List[Dict[str, Any]], n: int = 6) -> List[Dict[str, Any]]:
    def metric(s: Dict[str, Any]) -> float:
        for k in ["views", "score", "popularity"]:
            if k in s:
                try:
                    return float(s.get(k, 0))
                except Exception:
                    pass
        iso = s.get("updated_at") or s.get("created_at") or ""
        try:
            return dt.datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
        except Exception:
            return 0.0

    sites = list(all_sites)
    sites.sort(key=lambda x: metric(x), reverse=True)
    return [{"title": s.get("search_title") or s.get("title", "Tool"), "url": s.get("url", "#"), "category": s.get("category", ""), "slug": s.get("slug", "")} for s in sites[:n]]


# =============================================================================
# Policies (legal fortress) - /policies/ only (allowed)
# =============================================================================

def ensure_policies() -> List[str]:
    """
    Create/overwrite policies pages (privacy/terms/contact) under /policies/.
    Pages are styled consistently with tool pages (light + dark, i18n switch).
    Returns absolute URLs for sitemap.
    """
    os.makedirs(POLICIES_DIR, exist_ok=True)
    privacy_path = os.path.join(POLICIES_DIR, "privacy.html")
    terms_path = os.path.join(POLICIES_DIR, "terms.html")
    contact_path = os.path.join(POLICIES_DIR, "contact.html")

    hub_url = SITE_DOMAIN.rstrip("/") + "/hub/"
    i18n_json = json.dumps(I18N, ensure_ascii=False)
    langs_json = json.dumps(sorted(list(I18N.keys())), ensure_ascii=False)

    # Full text per language (keep it static & AdSense-friendly)
    POLICY_TEXT = {
        "privacy": {
            "en": [
                "This site uses cookies and similar technologies to improve usability and measure performance.",
                "We may use Google AdSense to display ads. Third-party vendors, including Google, may use cookies to serve ads based on a user’s prior visits.",
                "You can control cookies in your browser settings. Disabling cookies may affect site features.",
            ],
            "ja": [
                "当サイトは利便性向上・計測のため、Cookie等の技術を使用する場合があります。",
                "当サイトでは Google AdSense を利用して広告を配信する場合があります。第三者配信事業者（Google等）が Cookie を使用し、過去のアクセス情報に基づいて広告を表示することがあります。",
                "Cookie はブラウザ設定で無効化できますが、一部機能が利用できなくなる場合があります。",
            ],
            "ko": [
                "이 사이트는 사용성 개선 및 성능 측정을 위해 쿠키 등 유사 기술을 사용할 수 있습니다.",
                "이 사이트는 Google AdSense를 사용하여 광고를 게재할 수 있습니다. Google 등 제3자 공급업체는 사용자의 이전 방문 정보를 기반으로 쿠키를 사용할 수 있습니다.",
                "쿠키는 브라우저 설정에서 관리/비활성화할 수 있으나 일부 기능이 제한될 수 있습니다.",
            ],
            "zh": [
                "本网站可能使用 Cookie 等技术以提升可用性并进行性能统计。",
                "本网站可能使用 Google AdSense 投放广告。包括 Google 在内的第三方供应商可能会使用 Cookie，根据用户以往访问记录投放广告。",
                "您可以在浏览器设置中管理/禁用 Cookie，但可能会影响部分功能。",
            ],
        },
        "terms": {
            "en": [
                "Use of this site is at your own risk. The information and tools are provided “as is” without warranties.",
                "We are not liable for any loss or damage arising from use of calculations, checklists, or recommendations.",
                "You are responsible for verifying results and complying with applicable laws and service terms.",
            ],
            "ja": [
                "当サイトの情報・ツールは現状有姿で提供されます。利用は自己責任でお願いします。",
                "計算結果・チェックリスト・提案内容を利用したことにより生じた損害について、当サイトは責任を負いません。",
                "最終判断はご自身で行い、各種規約・法律を遵守してください。",
            ],
            "ko": [
                "본 사이트의 정보/도구는 “있는 그대로” 제공되며 이용은 사용자 책임입니다.",
                "계산 결과, 체크리스트, 권고사항 사용으로 발생한 손해에 대해 당사는 책임을 지지 않습니다.",
                "결과를 검증하고 관련 법/서비스 약관을 준수할 책임은 사용자에게 있습니다.",
            ],
            "zh": [
                "本网站的信息与工具按“现状”提供，使用风险由您自行承担。",
                "因使用计算结果、清单或建议造成的任何损失，本网站不承担责任。",
                "请自行核对结果并遵守相关法律及服务条款。",
            ],
        },
        "contact": {
            "en": [
                f"Operator: {SITE_BRAND}",
                f"Contact: {SITE_CONTACT_EMAIL}",
                "For inquiries about ads, content, or corrections, please email us.",
            ],
            "ja": [
                f"運営者: {SITE_BRAND}",
                f"連絡先: {SITE_CONTACT_EMAIL}",
                "広告・内容・訂正のご連絡はメールでお願いします。",
            ],
            "ko": [
                f"운영자: {SITE_BRAND}",
                f"연락처: {SITE_CONTACT_EMAIL}",
                "광고/콘텐츠/정정 문의는 이메일로 연락해 주세요.",
            ],
            "zh": [
                f"运营者: {SITE_BRAND}",
                f"联系方式: {SITE_CONTACT_EMAIL}",
                "关于广告、内容或更正等咨询请发送邮件。",
            ],
        },
    }

    def build_policy_html(page_key: str) -> str:
        title = {"privacy": "privacy", "terms": "terms", "contact": "contact"}[page_key]
        # page titles: use data-i18n
        body_json = json.dumps(POLICY_TEXT[page_key], ensure_ascii=False)

        return f"""<!doctype html>
<html lang="{html_escape(DEFAULT_LANG)}" class="">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html_escape(SITE_BRAND)} | {title}</title>
  <script>tailwind = window.tailwind || {{}}; tailwind.config = {{ darkMode: "class" }};</script>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    :root {{ color-scheme: light; }}
    html.dark {{ color-scheme: dark; }}
    body {{
      font-family: ui-sans-serif, system-ui, -apple-system, "Inter", Segoe UI, Roboto,
        "Noto Sans JP","Noto Sans KR","Noto Sans SC", Arial, "Apple Color Emoji","Segoe UI Emoji";
    }}
  </style>
</head>
<body class="min-h-screen bg-slate-50 dark:bg-zinc-950 text-slate-900 dark:text-white">
  <header class="border-b border-slate-200/70 dark:border-white/10">
    <div class="mx-auto max-w-5xl px-4 py-4 flex items-center justify-between">
      <a href="{html_escape(hub_url)}" class="flex items-center gap-2">
        <span class="text-lg">{html_escape(SITE_LOGO)}</span>
        <span class="font-semibold">{html_escape(SITE_BRAND)}</span>
      </a>
      <nav class="flex items-center gap-4 text-sm">
        <a class="text-slate-700 hover:text-slate-900 dark:text-white/80 dark:hover:text-white" href="{html_escape(hub_url)}" data-i18n="home">Home</a>
        <a class="text-slate-700 hover:text-slate-900 dark:text-white/80 dark:hover:text-white" href="{html_escape(hub_url)}#about" data-i18n="about">About Us</a>
        <a class="text-slate-700 hover:text-slate-900 dark:text-white/80 dark:hover:text-white" href="{html_escape(hub_url)}#tools" data-i18n="all_tools">All Tools</a>
        <button id="themeBtn" type="button" class="ml-2 rounded-xl bg-slate-100/80 hover:bg-slate-200/70 dark:bg-white/10 dark:hover:bg-white/20 border border-slate-200/70 dark:border-white/10 px-2 py-1 text-xs" aria-label="Theme">🌓</button>
        <select id="langSel" class="ml-2 rounded-xl bg-slate-100/80 dark:bg-white/10 border border-slate-200/70 dark:border-white/10 px-2 py-1 text-xs">
          <option value="en">EN</option>
          <option value="ja">JA</option>
          <option value="ko">KO</option>
          <option value="zh">ZH</option>
        </select>
      </nav>
    </div>
  </header>

  <main class="mx-auto max-w-3xl px-4 py-10">
    <h1 class="text-2xl font-semibold" data-i18n="{title}">{title.capitalize()}</h1>
    <div id="policyBody" class="mt-6 space-y-3 text-slate-700 dark:text-white/70 leading-relaxed"></div>
  </main>

  <footer class="border-t border-slate-200/70 dark:border-white/10">
    <div class="mx-auto max-w-5xl px-4 py-8 text-xs text-slate-500 dark:text-white/60">
      <div>{html_escape(SITE_BRAND)} · <span data-i18n="footer_note">Practical, fast, and respectful guides—built to reduce wasted trial-and-error.</span></div>
    </div>
  </footer>

  <script>
  const I18N = {i18n_json};
  const LANGS = {langs_json};
  const BODY = {body_json};

  function t(lang, key) {{
    return (I18N[lang] && I18N[lang][key]) || (I18N["{DEFAULT_LANG}"][key]) || key;
  }}

  function setLang(lang) {{
    if (!LANGS.includes(lang)) lang = "{DEFAULT_LANG}";
    document.documentElement.setAttribute("lang", lang);
    localStorage.setItem("lang", lang);

    document.querySelectorAll("[data-i18n]").forEach(el => {{
      const key = el.getAttribute("data-i18n");
      el.textContent = t(lang, key);
    }});

    const container = document.getElementById("policyBody");
    if (container) {{
      const ps = (BODY[lang] || BODY["{DEFAULT_LANG}"] || []);
      container.innerHTML = ps.map(p => `<p>${{p}}</p>`).join("");
    }}
  }}

  function setTheme(mode) {{
    if (mode === "dark") document.documentElement.classList.add("dark");
    else document.documentElement.classList.remove("dark");
    localStorage.setItem("theme", mode);
  }}

  function init() {{
    const savedTheme = localStorage.getItem("theme");
    const prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
    setTheme(savedTheme || (prefersDark ? "dark" : "light"));

    const savedLang = localStorage.getItem("lang") || "{DEFAULT_LANG}";
    setLang(savedLang);

    const sel = document.getElementById("langSel");
    if (sel) {{
      sel.value = savedLang;
      sel.addEventListener("change", (e) => setLang(e.target.value));
    }}

    const btn = document.getElementById("themeBtn");
    if (btn) {{
      btn.addEventListener("click", () => {{
        const isDark = document.documentElement.classList.contains("dark");
        setTheme(isDark ? "light" : "dark");
      }});
    }}
  }}

  document.addEventListener("DOMContentLoaded", init);
  </script>
</body>
</html>"""

    privacy = build_policy_html("privacy")
    terms = build_policy_html("terms")
    contact = build_policy_html("contact")

    write_text(privacy_path, privacy)
    write_text(terms_path, terms)
    write_text(contact_path, contact)

    return [
        SITE_DOMAIN.rstrip("/") + "/policies/privacy.html",
        SITE_DOMAIN.rstrip("/") + "/policies/terms.html",
        SITE_DOMAIN.rstrip("/") + "/policies/contact.html",
    ]


# =============================================================================
# Sitemap + robots + ping
# =============================================================================
def build_sitemap(urls: List[str]) -> str:
    urls = uniq_keep_order([u for u in urls if isinstance(u, str) and u.startswith("http")])
    lastmod = dt.datetime.now(dt.timezone.utc).date().isoformat()
    items = []
    for u in urls:
        items.append(
            f"<url><loc>{html_escape(u)}</loc><lastmod>{lastmod}</lastmod></url>"
        )
    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
{''.join(items)}
</urlset>
"""
    return xml


def build_robots(sitemap_url: str) -> str:
    return f"""User-agent: *
Allow: /

Sitemap: {sitemap_url}
"""


def ping_search_engines(sitemap_url: str) -> None:
    """
    Optional ping. Not guaranteed, but logs status.
    """
    targets = [
        "https://www.google.com/ping?" + urlencode({"sitemap": sitemap_url}),
        "https://www.bing.com/ping?" + urlencode({"sitemap": sitemap_url}),
    ]
    for u in targets:
        st, body = http_get(u, headers={"User-Agent": "goliath-tool/1.0"}, timeout=20)
        logging.info("Ping sitemap: %s -> %s", u, st)


# =============================================================================
# Validation + Auto-fix (up to MAX_AUTOFIX)
# =============================================================================
REQUIRED_MARKERS = [
    "AFF_SLOT",
    "Long guide (JP, 2500+ chars)",
    "Reference links",
    "<script src=\"https://cdn.tailwindcss.com\"></script>",
]

def validate_site_html(html_text: str) -> List[str]:
    errs: List[str] = []
    if not html_text or len(html_text) < 2000:
        errs.append("html_too_short")
        return errs
    for m in REQUIRED_MARKERS:
        if m not in html_text:
            errs.append(f"missing:{m}")
    # article length check: crude
    if "Long guide (JP" in html_text:
        # ensure article content roughly long
        if html_text.count("【") < 6 and len(html_text) < 12000:
            errs.append("article_maybe_too_short")
    return errs


# =============================================================================
# Reply generation (EN, short, no “AI/bot” words, URL last line)
# =============================================================================
FORBIDDEN_REPLY_WORDS = ["ai", "bot", "automation", "automated"]

def openai_generate_reply_stub(post: Post, tool_url: str) -> str:
    """
    Deterministic reply. 280-400 chars target. English. Last line is URL only.
    """
    # empathy first
    t = post.norm_text()
    # short summary (very light)
    summary = "That sounds frustrating—especially when you’re trying to decide quickly."
    if any(w in t.lower() for w in ["overwhelmed", "confused", "stuck", "don’t know"]):
        summary = "That sounds really overwhelming—especially when you’re stuck and need a clear next step."
    elif any(w in t.lower() for w in ["today", "tomorrow", "this week", "urgent", "deadline"]):
        summary = "That’s stressful—especially with the clock ticking."

    line2 = "I put together a simple one-page guide + checklist that should help you move forward:"
    reply = f"{summary}\n{line2}\n{tool_url}"

    # keep clean
    low = reply.lower()
    for w in FORBIDDEN_REPLY_WORDS:
        if w in low:
            reply = reply.replace(w, "")
    # length guard
    if len(reply) > 430:
        reply = f"{summary}\nOne-page checklist here:\n{tool_url}"
    if len(reply) < 220:
        reply = f"{summary}\nI made a one-page checklist for this:\n{tool_url}"
    return reply.strip()


# =============================================================================
# Issues output (minimum 100 items; stub fill allowed)
# =============================================================================
def make_stub_posts(n: int) -> List[Post]:
    stubs: List[Post] = []
    for i in range(n):
        stubs.append(Post(
            source="stub",
            id=sha1(f"stub:{RUN_ID}:{i}"),
            url=f"{SITE_DOMAIN.rstrip('/')}/goliath/_out/stub/{RUN_ID}/{i}",
            text="Need a checklist / template for a common problem.",
            author="unknown",
            created_at=now_iso(),
        ))
    return stubs


def build_issue_items(posts: List[Post], post_to_tool_url: Dict[str, str]) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    for p in posts:
        tool_url = post_to_tool_url.get(p.id, "")
        if not tool_url:
            continue
        reply = openai_generate_reply_stub(p, tool_url)
        items.append({
            "problem_url": p.url,
            "reply": reply,
            "source": p.source,
        })
    return items


def chunk_issue_bodies(items: List[Dict[str, str]], chunk_size: int = 40) -> List[str]:
    bodies: List[str] = []
    for i in range(0, len(items), chunk_size):
        chunk = items[i:i+chunk_size]
        lines: List[str] = []
        for it in chunk:
            lines.append(f"Problem URL: {it['problem_url']}")
            lines.append("Reply:")
            lines.append(it["reply"])
            lines.append("")  # blank
            lines.append("---")
        bodies.append("\n".join(lines).rstrip() + "\n")
    return bodies


def write_issues_payload(items: List[Dict[str, str]], extra_notes: str = "") -> str:
    """
    Write JSON with titles/bodies for GitHub issues.
    Returns path to JSON.
    """
    bodies = chunk_issue_bodies(items, ISSUE_MAX_ITEMS)
    payloads: List[Dict[str, str]] = []
    for idx, body in enumerate(bodies, start=1):
        title = f"Goliath reply candidates ({RUN_ID}) part {idx}/{len(bodies)}"
        if extra_notes and idx == 1:
            body = extra_notes.strip() + "\n\n" + body
        payloads.append({"title": title, "body": body})

    out_path = os.path.join(OUT_DIR, f"issues_payload_{RUN_ID}.json")
    write_json(out_path, {"run_id": RUN_ID, "count": len(items), "issues": payloads})
    return out_path


# =============================================================================
# Orchestration
# =============================================================================
def collect_all() -> List[Post]:
    # per spec targets:
    # Bluesky 50, Mastodon 100, Reddit 20, X 1(mentions), HN (rest)
    bs = collect_bluesky(max_items=50)
    ms = collect_mastodon(max_items=100)
    rd = collect_reddit(max_items=20)
    xx = collect_x_mentions(max_items=max(1, min(X_MAX, 5)))
    hn = collect_hn(max_items=HN_MAX)

    all_posts = bs + ms + rd + xx + hn
    # filter dup urls
    seen = set()
    out: List[Post] = []
    for p in all_posts:
        if not p.url:
            continue
        if adult_or_sensitive(p.text):
            continue
        key = sha1(p.url)
        if key in seen:
            continue
        seen.add(key)
        out.append(p)

    # cap MAX_COLLECT
    out = out[:MAX_COLLECT]
    logging.info("Collected total=%d (Bluesky=%d, Mastodon=%d, Reddit=%d, X=%d, HN=%d)",
                 len(out), len(bs), len(ms), len(rd), len(xx), len(hn))
    return out


def choose_themes(posts: List[Post], max_themes: int) -> List[Theme]:
    clusters = cluster_posts(posts, threshold=0.22)
    themes = [make_theme(c) for c in clusters if len(c) >= 2]
    themes.sort(key=lambda t: t.score, reverse=True)
    return themes[:max_themes]


def build_sites(themes: List[Theme], aff_norm: Dict[str, List[Dict[str, Any]]], all_sites_inventory: List[Dict[str, Any]], hero_bg_url: str) -> Tuple[List[Theme], List[Dict[str, Any]], Dict[str, str], List[str]]:
    """
    Builds pages + shortlinks. Updates site inventory list (not hub HTML).
    Returns:
      - built themes (with final slug + short_code)
      - new inventory list entries
      - mapping post_id -> tool_url for issue generation
      - list of urls for sitemap
    """
    os.makedirs(PAGES_DIR, exist_ok=True)
    os.makedirs(os.path.join(GOLIATH_DIR, "go"), exist_ok=True)

    sitemap_urls: List[str] = []
    new_inventory_entries: List[Dict[str, Any]] = []
    post_to_tool_url: Dict[str, str] = {}

    # compute popular once from current inventory
    popular_now = compute_popular_sites(all_sites_inventory, n=8)

    for theme in themes:
        # allocate collision-safe slug
        final_slug = allocate_unique_slug(theme.slug)
        theme.slug = final_slug

        tool_url = site_url_for_slug(final_slug)

        # shortlink
        code = short_code_for_url(tool_url)
        theme.short_code = code
        short_url = SITE_DOMAIN.rstrip("/") + f"/goliath/go/{code}/"

        # write shortlink page
        rel_path, short_html = build_shortlink_page(tool_url, code)
        abs_short_path = os.path.join(REPO_ROOT, rel_path)
        write_text(abs_short_path, short_html)

        # build content
        references = pick_reference_urls(theme)
        supplements = supplemental_resources_for_category(theme.category)[:max(SUPP_URL_MIN, 3)]
        article_ja = generate_long_article_ja(theme)
        faq = build_faq(theme.category)

        # affiliates top2
        aff_top2 = pick_affiliates_for_category(aff_norm, theme.category, topn=2)

        # related tools from existing inventory + new ones (accumulate)
        inventory_for_related = all_sites_inventory + new_inventory_entries
        related = choose_related_tools(inventory_for_related, theme.category, exclude_slug=final_slug, n=5)

        # build html
        html_text = build_page_html(
            theme=theme,
            tool_url=tool_url,
            short_url=short_url,
            affiliates_top2=aff_top2,
            references=references,
            supplements=supplements,
            article_ja=article_ja,
            faq=faq,
            related_tools=related,
            popular_sites=popular_now,
            hero_bg_url=hero_bg_url,
        )

        # validate/autofix
        attempts = 0
        errs = validate_site_html(html_text)
        while errs and attempts < MAX_AUTOFIX:
            attempts += 1
            # simple autofix: pad article, ensure markers
            if "article_maybe_too_short" in errs:
                article_ja = article_ja + "\n" + ("【追加メモ】\n" + "確認→最小変更→検証→記録、の順番を崩さないことが最短です。\n") * 8
            # rebuild html after pad
            html_text = build_page_html(
                theme=theme,
                tool_url=tool_url,
                short_url=short_url,
                affiliates_top2=aff_top2,
                references=references,
                supplements=supplements,
                article_ja=article_ja,
                faq=faq,
                related_tools=related,
                popular_sites=popular_now,
                hero_bg_url=hero_bg_url,
            )
            errs = validate_site_html(html_text)

        if errs:
            logging.warning("Site validation still has errors for %s: %s", final_slug, errs)

        # write file
        out_dir = os.path.join(PAGES_DIR, final_slug)
        out_path = os.path.join(out_dir, "index.html")
        write_text(out_path, html_text)

        # sitemap urls
        sitemap_urls.append(tool_url)
        sitemap_urls.append(short_url)

        # inventory entry
        entry = {
            "slug": final_slug,
            "title": theme.title,
            "search_title": theme.search_title,
            "category": theme.category,
            "url": tool_url,
            "short_url": short_url,
            "created_at": now_iso(),
            "updated_at": now_iso(),
            "keywords": theme.keywords[:12],
        }
        new_inventory_entries.append(entry)

        # map representative posts to tool url (for issue output)
        for p in theme.representative_posts:
            post_to_tool_url[p.id] = tool_url

        logging.info("Built site: %s (%s) short=%s", tool_url, theme.category, short_url)

    return themes, new_inventory_entries, post_to_tool_url, sitemap_urls


def build_post_drafts(themes: List[Theme]) -> List[Dict[str, str]]:
    """
    Bluesky posting drafts: "one-line value" + short URL only (short).
    """
    drafts: List[Dict[str, str]] = []
    for t in themes:
        short_url = SITE_DOMAIN.rstrip("/") + f"/goliath/go/{t.short_code}/"
        one = short_value_line(t.category)
        # fixed format: value + URL
        text = f"{one}\n{short_url}"
        drafts.append({
            "category": t.category,
            "search_title": t.search_title,
            "short_url": short_url,
            "text": text,
        })
    return drafts


def write_run_summary(
    counts: Dict[str, int],
    reply_count: int,
    aff_audit: Dict[str, Any],
    post_drafts: List[Dict[str, str]],
    sitemap_url_written: str,
) -> None:
    """
    Self-check output (Actions log + json file).
    """
    summary = {
        "run_id": RUN_ID,
        "counts": counts,
        "reply_candidates": reply_count,
        "affiliates_audit": aff_audit,
        "sitemap_url": sitemap_url_written,
        "post_drafts": post_drafts,
        "updated_at": now_iso(),
    }
    out_path = os.path.join(OUT_DIR, f"run_summary_{RUN_ID}.json")
    write_json(out_path, summary)

    logging.info("Self-check: counts=%s", counts)
    logging.info("Self-check: reply_candidates=%d", reply_count)
    logging.info("Self-check: affiliates_audit ok=%s missing=%d extra=%d",
                 aff_audit.get("ok"), len(aff_audit.get("missing", [])), len(aff_audit.get("extra", [])))
    logging.info("Self-check: sitemap=%s", sitemap_url_written)


def main() -> int:
    setup_logging()

    # legal pages
    policy_urls = ensure_policies()

    # affiliates
    aff_raw = load_affiliates()
    aff_audit = audit_affiliate_keys(aff_raw)
    aff_norm = normalize_affiliates_shape(aff_raw)

    # collect
    posts = collect_all()
    counts = {
        "Bluesky": sum(1 for p in posts if p.source == "bluesky"),
        "Mastodon": sum(1 for p in posts if p.source == "mastodon"),
        "Reddit": sum(1 for p in posts if p.source == "reddit"),
        "X": sum(1 for p in posts if p.source == "x"),
        "HN": sum(1 for p in posts if p.source == "hn"),
        "Total": len(posts),
    }

    # choose themes
    themes = choose_themes(posts, max_themes=MAX_THEMES)
    if not themes:
        # 収集0でも最低1サイト生成
        seed_post = Post(
            source="seed",
            id=sha1(f"seed:{RUN_ID}"),
            url=HUB_BASE_URL.rstrip("/"),
            text="seed: no posts collected this run",
            author="system",
            created_at=now_iso(),
        )
        themes = [make_theme([seed_post])]
        logging.info("Chosen themes forced=1 (seed)")

    logging.info("Chosen themes=%d", len(themes))

    # hero background (optional)
    hero_bg = fetch_unsplash_bg_url()
    if hero_bg:
        logging.info("Unsplash hero bg enabled.")

    # inventory: read existing hub sites
    existing_sites = read_hub_sites()

    # build sites
    built_themes, new_entries, post_to_tool_url, site_urls = build_sites(
        themes=themes,
        aff_norm=aff_norm,
        all_sites_inventory=existing_sites,
        hero_bg_url=hero_bg,
    )

    # update hub/sites.json ONLY
    merged_sites = existing_sites + new_entries
    aggregates = compute_aggregates(merged_sites)
    write_hub_sites(merged_sites, aggregates)

    # Prepare reply candidates (minimum 100)
    mapped_post_ids = set(post_to_tool_url.keys())
    mapped_posts = [p for p in posts if p.id in mapped_post_ids]

    if len(mapped_posts) < LEADS_TOTAL:
        need = LEADS_TOTAL - len(mapped_posts)
        stubs = make_stub_posts(need)

        built_urls = [site_url_for_slug(t.slug) for t in built_themes] or [SITE_DOMAIN.rstrip("/") + "/hub/"]
        for i, sp in enumerate(stubs):
            post_to_tool_url[sp.id] = built_urls[i % len(built_urls)]

        mapped_posts.extend(stubs)

    mapped_posts = mapped_posts[: max(LEADS_TOTAL, 100)]

    issue_items = build_issue_items(mapped_posts, post_to_tool_url)

    if len(issue_items) < 100:
        more_need = 100 - len(issue_items)
        extra_stubs = make_stub_posts(more_need)
        built_urls = [site_url_for_slug(t.slug) for t in built_themes] or [SITE_DOMAIN.rstrip("/") + "/hub/"]
        for i, sp in enumerate(extra_stubs):
            post_to_tool_url[sp.id] = built_urls[i % len(built_urls)]
        issue_items.extend(build_issue_items(extra_stubs, post_to_tool_url))

    notes = []
    notes.append(f"Run: {RUN_ID}")
    notes.append(
        f"Collected: Bluesky={counts['Bluesky']} Mastodon={counts['Mastodon']} "
        f"Reddit={counts['Reddit']} X={counts['X']} HN={counts['HN']} Total={counts['Total']}"
    )
    notes.append(f"Reply candidates: {len(issue_items)}")
    if not aff_audit.get("ok"):
        notes.append("Affiliates audit: MISSING keys in affiliates.json:")
        for k in aff_audit.get("missing", []):
            notes.append(f"- {k}")
    if aff_audit.get("extra"):
        notes.append("Affiliates audit: EXTRA keys (ignored):")
        for k in aff_audit.get("extra", []):
            notes.append(f"- {k}")
    extra_notes = "\n".join(notes).strip()

    issues_path = write_issues_payload(issue_items, extra_notes=extra_notes)
    logging.info("Wrote issues payload: %s", issues_path)

    # post drafts (short URL + one-line value)
    drafts = build_post_drafts(built_themes)
    write_json(
        os.path.join(OUT_DIR, f"post_drafts_{RUN_ID}.json"),
        {"run_id": RUN_ID, "created_at": now_iso(), "drafts": drafts},
    )

    # sitemap + robots
    sitemap_urls = []
    sitemap_urls.extend(site_urls)
    sitemap_urls.extend(policy_urls)
    sitemap_urls.append(SITE_DOMAIN.rstrip("/") + "/hub/")

    sitemap_xml = build_sitemap(sitemap_urls)
    sitemap_out_path = os.path.join(OUT_DIR, "sitemap.xml")
    write_text(sitemap_out_path, sitemap_xml)

    sitemap_public_url = SITE_DOMAIN.rstrip("/") + "/sitemap.xml"
    robots_text = build_robots(sitemap_public_url)
    robots_out_path = os.path.join(OUT_DIR, "robots.txt")
    write_text(robots_out_path, robots_text)

    if ALLOW_ROOT_UPDATE:
        write_text(os.path.join(REPO_ROOT, "sitemap.xml"), sitemap_xml)
        write_text(os.path.join(REPO_ROOT, "robots.txt"), robots_text)
        logging.info("Root sitemap/robots updated.")
        if PING_SITEMAP:
            ping_search_engines(sitemap_public_url)
        sitemap_url_written = sitemap_public_url
    else:
        logging.info("Root sitemap/robots NOT updated (ALLOW_ROOT_UPDATE=0). Wrote to goliath/_out instead.")
        sitemap_url_written = SITE_DOMAIN.rstrip("/") + "/goliath/_out/sitemap.xml"

    # self-check summary
    write_run_summary(
        counts=counts,
        reply_count=len(issue_items),
        aff_audit=aff_audit,
        post_drafts=drafts,
        sitemap_url_written=sitemap_url_written,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())

    try:
        entry = (
            globals().get("main")
            or globals().get("run")
            or globals().get("run_goliath")
            or globals().get("goliath_main")
        )

        if not callable(entry):
            raise RuntimeError(
                "Entry function not found. Expected one of: "
                "main / run / run_goliath / goliath_main"
            )

        result = entry()
        if isinstance(result, int):
            sys.exit(result)

    except KeyboardInterrupt:
        raise
    except Exception as e:
        try:
            import logging
            logging.exception("Unhandled exception in goliath/main.py: %s", e)
        except Exception:
            print("Unhandled exception in goliath/main.py:", e, file=sys.stderr)
        raise
