import os
import re
import json
import time
import random
import hashlib
import datetime
from typing import List, Dict, Any, Tuple, Optional

import requests
from openai import OpenAI

# Optional SNS libs (missing secretsなら黙ってスキップ)
try:
    from atproto import Client as BskyClient
except Exception:
    BskyClient = None

try:
    from mastodon import Mastodon
except Exception:
    Mastodon = None

# X (Twitter) - OAuth 1.0a
try:
    from requests_oauthlib import OAuth1
except Exception:
    OAuth1 = None


ROOT = "goliath"
PAGES_DIR = f"{ROOT}/pages"
DB_PATH = f"{ROOT}/db.json"
INDEX_PATH = f"{ROOT}/index.html"
SEED_SITES_PATH = f"{ROOT}/sites.seed.json"


def now_utc_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def slugify(s: str, max_len: int = 60) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s[:max_len] or "tool"


def ensure_dirs():
    os.makedirs(PAGES_DIR, exist_ok=True)


def read_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str, obj: Any):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def write_text(path: str, text: str):
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def extract_html_only(raw: str) -> str:
    # 余計な挨拶/markdown/``` を排除して <!DOCTYPE html>..</html> のみ切り出す
    m = re.search(r"(<!DOCTYPE\s+html.*?</html\s*>)", raw, flags=re.IGNORECASE | re.DOTALL)
    if m:
        return m.group(1).strip()
    # それでも無理なら、コードフェンス除去して返す
    raw = re.sub(r"^```[a-zA-Z]*\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw.strip())
    return raw.strip()


def stable_id(*parts: str) -> str:
    h = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return h[:16]


# ---------------------------
# Collector / Cluster
# ---------------------------

def hn_collect(limit: int = 20) -> List[Dict[str, str]]:
    """
    HNはキー不要で取れる。Algolia検索APIを使って「悩みっぽい」クエリで引っ張る。
    失敗したら空配列。
    """
    queries = ["how to", "help", "issue", "problem", "can't", "error"]
    q = random.choice(queries)
    url = "https://hn.algolia.com/api/v1/search_by_date"
    params = {"query": q, "tags": "story", "hitsPerPage": max(10, limit)}
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        out = []
        for hit in data.get("hits", [])[:limit]:
            title = hit.get("title") or ""
            u = hit.get("url") or f"https://news.ycombinator.com/item?id={hit.get('objectID')}"
            if title:
                out.append({"text": title, "url": u})
        return out[:limit]
    except Exception:
        return []


def reddit_collect(limit: int = 20) -> List[Dict[str, str]]:
    """
    Redditは未承認/認証が面倒なケースがあるので、公開検索が通る時だけ使う。
    通らなかったら空配列。
    """
    queries = ["help", "how do i", "tool", "calculator", "convert", "template"]
    q = random.choice(queries)
    url = "https://www.reddit.com/r/all/search.json"
    params = {"q": q, "sort": "new", "t": "year", "limit": min(100, limit * 3)}
    headers = {"User-Agent": "goliath-auto-tool/0.1"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
        out = []
        for child in (data.get("data", {}).get("children", []) or []):
            d = child.get("data", {}) or {}
            title = d.get("title") or ""
            permalink = d.get("permalink") or ""
            u = f"https://www.reddit.com{permalink}" if permalink else "https://www.reddit.com/"
            if title:
                out.append({"text": title, "url": u})
            if len(out) >= limit:
                break
        return out[:limit]
    except Exception:
        return []


def collector() -> List[Dict[str, str]]:
    """
    優先順：Reddit(通るなら) -> HN -> スタブ
    """
    items: List[Dict[str, str]] = []
    r = reddit_collect(20)
    if r:
        items.extend(r)
    h = hn_collect(20)
    if h:
        items.extend(h)

    # 20件に満たない場合はスタブで埋める
    if len(items) < 20:
        samples = [
            ("need a simple calculator to compare subscription plans with hidden fees", "https://news.ycombinator.com/"),
            ("time zone converter with meeting overlap and daylight saving awareness", "https://news.ycombinator.com/"),
            ("convert a messy checklist into a clean template instantly", "https://www.reddit.com/"),
            ("estimate take-home pay after taxes for a country", "https://news.ycombinator.com/"),
            ("compare two options with a scoring rubric", "https://www.reddit.com/"),
        ]
        random.shuffle(samples)
        for t, u in samples:
            items.append({"text": t, "url": u})
            if len(items) >= 20:
                break

    random.shuffle(items)
    return items[:20]


def cluster_20(items: List[Dict[str, str]]) -> Dict[str, Any]:
    items = items[:20]
    theme = items[0]["text"] if items else "useful calculator tool"
    urls = [x["url"] for x in items]
    texts = [x["text"] for x in items]
    return {"theme": theme, "items": items, "urls": urls, "texts": texts}


# ---------------------------
# Related Sites Logic
# ---------------------------

def load_seed_sites() -> List[Dict[str, Any]]:
    """
    既存資産（あなたの過去サイト群）をここに列挙しておくと、
    新規生成ページのtagsに近いものを「関連サイト」として自動で出す。
    """
    if os.path.exists(SEED_SITES_PATH):
        return read_json(SEED_SITES_PATH, [])
    return []


def jaccard(a: List[str], b: List[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def pick_related(current_tags: List[str], all_entries: List[Dict[str, Any]], seed_sites: List[Dict[str, Any]], k: int = 8) -> List[Dict[str, str]]:
    candidates: List[Tuple[float, Dict[str, str]]] = []

    # goliath内の過去ページ
    for e in all_entries:
        tags = e.get("tags", [])
        score = jaccard(current_tags, tags)
        if score <= 0:
            continue
        candidates.append((score, {"title": e.get("title", "page"), "url": e.get("public_url", "")}))

    # seed（過去サイト・Hub等）
    for s in seed_sites:
        tags = s.get("tags", [])
        score = jaccard(current_tags, tags)
        if score <= 0:
            continue
        candidates.append((score, {"title": s.get("title", "site"), "url": s.get("url", "")}))

    candidates.sort(key=lambda x: x[0], reverse=True)

    seen = set()
    related = []
    for _, item in candidates:
        u = item.get("url", "")
        if not u or u in seen:
            continue
        seen.add(u)
        related.append(item)
        if len(related) >= k:
            break
    return related


# ---------------------------
# Builder / Validator / Auto-fix
# ---------------------------

def build_prompt(theme: str, canonical_url: str) -> str:
    return f"""
You are generating a production-grade single-file HTML tool site.

STRICT OUTPUT RULE:
- Output ONLY raw HTML that starts with <!DOCTYPE html> and ends with </html>.
- No markdown, no backticks, no explanations.

[Goal]
Create a modern SaaS-style tool page to solve: "{theme}"

[Design]
- Use Tailwind CSS via CDN
- Clean SaaS UI: hero section + centered tool card + sections
- Dark/Light mode toggle (class switch)

[Tool]
- Implement an interactive JS mini-tool relevant to the theme (pure frontend).
- Must work without server.
- Do NOT use external JS libraries (Tailwind CDN is ok).

[Content]
- Include a Japanese long-form article >= 2500 Japanese characters.
- Use clear structure with H2/H3 headings, checklist, pitfalls, FAQ(>=5).

[References]
- Add "References" section with 8-12 reputable external links (official docs / well-known sites).
- Do NOT fabricate quotes.

[Multi-language]
- Provide language switcher for JA/EN/FR/DE.
- Translate: hero, tool labels, and footer policy pages.
- Article is JA primary; also include short EN/FR/DE summary blocks.

[Compliance / Footer]
- Create in-page sections (anchors) for:
  - Privacy Policy (cookie/ads explanation)
  - Terms of Service
  - Disclaimer
  - About / Operator info
  - Contact
- Footer must include links to those anchors.

[Related Sites]
- Include a "Related sites" section near bottom.
- The page MUST define:
  window.__RELATED__ = [];
  and render the list on load.
- If empty, hide the section.

[Hero Image]
- The page MUST define:
  window.__HERO_IMAGE__ = "";
  and if non-empty, show it in the hero section. If empty, hide the image.

[SEO]
- Include title/meta description/canonical.
- Canonical MUST be exactly:
  {canonical_url}

Return ONLY the final HTML.
""".strip()


def openai_generate_html(client: OpenAI, prompt: str, model: str) -> str:
    res = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = (res.choices[0].message.content or "")
    return extract_html_only(raw)


def validate_html(html: str) -> Tuple[bool, str]:
    low = html.lower()
    if "<!doctype html" not in low:
        return False, "missing doctype"
    if "</html>" not in low:
        return False, "missing </html>"
    if "tailwindcss" not in low:
        return False, "tailwind not found"
    if "__related__" not in low:
        return False, "window.__RELATED__ not found"
    if "__hero_image__" not in low:
        return False, "window.__HERO_IMAGE__ not found"
    must = ["privacy", "terms", "disclaimer", "about", "contact"]
    missing = [m for m in must if m not in low]
    if missing:
        return False, f"missing policy sections: {missing}"
    return True, "ok"


def prompt_for_fix(error: str, html: str) -> str:
    return f"""
You must return ONLY a unified diff patch for a single file named index.html.

Rules:
- Output ONLY the diff. No markdown. No explanations.
- The patch MUST fix this validation error: {error}
- Do not remove required features:
  Tailwind CDN, SaaS layout, dark/light toggle, language switcher,
  footer policy sections, window.__RELATED__ rendering, window.__HERO_IMAGE__ rendering.

Here is current index.html content:
{html}
""".strip()


def apply_unified_diff_to_text(original: str, diff_text: str) -> Optional[str]:
    if not diff_text.startswith("---"):
        return None

    lines = diff_text.splitlines()
    orig_lines = original.splitlines()
    result = []
    oidx = 0

    i = 0
    try:
        while i < len(lines):
            if not lines[i].startswith("@@"):
                i += 1
                continue

            header = lines[i]
            m = re.match(r"@@ -(\d+),?(\d*) \+(\d+),?(\d*) @@", header)
            if not m:
                return None
            old_start = int(m.group(1)) - 1

            while oidx < old_start and oidx < len(orig_lines):
                result.append(orig_lines[oidx])
                oidx += 1

            i += 1
            while i < len(lines) and not lines[i].startswith("@@"):
                l = lines[i]
                if l.startswith(" "):
                    result.append(l[1:])
                    oidx += 1
                elif l.startswith("-"):
                    oidx += 1
                elif l.startswith("+"):
                    result.append(l[1:])
                else:
                    return None
                i += 1

        while oidx < len(orig_lines):
            result.append(orig_lines[oidx])
            oidx += 1

        return "\n".join(result) + ("\n" if original.endswith("\n") else "")
    except Exception:
        return None


def infer_tags_simple(theme: str) -> List[str]:
    t = theme.lower()
    tags: List[str] = []
    rules = {
        "convert": "convert",
        "converter": "convert",
        "calculator": "calculator",
        "compare": "compare",
        "tax": "finance",
        "salary": "finance",
        "time": "time",
        "timezone": "time",
        "subscription": "pricing",
        "plan": "pricing",
        "checklist": "productivity",
        "template": "productivity",
        "pdf": "file",
        "mp4": "file",
        "compress": "file",
    }
    for k, v in rules.items():
        if k in t and v not in tags:
            tags.append(v)
    if not tags:
        tags = ["tools"]
    return tags[:6]


# ---------------------------
# Public base url / canonical
# ---------------------------

def normalize_base_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    if not re.match(r"^https?://", u):
        u = "https://" + u
    if not u.endswith("/"):
        u += "/"
    return u


def get_public_base() -> str:
    # 優先: PUBLIC_BASE_URL（Vercel / custom domain 用）
    base = normalize_base_url(os.getenv("PUBLIC_BASE_URL", ""))
    if base:
        return base

    # fallback: GitHub Pages の project pages 推定
    repo = os.getenv("GITHUB_REPOSITORY", "Mikann20041029/goliath-auto-tool")
    owner, name = repo.split("/", 1) if "/" in repo else ("Mikann20041029", "goliath-auto-tool")
    return f"https://{owner.lower()}.github.io/{name}/"


# ---------------------------
# Unsplash hero image (optional)
# ---------------------------

def fetch_unsplash_image_url(query: str) -> str:
    key = os.getenv("UNSPLASH_ACCESS_KEY", "")
    if not key:
        return ""
    try:
        url = "https://api.unsplash.com/search/photos"
        params = {"query": query, "per_page": 1, "orientation": "landscape"}
        headers = {"Authorization": f"Client-ID {key}"}
        r = requests.get(url, params=params, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
        results = data.get("results", [])
        if not results:
            return ""
        return (results[0].get("urls", {}) or {}).get("regular", "") or ""
    except Exception:
        return ""


def inject_json_assignment(html: str, var_name: str, payload: Any) -> str:
    # window.__XXX__ = ...; を置換。なければ head の末尾へ追加する。
    js = json.dumps(payload, ensure_ascii=False)
    pattern = rf"window\.{re.escape(var_name)}\s*=\s*.*?;"
    if re.search(pattern, html, flags=re.DOTALL):
        return re.sub(pattern, f"window.{var_name} = {js};", html, flags=re.DOTALL)

    # headがあるなら挿入、なければbody末尾
    inject = f"<script>window.{var_name} = {js};</script>"
    if "</head>" in html.lower():
        return re.sub(r"</head>", inject + "\n</head>", html, flags=re.IGNORECASE)
    return html + "\n" + inject + "\n"


# ---------------------------
# Publishing / Index / Notify / SNS
# ---------------------------

def update_db_and_index(entry: Dict[str, Any], all_entries: List[Dict[str, Any]]):
    all_entries.insert(0, entry)
    write_json(DB_PATH, all_entries)

    rows = []
    for e in all_entries[:50]:
        rows.append(
            f"""
<a class="block p-4 rounded-xl border border-slate-200 dark:border-slate-800 hover:bg-slate-50 dark:hover:bg-slate-900 transition"
   href="{e['path']}/">
  <div class="font-semibold">{e['title']}</div>
  <div class="text-sm opacity-70">{e['created_at']} • {", ".join(e.get("tags", []))}</div>
</a>
""".strip()
        )

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Goliath Tools</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="min-h-screen bg-white text-slate-900 dark:bg-slate-950 dark:text-slate-50">
  <div class="max-w-4xl mx-auto p-6">
    <div class="flex items-center justify-between gap-4">
      <div>
        <div class="text-2xl font-bold">Goliath Tools</div>
        <div class="opacity-70">Auto-generated tools + long-form guides</div>
      </div>
      <button id="themeBtn" class="px-3 py-2 rounded-lg border border-slate-200 dark:border-slate-800">Dark/Light</button>
    </div>

    <div class="mt-6 grid gap-3">
      {"".join(rows)}
    </div>

    <div class="mt-10 text-xs opacity-60">
      <a class="underline" href="./pages/">All pages</a>
    </div>
  </div>

<script>
  const root = document.documentElement;
  const k="goliath_theme";
  const saved = localStorage.getItem(k);
  if(saved==="dark") root.classList.add("dark");
  document.getElementById("themeBtn").onclick=()=>{{
    root.classList.toggle("dark");
    localStorage.setItem(k, root.classList.contains("dark") ? "dark" : "light");
  }};
</script>
</body>
</html>
"""
    write_text(INDEX_PATH, html)


def create_github_issue(title: str, body: str):
    pat = os.getenv("GH_PAT", "")
    repo = os.getenv("GITHUB_REPOSITORY", "")
    if not pat or not repo:
        return
    url = f"https://api.github.com/repos/{repo}/issues"
    headers = {"Authorization": f"token {pat}", "Accept": "application/vnd.github+json"}
    payload = {"title": title, "body": body}
    try:
        requests.post(url, headers=headers, json=payload, timeout=20)
    except Exception:
        pass


def post_bluesky(text: str):
    h = os.getenv("BSKY_HANDLE", "")
    p = os.getenv("BSKY_PASSWORD", "")
    if not h or not p or BskyClient is None:
        return
    try:
        c = BskyClient()
        c.login(h, p)
        c.send_post(text=text)
    except Exception:
        pass


def post_mastodon(text: str):
    tok = os.getenv("MASTODON_ACCESS_TOKEN", "")
    base = os.getenv("MASTODON_API_BASE", "")
    if not tok or not base or Mastodon is None:
        return
    try:
        m = Mastodon(access_token=tok, api_base_url=base)
        m.status_post(text)
    except Exception:
        pass


def post_x(text: str):
    """
    X API v2: POST https://api.x.com/2/tweets
    必要Secrets:
      X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_SECRET
    """
    if OAuth1 is None:
        return
    api_key = os.getenv("X_API_KEY", "")
    api_secret = os.getenv("X_API_SECRET", "")
    acc_token = os.getenv("X_ACCESS_TOKEN", "")
    acc_secret = os.getenv("X_ACCESS_SECRET", "")
    if not (api_key and api_secret and acc_token and acc_secret):
        return

    try:
        auth = OAuth1(api_key, api_secret, acc_token, acc_secret)
        url = "https://api.x.com/2/tweets"
        r = requests.post(url, auth=auth, json={"text": text}, timeout=20)
        # 失敗しても落とさない（宣伝はオマケ）
        _ = r.status_code
    except Exception:
        pass


def main():
    ensure_dirs()

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        create_github_issue(
            title="[Goliath] Missing OPENAI_API_KEY",
            body="OPENAI_API_KEY is not set in Actions secrets.",
        )
        return

    model = os.getenv("OPENAI_MODEL", "gpt-4o")
    client = OpenAI(api_key=api_key)

    # 1) Collector -> Cluster
    items = collector()
    cluster = cluster_20(items)
    theme = cluster["theme"]

    # 2) Identify output path (no overwrite)
    created_at = now_utc_iso()
    tags = infer_tags_simple(theme)
    slug = slugify(theme)
    folder = f"{int(time.time())}-{slug}"
    page_dir = f"{PAGES_DIR}/{folder}"
    os.makedirs(page_dir, exist_ok=True)

    # 3) canonical + public url
    base = get_public_base()
    public_url = f"{base}{ROOT}/pages/{folder}/"
    canonical = public_url.rstrip("/")

    # 4) Builder with Auto-fix loop
    prompt = build_prompt(theme, canonical)
    html = openai_generate_html(client, prompt, model=model)

    ok, msg = validate_html(html)
    attempts = 0
    while not ok and attempts < 5:
        attempts += 1
        fix_prompt = prompt_for_fix(msg, html)
        diff = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": fix_prompt}],
        ).choices[0].message.content or ""

        patched = apply_unified_diff_to_text(html, diff.strip())
        if patched is None:
            # diffが壊れてる場合は再生成
            regen = build_prompt(theme, canonical) + f"\n\nFix validation error: {msg}\nReturn ONLY corrected HTML.\n"
            html = openai_generate_html(client, regen, model=model)
        else:
            html = patched

        ok, msg = validate_html(html)

    if not ok:
        create_github_issue(
            title=f"[Goliath] Build failed after 5 fixes: {slug}",
            body=f"- theme: {theme}\n- error: {msg}\n- created_at: {created_at}\n",
        )
        return

    # 5) Related sites selection (過去ページ + seed)
    all_entries = read_json(DB_PATH, [])
    seed_sites = load_seed_sites()
    related = pick_related(tags, all_entries, seed_sites, k=8)

    # 6) Optional hero image (Unsplash)
    hero_url = fetch_unsplash_image_url(" ".join(tags) or "tool")

    # 7) Inject related + hero
    html = inject_json_assignment(html, "__RELATED__", related)
    html = inject_json_assignment(html, "__HERO_IMAGE__", hero_url)

    # 8) Save page
    page_path = f"{page_dir}/index.html"
    write_text(page_path, html)

    # 9) Update DB + index
    entry = {
        "id": stable_id(created_at, slug),
        "title": theme[:80],
        "created_at": created_at,
        "path": f"./pages/{folder}",
        "public_url": public_url,
        "tags": tags,
        "source_urls": cluster.get("urls", [])[:20],
        "related": related,
        "hero_image": hero_url,
    }
    update_db_and_index(entry, all_entries)

    # 10) Notify
    create_github_issue(
        title=f"[Goliath] New tool published: {slug}",
        body=(
            f"- theme: {theme}\n"
            f"- url: {public_url}\n"
            f"- tags: {', '.join(tags)}\n"
            f"- related_count: {len(related)}\n"
            f"- created_at: {created_at}\n"
        ),
    )

    # 11) SNS (optional)
    post_text = f"New tool: {theme}\n{public_url}"
    post_x(post_text)
    post_bluesky(post_text)
    post_mastodon(post_text)


if __name__ == "__main__":
    main()

