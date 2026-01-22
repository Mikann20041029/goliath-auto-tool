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
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def write_text(path: str, text: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def extract_html_only(raw: str) -> str:
    # 余計な挨拶/markdown/``` を排除して <!DOCTYPE html>..</html> のみ切り出す
    m = re.search(r"(<!DOCTYPE\s+html.*?</html\s*>)", raw, flags=re.IGNORECASE | re.DOTALL)
    if m:
        return m.group(1).strip()
    # それでも無理なら、とりあえずコードフェンス除去して返す
    raw = re.sub(r"^```[a-zA-Z]*\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw.strip())
    return raw.strip()


def stable_id(*parts: str) -> str:
    h = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return h[:16]


# ---------------------------
# Collector / Cluster
# ---------------------------

def collector_stub() -> List[Dict[str, str]]:
    """
    Reddit/HN APIがまだでも動くように、今はスタブ。
    返す形式は:
      [{"text": "...problem...", "url": "https://..."}, ...]
    """
    samples = [
        ("need a simple calculator to compare subscription plans with hidden fees", "https://news.ycombinator.com/"),
        ("how to convert a messy checklist into a clean template instantly", "https://www.reddit.com/"),
        ("time zone converter with meeting overlap and daylight saving awareness", "https://www.reddit.com/"),
        ("estimate freelance take-home pay after taxes for a specific country", "https://news.ycombinator.com/"),
        ("compare two products with pros/cons and scoring without tracking", "https://www.reddit.com/"),
    ]
    random.shuffle(samples)
    out = [{"text": t, "url": u} for t, u in samples[:20]]
    return out


def cluster_20(items: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    今は「20件束ねる」最小実装。
    将来はembedding等で類似クラスタ化に差し替え。
    """
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
    既存資産（hubや既存サイト）を「関連サイト候補」として入れておける。
    形式:
      [{"title":"Hub","url":"https://mikann20041029.github.io/hub/","tags":["hub","tools"]}, ...]
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
        candidates.append((score, {"title": e["title"], "url": e["public_url"]}))

    # 既存の外部/既存サイト
    for s in seed_sites:
        tags = s.get("tags", [])
        score = jaccard(current_tags, tags)
        if score <= 0:
            continue
        candidates.append((score, {"title": s["title"], "url": s["url"]}))

    candidates.sort(key=lambda x: x[0], reverse=True)

    # 重複URLを落として上位k
    seen = set()
    related = []
    for _, item in candidates:
        if item["url"] in seen:
            continue
        seen.add(item["url"])
        related.append(item)
        if len(related) >= k:
            break
    return related


# ---------------------------
# Builder / Validator / Auto-fix
# ---------------------------

def build_prompt(theme: str, cluster: Dict[str, Any], base_url: str) -> str:
    # 重要: 余計な文章禁止、HTMLのみ
    # 重要: フッターに規約系リンク、言語切替、関連サイト欄（プレースホルダ）
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
- Dark/Light mode toggle (CSS class switch)

[Content]
- Include a Japanese long-form article >= 2500 Japanese characters (not words).
- Use clear structure with H2/H3 headings, checklist, pitfalls, FAQ(>=5).
- Add "References" section with 8-12 reputable external links (official docs / well-known sites). Do NOT fabricate exact quotes.

[Tool]
- Implement an interactive JS mini-tool relevant to the theme (static, no server).
- Must work offline except CDN.

[Multi-language]
- Provide language switcher for JA/EN/FR/DE.
- At minimum translate: hero, tool labels, and footer pages (policy/about/contact/disclaimer/terms).
- Article can be JA primary; provide short EN/FR/DE summary sections.

[Compliance / Footer]
- Auto-generate pages/sections for:
  - Privacy Policy (cookie/ads explanation)
  - Terms of Service
  - Disclaimer
  - About / Operator info
  - Contact
- These must be accessible via footer links using in-page anchors.

[Related Sites]
- Include a "Related sites" section near bottom as a list:
  - It must be filled from a JSON embedded in the page like: window.__RELATED__ = [...]
  - Render it into the list on load.
  - If empty, hide the section.

[SEO]
- Include title/meta description/canonical.
- Canonical must be: {base_url}

Return ONLY the final HTML.
""".strip()


def openai_generate_html(client: OpenAI, prompt: str) -> str:
    res = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
    )
    raw = res.choices[0].message.content or ""
    return extract_html_only(raw)


def validate_html(html: str) -> Tuple[bool, str]:
    if "<!DOCTYPE html" not in html.lower():
        return False, "missing doctype"
    if "</html>" not in html.lower():
        return False, "missing </html>"
    # Tailwind CDNがないと「SaaS外装」条件を満たしにくい
    if "tailwind" not in html.lower():
        return False, "tailwind not found"
    # 関連サイトのレンダリングの最低限（window.__RELATED__）
    if "__RELATED__" not in html:
        return False, "related-sites placeholder window.__RELATED__ not found"
    # 規約系アンカーの最低限
    must = ["privacy", "terms", "disclaimer", "about", "contact"]
    missing = [m for m in must if m not in html.lower()]
    if missing:
        return False, f"missing policy sections: {missing}"
    return True, "ok"


def prompt_for_fix(theme: str, error: str, html: str) -> str:
    return f"""
You must return ONLY a unified diff patch for a single file named index.html.

Rules:
- Output ONLY the diff. No markdown. No explanations.
- The patch MUST fix this validation error: {error}
- Do not remove required features: Tailwind CDN, SaaS layout, dark/light toggle, language switcher, footer policy sections, window.__RELATED__ rendering.

Here is current index.html content:
{html}
""".strip()


def apply_unified_diff_to_text(original: str, diff_text: str) -> Optional[str]:
    """
    単一ファイル(index.html)用の最小パッチ適用。
    OpenAIが出す一般的な unified diff を想定。
    """
    if not diff_text.startswith("---"):
        return None

    lines = diff_text.splitlines()
    # find hunks
    hunks = []
    i = 0
    while i < len(lines):
        if lines[i].startswith("@@"):
            hunks.append(i)
        i += 1

    if not hunks:
        return None

    orig_lines = original.splitlines()
    out_lines = orig_lines[:]
    # ざっくり適用：各hunkを順に処理（厳密じゃないが単一HTMLの修正なら十分機能しやすい）
    # 失敗したら None を返してリトライに任せる
    try:
        # Convert to mutable list with pointer
        result = []
        oidx = 0
        i = 0
        while i < len(lines):
            if not lines[i].startswith("@@"):
                i += 1
                continue
            header = lines[i]
            m = re.match(r"@@ -(\d+),?(\d*) \+(\d+),?(\d*) @@", header)
            if not m:
                return None
            old_start = int(m.group(1)) - 1
            # old_len = int(m.group(2) or "1")
            # Copy unchanged lines up to old_start
            while oidx < old_start and oidx < len(orig_lines):
                result.append(orig_lines[oidx])
                oidx += 1

            i += 1
            # Apply hunk body
            while i < len(lines) and not lines[i].startswith("@@"):
                l = lines[i]
                if l.startswith(" "):
                    # context line
                    result.append(l[1:])
                    oidx += 1
                elif l.startswith("-"):
                    # deletion
                    oidx += 1
                elif l.startswith("+"):
                    # addition
                    result.append(l[1:])
                else:
                    # unexpected
                    return None
                i += 1

        # append remaining
        while oidx < len(orig_lines):
            result.append(orig_lines[oidx])
            oidx += 1

        return "\n".join(result) + ("\n" if original.endswith("\n") else "")
    except Exception:
        return None


def infer_tags_simple(theme: str) -> List[str]:
    # 最小の自動タグ付け（あとで強化）
    t = theme.lower()
    tags = []
    rules = {
        "convert": "convert",
        "calculator": "calculator",
        "compare": "compare",
        "tax": "finance",
        "time": "time",
        "timezone": "time",
        "subscription": "pricing",
        "plan": "pricing",
        "checklist": "productivity",
        "template": "productivity",
    }
    for k, v in rules.items():
        if k in t and v not in tags:
            tags.append(v)
    if not tags:
        tags = ["tools"]
    return tags[:6]


# ---------------------------
# Publishing / Index / Notify / SNS
# ---------------------------

def get_repo_pages_base() -> str:
    # GitHub PagesのベースURLを確定できないので、Project Pages前提の推定。
    # 公開がVercel/custom domainの場合でも、ここは「相対パス」で壊れないようにしている。
    repo = os.getenv("GITHUB_REPOSITORY", "Mikann20041029/goliath-auto-tool")
    owner = repo.split("/")[0]
    name = repo.split("/")[1]
    return f"https://{owner.lower()}.github.io/{name}/"


def update_db_and_index(entry: Dict[str, Any], all_entries: List[Dict[str, Any]]):
    # db.json 先頭に追加
    all_entries.insert(0, entry)
    write_json(DB_PATH, all_entries)

    # index.html を更新（新着一覧）
    rows = []
    for e in all_entries[:50]:
        rows.append(f"""
        <a class="block p-4 rounded-xl border border-slate-200 dark:border-slate-800 hover:bg-slate-50 dark:hover:bg-slate-900 transition"
           href="{e['path']}/">
          <div class="font-semibold">{e['title']}</div>
          <div class="text-sm opacity-70">{e['created_at']} • {", ".join(e.get("tags", []))}</div>
        </a>
        """.strip())

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
        <h1 class="text-2xl font-bold">Goliath Tools</h1>
        <p class="opacity-70">Auto-generated tools + long-form guides</p>
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
  document.getElementById("themeBtn").onclick=()=>{
    root.classList.toggle("dark");
    localStorage.setItem(k, root.classList.contains("dark") ? "dark" : "light");
  };
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


def inject_related_json(html: str, related: List[Dict[str, str]]) -> str:
    # window.__RELATED__ = [...] を差し込む。既にある前提で置換する。
    rel_json = json.dumps(related, ensure_ascii=False)
    new = re.sub(
        r"window\.__RELATED__\s*=\s*\[[\s\S]*?\]\s*;",
        f"window.__RELATED__ = {rel_json};",
        html
    )
    return new


def main():
    ensure_dirs()

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY", ""))

    # 1) Collector -> Cluster
    items = collector_stub()
    cluster = cluster_20(items)
    theme = cluster["theme"]

    # 2) Identify output path (no overwrite)
    created_at = now_utc_iso()
    tags = infer_tags_simple(theme)
    slug = slugify(theme)
    folder = f"{int(time.time())}-{slug}"
    page_dir = f"{PAGES_DIR}/{folder}"
    os.makedirs(page_dir, exist_ok=True)

    # base url (relative-safe). public_url used for related links + notify.
    pages_base = get_repo_pages_base()
    public_url = f"{pages_base}{ROOT}/pages/{folder}/"
    canonical = public_url.rstrip("/")

    # 3) Builder with Auto-fix loop
    prompt = build_prompt(theme, cluster, canonical)
    html = openai_generate_html(client, prompt)

    ok, msg = validate_html(html)
    attempts = 0
    while not ok and attempts < 5:
        attempts += 1
        fix_prompt = prompt_for_fix(theme, msg, html)
        diff = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": fix_prompt}],
        ).choices[0].message.content or ""

        patched = apply_unified_diff_to_text(html, diff.strip())
        if patched is None:
            # diffが壊れてる場合は、失敗理由を明示して“HTML再生成”でリカバリ（最終保険）
            regen_prompt = build_prompt(theme, cluster, canonical) + f"\n\n[Fix needed]\nValidation error: {msg}\nReturn ONLY corrected HTML.\n"
            html = openai_generate_html(client, regen_prompt)
        else:
            html = patched

        ok, msg = validate_html(html)

    if not ok:
        # ここまで来たら失敗通知して終了（壊れたHTMLは公開しない）
        create_github_issue(
            title=f"[Goliath] Build failed after 5 fixes: {slug}",
            body=f"- theme: {theme}\n- error: {msg}\n- created_at: {created_at}\n"
        )
        return

    # 4) Related sites list generation
    all_entries = read_json(DB_PATH, [])
    seed_sites = load_seed_sites()
    related = pick_related(tags, all_entries, seed_sites, k=8)

    # Ensure page has a window.__RELATED__ assignment filled
    html = inject_related_json(html, related)

    # 5) Save page
    page_path = f"{page_dir}/index.html"
    write_text(page_path, html)

    # 6) Update DB + index
    entry = {
        "id": stable_id(created_at, slug),
        "title": theme[:80],
        "created_at": created_at,
        "path": f"./pages/{folder}",
        "public_url": public_url,
        "tags": tags,
        "source_urls": cluster.get("urls", [])[:20],
        "related": related,
    }
    update_db_and_index(entry, all_entries)

    # 7) Notify
    create_github_issue(
        title=f"[Goliath] New tool published: {slug}",
        body=f"- theme: {theme}\n- url: {public_url}\n- tags: {', '.join(tags)}\n- related_count: {len(related)}\n- created_at: {created_at}\n"
    )

    # 8) SNS (optional)
    post_text = f"New tool: {theme}\n{public_url}"
    post_bluesky(post_text)
    post_mastodon(post_text)


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
