#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
advertise.py  (repo root)

目的:
- GitHub Issue本文（添付のように "forward:" の後に生成サイトURLが並ぶ）から forward URL を抽出
- 各URL → slug抽出 → goliath/pages/{slug}/index.html を特定
- ページから genre を best-effort で検出
- affiliates.json から genre別に URL（文字列）を1つ選ぶ（惺一さまはURLをペーストするだけ運用）
- index.html の固定スロットに広告を1つだけ注入
  <!-- AFF_SLOT_MID: BEGIN --> ... <!-- AFF_SLOT_MID --> ... <!-- AFF_SLOT_MID: END -->
- 既に埋め込み済みならスキップ（重複挿入しない）
- 任意で commit & push

想定 affiliates.json（惺一さまの「コピペだけ」運用）
{
  "categories": {
    "Web/Hosting": ["https://rakuten....", "..."],
    "Dev/Tools": ["https://rakuten...."],
    ...
    "default": ["https://rakuten...."]  // 保険（推奨）
  }
}

使い方:
  # 1) 単一URL
  python advertise.py --url "https://.../goliath/pages/slug/"

  # 2) Issue本文を直接渡す
  python advertise.py --issue-body "${ISSUE_BODY}"

  # 3) Issue本文をファイルで渡す
  python advertise.py --issue-body-file issue_body.txt

  # 4) 変更をcommit & pushまで
  python advertise.py --issue-body "${ISSUE_BODY}" --do-git

環境変数:
  AFFILIATES_JSON : affiliates.json のパス（省略時は repo直下 affiliates.json）
  DO_GIT=1        : --do-git と同等
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_AFFILIATES_JSON = REPO_ROOT / "affiliates.json"

SLOT_BEGIN = "<!-- AFF_SLOT_MID: BEGIN -->"
SLOT_END = "<!-- AFF_SLOT_MID: END -->"
SLOT_MARKER = "<!-- AFF_SLOT_MID -->"

FILLED_ATTR = 'data-aff-filled="1"'
FILLED_COMMENT_PREFIX = "<!-- AFF_FILLED:"


@dataclass
class Offer:
    id: str
    url: str
    title: str


def eprint(*args: Any) -> None:
    print(*args, file=sys.stderr)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, s: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(s, encoding="utf-8")


def run(cmd: List[str]) -> None:
    subprocess.check_call(cmd)


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def escape_html(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def extract_forward_urls(issue_body: str) -> List[str]:
    """
    Issue本文から forward の後ろのURLを抽出（添付の形式に合わせる）
    例:
      forward:
      https://.../goliath/pages/slug/

    追加で保険:
    - 本文中の /goliath/pages/{slug}/ っぽいURLも拾う
    """
    if not issue_body:
        return []

    urls = []

    # 1) forward: の直後
    urls += re.findall(r"(?im)\bforward\s*:\s*(https?://[^\s)]+)", issue_body)

    # 2) forward: が単独行で、次行にURLが来るパターン
    #    forward:\nhttps://....
    urls += re.findall(r"(?im)\bforward\s*:\s*\n\s*(https?://[^\s)]+)", issue_body)

    # 3) 保険: goliath/pages のURL全部
    urls += re.findall(r"(https?://[^\s)]+/goliath/pages/[A-Za-z0-9_-]+/?(?:index\.html)?)", issue_body)

    cleaned: List[str] = []
    for u in urls:
        u = u.strip().strip(">").strip().strip("`").strip().strip("*").strip()
        u = u.rstrip(".,;:!?)")
        cleaned.append(u)

    # 重複排除（順序維持）
    seen = set()
    out: List[str] = []
    for u in cleaned:
        if u not in seen:
            seen.add(u)
            out.append(u)

    return out


def extract_slug_from_url(url: str) -> Optional[str]:
    """
    .../goliath/pages/{slug}/
    .../goliath/pages/{slug}/index.html
    """
    u = url.strip()
    u = u.rstrip("/")
    m = re.search(r"/goliath/pages/([^/]+)(?:/index\.html)?$", u)
    if m:
        return m.group(1)

    m = re.search(r"/goliath/pages/([^/]+)/", url)
    if m:
        return m.group(1)

    return None


def slug_to_index_html(slug: str) -> Path:
    return REPO_ROOT / "goliath" / "pages" / slug / "index.html"


def detect_genre(slug: str, index_html_path: Path) -> Optional[str]:
    """
    genre検出（best-effort）
    1) goliath/pages/{slug}/meta.json or site.json の genre/category
    2) index.html 内:
       - <meta name="goliath:genre" content="...">
       - data-goliath-genre="..."
       - <!-- GENRE: ... -->
    """
    candidates = [
        index_html_path.parent / "meta.json",
        index_html_path.parent / "site.json",
    ]
    for p in candidates:
        if p.exists():
            try:
                data = json.loads(read_text(p))
                g = data.get("genre") or data.get("category") or data.get("vertical")
                if isinstance(g, str) and g.strip():
                    return g.strip()
            except Exception:
                pass

    if not index_html_path.exists():
        return None

    html = read_text(index_html_path)

    m = re.search(r'<meta\s+name="goliath:genre"\s+content="([^"]+)"\s*/?>', html, re.I)
    if m:
        return m.group(1).strip()

    m = re.search(r'data-goliath-genre="([^"]+)"', html, re.I)
    if m:
        return m.group(1).strip()

    m = re.search(r"<!--\s*GENRE:\s*([^>]+?)\s*-->", html, re.I)
    if m:
        return m.group(1).strip()

    # genreが取れない場合は None（defaultへ）
    return None


def load_affiliates(path: Path) -> Dict[str, List[Offer]]:
    """
    惺一さまの運用:
    - affiliates.json は「ジャンル配列にURL文字列をコピペ」だけ
    - titleなどは不要。こちらで自動生成する。

    受け付ける形:
    A) {"categories": {"Dev/Tools": ["https://...", "..."], ...}}
    B) {"Dev/Tools": ["https://...", "..."], ...}  （保険）
    """
    raw = json.loads(read_text(path))

    if isinstance(raw, dict) and "categories" in raw and isinstance(raw["categories"], dict):
        raw = raw["categories"]

    if not isinstance(raw, dict):
        raise ValueError("affiliates.json must be a dict (or {'categories': dict}).")

    by_genre: Dict[str, List[Offer]] = {}

    def make_title(url: str) -> str:
        # できるだけ無難なタイトル（コピペ運用のため）
        m = re.search(r"^https?://([^/]+)", url.strip())
        host = m.group(1) if m else "link"
        return f"Sponsored ({host})"

    for genre, lst in raw.items():
        if not isinstance(lst, list):
            continue
        for item in lst:
            if not isinstance(item, str):
                # 文字列以外は無視（惺一さまの運用は文字列だけ）
                continue
            u = item.strip()
            if not u:
                continue
            oid = sha1(f"{genre}|{u}")[:10]
            by_genre.setdefault(str(genre), []).append(Offer(id=oid, url=u, title=make_title(u)))

    return by_genre


def choose_one_offer(by_genre: Dict[str, List[Offer]], genre: Optional[str], slug: str) -> Optional[Offer]:
    """
    1つだけ選ぶ（毎回同じslugなら同じ広告になるように安定選択）
    優先:
      1) genre完全一致
      2) default
      3) All
      4) どれか最初に見つかったジャンル
    """
    candidates: List[Offer] = []

    if genre and genre in by_genre and by_genre[genre]:
        candidates = by_genre[genre]
    elif "default" in by_genre and by_genre["default"]:
        candidates = by_genre["default"]
    elif "All" in by_genre and by_genre["All"]:
        candidates = by_genre["All"]
    else:
        for _, lst in by_genre.items():
            if lst:
                candidates = lst
                break

    if not candidates:
        return None

    idx = int(sha1(slug)[:8], 16) % len(candidates)
    return candidates[idx]


def render_offer_html(offer: Offer) -> str:
    """
    最低限の広告カード（サイズ問題を吸収）
    - 画像なしで安定
    - rel="nofollow sponsored" を付ける
    """
    return f"""<!-- AFF_FILLED:{escape_html(offer.id)} -->
<div class="aff-card" data-aff-filled="1">
  <div class="aff-card-head">
    <span class="aff-label">Sponsored</span>
    <div class="aff-title">{escape_html(offer.title)}</div>
  </div>
  <div class="aff-cta">
    <a class="aff-btn" href="{escape_html(offer.url)}" target="_blank" rel="nofollow sponsored noopener">Open</a>
  </div>
</div>"""


def inject_into_slot(html: str, offer_html: str) -> Tuple[str, bool, str]:
    """
    BEGIN/END のガード内の <!-- AFF_SLOT_MID --> を1回だけ置換
    - スロットがない: 変更しない
    - 既に埋まってる: 変更しない
    """
    if SLOT_BEGIN not in html or SLOT_END not in html:
        return html, False, "slot_guard_not_found"

    start = html.find(SLOT_BEGIN)
    end = html.find(SLOT_END, start)
    if end == -1:
        return html, False, "slot_end_not_found"

    guard_block = html[start : end + len(SLOT_END)]

    if FILLED_ATTR in guard_block or FILLED_COMMENT_PREFIX in guard_block:
        return html, False, "already_filled"

    if SLOT_MARKER not in guard_block:
        return html, False, "slot_marker_not_found"

    new_block = guard_block.replace(SLOT_MARKER, offer_html, 1)
    new_html = html[:start] + new_block + html[end + len(SLOT_END) :]
    return new_html, True, "injected"


def git_commit_push(changed_files: List[Path], message: str) -> None:
    rels = [str(p.relative_to(REPO_ROOT)) for p in changed_files]
    run(["git", "add", "--"] + rels)
    run(["git", "commit", "-m", message])
    run(["git", "push"])


def process_url(url: str, affiliates_by_genre: Dict[str, List[Offer]], dry_run: bool) -> Tuple[bool, str, Optional[Path]]:
    slug = extract_slug_from_url(url)
    if not slug:
        return False, f"skip: invalid_url (no slug): {url}", None

    index_path = slug_to_index_html(slug)
    if not index_path.exists():
        return False, f"skip: index_not_found: {index_path}", None

    genre = detect_genre(slug, index_path)
    offer = choose_one_offer(affiliates_by_genre, genre, slug)
    if not offer:
        return False, f"skip: no_offer (genre={genre!r})", None

    html = read_text(index_path)
    offer_html = render_offer_html(offer)

    new_html, changed, reason = inject_into_slot(html, offer_html)
    if not changed:
        return False, f"skip: {reason} (slug={slug}, genre={genre!r})", None

    if not dry_run:
        write_text(index_path, new_html)

    return True, f"ok: injected (slug={slug}, genre={genre!r}, offer_id={offer.id})", index_path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", help="Single generated site URL")
    ap.add_argument("--issue-body", help="Issue body text (contains forward URLs)")
    ap.add_argument("--issue-body-file", help="Path to file containing Issue body")
    ap.add_argument("--affiliates-json", help="Path to affiliates.json (default: repo root affiliates.json)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write files")
    ap.add_argument("--do-git", action="store_true", help="Commit & push after edits")
    args = ap.parse_args()

    affiliates_path = Path(args.affiliates_json) if args.affiliates_json else Path(
        os.environ.get("AFFILIATES_JSON", str(DEFAULT_AFFILIATES_JSON))
    )

    if not affiliates_path.exists():
        eprint(f"ERROR: affiliates.json not found: {affiliates_path}")
        return 2

    # URLs収集
    urls: List[str] = []
    if args.url:
        urls = [args.url.strip()]
    else:
        body = ""
        if args.issue_body_file:
            body = read_text(Path(args.issue_body_file))
        elif args.issue_body:
            body = args.issue_body
        else:
            # stdinも許可（Actions向け）
            if not sys.stdin.isatty():
                body = sys.stdin.read()
        urls = extract_forward_urls(body)

    if not urls:
        print("No target URLs found. Need --url or Issue body containing forward URLs.")
        return 0

    affiliates_by_genre = load_affiliates(affiliates_path)

    changed_files: List[Path] = []
    ok_count = 0
    logs: List[str] = []

    for u in urls:
        ok, msg, changed_path = process_url(u, affiliates_by_genre, dry_run=args.dry_run)
        logs.append(msg)
        if ok and changed_path:
            ok_count += 1
            changed_files.append(changed_path)

    # 出力
    print("\n".join(logs))
    print(f"\nSummary: urls={len(urls)} injected={ok_count} dry_run={args.dry_run}")

    do_git = args.do_git or (os.environ.get("DO_GIT", "").strip() == "1")
    if do_git and ok_count > 0 and not args.dry_run:
        # de-dupe
        uniq: List[Path] = []
        seen = set()
        for p in changed_files:
            rp = str(p.resolve())
            if rp not in seen:
                seen.add(rp)
                uniq.append(p)
        git_commit_push(uniq, f"ads: inject 1 affiliate into AFF_SLOT_MID ({ok_count} pages)")
        print("Git: committed & pushed.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

