#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

import requests

# Playwright is installed in workflow
from playwright.sync_api import sync_playwright


FORWARD_RE = re.compile(r"forward:\s*(https?://\S+)", re.IGNORECASE)


def eprint(*a):
    print(*a, file=sys.stderr)


def getenv_int(name: str, default: int) -> int:
    v = (os.getenv(name) or "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


def getenv_str(name: str, default: str) -> str:
    v = (os.getenv(name) or "").strip()
    return v if v else default


def gh_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "goliath-screenshot-layer1",
    }


def fetch_issue_body(repo: str, issue_number: int, token: str) -> str:
    url = f"https://api.github.com/repos/{repo}/issues/{issue_number}"
    r = requests.get(url, headers=gh_headers(token), timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("body") or ""


def post_issue_comment(repo: str, issue_number: int, token: str, body: str) -> None:
    url = f"https://api.github.com/repos/{repo}/issues/{issue_number}/comments"
    r = requests.post(url, headers=gh_headers(token), json={"body": body}, timeout=30)
    r.raise_for_status()


def extract_forward_urls(issue_body: str) -> list[str]:
    urls = []
    for m in FORWARD_RE.finditer(issue_body or ""):
        u = m.group(1).strip()
        # strip trailing punctuation that sometimes sticks to URLs
        u = u.rstrip(").,]}>\"'")
        urls.append(u)

    # de-dup while preserving order
    seen = set()
    out = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def url_to_slug(u: str) -> str:
    p = urlparse(u)
    path = (p.path or "").strip("/")
    if not path:
        return "home"

    # if ends with index.html -> use parent dir name
    if path.endswith("index.html"):
        parts = path.split("/")
        if len(parts) >= 2:
            slug = parts[-2]
        else:
            slug = "index"
    else:
        slug = path.split("/")[-1]

    # sanitize
    slug = re.sub(r"[^a-zA-Z0-9\-_.]+", "-", slug).strip("-")
    if not slug:
        slug = "page"
    return slug


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def git_commit_push(message: str) -> bool:
    # Configure identity
    subprocess.run(["git", "config", "user.name", "Goliath-Bot"], check=False)
    subprocess.run(["git", "config", "user.email", "bot@mikanntool.com"], check=False)

    subprocess.run(["git", "add", "-A"], check=False)

    # If no changes, nothing to do
    r = subprocess.run(["git", "diff", "--cached", "--quiet"])
    if r.returncode == 0:
        return False

    subprocess.run(["git", "commit", "-m", message], check=True)
    subprocess.run(["git", "push"], check=True)
    return True


def take_screenshots(urls: list[str], out_dir: Path, quality: int) -> list[dict]:
    ensure_dir(out_dir)

    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1200, "height": 630},
            device_scale_factor=1,
            locale="en-US",
        )
        page = context.new_page()

        for i, u in enumerate(urls, start=1):
            slug = url_to_slug(u)
            filename = f"{slug}__og.jpg"
            out_path = out_dir / filename

            status = "ok"
            err = ""

            try:
                page.goto(u, wait_until="networkidle", timeout=45000)
                page.wait_for_timeout(1500)
                page.screenshot(
                    path=str(out_path),
                    type="jpeg",
                    quality=quality,
                    full_page=False,
                )
            except Exception as ex:
                status = "failed"
                err = str(ex)
                # write a small marker file so you can see failure in git
                try:
                    out_path.write_bytes(b"")
                except Exception:
                    pass

            results.append(
                {
                    "n": i,
                    "url": u,
                    "slug": slug,
                    "file": str(out_path.as_posix()),
                    "status": status,
                    "error": err[:500],
                }
            )

        context.close()
        browser.close()

    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True, help="owner/repo")
    ap.add_argument("--issue-number", required=True, type=int)
    ap.add_argument("--do-git", action="store_true")
    args = ap.parse_args()

    token = getenv_str("GH_TOKEN", "")
    if not token:
        raise SystemExit("GH_TOKEN is empty. Set GH_PAT or use github.token.")

    public_base = getenv_str("PUBLIC_BASE_URL", "")
    # if not set, we still can run; links will be relative
    max_shots = getenv_int("SHOTS_MAX", 3)        # デフォルトは “最後の3件”
    quality = getenv_int("SHOTS_QUALITY", 70)     # JPEG品質

    issue_body = fetch_issue_body(args.repo, args.issue_number, token)
    urls_all = extract_forward_urls(issue_body)

    if not urls_all:
        post_issue_comment(
            args.repo,
            args.issue_number,
            token,
            "Screenshot (Layer1): forward: URL が見つかりませんでした。何もしませんでした。",
        )
        return 0

    # take LAST N (あなたの「最後のURLを拾う」方針と同じ)
    urls = urls_all[-max_shots:] if len(urls_all) > max_shots else urls_all

    out_dir = Path("goliath/assets/screenshots")
    results = take_screenshots(urls, out_dir, quality)

    # manifest (番号でズレ防止)
    manifest = {
        "generated_at": dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "repo": args.repo,
        "issue_number": args.issue_number,
        "picked": {
            "policy": "last_n",
            "max": max_shots,
            "total_found": len(urls_all),
            "used": len(urls),
        },
        "items": results,
    }
    manifest_path = out_dir / "manifest.json"
    ensure_dir(out_dir)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    committed = False
    if args.do_git:
        committed = git_commit_push(f"Shots: update screenshots for issue #{args.issue_number}")

    # Build comment
    lines = []
    lines.append("✅ Screenshot (Layer1) 完了")
    lines.append(f"- forward URL found: {len(urls_all)}")
    lines.append(f"- used(last {len(urls)}): {len(urls)}")
    lines.append(f"- saved: `goliath/assets/screenshots/<slug>__og.jpg`")
    lines.append(f"- manifest: `goliath/assets/screenshots/manifest.json`")
    lines.append(f"- git_commit: {'yes' if committed else 'no (no changes)'}")
    lines.append("")
    lines.append("結果:")
    for it in results:
        slug = it["slug"]
        rel = f"/goliath/assets/screenshots/{slug}__og.jpg"
        if public_base:
            img = public_base.rstrip("/") + rel
        else:
            img = rel
        st = it["status"]
        lines.append(f"- [{it['n']}] {st} | {it['url']}")
        lines.append(f"  - image: {img}")

    post_issue_comment(args.repo, args.issue_number, token, "\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
