#!/usr/bin/env python3
import argparse
import os
import re
import subprocess
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
import xml.etree.ElementTree as ET

DEFAULT_HUB = "https://pubsubhubbub.appspot.com/"  # Hubの案内ページ :contentReference[oaicite:3]{index=3}


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return ""
    return u.rstrip(").,]}>\"'")


def extract_forward_last(issue_body: str) -> str:
    if not issue_body:
        return ""
    matches = re.findall(r"forward\s*:\s*(https?://\S+)", issue_body, flags=re.IGNORECASE)
    matches = [normalize_url(x) for x in matches if normalize_url(x)]
    if not matches:
        return ""
    return matches[-1]


def atom_feed_template(feed_url: str, hub_url: str, title: str = "Mikanntool Updates") -> ET.Element:
    ns = {"atom": "http://www.w3.org/2005/Atom"}
    ET.register_namespace("", ns["atom"])
    feed = ET.Element("{http://www.w3.org/2005/Atom}feed")

    ET.SubElement(feed, "{http://www.w3.org/2005/Atom}id").text = feed_url
    ET.SubElement(feed, "{http://www.w3.org/2005/Atom}title").text = title
    ET.SubElement(feed, "{http://www.w3.org/2005/Atom}updated").text = now_iso()

    # rel="self" と rel="hub" はHub側の案内に合わせる :contentReference[oaicite:4]{index=4}
    link_self = ET.SubElement(feed, "{http://www.w3.org/2005/Atom}link")
    link_self.set("rel", "self")
    link_self.set("href", feed_url)

    link_hub = ET.SubElement(feed, "{http://www.w3.org/2005/Atom}link")
    link_hub.set("rel", "hub")
    link_hub.set("href", hub_url)

    return feed


def load_or_create_feed(feed_path: str, feed_url: str, hub_url: str) -> ET.ElementTree:
    if os.path.exists(feed_path):
        try:
            tree = ET.parse(feed_path)
            return tree
        except Exception:
            pass  # 壊れてたら作り直す

    feed = atom_feed_template(feed_url=feed_url, hub_url=hub_url)
    return ET.ElementTree(feed)


def guess_title_from_url(url: str) -> str:
    try:
        p = urlparse(url)
        path = (p.path or "").strip("/")
        if not path:
            return url
        slug = path.split("/")[-1] or path
        slug = slug.replace("-", " ").replace("_", " ")
        return slug[:80]
    except Exception:
        return url[:80]


def upsert_entry(tree: ET.ElementTree, page_url: str, max_entries: int = 50):
    feed = tree.getroot()
    atom_ns = "http://www.w3.org/2005/Atom"

    # 既存entryのidが一致してたら更新だけ
    entries = feed.findall(f"{{{atom_ns}}}entry")
    for e in entries:
        eid = e.find(f"{{{atom_ns}}}id")
        if eid is not None and (eid.text or "").strip() == page_url:
            upd = e.find(f"{{{atom_ns}}}updated")
            if upd is None:
                upd = ET.SubElement(e, f"{{{atom_ns}}}updated")
            upd.text = now_iso()
            feed.find(f"{{{atom_ns}}}updated").text = now_iso()
            return

    # 新規entryを先頭に追加
    entry = ET.Element(f"{{{atom_ns}}}entry")
    ET.SubElement(entry, f"{{{atom_ns}}}id").text = page_url
    ET.SubElement(entry, f"{{{atom_ns}}}title").text = guess_title_from_url(page_url)
    ET.SubElement(entry, f"{{{atom_ns}}}updated").text = now_iso()

    link = ET.SubElement(entry, f"{{{atom_ns}}}link")
    link.set("href", page_url)
    link.set("rel", "alternate")

    # feed直下のupdatedも更新
    feed.find(f"{{{atom_ns}}}updated").text = now_iso()

    # 先頭に挿入（self/hub linkの後ろあたりに入るよう軽く調整）
    insert_pos = len(feed.findall(f"{{{atom_ns}}}link")) + 4
    feed.insert(insert_pos, entry)

    # 多すぎたら末尾から削る
    entries = feed.findall(f"{{{atom_ns}}}entry")
    if len(entries) > max_entries:
        for e in entries[max_entries:]:
            feed.remove(e)


def write_feed(tree: ET.ElementTree, feed_path: str):
    os.makedirs(os.path.dirname(feed_path) or ".", exist_ok=True)
    tree.write(feed_path, encoding="utf-8", xml_declaration=True)


def post_websub_publish(hub_url: str, topic_url: str, timeout=20):
    hub = (hub_url or "").strip() or DEFAULT_HUB
    topic = (topic_url or "").strip()
    if not topic:
        return False, 0, "topic_url empty"

    # Hubの案内どおり、form-encoded で hub.mode=publish と hub.url を送る :contentReference[oaicite:5]{index=5}
    data = {
        "hub.mode": "publish",
        "hub.url": topic,
        "hub.topic": topic,  # 実装差分の保険
    }
    r = requests.post(hub, data=data, timeout=timeout)
    ok = 200 <= r.status_code < 300
    return ok, r.status_code, (r.text or "").strip()


def gh_comment(repo: str, issue_number: int, body: str, token: str):
    if not token:
        return False, 0, "GH_TOKEN empty"

    url = f"https://api.github.com/repos/{repo}/issues/{issue_number}/comments"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "goliath-issue-router-websub",
    }
    r = requests.post(url, headers=headers, json={"body": body}, timeout=20)
    ok = 200 <= r.status_code < 300
    return ok, r.status_code, (r.text or "").strip()


def git_commit_push(paths, msg: str):
    subprocess.run(["git", "config", "user.name", "Goliath-Bot"], check=False)
    subprocess.run(["git", "config", "user.email", "bot@mikanntool.com"], check=False)

    for p in paths:
        subprocess.run(["git", "add", p], check=False)

    # 変更なしなら何もしない
    r = subprocess.run(["git", "diff", "--cached", "--quiet"])
    if r.returncode == 0:
        return False

    subprocess.run(["git", "commit", "-m", msg], check=False)
    subprocess.run(["git", "push"], check=False)
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--title", default="")
    ap.add_argument("--issue-body", default="")
    ap.add_argument("--issue-number", default="")
    ap.add_argument("--repo", default="")
    ap.add_argument("--do-git", action="store_true")
    args = ap.parse_args()

    title = args.title or ""
    # 事故防止：候補Issue以外は無視
    if not title.startswith("Goliath candidates"):
        print("Skip: not a Goliath candidates issue")
        return 0

    try:
        issue_number = int(args.issue_number)
    except Exception:
        print("Skip: invalid issue_number")
        return 0

    repo = (args.repo or "").strip()
    token = (os.getenv("GH_TOKEN") or "").strip()

    public_base = (os.getenv("PUBLIC_BASE_URL") or "").strip().rstrip("/")
    hub_url = (os.getenv("WEBSUB_HUB_URL") or "").strip() or DEFAULT_HUB
    feed_path = (os.getenv("FEED_PATH") or "").strip() or "feed.xml"

    if not public_base:
        msg = "WebSub: PUBLIC_BASE_URL が未設定なので停止（feed URLが作れない）"
        gh_comment(repo, issue_number, msg, token)
        print(msg)
        return 0

    forward_url = extract_forward_last(args.issue_body or "")
    if not forward_url:
        msg = "WebSub: forward URL が見つからないので停止（Issue本文に forward: を入れて）"
        gh_comment(repo, issue_number, msg, token)
        print(msg)
        return 0

    # feedの公開URL（GitHub Pages上で見えるURL）
    feed_url = f"{public_base}/feed.xml"

    tree = load_or_create_feed(feed_path=feed_path, feed_url=feed_url, hub_url=hub_url)
    upsert_entry(tree, forward_url, max_entries=50)
    write_feed(tree, feed_path)

    changed = False
    if args.do_git:
        changed = git_commit_push([feed_path], "WebSub: update feed.xml")

    # Hubへpublish（topicは feed_url）
    ok, status, text = post_websub_publish(hub_url, feed_url)

    lines = []
    lines.append("WebSub (Layer1)")
    lines.append(f"- forward(last): {forward_url}")
    lines.append(f"- feed_path: {feed_path}")
    lines.append(f"- feed_url(topic): {feed_url}")
    lines.append(f"- git_commit: {'yes' if changed else 'no'}")
    lines.append(f"- hub: {hub_url}")
    lines.append(f"- publish_status: {status}")
    lines.append(f"- publish: {'OK' if ok else 'FAILED'}")
    if (not ok) and text:
        lines.append(f"- hub_response: {text[:300]}")

    body = "\n".join(lines)
    gh_comment(repo, issue_number, body, token)
    print(body)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
