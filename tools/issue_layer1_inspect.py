import os
import re
import json
from typing import List, Dict, Any, Optional

import requests
from bs4 import BeautifulSoup


def extract_forward_urls(issue_body: str) -> List[str]:
    """
    Extract URLs that appear after 'forward:'.
    Handles:
      forward: https://...
      forward:
        https://...
    """
    body = issue_body or ""
    urls: List[str] = []

    # forward: same line
    urls += re.findall(r"forward\s*:\s*(https?://[^\s)]+)", body, flags=re.IGNORECASE)

    # forward: next lines
    lines = body.splitlines()
    for i, ln in enumerate(lines):
        if re.search(r"\bforward\b", ln, flags=re.IGNORECASE) and ":" in ln:
            for j in range(i + 1, min(i + 12, len(lines))):
                cand = lines[j].strip()
                if not cand:
                    break
                m = re.match(r"^(https?://\S+)$", cand)
                if m:
                    urls.append(m.group(1))

    # de-dup keep order
    out: List[str] = []
    seen = set()
    for u in urls:
        u = u.strip().rstrip(".")
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def fetch_html(url: str, timeout: int = 20) -> Dict[str, Any]:
    headers = {"User-Agent": "GoliathInspector/1.0 (+https://mikanntool.com)"}
    r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    ct = (r.headers.get("Content-Type") or "").lower()
    text = r.text if ("text" in ct or "<html" in r.text.lower()) else r.text
    return {
        "requested": url,
        "final_url": r.url,
        "status": r.status_code,
        "content_type": ct,
        "text": text,
    }


def meta_self_check(html: str, final_url: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "lxml")

    def meta_name(name: str) -> Optional[str]:
        tag = soup.find("meta", attrs={"name": name})
        return tag.get("content") if tag and tag.get("content") else None

    def meta_prop(prop: str) -> Optional[str]:
        tag = soup.find("meta", attrs={"property": prop})
        return tag.get("content") if tag and tag.get("content") else None

    robots = meta_name("robots") or meta_name("googlebot")
    noindex = False
    if robots:
        low = robots.lower()
        noindex = ("noindex" in low) or ("none" in low)

    canonical_tag = soup.find("link", rel=lambda v: v and "canonical" in v)
    canonical = canonical_tag.get("href") if canonical_tag and canonical_tag.get("href") else None

    hreflangs = []
    for link in soup.find_all("link"):
        rel = link.get("rel") or []
        if "alternate" in rel and link.get("hreflang") and link.get("href"):
            hreflangs.append({"hreflang": link.get("hreflang"), "href": link.get("href")})

    og = {
        "og:title": meta_prop("og:title"),
        "og:description": meta_prop("og:description"),
        "og:url": meta_prop("og:url"),
        "og:image": meta_prop("og:image"),
    }

    ldjson_count = 0
    ldjson_types = []
    for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
        txt = s.get_text(strip=True) or ""
        if not txt:
            continue
        try:
            data = json.loads(txt)
            ldjson_count += 1
            t = None
            if isinstance(data, dict):
                t = data.get("@type")
            elif isinstance(data, list) and data and isinstance(data[0], dict):
                t = data[0].get("@type")
            if t:
                ldjson_types.append(str(t))
        except Exception:
            ldjson_count += 1
            ldjson_types.append("INVALID_JSON")

    title = soup.title.get_text(strip=True) if soup.title else None
    desc = meta_name("description")

    problems = []
    if noindex:
        problems.append("NOINDEX(meta robots/googlebot)")
    if not canonical:
        problems.append("MISSING canonical")
    if not title:
        problems.append("MISSING <title>")
    if not desc:
        problems.append("MISSING meta description")
    if not og.get("og:title") or not og.get("og:description") or not og.get("og:image"):
        problems.append("OG incomplete (title/desc/image)")
    if ldjson_count == 0:
        problems.append("No JSON-LD found")

    return {
        "final_url": final_url,
        "title": title,
        "description": desc,
        "robots_meta": robots,
        "noindex": noindex,
        "canonical": canonical,
        "hreflang_count": len(hreflangs),
        "hreflangs_sample": hreflangs[:6],
        "og": og,
        "jsonld_count": ldjson_count,
        "jsonld_types": ldjson_types[:8],
        "problems": problems,
    }


def inspect_url_with_gsc(url: str, site_url: str, sa_json_str: str) -> Optional[Dict[str, Any]]:
    """
    URL Inspection API. If credentials are missing/unavailable, return None.
    """
    if not site_url or not sa_json_str:
        return None

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except Exception:
        return {"_error": "google auth libs missing"}

    try:
        info = json.loads(sa_json_str)
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/webmasters"]
        )
        service = build("searchconsole", "v1", credentials=creds, cache_discovery=False)
        req = {"inspectionUrl": url, "siteUrl": site_url}
        resp = service.urlInspection().index().inspect(body=req).execute()
        return resp
    except Exception as e:
        return {"_error": f"{e!r}"}


def summarize_gsc(resp: Optional[Dict[str, Any]]) -> List[str]:
    if resp is None:
        return ["GSC: skipped (no creds)"]
    if isinstance(resp, dict) and resp.get("_error"):
        return [f"GSC: ERROR {resp['_error']}"]

    res = (resp or {}).get("inspectionResult", {})
    idx = res.get("indexStatusResult", {}) if isinstance(res, dict) else {}

    verdict = idx.get("verdict")
    coverage = idx.get("coverageState")
    indexing_state = idx.get("indexingState")
    robots_state = idx.get("robotsTxtState")
    last_crawl = idx.get("lastCrawlTime")

    out = []
    if verdict is not None: out.append(f"GSC verdict: {verdict}")
    if coverage is not None: out.append(f"coverageState: {coverage}")
    if indexing_state is not None: out.append(f"indexingState: {indexing_state}")
    if robots_state is not None: out.append(f"robotsTxtState: {robots_state}")
    if last_crawl is not None: out.append(f"lastCrawlTime: {last_crawl}")
    if not out:
        out = ["GSC: (no fields returned)"]
    return out


def gh_comment(issue_number: str, body_md: str) -> None:
    token = os.environ.get("GH_TOKEN", "")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if not token or not repo:
        raise SystemExit("Missing GH_TOKEN or GITHUB_REPOSITORY")

    url = f"https://api.github.com/repos/{repo}/issues/{issue_number}/comments"
    r = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        json={"body": body_md},
        timeout=20,
    )
    if r.status_code >= 300:
        raise SystemExit(f"Failed to comment: {r.status_code} {r.text[:500]}")


def main() -> None:
    issue_number = os.environ.get("ISSUE_NUMBER", "").strip()
    issue_body = os.environ.get("ISSUE_BODY", "")

    if not issue_number:
        raise SystemExit("ISSUE_NUMBER missing")

    urls = extract_forward_urls(issue_body)
    if not urls:
        gh_comment(issue_number, "Layer1 Inspect: forward URL が見つからなかったためスキップしました。")
        return

    max_inspect = int(os.environ.get("MAX_INSPECT", "20"))
    target_urls = urls[:max_inspect]
    truncated = len(urls) > len(target_urls)

    site_url = os.environ.get("GSC_SITE_URL", "").strip()
    sa_json = os.environ.get("GSC_SERVICE_ACCOUNT_JSON", "").strip()

    lines = []
    lines.append("## Layer1 Inspect Report")
    lines.append(f"- forward URLs found: **{len(urls)}**")
    lines.append(f"- inspected now: **{len(target_urls)}** (MAX_INSPECT={max_inspect})")
    if truncated:
        lines.append(f"- note: URLが多いので上限で打ち切り（残り {len(urls) - len(target_urls)} 件）")

    for u in target_urls:
        lines.append("")
        lines.append(f"### {u}")

        try:
            fetched = fetch_html(u)
            lines.append(f"- HTTP: **{fetched['status']}**")
            if fetched["final_url"] != u:
                lines.append(f"- redirect: {fetched['final_url']}")

            chk = meta_self_check(fetched["text"], fetched["final_url"])

            # meta summary
            lines.append("- Meta:")
            lines.append(f"  - noindex: `{chk['noindex']}` (robots: `{chk['robots_meta']}`)")
            lines.append(f"  - canonical: `{chk['canonical']}`")
            lines.append(f"  - hreflang_count: `{chk['hreflang_count']}`")
            lines.append(f"  - og:image: `{chk['og'].get('og:image')}`")
            lines.append(f"  - jsonld_count: `{chk['jsonld_count']}` types: `{chk['jsonld_types']}`")

            if chk["problems"]:
                lines.append("  - problems:")
                for p in chk["problems"]:
                    lines.append(f"    - {p}")
            else:
                lines.append("  - problems: none")

            # GSC inspection
            gsc = inspect_url_with_gsc(fetched["final_url"], site_url, sa_json)
            gsc_lines = summarize_gsc(gsc)
            lines.append("- URL Inspection:")
            for gl in gsc_lines:
                lines.append(f"  - {gl}")

        except Exception as e:
            lines.append(f"- ERROR: {e!r}")

    body_md = "\n".join(lines)
    gh_comment(issue_number, body_md)


if __name__ == "__main__":
    main()

