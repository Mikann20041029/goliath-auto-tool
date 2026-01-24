import os, json, urllib.request

def gh_api(method: str, url: str, token: str, payload=None):
    req = urllib.request.Request(url, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/vnd.github+json")
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        req.add_header("Content-Type", "application/json")
        req.data = data
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        return resp.getcode(), json.loads(body) if body else {}

def read_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def read_text(path: str, limit_chars: int = 12000) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            s = f.read()
        if len(s) <= limit_chars:
            return s
        return s[:limit_chars] + "\n...(truncated)..."
    except Exception:
        return "(file not found)"

def chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def format_leads_block(leads):
    # 必須フォーマット:
    # - 悩みURL
    # - 返信文（最後の行にページURLが入っている）
    out = []
    for it in leads:
        url = it.get("problem_url","")
        reply = it.get("reply","").strip()
        out.append(f"- URL: {url}\n- Reply:\n{reply}\n")
    return "\n".join(out).strip()

def main():
    token = os.environ.get("GITHUB_TOKEN", "")
    repo  = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    if not token or not repo or not run_id:
        raise SystemExit("Missing GITHUB_TOKEN/GITHUB_REPOSITORY/GITHUB_RUN_ID")

    stats = read_json("out/stats.json", {})
    leads = read_json("out/leads.json", [])
    run_url = f"{server}/{repo}/actions/runs/{run_id}"
    exit_code = os.environ.get("GOLIATH_EXIT_CODE", "")  # optional
    if not exit_code:
        exit_code = "unknown"

    # 100件以上を強制（無いならそのままでもIssueは作る）
    total = len(leads)

    # 1 issue あたり 30件に分割（長文対策）
    parts = list(chunk(leads, 30)) if leads else [[]]

    for idx, part in enumerate(parts, start=1):
        title = f"[Goliath] Run report #{run_id} (part {idx}/{len(parts)})"
        header = [
            f"Run: {run_url}",
            "",
            "## Self-check",
            f"- Counts: {json.dumps(stats.get('counts', {}), ensure_ascii=False)}",
            f"- Leads: {total} (target >= 100)",
            f"- Affiliates keys ok: {stats.get('affiliates_key_check', {}).get('ok', False)}",
        ]
        miss = stats.get("affiliates_key_check", {}).get("missing_keys", [])
        if miss:
            header.append(f"- Missing affiliate keys: {miss}")
        header.append("")
        header.append("## Reply candidates (manual)")
        body = "\n".join(header) + "\n\n" + format_leads_block(part)

        api = f"https://api.github.com/repos/{repo}/issues"
        payload = {"title": title, "body": body}
        code, res = gh_api("POST", api, token, payload)
        if code not in (200, 201):
            raise SystemExit(f"Failed to create issue: HTTP {code} {res}")

if __name__ == "__main__":
    main()