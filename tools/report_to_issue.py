import os, json, urllib.request

def read_file(path: str, limit_chars: int = 12000) -> str:
    if not os.path.exists(path):
        return "(run_log.txt not found)"
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        s = f.read()
    if len(s) <= limit_chars:
        return s
    return s[:limit_chars] + "\n...(truncated)..."

def main():
    token  = os.environ.get("GITHUB_TOKEN", "")
    repo   = os.environ.get("GITHUB_REPOSITORY", "")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    exitcode = os.environ.get("GOLIATH_EXITCODE", "(unknown)")

    if not token or not repo or not run_id:
        raise SystemExit("Missing GITHUB_TOKEN/GITHUB_REPOSITORY/GITHUB_RUN_ID")

    run_url = f"{server}/{repo}/actions/runs/{run_id}"
    log = read_file("run_log.txt")

    title = f"[Goliath] Run report #{run_id} (exit={exitcode})"
    body = (
        f"Run: {run_url}\n"
        f"Exit code: {exitcode}\n\n"
        f"### run_log.txt (excerpt)\n"
        f"```txt\n{log}\n```\n"
    )

    api = f"https://api.github.com/repos/{repo}/issues"
    payload = json.dumps({"title": title, "body": body}).encode("utf-8")

    req = urllib.request.Request(api, data=payload, method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Content-Type", "application/json")
    req.add_header("User-Agent", "goliath-auto-tool")

    with urllib.request.urlopen(req) as resp:
        if resp.getcode() not in (200, 201):
            raise SystemExit(f"Failed to create issue: HTTP {resp.getcode()}")

if __name__ == "__main__":
    main()