import os
import json
import pathlib
import datetime
import urllib.request

def _read_tail(path: str, max_chars: int = 12000) -> str:
    try:
        p = pathlib.Path(path)
        if not p.exists():
            return f"(no file: {path})"
        text = p.read_text(encoding="utf-8", errors="replace")
        if len(text) > max_chars:
            return text[-max_chars:]
        return text
    except Exception as e:
        return f"(failed to read {path}: {e})"

def main() -> int:
    # Always avoid failing the workflow because of reporting.
    token = os.getenv("GITHUB_TOKEN", "")
    repo  = os.getenv("GITHUB_REPOSITORY", "")
    if not token or not repo:
        print("[report_to_issue] skip: missing GITHUB_TOKEN or GITHUB_REPOSITORY")
        return 0

    run_id   = os.getenv("GITHUB_RUN_ID", "")
    server   = os.getenv("GITHUB_SERVER_URL", "https://github.com")
    sha      = os.getenv("GITHUB_SHA", "")
    wf_name  = os.getenv("GITHUB_WORKFLOW", "workflow")
    run_url  = f"{server}/{repo}/actions/runs/{run_id}" if run_id else "(no run url)"

    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    title = f"Goliath Report: {now}"

    log_tail = _read_tail("run_log.txt")
    body = "\n".join([
        f"Workflow: {wf_name}",
        f"Run: {run_url}",
        f"SHA: {sha}",
        "",
        "---- run_log.txt (tail) ----",
        "```",
        log_tail,
        "```",
    ])

    payload = {"title": title, "body": body}

    url = f"https://api.github.com/repos/{repo}/issues"
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": "goliath-auto-tool",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            status = getattr(resp, "status", 200)
            resp_text = resp.read().decode("utf-8", errors="replace")
            print(f"[report_to_issue] created issue: HTTP {status}")
            # 失敗しても落とさない設計だけど、目安で出す
            if status >= 300:
                print(resp_text[:2000])
    except Exception as e:
        # 報告失敗してもここで workflow を落とさない
        print(f"[report_to_issue] failed (non-fatal): {e}")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())