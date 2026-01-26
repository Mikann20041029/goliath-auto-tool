#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from pathlib import Path

def main() -> int:
    target = Path("goliath") / "main.py"
    src = target.read_text(encoding="utf-8")
    try:
        compile(src, str(target), "exec")
        print("[OK] Syntax check passed:", target)
        return 0
    except SyntaxError as e:
        print("[NG] SyntaxError:", e)
        # show context
        lines = src.splitlines()
        lineno = max(1, int(getattr(e, "lineno", 1)))
        start = max(1, lineno - 20)
        end = min(len(lines), lineno + 5)
        print(f"--- context {start}-{end} ---")
        for i in range(start, end + 1):
            mark = ">>" if i == lineno else "  "
            print(f"{mark} {i:5d}: {lines[i-1]}")
        print("--- end ---")
        return 2

if __name__ == "__main__":
    raise SystemExit(main())
