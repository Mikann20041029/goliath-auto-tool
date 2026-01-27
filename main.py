#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Repo root runner.

Purpose:
- Execute goliath/main.py as the real entrypoint.
- Preserve proper exit codes (DO NOT force green).
"""

from __future__ import annotations

import os
import runpy
import sys
import traceback


def _run_goliath_main() -> int:
    target = os.path.join("goliath", "main.py")

    # Make logs flush more reliably on CI.
    os.environ.setdefault("PYTHONUNBUFFERED", "1")

    try:
        runpy.run_path(target, run_name="__main__")
        return 0

    except SystemExit as e:
        # Preserve exit codes from goliath/main.py
        code = e.code
        if code is None:
            return 0
        if isinstance(code, int):
            return code
        # If it's not an int (rare), treat truthy as failure
        return 1 if code else 0

    except FileNotFoundError:
        print(f"[ROOT] ERROR: {target} not found.", file=sys.stderr)
        return 2

    except Exception:
        print("[ROOT] Unhandled exception inside goliath/main.py:", file=sys.stderr)
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(_run_goliath_main())

