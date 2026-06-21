#!/usr/bin/env python3
"""Backward-compatible wrapper for the generic MLB CLV analyzer.

Prefer scripts/analyze_mlb_clv.py for new stat-generic workflows. This module
re-exports the generic analyzer API so existing imports/tests/commands keep
working during the stat-suite migration.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))

from analyze_mlb_clv import *  # noqa: F401,F403
from analyze_mlb_clv import parse_args, run

if __name__ == "__main__":
    result = run(parse_args())
    print(f"Wrote Phase 1B CLV diagnostics to {result['output_dir']}")
    print(f"Decision: {result['decision']['decision']}")
