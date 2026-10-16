#!/usr/bin/env python3
"""Opt-in CDN-only NBA schedule refresh for the Railway scheduler."""

import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.scrapers.nba_schedule import ingest_schedule  # noqa: E402


def main() -> None:
    if os.getenv("NBA_SCHEDULE_INGEST_ENABLED", "").strip().lower() not in {"1", "true", "yes", "on"}:
        print("NBA schedule ingest disabled by NBA_SCHEDULE_INGEST_ENABLED=false")
        return
    print(f"schedule_rows_processed={ingest_schedule()}")


if __name__ == "__main__":
    main()
