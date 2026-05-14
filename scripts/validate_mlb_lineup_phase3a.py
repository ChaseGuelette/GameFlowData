#!/usr/bin/env python3
"""Validate MLB Phase 3A lineup/contact feature readiness.

Checks:
1. mlb_game_lineups coverage by season/date range for local and/or remote DB.
2. Actual MLBFeatureStore training feature path variation for the new lineup/contact
   features on the local DB.

This script is read-only.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db.client import get_engine
from src.models.mlb.mlb_feature_store import MLBFeatureStore

LINEUP_CONTACT_COLS = [
    "projected_lineup_k_pct",
    "projected_lineup_whiff_pct",
    "projected_lineup_chase_pct",
    "projected_lineup_contact_rate",
    "projected_lineup_same_hand_k_pct",
    "projected_lineup_opposite_hand_k_pct",
    "projected_lineup_hand_k_delta",
    "projected_lineup_top3_k_pct",
    "projected_lineup_mid3_k_pct",
    "projected_lineup_bot3_k_pct",
    "projected_lineup_k_concentration",
    "pct_opp_lineup_same_hand",
]

DEFAULTS = {
    "projected_lineup_k_pct": 0.22,
    "projected_lineup_whiff_pct": 0.22,
    "projected_lineup_chase_pct": 0.28,
    "projected_lineup_contact_rate": 0.78,
    "projected_lineup_same_hand_k_pct": 0.22,
    "projected_lineup_opposite_hand_k_pct": 0.22,
    "projected_lineup_hand_k_delta": 0.0,
    "projected_lineup_top3_k_pct": 0.22,
    "projected_lineup_mid3_k_pct": 0.22,
    "projected_lineup_bot3_k_pct": 0.22,
    "projected_lineup_k_concentration": 0.0,
    "pct_opp_lineup_same_hand": 0.50,
}


def print_coverage(name: str, db_url: str) -> None:
    print(f"\n== {name} lineup coverage ==")
    engine = create_engine(db_url)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT EXTRACT(YEAR FROM game_date)::int AS season,
                       COUNT(*) AS rows,
                       COUNT(DISTINCT game_date) AS dates,
                       COUNT(DISTINCT game_pk) AS games,
                       MIN(game_date)::date AS min_date,
                       MAX(game_date)::date AS max_date
                FROM mlb_game_lineups
                GROUP BY 1
                ORDER BY 1
                """
            )
        ).mappings().all()
    if not rows:
        print("no rows")
        return
    for row in rows:
        print(dict(row))


def validate_feature_variation(seasons: list[int]) -> bool:
    print("\n== LOCAL feature-path variation ==")
    fs = MLBFeatureStore(get_engine(local=True))
    all_passed = True

    for season in seasons:
        print(f"\n== season {season} ==")
        base = fs.get_training_dataset([season])
        enriched = fs.enrich_with_matchup_features(base)
        print(
            "rows",
            len(enriched),
            "date_range",
            enriched["game_date"].min(),
            enriched["game_date"].max(),
        )

        for col in LINEUP_CONTACT_COLS:
            if col not in enriched.columns:
                all_passed = False
                print(col, "MISSING")
                continue
            series = pd.to_numeric(enriched[col], errors="coerce")
            default = DEFAULTS[col]
            filled = series.fillna(default).round(10)
            non_default = int((filled != default).sum())
            nunique = int(series.nunique(dropna=True))
            if season in (2024, 2025) and (nunique <= 1 or non_default == 0):
                all_passed = False
                status = "FAIL"
            else:
                status = "ok"
            print(
                col,
                status,
                "nonnull", int(series.notna().sum()),
                "nunique", nunique,
                "non_default", non_default,
                "min", float(series.min()) if series.notna().any() else None,
                "max", float(series.max()) if series.notna().any() else None,
            )

    return all_passed


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate MLB Phase 3A lineup/contact readiness")
    parser.add_argument(
        "--coverage-only",
        action="store_true",
        help="Only print lineup table coverage; skip feature-store variation checks",
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        type=int,
        default=[2024, 2025, 2026],
        help="Seasons to check for feature-path variation (default: 2024 2025 2026)",
    )
    args = parser.parse_args()

    load_dotenv(Path(".env"))
    remote_url = os.getenv("DATABASE_URL")
    local_url = os.getenv("LOCAL_DATABASE_URL")

    if remote_url:
        print_coverage("REMOTE", remote_url)
    else:
        print("DATABASE_URL not set; skipping remote coverage")

    if local_url:
        print_coverage("LOCAL", local_url)
    else:
        print("LOCAL_DATABASE_URL not set; skipping local coverage")

    if args.coverage_only:
        return

    passed = validate_feature_variation(args.seasons)
    if not passed:
        print("\nRESULT: FAIL — 2024/2025 lineup/contact features still lack real variation.")
        sys.exit(1)
    print("\nRESULT: PASS — lineup/contact feature variation gate passed.")


if __name__ == "__main__":
    main()
