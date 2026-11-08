#!/usr/bin/env python3
"""Run a bounded, no-fit NBA training-input smoke against the local database."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DEFAULT_FIT_SEASONS = ("22023", "22024")
DEFAULT_CALIBRATION_SEASONS = ("22025",)
DEFAULT_OUTPUT_DIR = Path("logs/receipts/nba_training_smoke")


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Inspect a deterministic, bounded NBA training-input sample without "
            "fitting, tuning, selecting, or loading a model."
        )
    )
    parser.add_argument(
        "--local",
        action="store_true",
        required=True,
        help="Required safety acknowledgement: use only LOCAL_DATABASE_URL.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print effective scope without opening a database connection.")
    parser.add_argument("--rows-per-season", type=int, default=100)
    parser.add_argument("--fit-seasons", nargs="+", default=list(DEFAULT_FIT_SEASONS))
    parser.add_argument(
        "--calibration-seasons", nargs="+", default=list(DEFAULT_CALIBRATION_SEASONS)
    )
    parser.add_argument("--start-date", type=_parse_date)
    parser.add_argument("--end-date", type=_parse_date)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--statement-timeout-ms", type=int, default=300_000)
    parser.add_argument("--lock-timeout-ms", type=int, default=5_000)
    return parser


def _scope(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "mode": "dry_run" if args.dry_run else "local_data_acceptance",
        "database": "LOCAL_DATABASE_URL (loopback host required)",
        "sample": {
            "deterministic": True,
            "order": ["game_date", "game_id", "player_id"],
            "rows_per_season": args.rows_per_season,
            "fit_seasons": args.fit_seasons,
            "calibration_seasons": args.calibration_seasons,
            "start_date": args.start_date.isoformat() if args.start_date else None,
            "end_date": args.end_date.isoformat() if args.end_date else None,
        },
        "output": {
            "directory": str(args.output_dir),
            "summary": "summary.json",
            "feature_status": "feature_status.csv",
            "full_matrices_persisted": False,
        },
        "certification": (
            "Sampled input readiness only; not training, model, promotion, whole-season "
            "representativeness, or source-clock/leakage certification."
        ),
    }


def _validate_args(args: argparse.Namespace) -> None:
    if args.rows_per_season <= 0:
        raise ValueError("rows-per-season must be positive")
    if args.statement_timeout_ms <= 0 or args.lock_timeout_ms <= 0:
        raise ValueError("database timeouts must be positive")
    if args.start_date and args.end_date and args.start_date > args.end_date:
        raise ValueError("start-date must be on or before end-date")
    overlap = set(args.fit_seasons) & set(args.calibration_seasons)
    if overlap:
        raise ValueError("fit and calibration season labels must not overlap")


def _local_engine(args: argparse.Namespace):
    from dotenv import load_dotenv
    from sqlalchemy import create_engine
    from sqlalchemy.engine import make_url
    from sqlalchemy.pool import NullPool

    load_dotenv()
    database_url = os.getenv("LOCAL_DATABASE_URL")
    if not database_url:
        raise RuntimeError("LOCAL_DATABASE_URL is not configured")
    parsed = make_url(database_url)
    if parsed.host not in {"localhost", "127.0.0.1", "::1"}:
        raise RuntimeError("LOCAL_DATABASE_URL must use a loopback host")
    options = (
        "-c default_transaction_read_only=on "
        f"-c statement_timeout={args.statement_timeout_ms} "
        f"-c lock_timeout={args.lock_timeout_ms}"
    )
    return create_engine(
        database_url,
        poolclass=NullPool,
        connect_args={
            "application_name": "gameflow:nba_training_input_smoke",
            "options": options,
        },
    )


def _verify_read_only(engine, args: argparse.Namespace) -> dict[str, str]:
    from sqlalchemy import text

    with engine.connect() as conn:
        settings = {
            name: str(conn.execute(text(f"SHOW {name}")).scalar_one())
            for name in ("transaction_read_only", "statement_timeout", "lock_timeout")
        }
    if settings["transaction_read_only"].lower() != "on":
        raise RuntimeError("database session is not read-only")
    if settings["statement_timeout"] in {"0", "0ms"}:
        raise RuntimeError("statement timeout is not active")
    if settings["lock_timeout"] in {"0", "0ms"}:
        raise RuntimeError("lock timeout is not active")
    return settings


def _numeric_counts(series) -> dict[str, int]:
    import numpy as np
    import pandas as pd

    numeric = pd.to_numeric(series, errors="coerce")
    finite = np.isfinite(numeric.to_numpy(dtype=float, na_value=np.nan))
    return {
        "rows": int(len(numeric)),
        "null": int(numeric.isna().sum()),
        "zero": int(numeric.eq(0).sum()),
        "finite": int(finite.sum()),
        "infinite": int(np.isinf(numeric.to_numpy(dtype=float, na_value=np.nan)).sum()),
    }


def _analyze_frame(frame, role: str):
    import numpy as np
    import pandas as pd

    from src.models.features.nba.contracts import (
        MINUTES_FEATURES,
        RATE_FEATURES_AST,
        RATE_FEATURES_PTS,
        RATE_FEATURES_REB,
    )
    from src.models.quantile_trainer import prepare_quantile_features

    families = {
        "minutes": (MINUTES_FEATURES, "actual_minutes"),
        "pts": (RATE_FEATURES_PTS, "pts_per_min"),
        "reb": (RATE_FEATURES_REB, "reb_per_min"),
        "ast": (RATE_FEATURES_AST, "ast_per_min"),
    }
    required = {
        "game_id",
        "player_id",
        "game_date",
        "season_id",
        "commence_time_utc",
        "injury_cutoff_utc",
    }
    for features, target in families.values():
        required.update(features)
        required.add(target)
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"missing expected columns: {', '.join(missing)}")
    if frame.empty:
        raise ValueError(f"{role} sample is empty")
    if frame[["game_id", "player_id", "season_id"]].isna().any().any():
        raise ValueError(f"{role} sample contains missing identity fields")

    duplicate_count = int(frame.duplicated(["game_id", "player_id"]).sum())
    if duplicate_count:
        raise ValueError(f"{role} sample contains duplicate player-game keys")

    feature_rows = []
    family_summary = {}
    for family, (features, target) in families.items():
        eligible = frame[target].notna()
        consumed = frame.loc[eligible]
        if consumed.empty:
            raise ValueError(f"{role} {family} target population is empty")
        prepared = prepare_quantile_features(consumed, features)
        if prepared.shape != (len(consumed), len(features)):
            raise ValueError(f"{role} {family} prepared matrix shape mismatch")
        target_counts = _numeric_counts(consumed[target])
        if target_counts["finite"] != len(consumed):
            raise ValueError(f"{role} {family} target contains invalid numeric values")
        for feature in features:
            raw_counts = _numeric_counts(consumed[feature])
            prepared_counts = _numeric_counts(prepared[feature])
            if raw_counts["infinite"] or prepared_counts["infinite"]:
                raise ValueError(f"{role} {family} feature contains infinity: {feature}")
            feature_rows.append(
                {
                    "role": role,
                    "family": family,
                    "feature": feature,
                    **{f"raw_{key}": value for key, value in raw_counts.items()},
                    **{f"prepared_{key}": value for key, value in prepared_counts.items()},
                }
            )
        family_summary[family] = {
            "target": target,
            "target_status": target_counts,
            "rows_consumed": int(len(consumed)),
            "prepared_rows": int(prepared.shape[0]),
            "prepared_columns": int(prepared.shape[1]),
            "prepared_null_cells": int(prepared.isna().sum().sum()),
            "prepared_infinite_cells": int(
                np.isinf(prepared.to_numpy(dtype=float, na_value=np.nan)).sum()
            ),
        }

    dates = pd.to_datetime(frame["game_date"], errors="coerce")
    if dates.isna().any():
        raise ValueError(f"{role} sample contains invalid game dates")
    injury_features = sorted(
        {
            feature
            for features, _ in families.values()
            for feature in features
            if feature.startswith(("team_out_", "opp_out_"))
            or feature in {"player_is_questionable", "player_is_probable"}
        }
    )
    all_injury_unknown = frame[injury_features].isna().all(axis=1)
    return (
        {
            "rows": int(len(frame)),
            "seasons": {
                str(key): int(value)
                for key, value in frame["season_id"].value_counts().sort_index().items()
            },
            "unique_player_game_keys": int(
                frame[["game_id", "player_id"]].drop_duplicates().shape[0]
            ),
            "duplicate_player_game_keys": duplicate_count,
            "date_min": dates.min().date().isoformat(),
            "date_max": dates.max().date().isoformat(),
            "families": family_summary,
            "injury_missingness": {
                "features": injury_features,
                "fully_unknown_rows": int(all_injury_unknown.sum()),
                "warning": bool(all_injury_unknown.any()),
            },
            "cutoff_availability": {
                "injury_cutoff_non_null": int(frame["injury_cutoff_utc"].notna().sum()),
                "injury_cutoff_null": int(frame["injury_cutoff_utc"].isna().sum()),
                "commence_time_non_null": int(frame["commence_time_utc"].notna().sum()),
                "source_clock_certified": False,
            },
        },
        feature_rows,
    )


def _load_role(loader, seasons: list[str], args: argparse.Namespace):
    from src.models.features.nba.requests import TrainingFeatureRequest

    return loader.load(
        TrainingFeatureRequest(
            seasons=seasons,
            historical_role="diagnostic_sample",
            row_limit_per_season=args.rows_per_season,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    )


def _run(args: argparse.Namespace) -> dict[str, Any]:
    import pandas as pd

    from src.models.feature_store import FeatureStore
    from src.models.features.nba.training_loader import TrainingFeatureLoader

    engine = _local_engine(args)
    try:
        settings = _verify_read_only(engine, args)
        loader = TrainingFeatureLoader(FeatureStore(engine=engine))
        fit = _load_role(loader, args.fit_seasons, args)
        calibration = _load_role(loader, args.calibration_seasons, args)

        fit_keys = set(zip(fit["game_id"], fit["player_id"], strict=False))
        calibration_keys = set(
            zip(calibration["game_id"], calibration["player_id"], strict=False)
        )
        if fit_keys & calibration_keys:
            raise ValueError("fit and calibration samples overlap by player-game key")
        fit_dates = pd.to_datetime(fit["game_date"], errors="coerce")
        calibration_dates = pd.to_datetime(calibration["game_date"], errors="coerce")
        if fit_dates.max() >= calibration_dates.min():
            raise ValueError("fit and calibration sample dates overlap or are out of order")

        fit_summary, fit_feature_rows = _analyze_frame(fit, "fit")
        calibration_summary, calibration_feature_rows = _analyze_frame(
            calibration, "calibration"
        )
        per_season = {}
        seasonal_features = []
        for role, frame in (("fit", fit), ("calibration", calibration)):
            for season, season_frame in frame.groupby("season_id", sort=True):
                season_summary, feature_rows = _analyze_frame(season_frame, role)
                per_season[str(season)] = {"role": role, **season_summary}
                seasonal_features.extend(
                    {"season": str(season), **row} for row in feature_rows
                )
        args.output_dir.mkdir(parents=True, exist_ok=True)
        feature_status = pd.DataFrame(seasonal_features)
        feature_status.to_csv(args.output_dir / "feature_status.csv", index=False)

        summary = {
            "status": "PASS",
            "scope": _scope(args),
            "database_session": settings,
            "samples": {"fit": fit_summary, "calibration": calibration_summary},
            "per_season": per_season,
            "temporal_separation": {
                "overlapping_keys": 0,
                "fit_max_date": fit_summary["date_max"],
                "calibration_min_date": calibration_summary["date_min"],
                "ordered_without_overlap": True,
            },
            "warnings": [
                "Unknown injury-context NaNs are intentionally preserved by prepare_quantile_features.",
                "Existing raw_game_lines_staging MAX selection is not source-clock certified by this smoke.",
            ],
            "certification": (
                "PASS means only that sampled local training inputs loaded and were consumed by "
                "the current preparation boundary. It is not fit, model, promotion, leakage, "
                "source-clock, or whole-season certification."
            ),
        }
        (args.output_dir / "summary.json").write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return summary
    finally:
        engine.dispose()


def main() -> int:
    parser = _parser()
    args = parser.parse_args()
    try:
        _validate_args(args)
        if args.dry_run:
            print(json.dumps(_scope(args), indent=2, sort_keys=True))
            return 0
        summary = _run(args)
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0
    except Exception as exc:  # Keep DB details and credentials out of terminal output.
        failure = {
            "status": "FAIL",
            "error_type": type(exc).__name__,
            "certification": "No sampled-input acceptance was established.",
        }
        try:
            args.output_dir.mkdir(parents=True, exist_ok=True)
            (args.output_dir / "summary.json").write_text(
                json.dumps(failure, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
        except OSError:
            pass
        print(json.dumps(failure, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
