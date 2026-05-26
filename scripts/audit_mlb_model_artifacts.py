#!/usr/bin/env python3
"""Audit MLB production model artifacts before trading promotion.

Read-only gate for trading readiness. It resolves the same model directory shape
used by the sweep/inference paths, loads the MLBModelSuite, verifies required
stats are actually present, and prints artifact metadata/feature counts so a
missing model cannot be hidden by the suite's graceful-skip behavior.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_REQUIRED_STATS: tuple[str, ...] = ("pitcher_strikeouts", "batter_hits")
OPTIONAL_REQUIRES_VALIDATION = {"batter_hrr"}
KNOWN_NOT_LIVE_MLB_KALSHI_STATS = {
    "batter_total_bases",
    "batter_home_runs",
    "batter_runs_scored",
}


@dataclass(frozen=True)
class SuiteSnapshot:
    available_stats: list[str]
    predictor_classes: dict[str, str]


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_metadata_value(metadata: dict[str, Any] | None, keys: tuple[str, ...]) -> Any:
    if not metadata:
        return None
    for key in keys:
        if key in metadata:
            return metadata[key]
    config = metadata.get("config")
    if isinstance(config, dict):
        for key in keys:
            if key in config:
                return config[key]
    return None


def _feature_count_from_names(feature_names: Any) -> dict[str, Any]:
    if isinstance(feature_names, dict):
        per_key = {str(k): len(v or []) for k, v in feature_names.items()}
        union: set[str] = set()
        for values in feature_names.values():
            union.update(str(v) for v in (values or []))
        return {"per_quantile": per_key, "union_count": len(union)}
    if isinstance(feature_names, list):
        return {"count": len(feature_names)}
    return {"count": None}


def _load_pitcher_feature_counts(model_dir: Path) -> dict[str, Any]:
    feature_config = model_dir / "pitcher_k_feature_config.joblib"
    if not feature_config.exists():
        return {"count": None, "missing": str(feature_config)}
    try:
        import joblib

        data = joblib.load(feature_config)
        if isinstance(data, dict):
            return _feature_count_from_names(data.get("feature_names"))
    except Exception as exc:  # pragma: no cover - defensive audit output
        return {"count": None, "error": str(exc)}
    return {"count": None}


def _stat_model_name(stat: str) -> str:
    mapping = {
        "batter_hits": "batter_hits",
        "batter_total_bases": "batter_total_bases",
        "batter_rbis": "batter_rbis",
        "batter_runs_scored": "batter_runs",
        "batter_hrr": "batter_hrr",
    }
    return mapping.get(stat, stat)


def inspect_stat_artifacts(model_dir: Path, stat: str) -> dict[str, Any]:
    """Return static artifact metadata for one stat without DB access."""
    if stat == "pitcher_strikeouts":
        hyperparams = _read_json(model_dir / "pitcher_k_best_hyperparams.json")
        return {
            "stat": stat,
            "model_type": "quantile",
            "core_artifacts": {
                "pitcher_k_model.joblib": (model_dir / "pitcher_k_model.joblib").exists(),
                "pitcher_k_feature_config.joblib": (model_dir / "pitcher_k_feature_config.joblib").exists(),
            },
            "train_seasons": _safe_metadata_value(hyperparams, ("train_seasons", "seasons")),
            "calibration_cutoff": _safe_metadata_value(hyperparams, ("cal_end_date", "calibration_cutoff", "calibration_end_date")),
            "variant": _safe_metadata_value(hyperparams, ("variant", "ablation_variant")),
            "feature_counts": _load_pitcher_feature_counts(model_dir),
            "metadata_files": ["pitcher_k_best_hyperparams.json"] if hyperparams else [],
        }

    model_name = _stat_model_name(stat)
    negbin_meta = _read_json(model_dir / f"{model_name}_negbin_meta.json")
    binomial_meta = _read_json(model_dir / f"{model_name}_binomial_meta.json")
    metadata = negbin_meta or binomial_meta
    model_type = "negbin" if negbin_meta else "binomial" if binomial_meta else "unknown"
    booster_name = f"{model_name}_xgblss_booster.json" if negbin_meta else f"{model_name}_binomial_booster.json"
    hyperparams = _read_json(model_dir / f"{stat}_best_hyperparams.json") or _read_json(model_dir / f"{model_name}_best_hyperparams.json")

    return {
        "stat": stat,
        "model_type": model_type,
        "core_artifacts": {
            booster_name: (model_dir / booster_name).exists(),
            f"{model_name}_{model_type}_meta.json": metadata is not None if model_type != "unknown" else False,
        },
        "train_seasons": _safe_metadata_value(metadata, ("train_seasons", "seasons")),
        "calibration_cutoff": _safe_metadata_value(metadata, ("cal_end_date", "calibration_cutoff", "calibration_end_date")),
        "variant": _safe_metadata_value(metadata, ("variant", "ablation_variant")),
        "feature_counts": _feature_count_from_names(metadata.get("feature_names") if metadata else None),
        "metadata_files": [
            name
            for name, present in {
                f"{model_name}_negbin_meta.json": negbin_meta is not None,
                f"{model_name}_binomial_meta.json": binomial_meta is not None,
                f"{stat}_best_hyperparams.json": hyperparams is not None,
            }.items()
            if present
        ],
    }


def _load_suite_snapshot(model_dir: Path) -> SuiteSnapshot:
    from src.models.mlb.mlb_model_suite import MLBModelSuite

    suite = MLBModelSuite.from_directory(model_dir, n_samples=1000)
    return SuiteSnapshot(
        available_stats=list(suite.available_stats),
        predictor_classes={stat: type(suite.get_predictor(stat)).__name__ for stat in suite.available_stats},
    )


def _resolve_model_dir(model_dir: Path) -> Path:
    from src.backtesting.mlb.sweep_bootstrap import find_latest_model_dir

    return find_latest_model_dir(str(model_dir))


def run_audit(
    model_dir: Path,
    required_stats: list[str],
    *,
    validated_optional_stats: set[str] | None = None,
    resolver: Callable[[Path], Path] = _resolve_model_dir,
    suite_loader: Callable[[Path], SuiteSnapshot] = _load_suite_snapshot,
) -> tuple[dict[str, Any], list[str]]:
    """Run read-only model artifact audit.

    Dependency injection keeps tests from loading xgboost/joblib artifacts.
    """
    validated_optional_stats = validated_optional_stats or set()
    resolved_model_dir = resolver(model_dir)
    suite = suite_loader(resolved_model_dir)
    failures: list[str] = []
    warnings: list[str] = []

    missing_stats = [stat for stat in required_stats if stat not in suite.available_stats]
    if missing_stats:
        failures.append(f"Missing required loaded model stats: {missing_stats}")

    for stat in required_stats:
        if stat in OPTIONAL_REQUIRES_VALIDATION and stat not in validated_optional_stats:
            failures.append(
                f"{stat} was requested but is not marked validated; keep it out of live Kalshi trading until validation proof exists"
            )

    extra_loaded = [stat for stat in suite.available_stats if stat not in required_stats]
    if extra_loaded:
        warnings.append(
            "Suite loaded extra stats not required by this gate; do not treat them as live-trading support: "
            + ", ".join(extra_loaded)
        )
    unvalidated_optional_loaded = sorted(
        stat for stat in OPTIONAL_REQUIRES_VALIDATION.intersection(suite.available_stats)
        if stat not in validated_optional_stats and stat not in required_stats
    )
    if unvalidated_optional_loaded:
        warnings.append(
            "Optional Kalshi stat artifacts are present but not validated for live support: "
            + ", ".join(unvalidated_optional_loaded)
        )
    blocked_loaded = sorted(KNOWN_NOT_LIVE_MLB_KALSHI_STATS.intersection(suite.available_stats))
    if blocked_loaded:
        warnings.append(
            "Known non-live/unsupported Kalshi stats are present as artifacts and must stay out of live trading: "
            + ", ".join(blocked_loaded)
        )

    stats = {stat: inspect_stat_artifacts(resolved_model_dir, stat) for stat in sorted(set(required_stats + suite.available_stats))}
    for stat in required_stats:
        core_artifacts = stats.get(stat, {}).get("core_artifacts", {})
        missing_artifacts = [name for name, present in core_artifacts.items() if not present]
        if missing_artifacts:
            failures.append(f"{stat} missing core artifact files: {missing_artifacts}")
        if stats.get(stat, {}).get("train_seasons") is None:
            warnings.append(f"{stat} artifact metadata does not expose train_seasons")
        if stats.get(stat, {}).get("calibration_cutoff") is None:
            warnings.append(f"{stat} artifact metadata does not expose calibration cutoff")

    summary: dict[str, Any] = {
        "status": "fail" if failures else "ok",
        "requested_model_dir": str(model_dir),
        "resolved_model_dir": str(resolved_model_dir),
        "required_stats": required_stats,
        "loaded_stats": suite.available_stats,
        "predictor_classes": suite.predictor_classes,
        "stats": stats,
        "warnings": warnings,
        "failures": failures,
        "promotion_posture": (
            "artifact/functionality gate only; quote-clean CLV, ranking, dense intraday stability, "
            "and paper/live output gates are still required before live money"
        ),
    }
    return summary, failures


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit MLB production model artifacts for trading-readiness gates")
    parser.add_argument("--model-dir", default="src/models/mlb/artifacts", help="Base or production MLB artifact dir")
    parser.add_argument(
        "--require-stat",
        action="append",
        dest="required_stats",
        help="Required loaded model stat. Repeatable. Defaults to pitcher_strikeouts + batter_hits.",
    )
    parser.add_argument(
        "--include-batter-hrr",
        action="store_true",
        help="Require batter_hrr too. Fails unless --validated-optional-stat batter_hrr is also supplied.",
    )
    parser.add_argument(
        "--validated-optional-stat",
        action="append",
        default=[],
        help="Optional stat with external validation proof, e.g. batter_hrr. Repeatable.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON summary only")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    required_stats = args.required_stats or list(DEFAULT_REQUIRED_STATS)
    if args.include_batter_hrr and "batter_hrr" not in required_stats:
        required_stats.append("batter_hrr")
    summary, failures = run_audit(
        Path(args.model_dir),
        required_stats,
        validated_optional_stats=set(args.validated_optional_stat),
    )

    if args.json:
        print(json.dumps(summary, indent=2, default=str))
    else:
        print("MLB model artifact/functionality audit")
        print(f"Status: {summary['status'].upper()}")
        print(f"Requested model dir: {summary['requested_model_dir']}")
        print(f"Resolved model dir: {summary['resolved_model_dir']}")
        print(f"Required stats: {summary['required_stats']}")
        print(f"Loaded stats: {summary['loaded_stats']}")
        print(f"Predictor classes: {summary['predictor_classes']}")
        for stat in summary["required_stats"]:
            info = summary["stats"].get(stat, {})
            print(
                f"- {stat}: type={info.get('model_type')} features={info.get('feature_counts')} "
                f"train_seasons={info.get('train_seasons')} cal_cutoff={info.get('calibration_cutoff')}"
            )
        for warning in summary["warnings"]:
            print(f"WARNING: {warning}")
        for failure in failures:
            print(f"FAIL: {failure}")
        print(f"Posture: {summary['promotion_posture']}")

    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
