"""Pure argv builders for the profile-driven MLB lifecycle."""

from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path

from src.models.mlb.lifecycle.config import FeatureControlsConfig, ResolvedLifecycleConfig
from src.models.mlb.training.profiles import MLBTrainingProfile

TrainingBuilder = Callable[[MLBTrainingProfile, ResolvedLifecycleConfig, Path], list[str]]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _append_values(argv: list[str], flag: str, values: list[str]) -> None:
    if values:
        argv.extend([flag, *values])


def _append_feature_controls(
    argv: list[str],
    controls: FeatureControlsConfig,
    *,
    extra_excluded_features: list[str] | None = None,
) -> None:
    prefix = "include" if controls.mode == "include" else "exclude"
    _append_values(argv, f"--force-{prefix}-families", controls.families)
    extra_excluded = list(extra_excluded_features or [])
    if controls.mode == "exclude":
        _append_values(
            argv,
            "--force-exclude-features",
            list(dict.fromkeys([*controls.features, *extra_excluded])),
        )
    else:
        _append_values(argv, "--force-include-features", controls.features)
        _append_values(argv, "--force-exclude-features", extra_excluded)


def _common_training_args(resolved: ResolvedLifecycleConfig, artifact_root: Path) -> list[str]:
    return [
        "--local",
        "--train-seasons",
        *(str(season) for season in resolved.training.seasons),
        "--cal-season",
        str(resolved.training.calibration_season),
        "--cal-end-date",
        str(resolved.training.calibration_end),
        "--feature-tolerance",
        str(resolved.model.feature_tolerance),
        "--output-dir",
        str(artifact_root),
        "--tuning-trials",
        str(resolved.model.tuning_trials),
    ]


def _build_batter_training(
    profile: MLBTrainingProfile,
    resolved: ResolvedLifecycleConfig,
    artifact_root: Path,
) -> list[str]:
    if not profile.train_short_stat:
        raise ValueError(f"Batter profile {profile.stat_key!r} is missing train_short_stat")
    argv = [
        sys.executable,
        str(repo_root() / "src/models/mlb/mlb_batter_train_pipeline.py"),
        "--stat",
        profile.train_short_stat,
        *_common_training_args(resolved, artifact_root),
    ]
    if resolved.model.base == "no_prop_line":
        argv.append("--exclude-prop-line")
    if resolved.model.tune:
        argv.append("--tune")
    _append_feature_controls(argv, resolved.model.feature_controls)
    return argv


def _build_pitcher_quantile_training(
    profile: MLBTrainingProfile,
    resolved: ResolvedLifecycleConfig,
    artifact_root: Path,
) -> list[str]:
    argv = [
        sys.executable,
        str(repo_root() / "src/models/mlb/mlb_train_pipeline.py"),
        *_common_training_args(resolved, artifact_root),
    ]
    if resolved.model.tune:
        argv.append("--tune")
    if resolved.model.variant:
        argv.extend(["--ablation-variant", resolved.model.variant])
    extra_excluded = (
        [profile.prop_line_feature]
        if resolved.model.base == "no_prop_line" and profile.prop_line_feature
        else []
    )
    _append_feature_controls(
        argv,
        resolved.model.feature_controls,
        extra_excluded_features=extra_excluded,
    )
    return argv


TRAINING_ADAPTERS: dict[str, TrainingBuilder] = {
    "batter": _build_batter_training,
    "pitcher_quantile": _build_pitcher_quantile_training,
}


def build_training_command(
    profile: MLBTrainingProfile,
    resolved: ResolvedLifecycleConfig,
    *,
    artifact_root: Path | None = None,
) -> list[str]:
    try:
        builder = TRAINING_ADAPTERS[profile.train_entrypoint_kind]
    except KeyError as exc:
        valid = ", ".join(sorted(TRAINING_ADAPTERS))
        raise ValueError(
            f"No training adapter for {profile.train_entrypoint_kind!r}. Valid: {valid}"
        ) from exc
    return builder(profile, resolved, artifact_root or resolved.artifact_dir)


def build_sweep_command(
    resolved: ResolvedLifecycleConfig,
    *,
    artifact_dir: Path,
    output_dir: Path,
) -> list[str]:
    argv = [
        sys.executable,
        str(repo_root() / "src/backtesting/mlb/run_mlb_sweep.py"),
        "--local",
        "--start",
        str(resolved.evaluation.start),
        "--end",
        str(resolved.evaluation.end),
        "--model-dir",
        str(artifact_dir),
        "--stats",
        resolved.profile_obj.stat_key,
        "--output-dir",
        str(output_dir),
        "--direction",
        str(resolved.evaluation.direction),
        "--tau",
        *("none" if value is None else str(value) for value in resolved.evaluation.tau),
        "--z-max",
        *(str(value) for value in resolved.evaluation.z_max),
        "--max-weight",
        *(str(value) for value in resolved.evaluation.max_weight),
        "--edge",
        *(str(value) for value in resolved.evaluation.edge_thresholds),
        "--kelly",
        *(str(value) for value in resolved.evaluation.kelly_values),
    ]
    if resolved.evaluation.flat_bet is not None:
        argv.extend(["--flat", str(resolved.evaluation.flat_bet)])
    if resolved.quotes.clean:
        argv.extend(
            [
                "--quote-clean",
                "--quote-decision-policy",
                str(resolved.quotes.decision_policy),
                "--quote-relative-minutes",
                str(resolved.quotes.relative_minutes),
                "--line-source",
                str(resolved.quotes.line_source),
            ]
        )
        if resolved.quotes.coverage_audit_note:
            argv.extend(
                ["--dense-clv-linked-coverage-audit-note", resolved.quotes.coverage_audit_note]
            )
    if resolved.quotes.routing:
        argv.extend(["--book-routing-policy", resolved.quotes.routing])
    return argv


def build_audit_command(
    resolved: ResolvedLifecycleConfig,
    *,
    sweep_output_dir: Path,
    output_dir: Path,
    artifact_dir: Path,
    bets_csvs: list[Path] | None = None,
) -> list[str]:
    argv = [
        sys.executable,
        str(repo_root() / "scripts/run_mlb_quote_clean_audit_suite.py"),
        "--local",
        "--sweep-output-dir",
        str(sweep_output_dir),
        "--output-dir",
        str(output_dir),
        "--model-dir",
        str(artifact_dir),
        "--start",
        str(resolved.evaluation.start),
        "--end",
        str(resolved.evaluation.end),
        "--stats",
        resolved.profile_obj.stat_key,
        "--quote-decision-policy",
        str(resolved.quotes.decision_policy),
        "--quote-relative-minutes",
        str(resolved.quotes.relative_minutes),
        "--line-source",
        str(resolved.quotes.line_source),
        "--snapshots-table",
        str(resolved.quotes.line_source),
        "--batch-size",
        "50",
    ]
    if resolved.audit.mode == "clv_only":
        argv.append("--skip-dropout-audit")
    for bets_csv in bets_csvs or []:
        argv.extend(["--bets-csv", str(bets_csv)])
    return argv


def build_ranker_command(
    *,
    clv_matches_csv: Path,
    candidate_edges_csv: Path,
    output_dir: Path,
    bootstrap_samples: int,
    minimum_bets: int,
) -> list[str]:
    argv = [
        sys.executable,
        str(repo_root() / "scripts/analyze_mlb_clv_ranking_diagnostics.py"),
        "--clv-matches-csv",
        str(clv_matches_csv),
        "--output-dir",
        str(output_dir),
        "--bootstrap-samples",
        str(bootstrap_samples),
        "--min-n",
        str(minimum_bets),
        "--score-set",
        "all",
        "--candidate-edges-csv",
        str(candidate_edges_csv),
    ]
    return argv
