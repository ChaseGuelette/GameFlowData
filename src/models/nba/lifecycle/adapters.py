"""Pure argv adapters for the actual NBA CLIs; no execution or runtime imports."""
from __future__ import annotations

import sys
from pathlib import Path

from src.models.nba.lifecycle.config import ResolvedConfig, repo_root

__all__ = ["repo_root", "build_training_command", "build_sweep_command", "build_audit_command"]


def build_training_command(resolved: ResolvedConfig, *, artifact_root: Path) -> list[str]:
    training = resolved.config.training
    if resolved.config.artifact.mode != "train" or training is None:
        raise ValueError("Attach cannot launch training")
    argv = [sys.executable, "-m", "src.models.train_pipeline", "--train-seasons", *training.train_seasons,
            "--cal-season", training.calibration_season, "--cal-end-date", training.calibration_end.isoformat(),
            "--artifacts-dir", str(artifact_root), "--feature-tolerance", str(training.feature_tolerance),
            "--tuning-trials", str(training.tuning_trials), "--local"]
    if training.tuning_timeout is not None:
        argv += ["--tuning-timeout", str(training.tuning_timeout)]
    if training.tune:
        argv += ["--tune"]
    if resolved.hyperparams_path is not None:
        argv += ["--hyperparams-path", str(resolved.hyperparams_path)]
    return argv


def build_sweep_command(resolved: ResolvedConfig, *, artifact_dir: Path, output_dir: Path) -> list[str]:
    evaluation = resolved.config.evaluation
    argv = [sys.executable, "-m", "src.backtesting.nba.run_nba_sweep", "--model-dir", str(artifact_dir),
            "--output-dir", str(output_dir), "--start", evaluation.start.isoformat(),
            "--end", evaluation.end.isoformat(), "--stats", *evaluation.stats,
            "--bookmakers", *evaluation.bookmakers]
    for name in ("n_samples", "seed", "min_minutes_avg", "starting_bankroll", "flat_bet_size", "max_cells"):
        argv += ["--" + name.replace("_", "-"), str(getattr(evaluation, name))]
    for name in ("edge_thresholds", "allowed_bets", "bl_taus", "bl_z_max", "bl_max_weight"):
        values = getattr(evaluation, name)
        if values:
            argv += ["--" + name.replace("_", "-"), *(str(value) for value in values)]
    return argv + ["--local", "--diagnostic-history"]


def build_audit_command(resolved: ResolvedConfig, *, sweep_dir: Path, output_dir: Path) -> list[str]:
    return [sys.executable, "-m", "src.backtesting.nba.evidence", "--sweep-dir", str(sweep_dir),
            "--checks", *resolved.config.audit.checks, "--output-dir", str(output_dir)]
