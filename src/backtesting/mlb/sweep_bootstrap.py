"""Runtime bootstrap helpers for the MLB backtest sweep CLI.

This module owns model-directory resolution and DB/model/feature-store object
construction for the CLI runner. It deliberately does not run shared phases,
execute sweep configs, compute edges, or serialize results.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from src.db.client import get_engine
from src.models.mlb.mlb_batter_feature_store import MLBBatterFeatureStore
from src.models.mlb.mlb_feature_store import MLBFeatureStore
from src.models.mlb.mlb_model_suite import MLBModelSuite

logger = logging.getLogger("MLBBacktestSweep")


@dataclass
class SweepRuntime:
    """Runtime objects needed by the MLB sweep after CLI parsing."""

    engine: Any
    pitcher_feature_store: Any
    batter_feature_store: Any | None
    suite: Any
    model_path: Path


def find_latest_model_dir(base_dir: str) -> Path:
    """Find the best model directory for the suite.

    Priority:
    1. production/ subdirectory (unified suite location)
    2. Model files directly in base_dir (legacy)
    3. Latest mlb_run_* directory
    """
    base = Path(base_dir)
    if not base.exists():
        raise FileNotFoundError(f"Artifacts directory not found: {base}")

    prod = base / "production"
    if prod.exists() and prod.is_dir():
        return prod

    if (base / "pitcher_k_model.joblib").exists():
        return base
    if any(base.glob("*_binomial_booster.json")):
        return base

    runs = sorted([
        d for d in base.iterdir()
        if d.is_dir() and d.name.startswith("mlb_run_") and not d.name.endswith("_incomplete")
    ])
    if not runs:
        raise FileNotFoundError(f"No model directories found in {base}")
    return runs[-1]


def initialize_sweep_runtime(
    cli_config,
    *,
    get_engine_fn: Callable[..., Any] = get_engine,
    pitcher_feature_store_cls: type | Callable[..., Any] = MLBFeatureStore,
    batter_feature_store_cls: type | Callable[..., Any] = MLBBatterFeatureStore,
    suite_cls: type = MLBModelSuite,
    log: logging.Logger = logger,
) -> SweepRuntime:
    """Construct DB engine, feature stores, model suite, and selected model path."""
    engine = get_engine_fn(local=cli_config.local)
    if cli_config.local:
        log.info("Using LOCAL database")

    pitcher_feature_store = pitcher_feature_store_cls(engine)

    model_path = find_latest_model_dir(cli_config.model_dir)
    log.info(f"Using model directory: {model_path}")

    suite = suite_cls.from_directory(model_path, n_samples=cli_config.n_samples)
    log.info(f"Suite loaded stats: {suite.available_stats}")

    has_batter_stats = any(s.startswith("batter_") and suite.has_stat(s) for s in cli_config.stats)
    batter_feature_store = batter_feature_store_cls(engine) if has_batter_stats else None

    return SweepRuntime(
        engine=engine,
        pitcher_feature_store=pitcher_feature_store,
        batter_feature_store=batter_feature_store,
        suite=suite,
        model_path=model_path,
    )
