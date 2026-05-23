"""Tests for MLB sweep bootstrap/model initialization seam."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from src.backtesting.mlb.sweep_bootstrap import find_latest_model_dir, initialize_sweep_runtime


def test_find_latest_model_dir_prefers_production_directory(tmp_path):
    base = tmp_path / "artifacts"
    base.mkdir()
    (base / "production").mkdir()
    (base / "mlb_run_20250101").mkdir()

    assert find_latest_model_dir(str(base)) == base / "production"


def test_find_latest_model_dir_falls_back_to_latest_complete_run(tmp_path):
    base = tmp_path / "artifacts"
    base.mkdir()
    (base / "mlb_run_20250101").mkdir()
    (base / "mlb_run_20250103_incomplete").mkdir()
    (base / "mlb_run_20250102").mkdir()

    assert find_latest_model_dir(str(base)) == base / "mlb_run_20250102"


def test_initialize_sweep_runtime_constructs_engine_suite_and_needed_feature_stores(tmp_path):
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    (model_dir / "production").mkdir()
    calls = []

    def fake_get_engine(*, local):
        calls.append(("engine", local))
        return "engine"

    class FakePitcherStore:
        def __init__(self, engine):
            calls.append(("pitcher_store", engine))

    class FakeBatterStore:
        def __init__(self, engine):
            calls.append(("batter_store", engine))

    class FakeSuite:
        available_stats = ["pitcher_strikeouts", "batter_hits"]

        @classmethod
        def from_directory(cls, path, *, n_samples):
            calls.append(("suite", Path(path), n_samples))
            return cls()

        def has_stat(self, stat):
            return stat in self.available_stats

    cli_config = SimpleNamespace(
        local=True,
        model_dir=str(model_dir),
        n_samples=250,
        stats=["pitcher_strikeouts", "batter_hits"],
    )

    runtime = initialize_sweep_runtime(
        cli_config,
        get_engine_fn=fake_get_engine,
        pitcher_feature_store_cls=FakePitcherStore,
        batter_feature_store_cls=FakeBatterStore,
        suite_cls=FakeSuite,
    )

    assert runtime.engine == "engine"
    assert isinstance(runtime.pitcher_feature_store, FakePitcherStore)
    assert isinstance(runtime.batter_feature_store, FakeBatterStore)
    assert isinstance(runtime.suite, FakeSuite)
    assert runtime.model_path == model_dir / "production"
    assert calls == [
        ("engine", True),
        ("pitcher_store", "engine"),
        ("suite", model_dir / "production", 250),
        ("batter_store", "engine"),
    ]


def test_initialize_sweep_runtime_skips_batter_store_without_requested_batter_stats(tmp_path):
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    (model_dir / "pitcher_k_model.joblib").write_text("stub")

    class FakeSuite:
        available_stats = ["pitcher_strikeouts"]

        @classmethod
        def from_directory(cls, path, *, n_samples):
            return cls()

        def has_stat(self, stat):
            return stat in self.available_stats

    cli_config = SimpleNamespace(
        local=False,
        model_dir=str(model_dir),
        n_samples=100,
        stats=["pitcher_strikeouts"],
    )

    runtime = initialize_sweep_runtime(
        cli_config,
        get_engine_fn=lambda *, local: "engine",
        pitcher_feature_store_cls=lambda engine: f"pitcher:{engine}",
        batter_feature_store_cls=lambda engine: f"batter:{engine}",
        suite_cls=FakeSuite,
    )

    assert runtime.pitcher_feature_store == "pitcher:engine"
    assert runtime.batter_feature_store is None
    assert runtime.model_path == model_dir
