"""Strict sweep requests and DB/model-load-free planning.

Stats name model components; all combos supported by those components are also
quoted/evaluated, exactly as the replay combo seam derives them. Identifiers are
case-sensitive tokens, never stripped, lowercased, padded, or repaired.
"""
from __future__ import annotations

import copy
import importlib.metadata
import importlib.util
import json
import math
import os
from dataclasses import asdict, dataclass, field
from datetime import date
from itertools import product
from pathlib import Path

from src.backtesting.nba.historical_execution import (
    BOUNDED_HISTORICAL_SOURCE_FILES,
    HISTORICAL_SOURCE_FILES,
    load_attachment,
    recheck_attachment,
    source_policy,
    validate_artifact_window,
    validate_window,
)
from src.config.combo_config import COMBO_DEFINITIONS
from src.models.black_litterman import BLConfig
from src.models.features.nba.acquisition import AcquisitionLimits
from src.models.nba_artifacts import (
    _SAFE_FILENAME,
    SUPPORTED_STATS,
    ArtifactInspection,
    canonical_sha256,
    sha256_file,
    source_git_state,
    validate_artifact_bundle,
)

BOOKMAKERS = (
    "draftkings", "fanduel", "betmgm", "betrivers", "bovada", "williamhill_us",
    "betonlineag", "unibet_us", "mybookieag", "pointsbetus", "fanatics", "barstool",
    "wynnbet", "ballybet", "betopenly", "betparx", "espnbet", "fliff", "hardrockbet",
    "novig", "prophetx", "rebet", "windcreek",
)
READINESS = {
    "historical_readiness": "unverified", "decision_grade": False,
    "unresolved": ["historical timing", "source policy", "loader completeness", "query tie ordering"],
}
DEPENDENCIES = {
    "numpy": "numpy", "pandas": "pandas", "scipy": "scipy", "sqlalchemy": "SQLAlchemy",
    "joblib": "joblib", "xgboost": "xgboost", "sklearn": "scikit-learn", "psycopg2": "psycopg2-binary",
}
SOURCE_FILES = (
    "src/backtesting/nba/__init__.py", "src/backtesting/nba/sweep_config.py",
    "src/backtesting/nba/sweep_runner.py", "src/backtesting/nba/run_nba_sweep.py",
    "src/backtesting/backtest_harness.py", "src/backtesting/bet_simulator.py",
    "src/backtesting/performance_metrics.py", "src/models/nba_artifacts.py",
    "src/models/monte_carlo.py", "src/models/quantile_trainer.py",
    "src/models/feature_store.py", "src/config/combo_config.py", "src/models/black_litterman.py",
)


def finite_number(value: object, label: str, low: float, high: float | None = None,
                  *, positive: bool = False) -> float:
    if not isinstance(value, int | float) or type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(f"{label} must be a finite real number")
    if value < low or (positive and value == low) or (high is not None and value > high):
        raise ValueError(f"{label} is outside its supported domain")
    return float(value)


def _unique(values: tuple, label: str, *, empty: bool = False) -> None:
    if type(values) is not tuple or (not empty and not values):
        raise ValueError(f"{label} must be an ordered {'possibly empty' if empty else 'nonempty'} tuple")
    try:
        distinct = len(set(values))
    except TypeError:
        raise ValueError(f"Malformed {label} values") from None
    if distinct != len(values):
        raise ValueError(f"Duplicate {label} values are not allowed")


@dataclass(frozen=True)
class CellConfig:
    edge_threshold: float
    tau: float | None = None
    z_max: float | None = None
    max_weight: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "edge_threshold", finite_number(self.edge_threshold, "edge", 0, 1))
        if self.tau is None:
            if self.z_max is not None or self.max_weight is not None:
                raise ValueError("Raw controls cannot carry BL dimensions")
        else:
            # BLConfig has no validator. Its consumers divide by z_max and use
            # min(tau * confidence, max_weight) as a logit interpolation weight.
            object.__setattr__(self, "tau", finite_number(self.tau, "tau", 0))
            object.__setattr__(self, "z_max", finite_number(self.z_max, "z_max", 0, positive=True))
            object.__setattr__(self, "max_weight", finite_number(self.max_weight, "max_weight", 0, 1))

    def to_dict(self) -> dict:
        return {**asdict(self), "raw_control": self.tau is None}

    @property
    def cell_id(self) -> str:
        return "cell_" + canonical_sha256(self.to_dict())

    def bl_config(self) -> BLConfig | None:
        if self.tau is None:
            return None
        assert self.z_max is not None and self.max_weight is not None
        return BLConfig(tau=self.tau, z_max=self.z_max, max_weight=self.max_weight)


def _validate_output_location(model_dir: str, output_dir: str) -> None:
    """Keep final and incomplete output paths outside the exact model bundle."""
    model = Path(model_dir).resolve(strict=False)
    literal = Path(output_dir)
    final = literal.resolve(strict=False)
    candidates = (
        final,
        literal.with_name(literal.name + "_incomplete").resolve(strict=False),
        final.with_name(final.name + "_incomplete").resolve(strict=False),
    )
    if any(path.is_relative_to(model) for path in candidates):
        raise ValueError("Sweep final and incomplete output paths must be outside the model artifact")


@dataclass(frozen=True)
class SweepConfig:
    model_dir: str
    start: date
    end: date
    stats: tuple[str, ...]
    bookmakers: tuple[str, ...]
    n_samples: int
    seed: int
    min_minutes_avg: float
    starting_bankroll: float
    flat_bet_size: float
    edge_thresholds: tuple[float, ...]
    output_dir: str
    allowed_bets: tuple[tuple[str, str], ...] | None = None
    bl_taus: tuple[float, ...] = ()
    bl_z_max: tuple[float, ...] = (1.0,)
    bl_max_weight: tuple[float, ...] = (0.5,)
    max_cells: int = 64
    local: bool = False
    diagnostic_history: bool = False
    historical_policy: str | None = None
    historical_source_metadata: str | None = None
    acquisition_limits: AcquisitionLimits | None = None
    _historical_json: str = field(init=False, repr=False, default='null')

    def __post_init__(self) -> None:
        for name in ("model_dir", "output_dir"):
            value = getattr(self, name)
            if type(value) is not str or not value or "\x00" in value:
                raise ValueError(f"{name} must be an explicit path")
        # Inspect the literal final token, before Path can erase a trailing slash or dot.
        name = self.output_dir.replace("\\", "/").rsplit("/", 1)[-1]
        if not _SAFE_FILENAME.fullmatch(name) or name.endswith("_incomplete"):
            raise ValueError("output_dir must name a new final run directory, not an incomplete directory")
        _validate_output_location(self.model_dir, self.output_dir)
        if type(self.start) is not date or type(self.end) is not date or self.end < self.start:
            raise ValueError("Require inclusive ISO dates with start <= end")
        for label, supported in (("stats", SUPPORTED_STATS), ("bookmakers", BOOKMAKERS)):
            values = getattr(self, label)
            _unique(values, label)
            if any(type(value) is not str or value not in supported for value in values):
                raise ValueError(f"Unsupported literal {label} token")
        for label, low, high in (("n_samples", 1, None), ("seed", 0, 2**32 - 1), ("max_cells", 1, None)):
            value = getattr(self, label)
            # MonteCarloPredictor uses numpy.random.RandomState, not default_rng.
            if type(value) is not int or value < low or (high is not None and value > high):
                raise ValueError(f"Invalid {label}")
        for label, positive in (("min_minutes_avg", False), ("starting_bankroll", True), ("flat_bet_size", True)):
            object.__setattr__(self, label, finite_number(getattr(self, label), label, 0, positive=positive))
        for label, low, high, positive in (
            ("edge_thresholds", 0, 1, False), ("bl_taus", 0, None, False),
            ("bl_z_max", 0, None, True), ("bl_max_weight", 0, 1, False),
        ):
            values = getattr(self, label)
            _unique(values, label, empty=label == "bl_taus")
            normalized = tuple(finite_number(value, label, low, high, positive=positive) for value in values)
            _unique(normalized, label, empty=label == "bl_taus")
            object.__setattr__(self, label, normalized)
        for label in ("local", "diagnostic_history"):
            if type(getattr(self, label)) is not bool:
                raise ValueError(f"{label} must be boolean")
        if self.allowed_bets is not None:
            _unique(self.allowed_bets, "allowed_bets")
            for pair in self.allowed_bets:
                if (type(pair) is not tuple or len(pair) != 2 or pair[0] not in self.market_stats
                        or pair[1] not in ("over", "under")):
                    raise ValueError("allowed_bets must be exact supported stat:over/stat:under pairs")
        count = len(self.edge_thresholds) * (1 + len(self.bl_taus) * len(self.bl_z_max) * len(self.bl_max_weight))
        if count > self.max_cells:
            raise ValueError(f"Requested {count} cells exceeds max_cells={self.max_cells}")
        historical = load_attachment(self.historical_policy, self.historical_source_metadata, local=self.local)
        validate_window(historical, self.start, self.end)
        if historical is not None:
            if not isinstance(self.acquisition_limits, AcquisitionLimits):
                raise ValueError('Native historical sweeps require explicit acquisition limits')
            self.acquisition_limits.check_span(self.start, self.end)
        elif self.acquisition_limits is not None:
            raise ValueError('Acquisition limits require historical policy')
        if historical is not None:
            for item in (historical['policy'], historical['source_metadata']):
                if item['path'] is not None:
                    target = Path(item['path'])
                    output = Path(self.output_dir).resolve(strict=False)
                    if any(target.is_relative_to(candidate) for candidate in
                           (output, output.with_name(output.name + '_incomplete'))):
                        raise ValueError('Historical input collides with sweep output')
        object.__setattr__(self, '_historical_json', json.dumps(historical, sort_keys=True, allow_nan=False))

    @property
    def historical(self) -> dict | None:
        return json.loads(self._historical_json)

    @property
    def market_stats(self) -> tuple[str, ...]:
        return self.stats + tuple(stat for stat, definition in COMBO_DEFINITIONS.items()
                                  if set(definition["components"]).issubset(self.stats))

    def cells(self) -> tuple[CellConfig, ...]:
        cells = []
        for edge in self.edge_thresholds:
            cells.append(CellConfig(edge))
            cells.extend(CellConfig(edge, tau, z_max, weight)
                         for tau, z_max, weight in product(self.bl_taus, self.bl_z_max, self.bl_max_weight))
        return tuple(cells)

    def to_dict(self) -> dict:
        value = asdict(self)
        if self.acquisition_limits is None:
            value.pop('acquisition_limits')
        for name in ('historical_policy', 'historical_source_metadata', '_historical_json'):
            value.pop(name)
        if self.historical is not None:
            value['historical'] = self.historical
        value["start"], value["end"] = self.start.isoformat(), self.end.isoformat()
        for name in ("stats", "bookmakers", "edge_thresholds", "bl_taus", "bl_z_max", "bl_max_weight"):
            value[name] = list(value[name])
        value["allowed_bets"] = [list(pair) for pair in self.allowed_bets] if self.allowed_bets is not None else None
        value["market_stats"] = list(self.market_stats)
        return value


def predictor_settings(config: SweepConfig, inspection: ArtifactInspection) -> dict:
    # Import definitions only. No predictor/model/engine is constructed here.
    from src.models.monte_carlo import (
        DEFAULT_BLOWOUT_CONFIG,
        DEFAULT_CORRELATION_CONFIG,
        DEFAULT_TAIL_ADJUSTMENT,
        DEFAULT_VARIANCE_INFLATION,
        load_copula_params,
    )

    copula = load_copula_params(str(inspection.path))
    if copula is not None:
        if not isinstance(copula, dict) or not copula or any(stat not in SUPPORTED_STATS for stat in copula):
            raise ValueError("Malformed copula settings")
        for value in copula.values():
            finite_number(value, "copula correlation", -1, 1)
    return copy.deepcopy({
        "n_samples": config.n_samples, "random_state": config.seed, "use_correlated_sampling": True,
        "correlation_config": DEFAULT_CORRELATION_CONFIG, "variance_inflation": DEFAULT_VARIANCE_INFLATION,
        "tail_adjustment": DEFAULT_TAIL_ADJUSTMENT, "blowout_config": DEFAULT_BLOWOUT_CONFIG,
        "copula_params": copula, "combined_calibration_offsets": None,
    })


def _acquisition_settings(config: SweepConfig) -> dict:
    fixed = config.to_dict()
    result = {name: fixed[name] for name in (
        "model_dir", "start", "end", "stats", "market_stats", "bookmakers", "n_samples", "seed",
        "min_minutes_avg", "local",
    )}
    if config.acquisition_limits is not None:
        result['acquisition_limits'] = config.acquisition_limits.to_dict()
    return result


def _acquisition_identity(plan: dict) -> str:
    return canonical_sha256({
        "settings": plan["acquisition_settings"], "artifact": plan["artifact"],
        "predictor": plan["predictor_settings"], "features": plan["ordered_features"],
        "source": plan["source"], "policy": plan["source_policy"],
    })


def validate_plan(config: SweepConfig, plan: dict) -> None:
    """Check an already inspected plan in memory; no artifact/DB/model rereads.

    Hashes are content commitments, not signatures. Recomputing a request hash
    cannot make an old bundle eligible for a new plan: its observed acquisition
    commitments are checked separately by SharedInputs.
    """
    keys = {"schema_version", "fixed_config", "artifact", "predictor_settings", "dependencies", "source",
            "ordered_features", "source_policy", "cells", "cell_count", "acquisition_settings",
            "acquisition_identity", "request_identity", *READINESS}
    if type(plan) is not dict or set(plan) != keys:
        raise ValueError("Malformed sweep plan")
    expected = {"schema_version": 2 if config.historical else 1, "fixed_config": config.to_dict(),
                "cells": [{"cell_id": cell.cell_id, **cell.to_dict()} for cell in config.cells()],
                "cell_count": len(config.cells()), "acquisition_settings": _acquisition_settings(config),
                **READINESS}
    if any(canonical_sha256(plan[key]) != canonical_sha256(value) for key, value in expected.items()):
        raise ValueError("Sweep config/plan disagreement")
    if plan['source_policy'] != source_policy(config.historical):
        raise ValueError('Historical source-policy binding disagreement')
    if (plan["predictor_settings"]["n_samples"] != config.n_samples
            or plan["predictor_settings"]["random_state"] != config.seed
            or plan["predictor_settings"]["combined_calibration_offsets"] is not None
            or plan["artifact"]["state"] != "verified_manifest"
            or plan["artifact"]["identity_verified"] is not True
            or plan["artifact"]["path"] != str(Path(config.model_dir).resolve(strict=False))):
        raise ValueError("Inconsistent acquisition plan")
    if (plan["acquisition_identity"] != _acquisition_identity(plan)
            or plan["request_identity"] != canonical_sha256({k: v for k, v in plan.items()
                                                           if k != "request_identity"})):
        raise ValueError("Invalid canonical plan identity")


def build_plan(config: SweepConfig, *, check_output_collision: bool = True) -> dict:
    """Validate exact artifact bytes and prerequisites; never deserialize a model."""
    # Re-resolve at planning/execution preflight in case a path alias changed
    # after constructing this otherwise immutable configuration.
    _validate_output_location(config.model_dir, config.output_dir)
    # Advisory snapshot only: execution still uses the helper's exclusive mkdir.
    destination = Path(config.output_dir)
    if check_output_collision and (os.path.lexists(destination)
                                   or os.path.lexists(destination.with_name(destination.name + "_incomplete"))):
        raise FileExistsError("Sweep destination or incomplete destination already exists")
    versions = {}
    for module, distribution in DEPENDENCIES.items():
        if importlib.util.find_spec(module) is None:
            raise ValueError(f"Missing runtime dependency: {module}")
        try:
            versions[module] = {"distribution": distribution, "version": importlib.metadata.version(distribution)}
        except importlib.metadata.PackageNotFoundError:
            # The project also supports the source-built psycopg2 distribution;
            # both provide the same import, neither may be assumed installed.
            if module != "psycopg2":
                raise
            versions[module] = {"distribution": "psycopg2", "version": importlib.metadata.version("psycopg2")}
    inspection = validate_artifact_bundle(config.model_dir, config.stats, allow_legacy=False)
    if config.historical is not None:
        recheck_attachment(config.historical)
        validate_artifact_window(inspection, config.start)
    root = Path(__file__).resolve().parents[3]
    files = SOURCE_FILES + (HISTORICAL_SOURCE_FILES + BOUNDED_HISTORICAL_SOURCE_FILES if config.historical else ())
    source = {"base": source_git_state(root), "files": {name: sha256_file(root / name) for name in files}}
    settings = predictor_settings(config, inspection)
    body = {
        "schema_version": 2 if config.historical else 1, "fixed_config": config.to_dict(), "artifact": inspection.to_plan_dict(),
        "predictor_settings": settings, "dependencies": versions, "source": source,
        "ordered_features": inspection.manifest["features"]["ordered"],
        "source_policy": source_policy(config.historical),
        "cells": [{"cell_id": cell.cell_id, **cell.to_dict()} for cell in config.cells()],
        "cell_count": len(config.cells()), **copy.deepcopy(READINESS),
    }
    body["acquisition_settings"] = _acquisition_settings(config)
    body["acquisition_identity"] = _acquisition_identity(body)
    body["request_identity"] = canonical_sha256(body)
    validate_plan(config, body)
    return body
