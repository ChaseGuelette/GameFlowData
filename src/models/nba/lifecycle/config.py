"""Strict, identity-preserving NBA lifecycle configuration (no runtime imports)."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, StrictFloat, StrictInt, model_validator
from yaml.events import AliasEvent

from src.models.features.nba.acquisition import AcquisitionLimits
from src.models.nba_artifacts import canonical_json_bytes, canonical_sha256, sha256_file
from src.models.nba_training_chronology import validate_seasons

Number = StrictInt | StrictFloat
CHECKS = (
    "integrity", "concentration", "coverage", "timing", "clv", "intraday_stability",
    "source_verified_dropout", "historical_readiness", "promotion", "input_content_reconstruction",
    "execution_setting_application",
)


def exact_date(value: object) -> date:
    if type(value) is date:
        return value
    if type(value) is str and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        return date.fromisoformat(value)
    raise ValueError("Expected an exact ISO date")


Day = Annotated[date, BeforeValidator(exact_date)]


class UniqueSafeLoader(yaml.SafeLoader):
    """Reject aliases, merge keys, non-string and duplicate mapping keys."""

    def compose_node(self, parent, index):
        if self.check_event(AliasEvent):
            raise ValueError("YAML aliases are forbidden")
        return super().compose_node(parent, index)

    def construct_mapping(self, node, deep=False):
        result = {}
        for key_node, value_node in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                raise ValueError("YAML merge keys are forbidden")
            key = self.construct_object(key_node, deep=deep)
            if type(key) is not str or key in result:
                raise ValueError("Invalid or duplicate YAML mapping key")
            result[key] = self.construct_object(value_node, deep=deep)
        return result


def unique_json(pairs: list) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def read_json(path: Path):
    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_json,
                       parse_constant=lambda _: (_ for _ in ()).throw(ValueError("Nonfinite JSON")))
    canonical_sha256(value)
    return value


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False, frozen=True)

    @model_validator(mode="after")
    def unique_lists(self):
        for name in type(self).model_fields:
            value = getattr(self, name)
            if type(value) is list and len({canonical_sha256(item) for item in value}) != len(value):
                raise ValueError("Lists must be ordered and unique")
        return self


class Experiment(StrictModel):
    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$", max_length=100)
    purpose: Literal["discovery", "independent_validation", "finalist_certification"]
    output_root: str


class Artifact(StrictModel):
    mode: Literal["attach", "train"]
    path: str | None = None

    @model_validator(mode="after")
    def mode_path(self):
        if self.mode == "attach" and not self.path or self.mode == "train" and "path" in self.model_fields_set:
            raise ValueError("Artifact mode/path conflict")
        return self


class Training(StrictModel):
    train_seasons: list[str] = Field(min_length=1)
    calibration_season: str
    calibration_end: Day
    tune: bool
    tuning_trials: int = Field(gt=0)
    tuning_timeout: int | None
    hyperparams_path: str | None
    feature_tolerance: Number = Field(ge=0)
    local: bool

    @model_validator(mode="after")
    def settings(self):
        validate_seasons(self.train_seasons, self.calibration_season)
        if any(re.fullmatch(r"2[0-9]{4}", season) is None
               for season in [*self.train_seasons, self.calibration_season]):
            raise ValueError("Lifecycle training requires regular-season NBA tokens")
        if not self.local or self.tune and self.hyperparams_path is not None:
            raise ValueError("Local execution required; tuning and supplied hyperparameters conflict")
        if self.tuning_timeout is not None and self.tuning_timeout <= 0:
            raise ValueError("Invalid tuning timeout")
        return self


class Evaluation(StrictModel):
    start: Day
    end: Day
    stats: list[Literal["pts", "reb", "ast"]] = Field(min_length=1)
    bookmakers: list[str] = Field(min_length=1)
    n_samples: int = Field(ge=1)
    seed: int = Field(ge=0, le=4294967295)
    min_minutes_avg: Number = Field(ge=0)
    starting_bankroll: Number = Field(gt=0)
    flat_bet_size: Number = Field(gt=0)
    edge_thresholds: list[Number] = Field(min_length=1)
    allowed_bets: list[str] | None = None
    bl_taus: list[Number] = Field(default_factory=list)
    bl_z_max: list[Number] = Field(default_factory=lambda: [1.0])
    bl_max_weight: list[Number] = Field(default_factory=lambda: [0.5])
    max_cells: int = Field(ge=1, le=64)
    local: bool
    diagnostic_history: bool

    @model_validator(mode="after")
    def posture(self):
        if self.end < self.start or not self.local or not self.diagnostic_history:
            raise ValueError("Invalid evaluation window or execution posture")
        return self


class Audit(StrictModel):
    checks: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def supported(self):
        if "integrity" not in self.checks or any(item not in CHECKS for item in self.checks):
            raise ValueError("Mandatory integrity or supported audit checks missing")
        # The evidence API always places mandatory integrity first.
        if self.checks[0] != "integrity":
            raise ValueError("Integrity must be the first requested check")
        return self


class Ranker(StrictModel):
    mode: Literal["disabled"]


class Decision(StrictModel):
    require_integrity_pass: bool = True
    require_requested_audits_pass: bool = True

    @model_validator(mode="after")
    def mandatory(self):
        if not self.require_integrity_pass or not self.require_requested_audits_pass:
            raise ValueError("Evidence enforcement cannot be disabled")
        return self


class HistoricalLimits(StrictModel):
    max_dates: int = Field(gt=0)
    max_games: int = Field(gt=0)
    max_population_rows: int = Field(gt=0)
    population_timeout_ms: int = Field(gt=0)


class HistoricalInputs(StrictModel):
    policy: str
    source_metadata: str | None = None
    acquisition_limits: HistoricalLimits


class LifecycleConfig(StrictModel):
    schema_version: int
    experiment: Experiment
    artifact: Artifact
    training: Training | None = None
    evaluation: Evaluation
    audit: Audit
    ranker: Ranker
    decision: Decision
    historical: HistoricalInputs | None = None

    @model_validator(mode="after")
    def cross_fields(self):
        if self.schema_version != 1:
            raise ValueError("Unsupported lifecycle schema")

        if self.artifact.mode == "attach" and "training" in self.model_fields_set:
            raise ValueError("Attach forbids training settings")
        if self.artifact.mode == "train":
            if self.training is None:
                raise ValueError("Train requires complete training settings")
            if self.evaluation.start < self.training.calibration_end:
                raise ValueError("Evaluation overlaps requested exclusive calibration end")
        return self


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def resolve_path(token: str) -> Path:
    if not token or token != token.strip() or any(ord(char) < 32 for char in token):
        raise ValueError("Invalid path token")
    path = Path(token)
    return (path if path.is_absolute() else repo_root() / path).resolve(strict=False)


def disjoint(left: Path, right: Path) -> bool:
    return not (left.is_relative_to(right) or right.is_relative_to(left))


@dataclass(frozen=True, init=False)
class ResolvedConfig:
    """Canonical immutable bytes are authoritative; callers get detached views."""

    run_root: Path
    artifact_path: Path | None
    hyperparams_path: Path | None
    _snapshot: bytes

    def __init__(self, config: LifecycleConfig, run_root: Path, artifact_path: Path | None,
                 hyperparams_path: Path | None, hyperparams_receipt: dict | None):
        # Revalidate a detached value even if a caller mutated a Pydantic list.
        validated = LifecycleConfig.model_validate(config.model_dump(mode="python", exclude_unset=True))
        values = validated.model_dump(mode="json")
        # Mode-exclusive absent fields must remain absent when revalidated.
        if validated.artifact.mode == "train":
            values["artifact"].pop("path", None)
        else:
            values.pop("training", None)
        snapshot = {"config": values, "run_root": str(run_root),
                    "artifact_path": str(artifact_path) if artifact_path else None,
                    "hyperparams_path": str(hyperparams_path) if hyperparams_path else None,
                    "hyperparams_receipt": hyperparams_receipt}
        if validated.historical is None:
            values.pop('historical', None)
        else:
            from src.backtesting.nba.historical_execution import (
                load_attachment,
                validate_training_window,
                validate_window,
            )

            inputs = validated.historical
            historical = load_attachment(str(resolve_path(inputs.policy)),
                str(resolve_path(inputs.source_metadata)) if inputs.source_metadata is not None else None,
                local=validated.evaluation.local)
            validate_window(historical, validated.evaluation.start, validated.evaluation.end)
            AcquisitionLimits(**inputs.acquisition_limits.model_dump()).check_span(
                validated.evaluation.start, validated.evaluation.end)
            if validated.training is not None:
                training = validated.training
                validate_training_window(historical, training.train_seasons, training.calibration_season,
                                         training.calibration_end)
            for item in (historical['policy'], historical['source_metadata']):
                if item['path'] is not None and any(not disjoint(candidate, Path(item['path'])) for candidate in
                        (run_root, run_root.with_name(run_root.name + '_dry_run'))):
                    raise ValueError('Lifecycle output collides with historical input')
            snapshot['historical_attachment'] = historical
        object.__setattr__(self, "run_root", run_root)
        object.__setattr__(self, "artifact_path", artifact_path)
        object.__setattr__(self, "hyperparams_path", hyperparams_path)
        object.__setattr__(self, "_snapshot", canonical_json_bytes(snapshot))

    @property
    def config(self) -> LifecycleConfig:
        return LifecycleConfig.model_validate(self.to_dict()["config"])

    @property
    def hyperparams_receipt(self) -> dict | None:
        return self.to_dict()["hyperparams_receipt"]

    @property
    def historical_attachment(self) -> dict | None:
        return self.to_dict().get('historical_attachment')

    @property
    def acquisition_limits(self) -> AcquisitionLimits | None:
        historical = self.config.historical
        return AcquisitionLimits(**historical.acquisition_limits.model_dump()) if historical else None

    def to_dict(self) -> dict:
        return json.loads(self._snapshot)

    @property
    def config_identity(self) -> str:
        return canonical_sha256(self.to_dict())

    def sweep_config(self, *, artifact_dir: Path, output_dir: Path):
        from src.backtesting.nba.sweep_config import SweepConfig

        values = self.config.evaluation.model_dump()
        for key in ("stats", "bookmakers", "edge_thresholds", "bl_taus", "bl_z_max", "bl_max_weight"):
            values[key] = tuple(values[key])
        if values["allowed_bets"] is not None:
            values["allowed_bets"] = tuple(tuple(token.split(":")) for token in values["allowed_bets"])
        if self.historical_attachment is not None:
            from src.backtesting.nba.historical_execution import recheck_attachment

            receipt = self.historical_attachment
            recheck_attachment(receipt)
            values.update(historical_policy=receipt['policy']['path'],
                          historical_source_metadata=receipt['source_metadata']['path'],
                          acquisition_limits=self.acquisition_limits)
        return SweepConfig(model_dir=str(artifact_dir), output_dir=str(output_dir), **values)


def load_config(path: str | Path) -> ResolvedConfig:
    with resolve_path(str(path)).open(encoding="utf-8") as handle:
        raw = yaml.load(handle, Loader=UniqueSafeLoader)
    config = LifecycleConfig.model_validate(raw)
    root = resolve_path(config.experiment.output_root) / config.experiment.name
    if root.name.endswith("_incomplete") or root.name in (".", ".."):
        raise ValueError("Invalid run directory")
    artifact = resolve_path(config.artifact.path) if config.artifact.path else None
    hp = None
    receipt = None
    if config.training and config.training.hyperparams_path is not None:
        from src.models.training_preflight import validate_hyperparams_file

        hp = resolve_path(config.training.hyperparams_path)
        data = read_json(hp)
        _, validated = validate_hyperparams_file(hp)
        if canonical_sha256(data) != canonical_sha256(validated):
            raise ValueError("Hyperparameter input changed")
        receipt = {"sha256": sha256_file(hp), "canonical_sha256": canonical_sha256(data)}
    for target in (artifact, hp, resolve_path(str(path))):
        if target is not None and any(not disjoint(candidate, target) for candidate in
                                      (root, root.with_name(root.name + "_dry_run"))):
            raise ValueError("Lifecycle output collides with an input")
    resolved = ResolvedConfig(config, root, artifact, hp, receipt)
    # Delegate numeric/cell/bookmaker domains to the actual validator, lazily.
    resolved.sweep_config(artifact_dir=artifact or root / "training" / "planned_bundle",
                          output_dir=root / "sweep" / "attempt-0001" / "output")
    return resolved
