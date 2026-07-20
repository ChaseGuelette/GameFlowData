"""Typed configuration and resolution for the MLB lifecycle runner."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.models.mlb.training.feature_controls import expand_feature_families
from src.models.mlb.training.profiles import MLBTrainingProfile, get_training_profile

PurposeLiteral = Literal["discovery", "independent_validation", "finalist_certification"]
ModelBase = Literal["no_prop_line", "with_prop_line"]
AuditMode = Literal["clv_only", "full"]
PITCHER_VARIANTS = {
    "none",
    "static_no_l30",
    "hook_only",
    "ip_only",
    "ip_hook",
    "hook_avg_ip_l30",
    "hook_short_hook_l30",
    "hook_deep_start_l30",
}


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ExperimentConfig(StrictModel):
    """Top-level experiment metadata for one lifecycle run."""

    name: str
    profile: str
    purpose: PurposeLiteral
    output_root: str | None = None


class FeatureControlsConfig(StrictModel):
    """Declarative feature-control request for a single run."""

    mode: Literal["include", "exclude"] = "include"
    families: list[str] = Field(default_factory=list)
    features: list[str] = Field(default_factory=list)


class ModelConfig(StrictModel):
    base: ModelBase
    artifact_dir: str | None = None
    sweep_dir: str | None = None
    sweep_artifact_identity_sha256: str | None = None
    tune: bool = False
    feature_tolerance: float = 0.02
    variant: str | None = None
    tuning_trials: int = 50
    feature_controls: FeatureControlsConfig = Field(default_factory=FeatureControlsConfig)


class TrainingConfig(StrictModel):
    seasons: list[int]
    calibration_season: int
    calibration_end: date


class EvaluationConfig(StrictModel):
    start: date
    end: date
    direction: str | None = None
    edge_thresholds: list[float] = Field(default_factory=lambda: [0.05, 0.08, 0.10])
    flat_bet: float | None = None
    tau: list[float | None] = Field(default_factory=lambda: [None])
    kelly_values: list[float] = Field(default_factory=lambda: [0.125])


class QuotesConfig(StrictModel):
    clean: bool = False
    line_source: str | None = None
    decision_policy: str | None = None
    relative_minutes: int = 60
    routing: str | None = None
    coverage_audit_note: str | None = None


class AuditConfig(StrictModel):
    minimum_bets: int | None = None
    bootstrap_samples: int = 1000
    mode: AuditMode = "clv_only"


class DecisionConfig(StrictModel):
    max_drawdown: float = 0.25
    require_positive_roi: bool = True
    require_positive_mean_clv_ci_low: bool = True
    require_positive_ranker_ci_low: bool = True
    require_edge_bucket_monotonicity: bool = True
    require_independent_window: bool = True


class LifecycleConfig(StrictModel):
    """Raw lifecycle configuration contract before profile resolution."""

    experiment: ExperimentConfig
    model: ModelConfig
    training: TrainingConfig
    evaluation: EvaluationConfig
    quotes: QuotesConfig = Field(default_factory=QuotesConfig)
    audit: AuditConfig = Field(default_factory=AuditConfig)
    decision: DecisionConfig = Field(default_factory=DecisionConfig)

    @field_validator("experiment")
    @classmethod
    def _validate_profile(cls, value: ExperimentConfig) -> ExperimentConfig:
        # This raises a useful error for unknown profile names early.
        get_training_profile(value.profile)
        return value

    @field_validator("evaluation")
    @classmethod
    def _validate_temporal(cls, value: EvaluationConfig) -> EvaluationConfig:
        # Actual bound check depends on training config and therefore happens in
        # `resolve_lifecycle_config` with both sections available.
        return value

    @model_validator(mode="after")
    def _validate_profile_variant(self) -> LifecycleConfig:
        variant = self.model.variant
        if self.experiment.profile.startswith("batter_") and variant is not None:
            raise ValueError("model.variant must be null for batter profiles")
        if self.experiment.profile == "pitcher_strikeouts" and (
            variant is not None and variant not in PITCHER_VARIANTS
        ):
            raise ValueError(
                f"Unsupported pitcher_strikeouts variant {variant!r}; "
                f"expected one of {sorted(PITCHER_VARIANTS)}"
            )
        return self


class FeatureResolution(StrictModel):
    """Expanded, concrete feature-control artifact for manifest output."""

    mode: str
    requested_families: list[str]
    family_features: dict[str, list[str]]
    requested_features: list[str]
    resolved_features: list[str]
    include_features: list[str]
    exclude_features: list[str]
    requested_feature_count: int
    resolved_feature_count: int


class ResolvedLifecycleConfig(BaseModel):
    """Profile-resolved lifecycle config used by adapters and runner stages."""

    experiment_name: str
    profile: str
    profile_stat: str
    profile_obj: MLBTrainingProfile
    experiment: ExperimentConfig
    model: ModelConfig
    training: TrainingConfig
    evaluation: EvaluationConfig
    quotes: QuotesConfig
    audit: AuditConfig
    decision: DecisionConfig
    feature_controls: FeatureResolution
    purpose: PurposeLiteral
    run_root: Path
    artifact_dir: Path
    sweep_dir: Path
    attached_artifact: bool
    attached_sweep: bool

    def to_resolved_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dictionary for resolved_config.yaml."""

        return {
            "experiment": self.experiment.model_dump(mode="json"),
            "purpose": self.purpose,
            "profile": self.profile,
            "profile_stat": self.profile_stat,
            "model": self.model.model_dump(mode="json"),
            "training": self.training.model_dump(mode="json"),
            "evaluation": self.evaluation.model_dump(mode="json"),
            "quotes": self.quotes.model_dump(mode="json"),
            "audit": self.audit.model_dump(mode="json"),
            "decision": self.decision.model_dump(mode="json"),
            "features": self.feature_controls.model_dump(),
            "run_root": str(self.run_root),
            "artifact_dir": str(self.artifact_dir),
            "sweep_dir": str(self.sweep_dir),
            "attached_artifact": self.attached_artifact,
            "attached_sweep": self.attached_sweep,
        }


def _slugify(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "_-" else "_" for ch in value.strip()).strip("_\t\n")


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _resolve_run_root(experiment: ExperimentConfig) -> Path:
    if experiment.output_root:
        return Path(experiment.output_root)
    return Path("backtest_results") / "lifecycle" / _slugify(experiment.name)


def _resolve_feature_controls(profile: MLBTrainingProfile, feature_controls: FeatureControlsConfig) -> FeatureResolution:
    families = _dedupe([str(f).strip().lower() for f in feature_controls.families if str(f).strip()])
    requested_features = _dedupe([str(f).strip() for f in feature_controls.features if str(f).strip()])

    family_features: dict[str, list[str]] = {
        family: expand_feature_families(profile, [family]) for family in families
    } if families else {}

    resolved_from_families = [feature for features in family_features.values() for feature in features]
    resolved_features = _dedupe([*resolved_from_families, *requested_features])

    if feature_controls.mode == "include":
        include_features = list(resolved_features)
        exclude_features: list[str] = []
    elif feature_controls.mode == "exclude":
        include_features = []
        exclude_features = list(resolved_features)
    else:
        include_features = list(resolved_features)
        exclude_features = []

    return FeatureResolution(
        mode=feature_controls.mode,
        requested_families=families,
        family_features={name: list(values) for name, values in family_features.items()},
        requested_features=requested_features,
        resolved_features=resolved_features,
        include_features=include_features,
        exclude_features=exclude_features,
        requested_feature_count=len(requested_features) + len([f for feats in family_features.values() for f in feats]),
        resolved_feature_count=len(resolved_features),
    )


def load_lifecycle_config(config_path: str | Path) -> LifecycleConfig:
    raw = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    if raw is None:
        raise ValueError(f"Empty lifecycle config at {config_path}")
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid lifecycle config format at {config_path}")
    return LifecycleConfig.model_validate(raw)


def _validate_evaluation_timing(evaluation: EvaluationConfig, training: TrainingConfig) -> None:
    if evaluation.end < evaluation.start:
        raise ValueError(
            f"Invalid evaluation window: end {evaluation.end} precedes start {evaluation.start}."
        )
    if evaluation.start <= training.calibration_end:
        raise ValueError(
            f"Invalid evaluation timing for purpose-driven lifecycle: evaluation start {evaluation.start} "
            f"must be after calibration_end {training.calibration_end}."
        )


def resolve_lifecycle_config(config: LifecycleConfig | str | Path) -> ResolvedLifecycleConfig:
    if not isinstance(config, LifecycleConfig):
        config = load_lifecycle_config(config)

    _validate_evaluation_timing(config.evaluation, config.training)

    profile = get_training_profile(config.experiment.profile)
    if config.experiment.purpose in {"independent_validation", "finalist_certification"} and not config.quotes.clean:
        raise ValueError(
            f"Purpose '{config.experiment.purpose}' requires quotes.clean=true. "
            f"Quote-clean must be enabled to run independent-window or finalist stages."
        )

    feature_controls = _resolve_feature_controls(profile, config.model.feature_controls)

    run_root = _resolve_run_root(config.experiment)

    artifact_dir = Path(config.model.artifact_dir) if config.model.artifact_dir else (run_root / "artifacts")
    sweep_dir = Path(config.model.sweep_dir) if config.model.sweep_dir else (run_root / "sweep")

    attached_artifact = config.model.artifact_dir is not None
    attached_sweep = config.model.sweep_dir is not None

    if attached_artifact and artifact_dir.name.endswith("_incomplete"):
        raise ValueError(f"Refusing to attach incomplete artifact directory: {artifact_dir}")
    if attached_sweep and sweep_dir.name.endswith("_incomplete"):
        raise ValueError(f"Refusing to attach incomplete sweep directory: {sweep_dir}")

    if config.quotes.line_source is None:
        config.quotes.line_source = profile.default_line_source
    if config.quotes.decision_policy is None:
        config.quotes.decision_policy = profile.default_quote_policy
    if config.quotes.routing is None:
        config.quotes.routing = profile.default_book_routing_policy
    if config.evaluation.direction is None:
        config.evaluation.direction = profile.default_direction
    if config.audit.minimum_bets is None:
        config.audit.minimum_bets = profile.min_decision_grade_bets

    # Keep resolved values with stable, canonical defaults.
    return ResolvedLifecycleConfig(
        experiment_name=config.experiment.name,
        profile=config.experiment.profile,
        profile_stat=profile.stat_key,
        profile_obj=profile,
        experiment=config.experiment,
        model=config.model,
        training=config.training,
        evaluation=config.evaluation,
        quotes=config.quotes,
        audit=config.audit,
        decision=config.decision,
        feature_controls=feature_controls,
        purpose=config.experiment.purpose,
        run_root=run_root,
        artifact_dir=artifact_dir,
        sweep_dir=sweep_dir,
        attached_artifact=attached_artifact,
        attached_sweep=attached_sweep,
    )
