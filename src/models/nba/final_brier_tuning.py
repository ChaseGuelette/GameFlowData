"""Opt-in NBA REB hyperparameter selection by final-stat Brier score.

This module deliberately reuses the canonical rate-suite trainer and Monte Carlo
sampler.  The external cohort is a development/model-selection population, not
an independent calibration or validation set.
"""

from __future__ import annotations

import copy
import json
import logging
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import optuna
import pandas as pd
from optuna.samplers import TPESampler

from src.models.hyperparameter_tuner import QuantileHyperparameterTuner
from src.models.monte_carlo import MonteCarloPredictor
from src.models.quantile_trainer import PlayerPropsModelPipeline, prepare_quantile_features

FINAL_BRIER_SEARCH_SPACE = {
    "max_depth": (2, 5),
    "min_child_weight": (8, 40),
    "learning_rate": (0.015, 0.08),
    "n_estimators": (250, 1000),
    "subsample": (0.65, 0.9),
    "colsample_bytree": (0.65, 0.9),
    "reg_alpha": (0.01, 10.0),
    "reg_lambda": (1.0, 30.0),
    "early_stopping_rounds": (40, 100),
}
logger = logging.getLogger(__name__)
MIN_MINUTES_AVG = 10.0
_REQUIRED_COLUMNS = {
    "game_date",
    "game_id",
    "player_id",
    "prop_line_reb",
    "actual_reb",
    "actual_minutes",
    "reb_per_min",
    "player_avg_min_l5",
}
_PARAM_NAMES = tuple(FINAL_BRIER_SEARCH_SPACE)
_SCORE_QUANTILES = ((0.10, "q10"), (0.25, "q25"), (0.50, "q50"), (0.75, "q75"), (0.90, "q90"))
_DEFAULT_SCORE_BATCH_SIZE = 256


@dataclass(frozen=True)
class CohortReceipt:
    input_rows: int
    scored_rows: int
    missing_or_nonpositive_line: int
    missing_outcome: int
    void_or_dnp: int
    pushes: int
    invalid_date: int
    pregame_minutes_ineligible: int
    start_date: str
    end_date: str
    ordering: tuple[str, ...]
    source_market: str
    source_policy: str
    market_benchmark: str


@dataclass(frozen=True)
class ScoreResult:
    brier: float
    sampled_reb_pinball: float
    probabilities: tuple[float, ...]
    outcomes: tuple[int, ...]
    receipt: CohortReceipt


@dataclass(frozen=True)
class FinalBrierTuningResult:
    best_params: dict[str, Any]
    best_brier: float
    best_trial_number: int
    best_pinball_params: dict[str, Any]
    best_pinball: float
    best_pinball_trial_number: int
    parent_reference: dict[str, Any]
    trials: tuple[dict[str, Any], ...]
    settings: dict[str, Any]
    validation: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


CandidateFitter = Callable[[dict[str, Any]], PlayerPropsModelPipeline]


class _FinalBrierParameterSampler(QuantileHyperparameterTuner):
    """Expose the shared parameter sampler without changing its implementation."""

    def sample_params(self, trial: optuna.Trial) -> dict[str, Any]:
        return self._sample_params(trial)


def _iso_date(value: Any) -> str:
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError("Missing game_date in final-Brier population")
    return stamp.date().isoformat()


def validate_parent_chronology(parent_model_dir: str | Path, train_df: pd.DataFrame, validation_df: pd.DataFrame) -> dict:
    """Prove that parent fitting and current training rows precede the development cohort."""
    manifest_path = Path(parent_model_dir) / "artifact_manifest.json"
    if not manifest_path.is_file():
        raise ValueError("final_brier requires a manifest-backed parent artifact")
    with manifest_path.open(encoding="utf-8") as handle:
        manifest = json.load(handle)
    provenance = manifest.get("data_provenance", {}).get("training", {})
    chronology = provenance.get("execution", {}).get("chronology", {})
    parent_training = chronology.get("training", {})
    if not chronology.get("strict_fit_before_calibration") or not parent_training.get("end"):
        raise ValueError("Parent artifact does not prove strict fit-before-calibration chronology")
    if train_df.empty or validation_df.empty:
        raise ValueError("final_brier requires non-empty training and external development populations")
    train_end = _iso_date(pd.to_datetime(train_df["game_date"]).max())
    validation_start = _iso_date(pd.to_datetime(validation_df["game_date"]).min())
    parent_fit_end = _iso_date(parent_training["end"])
    if not (parent_fit_end < validation_start and train_end < validation_start):
        raise ValueError(
            "final_brier development cohort must start after both parent fitting and current training "
            f"(parent_end={parent_fit_end}, train_end={train_end}, development_start={validation_start})"
        )
    return {
        "parent_fit_end": parent_fit_end,
        "current_train_end": train_end,
        "development_start": validation_start,
        "strict_fit_before_development": True,
        "parent_training_seasons": list(parent_training.get("seasons", [])),
        "development_role": "external_tuning_validation_development_not_independent",
    }


def _ordered_scoring_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, CohortReceipt]:
    missing = sorted(_REQUIRED_COLUMNS - set(df.columns))
    if missing:
        raise ValueError(f"final_brier development population is missing columns: {missing}")
    frame = df.copy()
    frame["game_date"] = pd.to_datetime(frame["game_date"], errors="coerce")
    line = pd.Series(pd.to_numeric(frame["prop_line_reb"], errors="coerce"), index=frame.index, dtype=float)
    actual = pd.Series(pd.to_numeric(frame["actual_reb"], errors="coerce"), index=frame.index, dtype=float)
    minutes = pd.Series(pd.to_numeric(frame["actual_minutes"], errors="coerce"), index=frame.index, dtype=float)
    missing_line = ~np.isfinite(line) | (line <= 0)
    missing_outcome = ~np.isfinite(actual) | (actual < 0)
    void_or_dnp = ~np.isfinite(minutes) | (minutes <= 0)
    pushes = np.isfinite(line) & np.isfinite(actual) & (actual == line)
    invalid_date = frame["game_date"].isna()
    minutes_avg = pd.Series(pd.to_numeric(frame["player_avg_min_l5"], errors="coerce"), index=frame.index, dtype=float)
    pregame_ineligible = ~np.isfinite(minutes_avg) | (minutes_avg < MIN_MINUTES_AVG)
    valid = ~(missing_line | missing_outcome | void_or_dnp | pushes | invalid_date | pregame_ineligible)
    scored = frame.loc[valid].copy()
    scored["prop_line_reb"] = line.loc[valid].astype(float)
    scored["actual_reb"] = actual.loc[valid].astype(float)
    order = ("game_date", "game_id", "player_id")
    scored = scored.sort_values(list(order), kind="mergesort").reset_index(drop=True)
    if scored.empty:
        raise ValueError("No valid positive-line, non-push, played outcomes for final_brier scoring")
    receipt = CohortReceipt(
        input_rows=len(frame),
        scored_rows=len(scored),
        missing_or_nonpositive_line=int(missing_line.sum()),
        missing_outcome=int((missing_outcome & ~missing_line).sum()),
        void_or_dnp=int((void_or_dnp & ~missing_line & ~missing_outcome).sum()),
        pushes=int((pushes & ~missing_line & ~missing_outcome & ~void_or_dnp).sum()),
        invalid_date=int((invalid_date & ~missing_line & ~missing_outcome & ~void_or_dnp & ~pushes).sum()),
        pregame_minutes_ineligible=int((pregame_ineligible & ~missing_line & ~missing_outcome & ~void_or_dnp & ~pushes & ~invalid_date).sum()),
        start_date=_iso_date(scored["game_date"].min()),
        end_date=_iso_date(scored["game_date"].max()),
        ordering=order,
        source_market="player_rebounds",
        source_policy="canonical_nba_player_prop_feature_lines_t60_v1_snapshot_lte_decision_time",
        market_benchmark="unavailable_no_paired_prices",
    )
    return scored, receipt


def _assert_feature_contract(pipeline: PlayerPropsModelPipeline) -> None:
    if pipeline.minutes_model is None or "reb" not in pipeline.rate_models:
        raise ValueError("final_brier requires frozen minutes and a REB rate suite")
    forbidden = {"actual_minutes", "actual_reb", "reb_per_min"}
    used = set(pipeline.minutes_model.all_feature_names) | set(pipeline.rate_models["reb"].all_feature_names)
    leaked = sorted(used & forbidden)
    if leaked:
        raise ValueError(f"Outcome columns reached final-Brier inference features: {leaked}")


def _suite_feature_signature(suite: Any) -> tuple[tuple[float, tuple[str, ...]], ...]:
    return tuple(
        (float(quantile), tuple(names))
        for quantile, names in sorted(suite.feature_names_per_quantile.items())
    )


def _suite_cache_signature(suite: Any) -> tuple[Any, ...]:
    models = getattr(suite, "models", {})
    offsets = getattr(suite, "calibration_offsets", {})
    return (
        id(suite),
        _suite_feature_signature(suite),
        tuple((float(quantile), id(model)) for quantile, model in sorted(models.items())),
        tuple((float(quantile), float(offset)) for quantile, offset in sorted(offsets.items())),
    )


def _prepare_feature_matrix(scored: pd.DataFrame, feature_names: tuple[str, ...]) -> pd.DataFrame:
    missing = sorted(set(feature_names) - set(scored.columns))
    if missing:
        raise ValueError(f"Missing frozen scoring features: {missing}")
    return prepare_quantile_features(scored, feature_names).astype(np.float32)


class _CachedMinutesSuite:
    """Study-local adapter returning canonical frozen-minutes quantile outputs."""

    def __init__(
        self,
        feature_names: tuple[str, ...],
        prepared_features: pd.DataFrame,
        quantiles: pd.DataFrame,
    ) -> None:
        self.all_feature_names = list(feature_names)
        self._prepared_features = prepared_features
        self._quantiles = quantiles

    def predict_quantiles(self, X: pd.DataFrame) -> pd.DataFrame:
        expected = self._prepared_features.loc[X.index]
        if list(X.columns) != list(expected.columns) or not X.equals(expected):
            raise ValueError("Frozen minutes cache received incompatible prepared features")
        return self._quantiles.loc[X.index]


class _PreparedFeatureMonteCarloPredictor(MonteCarloPredictor):
    """Canonical sampler with study-local prepared batch feature matrices."""

    def __init__(self, *args: Any, prepared_features: dict[tuple[str, ...], pd.DataFrame], **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._prepared_features = prepared_features

    def _prepare_features_batch(self, features_df: pd.DataFrame, feature_names: list[str]) -> pd.DataFrame:
        prepared = self._prepared_features.get(tuple(feature_names))
        if prepared is None:
            raise ValueError("Scoring cache received incompatible frozen feature names")
        try:
            return prepared.loc[features_df.index]
        except KeyError as exc:
            raise ValueError("Scoring cache received an incompatible cohort") from exc


class PreparedFinalRebBrierScorer:
    """One-cohort scorer that caches only frozen inputs and minutes outputs."""

    def __init__(
        self,
        pipeline: PlayerPropsModelPipeline,
        validation_df: pd.DataFrame,
        *,
        copula_params: dict[str, float] | None,
        n_samples: int = 5000,
        random_state: int = 42,
        batch_size: int = _DEFAULT_SCORE_BATCH_SIZE,
    ) -> None:
        if not 100 <= n_samples <= 20000:
            raise ValueError("final_brier samples must be between 100 and 20000")
        if batch_size < 1:
            raise ValueError("final_brier score batch_size must be positive")
        _assert_feature_contract(pipeline)
        minutes_model = pipeline.minutes_model
        if minutes_model is None:
            raise ValueError("final_brier requires a frozen minutes model")

        scored, receipt = _ordered_scoring_rows(validation_df)
        identities = [
            (int(player_id), str(game_id), "reb")
            for player_id, game_id in zip(scored["player_id"], scored["game_id"], strict=True)
        ]
        if len(set(identities)) != len(identities):
            raise ValueError("Duplicate player/game/stat identity in final-Brier scoring cohort")

        self._scored = scored
        self._receipt = receipt
        self._minutes_model = minutes_model
        self._minutes_cache_signature = _suite_cache_signature(minutes_model)
        self._rate_signature = _suite_feature_signature(pipeline.rate_models["reb"])
        self._minutes_feature_names = tuple(minutes_model.all_feature_names)
        self._rate_feature_names = tuple(pipeline.rate_models["reb"].all_feature_names)
        self._prepared_features = {
            self._minutes_feature_names: _prepare_feature_matrix(scored, self._minutes_feature_names),
            self._rate_feature_names: _prepare_feature_matrix(scored, self._rate_feature_names),
        }
        self._copula_params = copy.deepcopy(copula_params)
        self._n_samples = n_samples
        self._random_state = random_state
        self._batch_size = batch_size

        minute_batches: list[pd.DataFrame] = []
        minute_features = self._prepared_features[self._minutes_feature_names]
        for start in range(0, len(scored), batch_size):
            chunk = minute_features.iloc[start : start + batch_size]
            quantiles = minutes_model.predict_quantiles(chunk)
            quantiles.index = chunk.index
            minute_batches.append(quantiles)
        self._minutes_quantiles = pd.concat(minute_batches).loc[scored.index]
        if self._minutes_quantiles.shape != (len(scored), len(_SCORE_QUANTILES)):
            raise ValueError("Invalid frozen minutes quantile cache shape")
        if not np.isfinite(self._minutes_quantiles.to_numpy(dtype=float)).all():
            raise ValueError("Non-finite frozen minutes quantile cache")

    @property
    def receipt(self) -> CohortReceipt:
        return self._receipt

    def _score_scalar_legacy_path(self, pipeline: PlayerPropsModelPipeline) -> ScoreResult:
        predictor = MonteCarloPredictor(
            pipeline,
            n_samples=self._n_samples,
            random_state=self._random_state,
            copula_params=copy.deepcopy(self._copula_params),
            combined_calibration_offsets=None,
        )
        feature_names = sorted(
            set(self._minutes_feature_names) | set(pipeline.rate_models["reb"].all_feature_names)
        )
        probabilities: list[float] = []
        outcomes: list[int] = []
        pinball_losses: list[float] = []
        for row in self._scored.to_dict(orient="records"):
            features = {name: row[name] for name in feature_names}
            prediction = predictor.predict(
                player_id=int(row["player_id"]),
                game_id=str(row["game_id"]),
                features=features,
                stats=["reb"],
            )["reb"]
            if len(prediction.samples) != self._n_samples or not np.isfinite(prediction.samples).all():
                raise ValueError("Invalid or incomplete final-REB Monte Carlo samples")
            line = float(row["prop_line_reb"])
            actual = float(row["actual_reb"])
            probability = float((prediction.samples > line).mean())
            if not np.isfinite(probability):
                raise ValueError("Non-finite empirical over probability")
            probabilities.append(probability)
            outcomes.append(int(actual > line))
            for quantile, field in _SCORE_QUANTILES:
                residual = actual - float(getattr(prediction, field))
                pinball_losses.append(max(quantile * residual, (quantile - 1.0) * residual))
        probs = np.asarray(probabilities, dtype=float)
        binary = np.asarray(outcomes, dtype=float)
        return ScoreResult(
            brier=float(np.mean((probs - binary) ** 2)),
            sampled_reb_pinball=float(np.mean(pinball_losses)),
            probabilities=tuple(probabilities),
            outcomes=tuple(outcomes),
            receipt=self._receipt,
        )

    def score(self, pipeline: PlayerPropsModelPipeline) -> ScoreResult:
        """Score a parent/candidate while recomputing its REB quantiles."""
        _assert_feature_contract(pipeline)
        if pipeline.minutes_model is not self._minutes_model:
            raise ValueError("Scoring cache is bound to a different frozen minutes model")
        if _suite_cache_signature(self._minutes_model) != self._minutes_cache_signature:
            raise ValueError("Frozen minutes model or feature contract changed after cache preparation")
        if _suite_feature_signature(pipeline.rate_models["reb"]) != self._rate_signature:
            raise ValueError("Scoring cache is incompatible with candidate REB features")
        if not self._copula_params:
            return self._score_scalar_legacy_path(pipeline)

        cached_minutes = _CachedMinutesSuite(
            self._minutes_feature_names,
            self._prepared_features[self._minutes_feature_names],
            self._minutes_quantiles,
        )
        scoring_pipeline: Any = copy.copy(pipeline)
        scoring_pipeline.minutes_model = cached_minutes
        predictor = _PreparedFeatureMonteCarloPredictor(
            scoring_pipeline,
            n_samples=self._n_samples,
            random_state=self._random_state,
            copula_params=copy.deepcopy(self._copula_params),
            combined_calibration_offsets=None,
            prepared_features=self._prepared_features,
        )
        probabilities: list[float] = []
        outcomes: list[int] = []
        pinball_losses: list[float] = []

        for start in range(0, len(self._scored), self._batch_size):
            chunk = self._scored.iloc[start : start + self._batch_size]
            predictions, samples = predictor.predict_batch_for_date(chunk, stats=["reb"])
            if len(predictions) != len(chunk) or len(samples) != len(chunk):
                raise ValueError("Incomplete final-REB batch prediction")
            for offset, (_, row) in enumerate(chunk.iterrows()):
                prediction = predictions[offset]
                key = (int(row["player_id"]), str(row["game_id"]), "reb")
                sample_values = samples.get((row["player_id"], row["game_id"], "reb"))
                if sample_values is None or len(sample_values) != self._n_samples:
                    raise ValueError("Invalid or incomplete final-REB Monte Carlo samples")
                if not np.isfinite(sample_values).all():
                    raise ValueError("Invalid or incomplete final-REB Monte Carlo samples")
                if (
                    int(prediction["player_id"]) != key[0]
                    or str(prediction["game_id"]) != key[1]
                    or prediction["stat"] != "reb"
                ):
                    raise ValueError("Final-REB batch prediction order changed")
                line = float(row["prop_line_reb"])
                actual = float(row["actual_reb"])
                probability = float((sample_values > line).mean())
                outcome = int(actual > line)
                if not np.isfinite(probability):
                    raise ValueError("Non-finite empirical over probability")
                probabilities.append(probability)
                outcomes.append(outcome)
                for quantile, field in _SCORE_QUANTILES:
                    residual = actual - float(prediction[f"pred_{field}"])
                    pinball_losses.append(max(quantile * residual, (quantile - 1.0) * residual))
            completed = start + len(chunk)
            if completed // 1000 != start // 1000 or completed == len(self._scored):
                logger.info("Final-Brier scored %s/%s quotes", completed, len(self._scored))

        probs = np.asarray(probabilities, dtype=float)
        binary = np.asarray(outcomes, dtype=float)
        return ScoreResult(
            brier=float(np.mean((probs - binary) ** 2)),
            sampled_reb_pinball=float(np.mean(pinball_losses)),
            probabilities=tuple(probabilities),
            outcomes=tuple(outcomes),
            receipt=self._receipt,
        )


def prepare_final_reb_brier_scorer(
    pipeline: PlayerPropsModelPipeline,
    validation_df: pd.DataFrame,
    *,
    copula_params: dict[str, float] | None,
    n_samples: int = 5000,
    random_state: int = 42,
    batch_size: int = _DEFAULT_SCORE_BATCH_SIZE,
) -> PreparedFinalRebBrierScorer:
    """Prepare one bounded, study-local scoring context for repeated candidates."""
    return PreparedFinalRebBrierScorer(
        pipeline,
        validation_df,
        copula_params=copula_params,
        n_samples=n_samples,
        random_state=random_state,
        batch_size=batch_size,
    )


def score_final_reb_brier(
    pipeline: PlayerPropsModelPipeline,
    validation_df: pd.DataFrame,
    *,
    copula_params: dict[str, float] | None,
    n_samples: int = 5000,
    random_state: int = 42,
) -> ScoreResult:
    """Standalone scorer; repeated studies should prepare one scorer and reuse it."""
    logger.info("Final-Brier preparing standalone scoring context (%s input rows)", len(validation_df))
    return prepare_final_reb_brier_scorer(
        pipeline,
        validation_df,
        copula_params=copula_params,
        n_samples=n_samples,
        random_state=random_state,
    ).score(pipeline)


def detached_reb_candidate(
    parent: PlayerPropsModelPipeline,
    train_df: pd.DataFrame,
    params: dict[str, Any],
) -> PlayerPropsModelPipeline:
    """Copy the container, then replace only the canonically trained REB suite."""
    candidate = copy.copy(parent)
    candidate.rate_models = dict(parent.rate_models)
    candidate.rate_features = {
        stat: {float(q): list(names) for q, names in features.items()}
        for stat, features in parent.rate_features.items()
    }
    candidate.minutes_features = {float(q): list(names) for q, names in parent.minutes_features.items()}
    required_features = {name for names in candidate.rate_features["reb"].values() for name in names}
    missing_features = sorted(required_features - set(train_df.columns))
    if missing_features:
        raise ValueError(f"Missing frozen REB fitting features: {missing_features}")
    before_minutes = candidate.minutes_model
    before_other = {stat: model for stat, model in candidate.rate_models.items() if stat != "reb"}
    candidate.train_rate_models(
        train_df,
        stats=["reb"],
        hyperparams={"reb": dict(params)},
        chronological_dates=True,
    )
    if candidate.minutes_model is not before_minutes or any(candidate.rate_models.get(k) is not v for k, v in before_other.items()):
        raise ValueError("final_brier candidate changed a frozen model component")
    if candidate.rate_features != parent.rate_features or candidate.minutes_features != parent.minutes_features:
        raise ValueError("final_brier candidate changed frozen ordered feature lists")
    if candidate.rate_models["reb"].feature_names_per_quantile != parent.rate_features["reb"]:
        raise ValueError("Fitted REB suite dropped or reordered frozen features")
    return candidate


def _in_search_space(params: dict[str, Any]) -> bool:
    for name, (low, high) in FINAL_BRIER_SEARCH_SPACE.items():
        value = params.get(name)
        if value is None or not low <= value <= high:
            return False
    return True


def tune_final_reb_brier(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    parent_pipeline: PlayerPropsModelPipeline,
    *,
    parent_model_dir: str | Path,
    copula_params: dict[str, float] | None,
    n_trials: int,
    timeout: int | None,
    n_samples: int = 5000,
    random_state: int = 42,
    candidate_fitter: CandidateFitter | None = None,
) -> FinalBrierTuningResult:
    """Run a bounded single-objective Optuna study and retain secondary diagnostics."""
    if not 1 <= n_trials <= 100:
        raise ValueError("final_brier trials must be between 1 and 100")
    if not copula_params or "reb" not in copula_params:
        raise ValueError("final_brier requires the parent REB copula parameter")
    chronology = validate_parent_chronology(parent_model_dir, train_df, validation_df)
    _assert_feature_contract(parent_pipeline)
    scorer = prepare_final_reb_brier_scorer(
        parent_pipeline,
        validation_df,
        copula_params=copula_params,
        n_samples=n_samples,
        random_state=random_state,
    )
    logger.info(
        "Final-Brier prepared %s eligible quotes once; frozen minutes quantiles cached in batches of %s",
        scorer.receipt.scored_rows,
        _DEFAULT_SCORE_BATCH_SIZE,
    )
    parent_score = scorer.score(parent_pipeline)
    fitter = candidate_fitter or (lambda params: detached_reb_candidate(parent_pipeline, train_df, params))
    sampler = TPESampler(seed=random_state)
    study = optuna.create_study(direction="minimize", sampler=sampler, study_name="nba_reb_final_brier")
    study.set_user_attr("objective", "final_brier")
    study.set_user_attr("sampler", "canonical_monte_carlo_predictor")
    study.set_user_attr("search_space", FINAL_BRIER_SEARCH_SPACE)
    inherited = {name: getattr(parent_pipeline.rate_models["reb"].config, name) for name in _PARAM_NAMES}
    if _in_search_space(inherited):
        study.enqueue_trial(inherited, user_attrs={"candidate_role": "inherited_config_retrained"})
    trial_rows: list[dict[str, Any]] = []
    helper = _FinalBrierParameterSampler(search_space=FINAL_BRIER_SEARCH_SPACE, random_state=random_state)

    def objective(trial: optuna.Trial) -> float:
        params = helper.sample_params(trial)
        candidate = fitter(params)
        score = scorer.score(candidate)
        trial.set_user_attr("sampled_reb_pinball", score.sampled_reb_pinball)
        trial.set_user_attr("scored_rows", score.receipt.scored_rows)
        trial_rows.append(
            {
                "number": trial.number,
                "state": "COMPLETE",
                "params": dict(params),
                "brier": score.brier,
                "sampled_reb_pinball": score.sampled_reb_pinball,
                "candidate_role": trial.user_attrs.get("candidate_role", "sampled"),
            }
        )
        return score.brier

    study.optimize(objective, n_trials=n_trials, timeout=timeout, n_jobs=1, gc_after_trial=True)
    completed = [trial for trial in study.trials if trial.state == optuna.trial.TrialState.COMPLETE]
    if not completed:
        raise ValueError("final_brier study completed no successful trials")
    by_number = {row["number"]: row for row in trial_rows}
    trials = tuple(by_number[trial.number] for trial in completed)
    best_pinball_trial = min(completed, key=lambda trial: float(trial.user_attrs["sampled_reb_pinball"]))
    return FinalBrierTuningResult(
        best_params=dict(study.best_trial.params),
        best_brier=float(study.best_value),
        best_trial_number=study.best_trial.number,
        best_pinball_params=dict(best_pinball_trial.params),
        best_pinball=float(best_pinball_trial.user_attrs["sampled_reb_pinball"]),
        best_pinball_trial_number=best_pinball_trial.number,
        parent_reference={
            "brier": parent_score.brier,
            "sampled_reb_pinball": parent_score.sampled_reb_pinball,
            "candidate_beats_parent": float(study.best_value) < parent_score.brier,
            "cohort": asdict(parent_score.receipt),
        },
        trials=trials,
        settings={
            "objective": "final_brier",
            "stat": "reb",
            "n_trials_requested": n_trials,
            "n_trials_completed": len(completed),
            "n_samples": n_samples,
            "random_state": random_state,
            "min_minutes_avg": MIN_MINUTES_AVG,
            "probability": "empirical_samples_gt_line",
            "scoring_backend": "prepared_features_batched_quantiles_cached_frozen_minutes",
            "score_batch_size": _DEFAULT_SCORE_BATCH_SIZE,
            "combined_global_offsets": "disabled",
            "copula": "frozen_parent",
            "fit_contract": "PlayerPropsModelPipeline.train_rate_models_chronological_dates_true",
            "search_space": FINAL_BRIER_SEARCH_SPACE,
            "market_benchmark": "unavailable_no_paired_prices",
        },
        validation=chronology,
    )
