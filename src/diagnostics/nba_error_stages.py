"""Local NBA component/oracle diagnostics; never train or publish a model.

Oracle arms use outcomes and are explicitly non-executable. They measure
sensitivity to perfect component information, not attainable betting returns.
"""
from __future__ import annotations

import copy
import hashlib
import json
import logging
import math
import re
from collections import Counter
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
QUANTILES = (0.10, 0.25, 0.50, 0.75, 0.90)
QCOLS = tuple(f"q{int(q * 100)}" for q in QUANTILES)
PCOLS = tuple(f"pred_{q}" for q in QCOLS)
KEYS = ["player_id", "game_id", "stat"]
ARMS = ("baseline", "actual_minutes", "actual_rates", "both_actual")


class DiagnosticError(ValueError):
    """Safe, actionable diagnostic failure without connection details."""


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _key(player_id, game_id) -> tuple[str, str]:
    # Preserve game IDs literally, including leading zeros.
    return str(player_id), str(game_id)


def _finite(value) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _csv_from_receipt(root: Path, receipt: dict, filename: str) -> pd.DataFrame:
    cid = receipt["cell_id"]
    if not re.fullmatch(r"cell_[0-9a-f]{64}", cid):
        raise DiagnosticError("Invalid reference cell identifier")
    path = root / "cells" / cid / filename
    if file_hash(path) != receipt["files"][filename]["sha256"]:
        raise DiagnosticError(f"Reference receipt mismatch: {cid}/{filename}")
    return pd.read_csv(path, dtype={"player_id": str, "game_id": str}, low_memory=False)


def prepare_diagnostic(model_dir: str, sweep_dir: str, output_dir: str, *, local: bool,
                       max_games: int = 200, max_player_games: int = 2000,
                       max_outcome_rows: int = 10000, max_outcome_games: int = 200) -> dict:
    """Filesystem-only preflight. No connections, model loads or output creation."""
    from src.backtesting.nba.sweep_config import SweepConfig, build_plan

    if not local:
        raise DiagnosticError("This diagnostic requires --local")
    if not 1 <= max_games <= 200 or not 1 <= max_player_games <= 10000:
        raise DiagnosticError("Bounds: max-games 1..200; max-player-games 1..10000")
    if not 1 <= max_outcome_rows <= 10000:
        raise DiagnosticError("Bounds: max-outcome-rows 1..10000")
    if not 1 <= max_outcome_games <= 200:
        raise DiagnosticError("Bounds: max-outcome-games 1..200")
    root = Path(sweep_dir).resolve(strict=True)
    model = Path(model_dir).resolve(strict=True)
    output = Path(output_dir).resolve()
    if output.is_relative_to(root) or output.is_relative_to(model):
        raise DiagnosticError("Diagnostic output must be outside the source sweep and model")
    manifest_path = root / "sweep_manifest.json"
    reference = _json(manifest_path)
    if reference.get("state") != "complete" or reference.get("failure"):
        raise DiagnosticError("Reference sweep is not complete")
    saved = reference["plan"]["fixed_config"]
    if not saved.get("local") or saved.get("historical"):
        raise DiagnosticError("Only ordinary local diagnostic-history sweeps are supported")
    start, end = date.fromisoformat(saved["start"]), date.fromisoformat(saved["end"])
    if not 0 <= (end - start).days < 31:
        raise DiagnosticError("Diagnostic window must contain at most 31 days")
    if not 1 <= saved["n_samples"] <= 20000:
        raise DiagnosticError("Reference sample count exceeds the diagnostic limit")
    cfg = SweepConfig(
        model_dir=str(model), start=start, end=end, stats=tuple(saved["stats"]),
        bookmakers=tuple(saved["bookmakers"]), n_samples=saved["n_samples"], seed=saved["seed"],
        min_minutes_avg=saved["min_minutes_avg"], starting_bankroll=saved["starting_bankroll"],
        flat_bet_size=saved["flat_bet_size"], edge_thresholds=(0.05,), output_dir=str(output),
        allowed_bets=tuple(tuple(x) for x in saved["allowed_bets"]) if saved["allowed_bets"] else None,
        bl_taus=(0.5,), bl_z_max=(0.5,), bl_max_weight=(0.5,), max_cells=2,
        local=True, diagnostic_history=True,
    )
    if cfg.stats != ("pts", "reb", "ast"):
        raise DiagnosticError("First-stage runner requires ordered stats pts reb ast")
    plan = build_plan(cfg)
    if plan["artifact"]["identity"] != reference["plan"]["artifact"]["identity"]:
        raise DiagnosticError("Model does not match the reference sweep artifact")
    if plan["predictor_settings"] != reference["plan"]["predictor_settings"]:
        raise DiagnosticError("Current predictor settings differ from the reference sweep")
    if plan["predictor_settings"]["combined_calibration_offsets"] is not None:
        raise DiagnosticError("Global offsets are forbidden")
    if not plan["predictor_settings"].get("copula_params"):
        raise DiagnosticError("This first diagnostic supports the current copula path only")
    if plan["predictor_settings"]["blowout_config"].get("enabled"):
        raise DiagnosticError("Enabled blowout simulation is outside this diagnostic scope")
    if plan["dependencies"] != reference["plan"]["dependencies"]:
        raise DiagnosticError("Runtime dependency versions differ from the reference sweep")
    run = _json(model / "run_config.json")
    chronology = run.get("execution", {}).get("chronology", {})
    for role in ("training", "calibration"):
        endpoint = chronology.get(role, {}).get("end")
        if not endpoint or date.fromisoformat(endpoint) >= start:
            raise DiagnosticError("Diagnostic window must follow recorded fit and calibration observations")
    receipts = {item["cell_id"]: item for item in reference["completed_cells"]}
    reference_cells = {item["cell_id"]: item for item in reference["plan"]["cells"]}
    tables = {}
    for cell in cfg.cells():
        if cell.cell_id not in receipts or cell.cell_id not in reference_cells:
            raise DiagnosticError("Reference lacks raw .05 or fixed BL .05/.5/.5/.5")
        tables[cell.cell_id] = {
            name: _csv_from_receipt(root, receipts[cell.cell_id], name)
            for name in ("all_bookmaker_edges.csv", "predictions.csv", "bets.csv")
        }
    raw = tables[cfg.cells()[0].cell_id]["all_bookmaker_edges.csv"]
    if raw.loc[raw["quote_status"] == "quoted", "stat"].isin(["pra", "pr", "pa", "ra"]).any():
        raise DiagnosticError("Quoted combo diagnostics are not supported in this first runner")
    observed = reference["observed"]
    if sum(x["counts"]["features"] for x in observed) > max_player_games:
        raise DiagnosticError("Reference feature-row count exceeds --max-player-games")
    if raw["game_id"].nunique() > max_games:
        raise DiagnosticError("Reference game count exceeds --max-games")
    info = {
        "command": "nba-error-stages", "execution": False, "status": "preflight_only",
        "database_target": "LOCAL", "model_dir": str(model), "sweep_dir": str(root),
        "output_dir": str(output), "start": start.isoformat(), "end": end.isoformat(),
        "model_identity": plan["artifact"]["identity"], "reference_manifest_sha256": file_hash(manifest_path),
        "max_games": max_games, "max_player_games": max_player_games,
        "max_outcome_rows": max_outcome_rows,
        "max_outcome_games": max_outcome_games,
        "statement_timeout": "15s", "n_samples": cfg.n_samples, "seed": cfg.seed,
        "reference_feature_rows": sum(x["counts"]["features"] for x in observed),
        "reference_eligible_rows": sum(x["counts"]["eligible"] for x in observed),
        "oracle_arms": list(ARMS[1:]), "oracle_non_executable": True,
        "historical_readiness": "unverified", "decision_grade": False,
        "live_data_checked": False, "model_deserialized": False,
    }
    return {"config": cfg, "plan": plan, "reference": reference, "tables": tables, "info": info}


def install_read_only_guard(engine) -> dict:
    """Enforce and read back guards on every transaction used by the local run."""
    from sqlalchemy import event

    if engine.dialect.name != "postgresql":
        raise DiagnosticError("Expected the local PostgreSQL backend")
    receipt = {"verified_transactions": 0, "read_only": True, "statement_timeout": "15s"}

    def guard(connection):
        connection.exec_driver_sql("SET TRANSACTION READ ONLY")
        connection.exec_driver_sql("SET LOCAL statement_timeout = '15s'")
        row = connection.exec_driver_sql(
            "SELECT current_setting('transaction_read_only'), current_setting('statement_timeout')"
        ).one()
        if tuple(row) != ("on", "15s"):
            raise DiagnosticError("Database read-only/timeout guard readback failed")
        receipt["verified_transactions"] += 1

    event.listen(engine, "begin", guard)
    return receipt


def load_actual_minutes(engine, cfg, max_outcome_rows: int) -> pd.DataFrame:
    """Bound all non-DNP outcomes separately from the feature-eligible cohort."""
    from sqlalchemy import text

    if not 1 <= max_outcome_rows <= 10000:
        raise DiagnosticError("Bounds: max-outcome-rows 1..10000")
    query = text("""
        SELECT player_id, game_id, game_date::date AS game_date,
               min AS actual_minutes, pts, reb, ast
        FROM player_game_stats
        WHERE game_date >= :start AND game_date <= :end
          AND did_not_play IS NOT TRUE
        ORDER BY game_date, game_id, player_id
        LIMIT :row_limit
    """)
    with engine.connect() as conn:
        frame = pd.read_sql(query, conn, params={
            "start": cfg.start, "end": cfg.end, "row_limit": max_outcome_rows + 1,
        })
    if len(frame) > max_outcome_rows:
        raise DiagnosticError(
            f"Actual-outcome row cap exceeded: at least {len(frame)} rows, "
            f"--max-outcome-rows={max_outcome_rows}; no partial cohort was accepted"
        )
    if frame.empty or frame.duplicated(["player_id", "game_id"]).any():
        raise DiagnosticError("Actual outcomes are empty or have duplicate player-game keys")
    return frame


class BoundedFeatureStore:
    """Delegate the existing reader and check its bounded output before inference."""

    def __init__(self, store, max_games: int, max_player_games: int):
        self.store = store
        self.max_games = max_games
        self.max_player_games = max_player_games

    def __getattr__(self, name):
        return getattr(self.store, name)

    def get_features_for_date_range(self, *args, **kwargs):
        frames = self.store.get_features_for_date_range(*args, **kwargs)
        row_count = sum(len(frame) for frame in frames.values())
        if row_count > self.max_player_games:
            raise DiagnosticError(
                f"Feature row cap exceeded before model prediction: {row_count} rows, "
                f"--max-player-games={self.max_player_games}"
            )
        games = set()
        for frame in frames.values():
            if not frame.empty:
                if frame.duplicated(["player_id", "game_id"]).any():
                    raise DiagnosticError("Duplicate feature player-game keys")
                games.update(frame["game_id"].map(str))
        if len(games) > self.max_games:
            raise DiagnosticError(
                f"Feature game cap exceeded before model prediction: {len(games)} games, "
                f"--max-games={self.max_games}"
            )
        return frames


def _assert_same_frame(expected: pd.DataFrame, observed: pd.DataFrame, *, bets: bool = False):
    columns = KEYS + (["side", "line", "odds", "stake", "profit"] if bets else [
        "bookmaker", "line", "over_odds", "under_odds", "over_prob", "under_prob", *PCOLS,
        "selected_snapshot_time", "selected_market_last_update", "selected_decision_time",
    ])
    if not set(columns).issubset(expected) or not set(columns).issubset(observed):
        raise DiagnosticError("Reference comparison columns are missing")
    keys = KEYS + (["side"] if bets else ["bookmaker"])
    frames = []
    for original in (expected, observed):
        frame = original[columns].copy()
        for name in keys:
            frame[name] = frame[name].fillna("<missing>").map(str)
        for name in columns:
            if name.endswith("time") or name.endswith("last_update"):
                present = frame[name].notna()
                parsed = pd.to_datetime(frame[name], utc=True, errors="coerce")
                if (present & parsed.isna()).any():
                    raise DiagnosticError("Malformed reference-comparison timestamp")
                frame[name] = parsed
        frames.append(frame.sort_values(keys + ["line"], kind="stable").reset_index(drop=True))
    try:
        pd.testing.assert_frame_equal(frames[0], frames[1], check_dtype=False, check_exact=False,
                                      rtol=1e-8, atol=1e-8)
    except AssertionError:
        raise DiagnosticError("Baseline differs from reference; refusing unmatched stage attribution") from None


def _scores(frame: pd.DataFrame) -> dict:
    """Scores retain their own valid-row counts; missing evidence is never zero loss."""
    result = {"rows": len(frame)}
    actual = pd.to_numeric(frame["actual"], errors="coerce")
    finite_actual = np.isfinite(actual)
    if "pred_q50" in frame:
        pred = pd.to_numeric(frame["pred_q50"], errors="coerce")
        valid = finite_actual & np.isfinite(pred)
        error = pred[valid] - actual[valid]
        result.update(n=int(valid.sum()), mae=float(error.abs().mean()) if valid.any() else None,
                      rmse=float(np.sqrt((error ** 2).mean())) if valid.any() else None,
                      bias=float(error.mean()) if valid.any() else None)
    quantiles = {}
    for q, col in zip(QUANTILES, PCOLS):
        if col not in frame:
            continue
        pred = pd.to_numeric(frame[col], errors="coerce")
        valid = finite_actual & np.isfinite(pred)
        residual = actual[valid] - pred[valid]
        quantiles[str(q)] = {
            "n": int(valid.sum()), "coverage": float((residual <= 0).mean()) if valid.any() else None,
            "pinball": float(np.maximum(q * residual, (q - 1) * residual).mean()) if valid.any() else None,
        }
    result["quantiles"] = quantiles
    losses = [x["pinball"] for x in quantiles.values() if x["pinball"] is not None]
    result["mean_pinball"] = float(np.mean(losses)) if losses else None
    if "lagged_baseline" in frame:
        baseline = pd.to_numeric(frame["lagged_baseline"], errors="coerce")
        valid = finite_actual & np.isfinite(baseline)
        result["lagged_baseline_n"] = int(valid.sum())
        result["lagged_baseline_mae"] = float((baseline[valid] - actual[valid]).abs().mean()) if valid.any() else None
    if {"over_prob", "line", "market_over"}.issubset(frame):
        p = pd.to_numeric(frame["over_prob"], errors="coerce")
        market = pd.to_numeric(frame["market_over"], errors="coerce")
        line = pd.to_numeric(frame["line"], errors="coerce")
        valid = finite_actual & np.isfinite(p) & np.isfinite(market) & np.isfinite(line) & (actual != line)
        if ((p[valid] < 0) | (p[valid] > 1) | (market[valid] < 0) | (market[valid] > 1)).any():
            raise DiagnosticError("Probability outside [0, 1]")
        outcome = (actual[valid] > line[valid]).astype(float)
        result.update(probability_n=int(valid.sum()), pushes_excluded=int((finite_actual & (actual == line)).sum()),
                      brier=float(((p[valid] - outcome) ** 2).mean()) if valid.any() else None,
                      market_brier=float(((market[valid] - outcome) ** 2).mean()) if valid.any() else None)
    result["status"] = "scored" if result.get("n", result.get("probability_n", 0)) else "insufficient_evidence"
    return result


def grouped_scores(frame: pd.DataFrame, names: list[str]) -> dict:
    return {"/".join(map(str, key if isinstance(key, tuple) else (key,))): _scores(group)
            for key, group in frame.groupby(names, dropna=False, sort=True)}


class _CachedHead:
    """Detached batch head; only oracle rows receive outcome-derived constants."""

    def __init__(self, original, quantiles: pd.DataFrame, replacement=None, mask=None):
        self.all_feature_names = list(original.all_feature_names)
        self.values = quantiles.copy(deep=True).reset_index(drop=True)
        if replacement is not None:
            # Oracle ratios need float64; pandas rejects lossy writes into
            # float32 model outputs. Keep the baseline's original dtype intact.
            self.values = self.values.astype(np.float64)
            values = np.asarray(replacement, dtype=float)
            valid = np.asarray(mask, dtype=bool)
            self.values.loc[valid, :] = np.repeat(values[valid, None], len(QCOLS), axis=1)

    def predict_quantiles(self, features: pd.DataFrame) -> pd.DataFrame:
        if len(features) != len(self.values):
            raise DiagnosticError("Oracle batch alignment failed")
        return self.values.copy(deep=True)


def _checked_quantiles(predictor, head, features):
    prepared = predictor._prepare_features_batch(features, head.all_feature_names)
    result = head.predict_quantiles(prepared).reset_index(drop=True)
    if list(result.columns) != list(QCOLS) or len(result) != len(features):
        raise DiagnosticError("Unsupported component quantile contract")
    values = result.to_numpy(dtype=float)
    if not np.isfinite(values).all() or (np.diff(values, axis=1) < -1e-9).any():
        raise DiagnosticError("Invalid or non-monotone component quantiles")
    return result


def component_and_oracle_exports(shared, runtime, cfg, settings: dict, outcomes: pd.DataFrame):
    """Reuse canonical sampling with cached heads; baseline values are never replaced."""
    from src.models.monte_carlo import MonteCarloPredictor

    pipeline = runtime.harness.model_pipeline
    lookup = {_key(r["player_id"], r["game_id"]): r for r in outcomes.to_dict("records")}
    state = {arm: np.random.RandomState(cfg.seed).get_state() for arm in ARMS}
    component_rows, total_rows, probability_rows = [], [], []
    excluded = Counter()
    for day, record in shared.evidence["dates"].items():
        features = record["eligible_features"]
        if features.empty:
            continue
        logger.info("Component/oracle diagnostics %s: %d player-games", day, len(features))
        truth = [lookup.get(_key(r.player_id, r.game_id), {}) for r in features.itertuples()]
        minutes = np.asarray([float(r["actual_minutes"]) if _finite(r.get("actual_minutes")) else np.nan for r in truth])
        totals = {s: np.asarray([float(r[s]) if _finite(r.get(s)) and float(r[s]) >= 0 else np.nan
                                for r in truth]) for s in cfg.stats}
        valid = np.isfinite(minutes) & (minutes > 0)
        for values in totals.values():
            valid &= np.isfinite(values) & (values >= 0)
        excluded["eligible_player_games"] += len(features)
        excluded["oracle_valid_player_games"] += int(valid.sum())
        excluded["missing_actual_minutes"] += int((~np.isfinite(minutes)).sum())
        excluded["nonpositive_actual_minutes"] += int((np.isfinite(minutes) & (minutes <= 0)).sum())
        excluded["missing_or_invalid_stat_actuals"] += int((np.isfinite(minutes) & (minutes > 0) & ~valid).sum())
        predictor = runtime.harness.predictor
        quantiles = {"minutes": _checked_quantiles(predictor, pipeline.minutes_model, features)}
        quantiles.update({s: _checked_quantiles(predictor, pipeline.rate_models[s], features) for s in cfg.stats})
        rates = {s: np.divide(values, minutes, out=np.full(len(minutes), np.nan), where=valid)
                 for s, values in totals.items()}
        for component in ("minutes", *cfg.stats):
            target = np.where(minutes >= 0, minutes, np.nan) if component == "minutes" else rates[component]
            for i, row in enumerate(features.to_dict("records")):
                baseline = row.get("player_avg_min_l5")
                if component != "minutes":
                    numerator = row.get(f"player_avg_{component}_l5")
                    baseline = float(numerator) / float(baseline) if (
                        _finite(numerator) and _finite(baseline) and float(baseline) > 0) else np.nan
                component_rows.append({
                    "player_id": row["player_id"], "game_id": row["game_id"], "game_date": day,
                    "component": "minutes" if component == "minutes" else f"{component}_rate",
                    "actual": target[i], "actual_minutes": minutes[i], "oracle_eligible": bool(valid[i]),
                    "lagged_baseline": baseline if _finite(baseline) else np.nan,
                    **dict(zip(PCOLS, quantiles[component].iloc[i].to_numpy(dtype=float))),
                })
        row_index = {_key(r.player_id, r.game_id): i for i, r in enumerate(features.itertuples())}
        quotes = record["quotes"].copy()
        market_to_stat = {"player_points": "pts", "player_rebounds": "reb", "player_assists": "ast"}
        quotes["stat"] = quotes["market_key"].map(market_to_stat)
        for arm in ARMS:
            detached = copy.copy(pipeline)
            detached.minutes_model = _CachedHead(
                pipeline.minutes_model, quantiles["minutes"],
                minutes if arm in ("actual_minutes", "both_actual") else None, valid,
            )
            detached.rate_models = {s: _CachedHead(
                pipeline.rate_models[s], quantiles[s],
                rates[s] if arm in ("actual_rates", "both_actual") else None, valid,
            ) for s in cfg.stats}
            mc = MonteCarloPredictor(detached, **copy.deepcopy(settings))
            mc.rng.set_state(state[arm])
            predictions, samples = mc.predict_batch_for_date(features, stats=list(cfg.stats))
            state[arm] = mc.rng.get_state()
            for p in predictions:
                key = _key(p["player_id"], p["game_id"])
                i = row_index[key]
                sample_key = (p["player_id"], p["game_id"], p["stat"])
                if arm == "baseline" and not np.array_equal(samples[sample_key], record["samples"][sample_key]):
                    raise DiagnosticError("Cached-head baseline failed exact canonical sample reproduction")
                total_rows.append({**p, "game_date": day, "arm": arm,
                                   "actual": totals[p["stat"]][i], "actual_minutes": minutes[i],
                                   "oracle_eligible": bool(valid[i]), "oracle_non_executable": arm != "baseline"})
            for q in quotes.to_dict("records"):
                if q["stat"] not in cfg.stats:
                    continue
                key = _key(q["player_id"], q["game_id"])
                if key not in row_index:
                    continue
                i = row_index[key]
                sample_key = (features.iloc[i]["player_id"], features.iloc[i]["game_id"], q["stat"])
                if sample_key not in samples:
                    raise DiagnosticError("Oracle sample key missing")
                line = float(q["line"])
                over_odds, under_odds = float(q["over_odds"]), float(q["under_odds"])
                if not all(math.isfinite(x) for x in (line, over_odds, under_odds)) or not over_odds or not under_odds:
                    raise DiagnosticError("Invalid quoted line/price")
                po = 100 / (over_odds + 100) if over_odds > 0 else -over_odds / (-over_odds + 100)
                pu = 100 / (under_odds + 100) if under_odds > 0 else -under_odds / (-under_odds + 100)
                probability_rows.append({
                    "player_id": q["player_id"], "game_id": q["game_id"], "game_date": day,
                    "stat": q["stat"], "bookmaker": q["bookmaker"], "line": line,
                    "actual": totals[q["stat"]][i], "arm": arm, "oracle_eligible": bool(valid[i]),
                    "oracle_non_executable": arm != "baseline",
                    "over_prob": float((samples[sample_key] > line).mean()), "market_over": po / (po + pu),
                })
            del samples
    for arm in ARMS:
        if not _same_rng(state[arm], shared.evidence["rng_state_after"]):
            raise DiagnosticError("Oracle random-draw alignment differs from canonical baseline")
    if not total_rows or not probability_rows or excluded.get("oracle_valid_player_games", 0) == 0:
        raise DiagnosticError("Insufficient matched component/oracle/quote evidence")
    return pd.DataFrame(component_rows), pd.DataFrame(total_rows), pd.DataFrame(probability_rows), dict(excluded)


def _same_rng(left, right) -> bool:
    return left[0] == right[0] and np.array_equal(left[1], right[1]) and left[2:] == right[2:]


def selection_scores(result) -> dict:
    """Score the actual canonical selection stages, not a substitute betting rule."""
    actuals = result.predictions_df[KEYS + ["actual"]].copy()
    # Selected exports do not contain all labels. The caller supplies full labels below.
    frames = {"selected_before_threshold": result.predictions_df.copy()}
    if not result.bets_df.empty:
        bets = result.bets_df.copy()
        bets["over_prob"] = np.where(bets["side"] == "over", bets["model_prob"], 1 - bets["model_prob"])
        bets["market_over"] = np.where(bets["side"] == "over", bets["implied_prob"], 1 - bets["implied_prob"])
        frames["after_policy_and_cash"] = bets
    else:
        frames["after_policy_and_cash"] = pd.DataFrame(columns=["stat", "actual", "line", "over_prob", "market_over"])
    output = {}
    for stage, frame in frames.items():
        if "market_over" not in frame or (not frame.empty and frame["market_over"].isna().all()):
            frame["market_over"] = frame["implied_over"]
        output[stage] = grouped_scores(frame, ["stat"])
        output[stage]["ALL"] = _scores(frame)
    output["selection_policy"] = "one best stat/side per player-game; then odds/edge/cash rules"
    output["selected_actual_rows"] = len(actuals)
    return output


def analyze_acquired(prepared: dict, runtime, shared, outcomes: pd.DataFrame) -> tuple[dict, dict]:
    """Public populated analysis seam; accepts a canonical acquired bundle."""
    from src.backtesting.nba.sweep_runner import content_digest, evaluate_cell

    cfg, plan = prepared["config"], prepared["plan"]
    ref = prepared["reference"]
    for name in ("actuals", "voids"):
        if content_digest(shared.evidence[name]) != ref["input_component_digests"][name]:
            raise DiagnosticError(f"Reference {name} changed; a matched diagnostic needs a new reference")
    targets = outcomes.melt(id_vars=["player_id", "game_id"], value_vars=list(cfg.stats),
                            var_name="stat", value_name="actual_value")
    expected = shared.evidence["actuals"]
    expected = expected[expected["stat"].isin(cfg.stats)]
    for frame in (targets, expected):
        if frame.duplicated(KEYS).any():
            raise DiagnosticError("Duplicate actual-stat keys")
    left = targets.assign(player_id=targets.player_id.map(str), game_id=targets.game_id.map(str))
    right = expected.assign(player_id=expected.player_id.map(str), game_id=expected.game_id.map(str))
    joined = left.merge(right, on=KEYS, how="outer", suffixes=("_minutes_read", "_replay"), indicator=True)
    if not (joined["_merge"] == "both").all() or not np.allclose(
            joined.actual_value_minutes_read, joined.actual_value_replay, equal_nan=True):
        raise DiagnosticError("Actual-minute read disagrees with replay outcomes")
    selection = {}
    for cell in cfg.cells():
        result = evaluate_cell(cfg, plan, shared, cell)
        saved = prepared["tables"][cell.cell_id]
        _assert_same_frame(saved["all_bookmaker_edges.csv"], result.all_edges_df)
        _assert_same_frame(saved["bets.csv"], result.bets_df, bets=True)
        mode = "raw" if cell.tau is None else "bl"
        selection[mode] = selection_scores(result)
        all_quotes = result.all_edges_df[result.all_edges_df["quote_status"] == "quoted"].copy()
        all_quotes = all_quotes.merge(expected.rename(columns={"actual_value": "actual"}), on=KEYS, how="left")
        all_quotes["market_over"] = all_quotes["implied_over"]
        selection[mode]["all_quoted"] = grouped_scores(all_quotes, ["stat"])
        selection[mode]["all_quoted"]["ALL"] = _scores(all_quotes)
    components, totals, probabilities, counts = component_and_oracle_exports(
        shared, runtime, cfg, plan["predictor_settings"], outcomes,
    )
    common_totals = totals[totals["oracle_eligible"]]
    common_probabilities = probabilities[probabilities["oracle_eligible"]]
    total_scores = grouped_scores(common_totals, ["arm", "stat"])
    probability_scores = grouped_scores(common_probabilities, ["arm", "stat"])
    sensitivities = {}
    for stat in cfg.stats:
        base_t = total_scores[f"baseline/{stat}"]
        base_p = probability_scores[f"baseline/{stat}"]
        values = {}
        for arm in ARMS[1:]:
            other_t, other_p = total_scores[f"{arm}/{stat}"], probability_scores[f"{arm}/{stat}"]
            values[arm] = {
                "pinball_reduction": base_t["mean_pinball"] - other_t["mean_pinball"],
                "mae_reduction": base_t["mae"] - other_t["mae"],
                "brier_reduction": (base_p["brier"] - other_p["brier"]
                                    if base_p["brier"] is not None and other_p["brier"] is not None else None),
            }
        single = ("actual_minutes", "actual_rates")
        best_pinball = max(single, key=lambda a: values[a]["pinball_reduction"])
        best_brier = (max(single, key=lambda a: values[a]["brier_reduction"])
                      if all(values[a]["brier_reduction"] is not None for a in single) else None)
        suggestion = "inconclusive_or_mixed"
        if (best_pinball == best_brier and values[best_pinball]["pinball_reduction"] > 0
                and values[best_pinball]["brier_reduction"] > 0):
            suggestion = "investigate_minutes" if best_pinball == "actual_minutes" else "investigate_rate"
        sensitivities[stat] = {"arms": values, "triage_suggestion": suggestion,
                               "causal_dominant_stage_proven": False}
    summary = {
        "state": "complete", "decision_grade": False, "historical_readiness": "unverified",
        "baseline_reproduced": True, "canonical_rng_reproduced": True,
        "counts": counts, "component_scores": grouped_scores(components, ["component"]),
        "baseline_total_full_cohort": grouped_scores(totals[totals.arm == "baseline"], ["stat"]),
        "total_scores_common_cohort": total_scores, "probability_scores_common_cohort": probability_scores,
        "selection_stages": selection, "oracle_sensitivity": sensitivities,
        "limitations": [
            "Oracles use outcomes and are non-executable; gains are not attainable trading returns.",
            "Minutes/rates are dependent. Replacing a marginal is sensitivity, not additive causal attribution.",
            "Component metrics have their own valid counts; oracle arms use the same positive-minute, complete-actual cohort.",
            "Selected-stat composition changes after ranking; compare per-stat and same-cohort results.",
            "This is previously consumed discovery data, not independent validation or permission to tune/promote.",
        ],
    }
    exports = {"components.csv": components, "totals.csv": totals, "quote_probabilities.csv": probabilities}
    return summary, exports


def render_report(summary: dict) -> str:
    lines = ["# NBA error-stage diagnostic", "", "Diagnostic only: no model was trained or promoted.",
             "Oracle arms use realized outcomes and cannot be traded. They do not prove a causal dominant stage.", "",
             "## Component quality", "", "Component | N | MAE | L5 baseline MAE | Mean pinball",
             "---|---:|---:|---:|---:"]
    for name, values in summary["component_scores"].items():
        lines.append(f"{name} | {values.get('n')} | {values.get('mae')} | "
                     f"{values.get('lagged_baseline_mae')} | {values.get('mean_pinball')}")
    lines.extend(["", "## Hindsight component sensitivity", "",
                  "Positive reductions mean better than the baseline on the same oracle-valid cohort.", ""])
    for stat, value in summary["oracle_sensitivity"].items():
        lines.append(f"### {stat}: {value['triage_suggestion']}")
        for arm, scores in value["arms"].items():
            lines.append(f"- {arm}: pinball reduction {scores['pinball_reduction']:.6f}; "
                         f"MAE reduction {scores['mae_reduction']:.6f}; Brier reduction {scores['brier_reduction']}")
    lines.extend(["", "## How to use this result", "",
                  "- Investigate the suggested component only if its ordinary component scores and lagged baseline support it.",
                  "- If sensitivity is mixed, do not manufacture a winner: inspect per-stat residuals in components.csv.",
                  "- Compare all_quoted, selected_before_threshold and after_policy_and_cash in summary.json.",
                  "- Weakness that appears only after selection suggests selection amplification; it is not proof of a bad head.",
                  "- Good component scores with poor combined line probabilities motivate a composition experiment, not global offsets.",
                  "", "## Limitations", "", *[f"- {x}" for x in summary["limitations"]], ""])
    return "\n".join(lines)
