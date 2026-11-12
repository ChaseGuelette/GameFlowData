"""Build a verified, saved-file-only NBA REB opportunity view."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import shutil
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.backtesting.nba.direct_reb_sweep import _load_verified_sweep  # noqa: E402

SCHEMA_VERSION = "nba_reb_opportunity_view_v1"
KEY = ["player_id", "game_id", "game_date", "stat"]
QUOTE_KEY = [*KEY, "bookmaker", "line"]
RAW_CELL = "direct_reb_9af8c04906d4f2068b5c2c076e9f21e9c9d05d15ae09cfb257c6fb44d4c84c59"
BL_CELL = "direct_reb_e56b9b4fda1cd9eb9971894fb9eaa6e8d0376c41d40790b499e2567e8d2896d5"


class ViewError(RuntimeError):
    """Raised when saved evidence cannot support an unambiguous view."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ViewError(f"Expected JSON object: {path}")
    return value


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(
        path,
        dtype={"player_id": "string", "game_id": "string", "stat": "string", "bookmaker": "string"},
        float_precision="round_trip",
    )


def _numbers(values: Any) -> np.ndarray[Any, np.dtype[np.float64]]:
    return np.asarray(pd.to_numeric(values), dtype=np.float64)


def _same_number(left: Any, right: Any, *, name: str) -> None:
    if not math.isclose(float(left), float(right), rel_tol=1e-11, abs_tol=1e-10):
        raise ViewError(f"Numeric contradiction for {name}: {left!r} != {right!r}")


def _require_unique(frame: pd.DataFrame, columns: list[str], label: str) -> None:
    if frame.duplicated(columns, keep=False).any():
        raise ViewError(f"Ambiguous duplicate {label} keys: {columns}")


def _require_unique_columns(frame: pd.DataFrame, label: str) -> None:
    duplicated = frame.columns[frame.columns.duplicated()].tolist()
    if duplicated:
        raise ViewError(f"Duplicate {label} columns: {duplicated}")


def _verify_diagnostic(root: Path) -> tuple[dict[str, Any], dict[str, str]]:
    manifest_path = root / "diagnostic_manifest.json"
    manifest = _json(manifest_path)
    if manifest.get("state") != "complete" or not manifest.get("baseline_reproduced"):
        raise ViewError("Diagnostic bundle is not complete/reproduced")
    files = manifest.get("files")
    required = {"components.csv", "totals.csv", "quote_probabilities.csv", "summary.json", "report.md"}
    if not isinstance(files, dict) or set(files) != required:
        raise ViewError("Diagnostic receipt inventory is incomplete or unexpected")
    receipts: dict[str, str] = {"diagnostic_manifest.json": _sha256(manifest_path)}
    for name, receipt in files.items():
        path = root / name
        if not isinstance(receipt, dict) or not path.is_file():
            raise ViewError(f"Invalid diagnostic receipt: {name}")
        actual = _sha256(path)
        if actual != receipt.get("sha256") or path.stat().st_size != receipt.get("bytes"):
            raise ViewError(f"Diagnostic receipt mismatch: {name}")
        receipts[name] = actual
    return manifest, receipts


def _verify_identity_linkage(diag: dict[str, Any], sweep: dict[str, Any]) -> dict[str, Any]:
    plan = sweep.get("plan", {})
    reference = plan.get("reference_sweep", {})
    benchmark = plan.get("benchmark", {})
    reference_path = Path(str(reference.get("path"))).resolve(strict=True)
    diagnostic_reference_path = Path(str(diag.get("sweep_dir"))).resolve(strict=True)
    reference_manifest = reference_path / "sweep_manifest.json"
    benchmark_path = Path(str(benchmark.get("path"))).resolve(strict=True) / "benchmark_manifest.json"
    if reference_path != diagnostic_reference_path:
        raise ViewError("Diagnostic and direct sweep reference different saved sweeps")
    reference_hash = _sha256(reference_manifest)
    if not (
        reference_hash == reference.get("manifest_sha256") == diag.get("reference_manifest_sha256")
    ):
        raise ViewError("Reference sweep manifest identity linkage failed")
    benchmark_hash = _sha256(benchmark_path)
    benchmark_manifest = _json(benchmark_path)
    if benchmark_hash != benchmark.get("manifest_sha256"):
        raise ViewError("Benchmark manifest receipt mismatch")
    if benchmark.get("model_identity") != diag.get("model_identity"):
        raise ViewError("Diagnostic and sweep benchmark model identities differ")
    manifest_model = benchmark_manifest.get("model_identity")
    if manifest_model is not None and manifest_model != diag.get("model_identity"):
        raise ViewError("Benchmark manifest model identity differs")
    return {
        "model_identity": diag["model_identity"],
        "reference_sweep_path": str(reference_path),
        "reference_manifest_sha256": reference_hash,
        "benchmark_path": str(benchmark_path.parent),
        "benchmark_manifest_sha256": benchmark_hash,
        "sweep_plan_identity": plan.get("plan_identity"),
        "diagnostic_canonical_input_identity": diag.get("canonical_input_identity"),
        "reference_input_identity": reference.get("input_identity"),
    }


def _build_player_games(diagnostic: Path) -> pd.DataFrame:
    totals = _read_csv(diagnostic / "totals.csv")
    total = totals.loc[(totals["stat"] == "reb") & (totals["arm"] == "baseline")].copy()
    if len(total) != 1579:
        raise ViewError(f"Expected 1579 baseline REB player-games, found {len(total)}")
    _require_unique(total, KEY, "baseline player-game")
    components = _read_csv(diagnostic / "components.csv")
    parts: list[pd.DataFrame] = []
    for component in ("minutes", "reb_rate"):
        part = components.loc[components["component"] == component].copy()
        _require_unique(part, ["player_id", "game_id", "game_date"], component)
        keep = ["player_id", "game_id", "game_date", "actual", "lagged_baseline", "pred_q10", "pred_q25", "pred_q50", "pred_q75", "pred_q90"]
        part = part[keep].rename(columns={name: f"{component}_{name}" for name in keep[3:]})
        parts.append(part)
    columns = [
        *KEY, "player_name", "team_id", "game_time", "pred_mean", "pred_std", "pred_median",
        "pred_q10", "pred_q25", "pred_q50", "pred_q75", "pred_q90", "actual", "actual_minutes",
        "oracle_eligible", "oracle_non_executable",
    ]
    result = total[columns].rename(
        columns={
            "pred_mean": "combined_reb_pred_mean", "pred_std": "combined_reb_pred_std",
            "pred_median": "combined_reb_pred_median", "pred_q10": "combined_reb_pred_q10",
            "pred_q25": "combined_reb_pred_q25", "pred_q50": "combined_reb_pred_q50",
            "pred_q75": "combined_reb_pred_q75", "pred_q90": "combined_reb_pred_q90",
            "actual": "hindsight_actual_rebounds", "actual_minutes": "hindsight_actual_minutes",
        }
    )
    for part in parts:
        result = result.merge(part, on=["player_id", "game_id", "game_date"], how="left", validate="one_to_one")
    if result.isna().any(axis=None) and result[["minutes_pred_q50", "reb_rate_pred_q50"]].isna().any(axis=None):
        raise ViewError("Missing required component join")
    _same_number((result["hindsight_actual_rebounds"] - result["reb_rate_actual"] * result["hindsight_actual_minutes"]).abs().max(), 0.0, name="actual REB rate identity")
    result["combined_reb_median_signed_error_actual_minus_pred"] = result["hindsight_actual_rebounds"] - result["combined_reb_pred_median"]
    result["combined_reb_median_absolute_error"] = result["combined_reb_median_signed_error_actual_minus_pred"].abs()
    result["combined_reb_q90_minus_q10_width"] = result["combined_reb_pred_q90"] - result["combined_reb_pred_q10"]
    result["minutes_median_signed_error_actual_minus_pred"] = result["hindsight_actual_minutes"] - result["minutes_pred_q50"]
    result["minutes_median_absolute_error"] = result["minutes_median_signed_error_actual_minus_pred"].abs()
    result["minutes_q90_minus_q10_width"] = result["minutes_pred_q90"] - result["minutes_pred_q10"]
    result["reb_rate_median_signed_error_actual_minus_pred"] = result["reb_rate_actual"] - result["reb_rate_pred_q50"]
    result["reb_rate_median_absolute_error"] = result["reb_rate_median_signed_error_actual_minus_pred"].abs()
    result["reb_rate_q90_minus_q10_width"] = result["reb_rate_pred_q90"] - result["reb_rate_pred_q10"]
    return result.sort_values(["game_date", "game_id", "player_id"], kind="stable").reset_index(drop=True)


def _selected_cells(summary: pd.DataFrame) -> dict[str, str]:
    chosen = summary.loc[
        (summary["model"] == "control")
        & (np.isclose(_numbers(summary["edge_threshold"]), 0.05))
        & summary["treatment"].isin(["raw", "fixed_bl"])
    ]
    found = dict(zip(chosen["treatment"].astype(str), chosen["cell_id"].astype(str), strict=True))
    if found != {"raw": RAW_CELL, "fixed_bl": BL_CELL}:
        raise ViewError(f"Unexpected selected control cells: {found}")
    return found


def _quote_probability_map(diagnostic: Path) -> pd.DataFrame:
    frame = _read_csv(diagnostic / "quote_probabilities.csv")
    frame = frame.loc[(frame["stat"] == "reb") & (frame["arm"] == "baseline")].copy()
    _require_unique(frame, QUOTE_KEY, "diagnostic quote probability")
    if len(frame) != 1033:
        raise ViewError(f"Expected 1033 diagnostic REB quotes, found {len(frame)}")
    return frame[QUOTE_KEY + ["over_prob", "market_over", "actual"]].rename(
        columns={"over_prob": "diagnostic_empirical_over_prob_unclipped", "market_over": "diagnostic_market_over_prob", "actual": "diagnostic_actual"}
    )


def _side_rows(
    treatment: str,
    edges: pd.DataFrame,
    predictions: pd.DataFrame,
    bets: pd.DataFrame,
    diagnostic_quotes: pd.DataFrame,
    player_games: pd.DataFrame,
) -> pd.DataFrame:
    quoted = edges.loc[edges["quote_status"] == "quoted"].copy()
    if len(quoted) != 1033 or len(predictions) != 1033:
        raise ViewError(f"{treatment}: quoted/prediction inventory mismatch")
    _require_unique(quoted, QUOTE_KEY, f"{treatment} quote")
    _require_unique(predictions, QUOTE_KEY, f"{treatment} prediction")
    if set(map(tuple, quoted[QUOTE_KEY].itertuples(index=False, name=None))) != set(map(tuple, predictions[QUOTE_KEY].itertuples(index=False, name=None))):
        raise ViewError(f"{treatment}: predictions do not cover quoted occurrences one-to-one")
    base = quoted.merge(diagnostic_quotes, on=QUOTE_KEY, how="left", validate="one_to_one")
    if base["diagnostic_empirical_over_prob_unclipped"].isna().any():
        raise ViewError(f"{treatment}: missing diagnostic probability")
    raw_expected = np.clip(_numbers(base["diagnostic_empirical_over_prob_unclipped"]), 0.05, 0.90)
    if treatment == "raw" and not np.allclose(raw_expected, _numbers(base["over_prob"]), rtol=0, atol=1e-12):
        raise ViewError("Raw replay probabilities do not match diagnostic empirical clamp")
    prediction_fields = [*QUOTE_KEY, "selected_side", "selected_line", "selected_price", "selected_bookmaker", "selected_snapshot_time", "selected_market_last_update", "selected_bookmaker_last_update", "selected_decision_time", "max_edge", "actual"]
    base = base.drop(columns=[name for name in prediction_fields[len(QUOTE_KEY):] if name in base.columns])
    base = base.merge(predictions[prediction_fields], on=QUOTE_KEY, how="left", validate="one_to_one")
    if not np.allclose(_numbers(base["actual"]), _numbers(base["diagnostic_actual"]), rtol=0, atol=0):
        raise ViewError(f"{treatment}: diagnostic/source actual contradiction")
    rows: list[pd.DataFrame] = []
    for side in ("over", "under"):
        row = base.copy()
        row["treatment"] = treatment
        row["side"] = side
        row["price"] = row[f"{side}_odds"]
        row["replay_model_prob"] = row[f"{side}_prob"]
        row["paired_no_vig_market_prob"] = row[f"implied_{side}"]
        row["edge"] = row[f"{side}_edge"]
        row["diagnostic_empirical_side_prob_unclipped"] = np.where(side == "over", row["diagnostic_empirical_over_prob_unclipped"], 1.0 - row["diagnostic_empirical_over_prob_unclipped"])
        row["diagnostic_probability_semantics"] = "strict empirical P(samples>line)" if side == "over" else "complement 1-P(samples>line); includes pushes in complement"
        row["raw_replay_clamped_side_prob"] = np.where(side == "over", raw_expected, 1.0 - raw_expected)
        row["bl_posterior_side_prob"] = row[f"posterior_{side}"] if f"posterior_{side}" in row else np.nan
        row["saved_best_side"] = row["selected_side"]
        row["saved_selected_this_side"] = row["selected_side"] == side
        selected = row["saved_selected_this_side"]
        exact_selection = (
            np.isclose(_numbers(row["selected_line"]), _numbers(row["line"]), equal_nan=False)
            & np.isclose(_numbers(row["selected_price"]), _numbers(row["price"]), equal_nan=False)
            & (row["selected_bookmaker"].astype(str) == row["bookmaker"].astype(str))
            & (row["selected_snapshot_time"].astype(str) == row[f"{side}_snapshot_time"].astype(str))
            & (row["selected_market_last_update"].astype(str) == row[f"{side}_market_last_update"].astype(str))
            & (row["selected_bookmaker_last_update"].astype(str) == row[f"{side}_bookmaker_last_update"].astype(str))
            & (row["selected_decision_time"].astype(str) == row[f"{side}_decision_time"].astype(str))
        )
        if not exact_selection.loc[selected].all():
            raise ViewError(f"{treatment}/{side}: saved selection provenance mismatch")
        row["meets_min_edge_threshold"] = selected & (_numbers(row["edge"]) >= 0.05 - 1e-12)
        actual = _numbers(row["actual"])
        line = _numbers(row["line"])
        row["event_push"] = actual == line
        win = (actual > line) if side == "over" else (actual < line)
        row["binary_event_win_push_excluded"] = pd.array([pd.NA if push else bool(won) for push, won in zip(row["event_push"], win, strict=True)], dtype="boolean")
        row["side_snapshot_time"] = row[f"{side}_snapshot_time"]
        row["side_market_last_update"] = row[f"{side}_market_last_update"]
        row["side_bookmaker_last_update"] = row[f"{side}_bookmaker_last_update"]
        row["side_decision_time"] = row[f"{side}_decision_time"]
        row["side_bookmaker"] = row[f"{side}_bookmaker"]
        rows.append(row)
    long = pd.concat(rows, ignore_index=True)
    bet_key = [*KEY, "side", "bookmaker", "line", "price", "selected_snapshot_time", "selected_market_last_update", "selected_bookmaker_last_update", "selected_decision_time"]
    ledger = bets.rename(columns={"odds": "price"}).copy()
    _require_unique(ledger, bet_key, f"{treatment} funded bet")
    ledger_fields = bet_key + ["stake", "outcome", "profit", "model_prob", "implied_prob", "edge", "posterior_prob", "actual"]
    merged = long.merge(ledger[ledger_fields].assign(funded_wager=True), on=bet_key, how="left", validate="one_to_one", suffixes=("", "_ledger"))
    merged["funded_wager"] = merged["funded_wager"].fillna(False).astype(bool)
    if int(np.asarray(merged["funded_wager"], dtype=bool).sum()) != len(bets):
        raise ViewError(f"{treatment}: not every funded bet matched exactly once")
    if not bool(np.asarray(merged.loc[merged["funded_wager"], "saved_selected_this_side"] & merged.loc[merged["funded_wager"], "meets_min_edge_threshold"], dtype=bool).all()):
        raise ViewError(f"{treatment}: funded wager was not saved-selected and threshold eligible")
    for left, right in (("replay_model_prob", "model_prob"), ("paired_no_vig_market_prob", "implied_prob"), ("edge", "edge_ledger"), ("actual", "actual_ledger")):
        funded = merged["funded_wager"]
        if not np.allclose(_numbers(merged.loc[funded, left]), _numbers(merged.loc[funded, right]), rtol=0, atol=1e-12):
            raise ViewError(f"{treatment}: funded {left} contradicts ledger")
    if treatment == "fixed_bl" and not np.allclose(_numbers(merged.loc[merged["funded_wager"], "bl_posterior_side_prob"]), _numbers(merged.loc[merged["funded_wager"], "posterior_prob"]), rtol=0, atol=1e-12):
        raise ViewError("fixed_bl: posterior probability contradicts ledger")
    merged["stake"] = merged["stake"].where(merged["funded_wager"])
    merged["profit"] = merged["profit"].where(merged["funded_wager"])
    merged["outcome"] = merged["outcome"].where(merged["funded_wager"])
    source_names = {
        "player_name": "quote_source_player_name",
        "team_id": "quote_source_team_id",
        "game_time": "quote_source_game_time",
        "pred_mean": "quote_source_combined_reb_pred_mean",
        "pred_std": "quote_source_combined_reb_pred_std",
        "pred_median": "quote_source_combined_reb_pred_median",
        "pred_q10": "quote_source_combined_reb_pred_q10",
        "pred_q25": "quote_source_combined_reb_pred_q25",
        "pred_q50": "quote_source_combined_reb_pred_q50",
        "pred_q75": "quote_source_combined_reb_pred_q75",
        "pred_q90": "quote_source_combined_reb_pred_q90",
    }
    merged = merged.rename(columns={name: renamed for name, renamed in source_names.items() if name in merged.columns})
    component_columns = [name for name in player_games.columns if name not in KEY]
    merged = merged.merge(player_games[KEY + component_columns], on=KEY, how="left", validate="many_to_one")
    _require_unique_columns(merged, f"{treatment} joined opportunity")
    if bool(np.asarray(merged["combined_reb_pred_mean"].isna(), dtype=bool).any()):
        raise ViewError(f"{treatment}: missing player-game component join")
    if not np.allclose(_numbers(merged["hindsight_actual_rebounds"]), _numbers(merged["actual"]), rtol=0, atol=0):
        raise ViewError(f"{treatment}: player-game actual does not match original edge row")
    for suffix in ("mean", "std", "median", "q10", "q25", "q50", "q75", "q90"):
        source = f"quote_source_combined_reb_pred_{suffix}"
        authoritative = f"combined_reb_pred_{suffix}"
        if not np.allclose(_numbers(merged[source]), _numbers(merged[authoritative]), rtol=0, atol=1e-12):
            raise ViewError(f"{treatment}: {authoritative} does not match original edge row")
    keep = [
        "treatment", *KEY, "player_name", "team_id", "quote_source_player_name", "quote_source_team_id",
        "quote_source_game_time", "bookmaker", "line", "side", "price", "over_odds", "under_odds",
        "paired_no_vig_market_prob", "diagnostic_market_over_prob", "diagnostic_empirical_over_prob_unclipped",
        "diagnostic_empirical_side_prob_unclipped", "diagnostic_probability_semantics", "raw_replay_clamped_side_prob",
        "bl_posterior_side_prob", "replay_model_prob", "edge", "saved_best_side", "saved_selected_this_side",
        "meets_min_edge_threshold", "funded_wager", "stake", "outcome", "profit", "event_push",
        "binary_event_win_push_excluded", "actual", "sample_status", "quote_status", "market_key", "quote_decision_policy",
        "bet_decision_time", "side_snapshot_time", "side_market_last_update", "side_bookmaker_last_update",
        "side_decision_time", "side_bookmaker", "selected_snapshot_time", "selected_market_last_update",
        "selected_bookmaker_last_update", "selected_decision_time", "selected_bookmaker", "source_kind", "source_snapshot_time",
        "task_id", *[name for name in source_names.values() if name.startswith("quote_source_combined_")],
        *[name for name in component_columns if name not in {"player_name", "team_id"}],
    ]
    result = merged.loc[:, keep].rename(columns={"actual": "hindsight_actual_rebounds_at_quote"})
    _require_unique_columns(result, f"{treatment} output")
    return result


def _add_quote_reconciliation(player_games: pd.DataFrame, opportunities: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    quoted = opportunities.loc[(opportunities["treatment"] == "raw") & (opportunities["side"] == "over"), QUOTE_KEY]
    _require_unique(quoted, QUOTE_KEY, "real quoted occurrence")
    counts = quoted.groupby(KEY, dropna=False).size().reset_index(name="quoted_occurrences")
    result = player_games.merge(counts, on=KEY, how="left", validate="one_to_one")
    result["quoted_occurrences"] = result["quoted_occurrences"].fillna(0).astype("int64")
    result["has_quote"] = result["quoted_occurrences"] > 0
    _require_unique(result, KEY, "reconciled player-game")
    if int(_numbers(result["quoted_occurrences"]).sum()) != len(quoted):
        raise ViewError("Quoted occurrence counts do not reconcile to real quote keys")
    quoted_games = int(np.asarray(result["has_quote"], dtype=bool).sum())
    no_quote_games = int((~np.asarray(result["has_quote"], dtype=bool)).sum())
    if quoted_games + no_quote_games != len(result) or len(result) != len(player_games):
        raise ViewError("Player-game quote reconciliation dropped or duplicated keys")
    return result, {"quoted_player_games": quoted_games, "no_quote_player_games": no_quote_games, "quoted_occurrences": len(quoted)}


def _funnel(opportunities: pd.DataFrame, player_games: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for treatment in ("raw", "fixed_bl"):
        subset = opportunities.loc[opportunities["treatment"] == treatment]
        stages = [
            ("eligible_player_games", player_games, 0, 0),
            ("quoted_occurrences", subset.loc[subset["side"] == "over"], 1033, 0),
            ("quoted_side_rows", subset, 1033, len(subset)),
            ("saved_best_side_selected", subset.loc[subset["saved_selected_this_side"]], 1033, 1033),
            ("minimum_edge_threshold_eligible", subset.loc[subset["meets_min_edge_threshold"]], None, None),
            ("funded_wagers", subset.loc[subset["funded_wager"]], None, None),
        ]
        for stage, frame, quote_count, side_count in stages:
            rows.append({
                "treatment": treatment, "stage": stage, "player_games": len(frame[KEY].drop_duplicates()),
                "game_dates": frame["game_date"].nunique(), "quote_occurrences": quote_count if quote_count is not None else len(frame),
                "side_rows": side_count if side_count is not None else len(frame),
                "funded_wagers": len(frame) if stage == "funded_wagers" else None,
                "stake": float(np.nansum(_numbers(frame["stake"]))) if stage == "funded_wagers" else None,
                "profit": float(np.nansum(_numbers(frame["profit"]))) if stage == "funded_wagers" else None,
            })
    return pd.DataFrame(rows)


def _field_roles(frame: pd.DataFrame) -> dict[str, str]:
    roles: dict[str, str] = {}
    for name in frame.columns:
        if "actual" in name or "error" in name or name.startswith("oracle_") or name in {"outcome", "profit", "event_push", "binary_event_win_push_excluded"}:
            roles[name] = "postgame_and_hindsight"
        elif name.startswith(("combined_reb_pred_", "minutes_pred_", "reb_rate_pred_", "quote_source_combined_reb_pred_")) or "lagged_baseline" in name or name.endswith("_q90_minus_q10_width"):
            roles[name] = "pregame_forecasts"
        elif name.startswith("saved_") or name in {"meets_min_edge_threshold", "funded_wager", "stake"}:
            roles[name] = "policy_and_selection"
        elif "prob" in name or name == "edge":
            roles[name] = "probability_and_market"
        elif name in {"treatment", *KEY, "player_name", "team_id", "bookmaker", "line", "side"}:
            roles[name] = "identity"
        else:
            roles[name] = "source_market_or_context"
    return roles


def _dictionary(player_games: pd.DataFrame, opportunities: pd.DataFrame) -> dict[str, Any]:
    table_roles = {"player_games.csv": _field_roles(player_games), "opportunities.csv": _field_roles(opportunities)}
    groups = {
        role: {table: [name for name, assigned in roles.items() if assigned == role] for table, roles in table_roles.items()}
        for role in ("identity", "pregame_forecasts", "probability_and_market", "policy_and_selection", "postgame_and_hindsight", "source_market_or_context")
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "grain": {
            "player_games.csv": "one baseline eligible REB player-game, including no-quote rows",
            "opportunities.csv": "one quoted bookmaker-line side per treatment; two sides x two treatments per quote",
            "selection_funnel.csv": "one aggregate stage per treatment",
        },
        "groups": groups,
        "field_roles": table_roles,
        "semantics": {
            "signed_error": "actual minus predicted median; positive means actual exceeded forecast",
            "diagnostic_empirical_over_prob_unclipped": "saved empirical P(samples > line), not Gaussian and not clamped",
            "diagnostic_empirical_side_prob_unclipped": "over uses empirical P(samples > line); under is the complement and includes sample pushes",
            "raw_replay_clamped_side_prob": "incumbent raw betting replay: over clipped to [0.05, 0.90], under=1-over",
            "bl_posterior_side_prob": "saved fixed-BL posterior; null for raw treatment",
            "replay_model_prob": "probability actually used by the saved treatment for edge/selection",
            "paired_no_vig_market_prob": "saved paired no-vig probability for the row side",
            "stake_profit_missingness": "stake/outcome/profit are null unless funded_wager=true; no hypothetical P&L",
            "funnel_accounting": "stake/profit/funded count are reported only at funded_wagers; upstream stages describe populations, not separate portfolios",
            "binary_event": "binary_event_win_push_excluded is null on an exact-line push",
            "hindsight": "actuals, errors, outcomes, and realized P&L are postgame diagnostics, never pregame features",
            "source_grain_limit": "bookmaker-line quote occurrences only; selection does not imply whole-market coverage",
            "units": {"minutes": "minutes", "reb_rate": "rebounds per minute", "rebounds": "count", "probabilities_and_edges": "0-1", "prices": "American odds", "stake_profit": "saved ledger currency units"},
        },
        "columns": {"player_games.csv": list(player_games.columns), "opportunities.csv": list(opportunities.columns)},
    }


def _write_csv(path: Path, frame: pd.DataFrame) -> dict[str, Any]:
    _require_unique_columns(frame, path.name)
    frame.to_csv(path, index=False)
    return {"rows": len(frame), "columns": list(frame.columns), "bytes": path.stat().st_size, "sha256": _sha256(path)}


def build(diagnostic_dir: Path, sweep_dir: Path, output_dir: Path, *, dry_run: bool = False) -> Path:
    diagnostic = diagnostic_dir.resolve(strict=True)
    sweep_root = sweep_dir.resolve(strict=True)
    output = output_dir.resolve(strict=False)
    if output.exists():
        raise ViewError(f"Output path already exists: {output}")
    if output == diagnostic or output == sweep_root or diagnostic in output.parents or sweep_root in output.parents:
        raise ViewError("Output must be isolated from source bundles")
    diag_manifest, diag_receipts = _verify_diagnostic(diagnostic)
    sweep_manifest_path = sweep_root / "direct_reb_sweep_manifest.json"
    initial_sweep_manifest_hash = _sha256(sweep_manifest_path)
    sweep_manifest, summary, tables = _load_verified_sweep(sweep_root)
    if _sha256(sweep_manifest_path) != initial_sweep_manifest_hash:
        raise ViewError("Sweep manifest changed during verification")
    identities = _verify_identity_linkage(diag_manifest, sweep_manifest)
    selected = _selected_cells(summary)
    source_hashes = {str(diagnostic / name): value for name, value in diag_receipts.items()}
    source_hashes[str(sweep_manifest_path)] = initial_sweep_manifest_hash
    cell_receipts = {cell["cell_id"]: cell["files"] for cell in sweep_manifest["completed_cells"]}
    for cell_id in selected.values():
        for name in ("all_bookmaker_edges.csv", "predictions.csv", "bets.csv", "config.json", "metrics.json"):
            path = sweep_root / "cells" / cell_id / name
            source_hashes[str(path)] = str(cell_receipts[cell_id][name]["sha256"])
    source_hashes[str(Path(identities["reference_sweep_path"]) / "sweep_manifest.json")] = str(identities["reference_manifest_sha256"])
    source_hashes[str(Path(identities["benchmark_path"]) / "benchmark_manifest.json")] = str(identities["benchmark_manifest_sha256"])

    player_games = _build_player_games(diagnostic)
    quote_map = _quote_probability_map(diagnostic)
    opportunity_parts = []
    for treatment, cell_id in selected.items():
        cell = tables[cell_id]
        opportunity_parts.append(_side_rows(treatment, cell["all_bookmaker_edges.csv"], cell["predictions.csv"], cell["bets.csv"], quote_map, player_games))
    opportunities = pd.concat(opportunity_parts, ignore_index=True).sort_values(["treatment", "game_date", "game_id", "player_id", "bookmaker", "line", "side"], kind="stable").reset_index(drop=True)
    _require_unique_columns(opportunities, "combined opportunities")
    output_key = ["treatment", *QUOTE_KEY, "side"]
    _require_unique(opportunities, output_key, "opportunity output")
    player_games, quote_reconciliation = _add_quote_reconciliation(player_games, opportunities)
    expected = {"raw": (612, 6120.0, -461.461819517935), "fixed_bl": (140, 1400.0, 36.274261801531864)}
    accounting: dict[str, Any] = {}
    for treatment, (count, stake, profit) in expected.items():
        rows = opportunities.loc[(opportunities["treatment"] == treatment) & opportunities["funded_wager"]]
        if len(rows) != count:
            raise ViewError(f"{treatment}: expected {count} funded wagers, found {len(rows)}")
        _same_number(rows["stake"].sum(), stake, name=f"{treatment} stake")
        _same_number(rows["profit"].sum(), profit, name=f"{treatment} profit")
        accounting[treatment] = {"wagers": len(rows), "stake": float(rows["stake"].sum()), "profit": float(rows["profit"].sum())}
    funnel = _funnel(opportunities, player_games)
    if dry_run:
        print(json.dumps({"player_games": len(player_games), "opportunities": len(opportunities), "accounting": accounting}, indent=2))
        return output
    temp = output.parent / f".{output.name}.incomplete-{os.getpid()}"
    if temp.exists():
        raise ViewError(f"Temporary output path already exists: {temp}")
    temp.mkdir(parents=True)
    try:
        files: dict[str, Any] = {}
        files["player_games.csv"] = _write_csv(temp / "player_games.csv", player_games)
        files["opportunities.csv"] = _write_csv(temp / "opportunities.csv", opportunities)
        files["selection_funnel.csv"] = _write_csv(temp / "selection_funnel.csv", funnel)
        dictionary = _dictionary(player_games, opportunities)
        (temp / "data_dictionary.json").write_text(json.dumps(dictionary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        files["data_dictionary.json"] = {"bytes": (temp / "data_dictionary.json").stat().st_size, "sha256": _sha256(temp / "data_dictionary.json")}
        manifest = {
            "schema_version": SCHEMA_VERSION, "state": "complete", "decision_grade": False,
            "settings": {"model": "game_market_free research control: minutes x reb_rate", "model_dir": diag_manifest["model_dir"], "excluded_game_market_features": ["line_spread", "line_total"], "injury_features_excluded": True, "player_prop_inputs_retained": True, "edge_threshold": 0.05, "treatments": ["raw", "fixed_bl"], "cells": selected},
            "identities": identities,
            "sources": {"diagnostic_dir": str(diagnostic), "sweep_dir": str(sweep_root), "sha256": source_hashes},
            "counts": {"player_games": len(player_games), **quote_reconciliation, "opportunity_side_rows": len(opportunities), "accounting": accounting},
            "join_reconciliation": {"player_game_key_unique": True, "all_unquoted_player_game_keys_retained": True, "quote_key_unique_per_treatment": True, "output_key_unique": True, "diagnostic_quote_match": f"{quote_reconciliation['quoted_occurrences']} exact one-to-one per treatment", "funded_membership": "exact side/book/line/price plus selected provenance"},
            "limitations": ["Saved discovery window only; no arbitrary-date inference.", "No role-change, injury, opponent, or newly acquired context.", "Under empirical diagnostic probability is complement semantics, not strict P(samples < line).", "This audit view makes no causal or profitable-policy claim."],
            "files": files,
        }
        report = f"""# NBA REB saved-file opportunity view\n\nDecision grade: **false**. This joins the original minutes x rebound-rate control forecast to its saved quote, selection, and wager evidence.\n\n- Player-games: {len(player_games):,} ({quote_reconciliation['quoted_player_games']:,} with quotes; {quote_reconciliation['no_quote_player_games']:,} no quote retained; {quote_reconciliation['quoted_occurrences']:,} quoted occurrences)\n- Opportunity rows: {len(opportunities):,} (two sides x {quote_reconciliation['quoted_occurrences']:,} quotes x raw/fixed-BL)\n- Raw funded wagers: 612; stake 6,120.00; profit -461.461819517935\n- Fixed-BL funded wagers: 140; stake 1,400.00; profit 36.274261801531864\n- Books represented: draftkings 866 quotes; pinnacle 167 quotes\n\nProbabilities remain distinct: diagnostic empirical over, its explicitly labeled under complement, incumbent raw clamp/complement, and fixed-BL posterior. Non-wagers have null stake/outcome/profit. Pushes are excluded from binary event scoring.\n\nLimitations: this is the reused 2026-10-21 through 2026-11-02 discovery window, not arbitrary-date inference. No role-change, injury, opponent, or other newly acquired context is joined. The output supports audit and downstream analysis; it does not establish model cause, policy profitability, promotion, or deployment readiness.\n"""
        (temp / "report.md").write_text(report, encoding="utf-8")
        files["report.md"] = {"bytes": (temp / "report.md").stat().st_size, "sha256": _sha256(temp / "report.md")}
        # Recheck every consumed source immediately before finalization.
        for name, expected_hash in source_hashes.items():
            if _sha256(Path(name)) != expected_hash:
                raise ViewError(f"Consumed source changed during build: {name}")
        (temp / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temp.rename(output)
    except BaseException:
        shutil.rmtree(temp, ignore_errors=True)
        raise
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--diagnostic-dir", type=Path, required=True)
    parser.add_argument("--sweep-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = build(args.diagnostic_dir, args.sweep_dir, args.output_dir, dry_run=args.dry_run)
    print(result)


if __name__ == "__main__":
    main()
