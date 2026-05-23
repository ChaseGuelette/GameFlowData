from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from scripts.audit_mlb_quote_clean_dropout import (
    REQUIRED_SAVED_PREDICTION_COLUMNS,
    classify_prediction_dropout,
    count_selected_quote_timing_violations,
    find_atomic_clean_quotes,
    resolve_model_dir,
    summarize_and_write_outputs,
    validate_saved_predictions_columns,
)


def _raw_rows(rows: list[dict]) -> pd.DataFrame:
    base = {
        "player_id": 10,
        "game_id": 1001,
        "market_key": "batter_hits",
        "bookmaker": "draftkings",
        "line": 0.5,
        "odds_american": -110,
        "snapshot_time": "2026-04-13T16:00:00Z",
        "inserted_at": None,
        "market_last_update": "2026-04-13T16:00:00Z",
        "commence_time": "2026-04-13T23:00:00Z",
    }
    out = []
    for row in rows:
        merged = base.copy()
        merged.update(row)
        out.append(merged)
    return pd.DataFrame(out)


def _prediction() -> pd.Series:
    return pd.Series(
        {
            "player_id": 10,
            "game_id": 1001,
            "stat": "batter_hits",
            "game_date": "2026-04-13",
        }
    )


def test_resolve_model_dir_accepts_artifacts_parent_with_production_child(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    production = artifacts / "production"
    production.mkdir(parents=True)

    assert resolve_model_dir(artifacts) == production


def test_atomic_pairing_requires_same_snapshot_for_over_and_under() -> None:
    raw = _raw_rows(
        [
            {"outcome_label": "Over", "snapshot_time": "2026-04-13T16:00:00Z"},
            {"outcome_label": "Under", "snapshot_time": "2026-04-13T16:05:00Z"},
        ]
    )

    clean = find_atomic_clean_quotes(raw, pd.Timestamp("2026-04-13T17:30:00Z"))

    assert clean.empty
    assert classify_prediction_dropout(_prediction(), raw, clean, placed_bet_keys=set()) == "no_paired_over_under"


def test_excluded_book_only_rows_classify_correctly() -> None:
    raw = _raw_rows(
        [
            {"bookmaker": "prizepicks", "outcome_label": "Over"},
            {"bookmaker": "prizepicks", "outcome_label": "Under"},
        ]
    )

    assert classify_prediction_dropout(_prediction(), raw, pd.DataFrame(), placed_bet_keys=set()) == "only_excluded_books"


def test_post_cutoff_rows_classify_as_only_after_cutoff() -> None:
    raw = _raw_rows(
        [
            {"outcome_label": "Over", "snapshot_time": "2026-04-13T18:00:00Z", "market_last_update": "2026-04-13T18:00:00Z"},
            {"outcome_label": "Under", "snapshot_time": "2026-04-13T18:00:00Z", "market_last_update": "2026-04-13T18:00:00Z"},
        ]
    )

    assert classify_prediction_dropout(
        _prediction(),
        raw,
        pd.DataFrame(),
        placed_bet_keys=set(),
        cutoff_ts=pd.Timestamp("2026-04-13T17:30:00Z"),
    ) == "only_after_cutoff"


def test_post_commence_rows_classify_as_only_post_commence() -> None:
    raw = _raw_rows(
        [
            {
                "outcome_label": "Over",
                "snapshot_time": "2026-04-13T23:30:00Z",
                "market_last_update": "2026-04-13T23:30:00Z",
                "commence_time": "2026-04-13T23:00:00Z",
            },
            {
                "outcome_label": "Under",
                "snapshot_time": "2026-04-13T23:30:00Z",
                "market_last_update": "2026-04-13T23:30:00Z",
                "commence_time": "2026-04-13T23:00:00Z",
            },
        ]
    )

    assert classify_prediction_dropout(
        _prediction(),
        raw,
        pd.DataFrame(),
        placed_bet_keys=set(),
        cutoff_ts=pd.Timestamp("2026-04-13T23:59:00Z"),
    ) == "only_post_commence"


def test_clean_quote_available_and_below_edge_are_distinct() -> None:
    raw = _raw_rows([
        {"outcome_label": "Over"},
        {"outcome_label": "Under"},
    ])
    clean = find_atomic_clean_quotes(raw, pd.Timestamp("2026-04-13T17:30:00Z"))
    bet_key = (10, 1001, "batter_hits")

    assert classify_prediction_dropout(_prediction(), raw, clean, placed_bet_keys={bet_key}) == "clean_quote_available"
    assert classify_prediction_dropout(_prediction(), raw, clean, placed_bet_keys=set()) == "clean_quote_exists_below_edge"


def test_saved_output_validator_fails_when_required_quote_columns_missing(tmp_path: Path) -> None:
    path = tmp_path / "predictions.csv"
    cols = sorted(REQUIRED_SAVED_PREDICTION_COLUMNS - {"selected_snapshot_time"})
    pd.DataFrame([{col: 1 for col in cols}]).to_csv(path, index=False)

    with pytest.raises(ValueError, match="selected_snapshot_time"):
        validate_saved_predictions_columns(path)


def test_slate_or_tminus_missing_decision_time_warns_not_fixed_cutoff_fail(tmp_path: Path) -> None:
    dropout_rows = pd.DataFrame(
        [
            {
                "player_id": 10,
                "game_id": 1001,
                "stat": "batter_hits",
                "game_date": "2026-04-13",
                "dropout_bucket": "clean_quote_exists_below_edge",
            }
        ]
    )
    clean_quotes = pd.DataFrame(
        [
            {
                "selected_snapshot_time": "2026-04-13T20:00:00Z",
                "game_date": "2026-04-13",
                "commence_time": "2026-04-13T23:00:00Z",
                "bookmaker": "draftkings",
            }
        ]
    )

    timing = count_selected_quote_timing_violations(
        clean_quotes,
        quote_decision_policy="slate_or_tminus",
        cutoff_time_et="13:30",
    )
    assert timing["cutoff_violations"] == 0
    assert timing["decision_time_missing"] == 1
    assert timing["commence_violations"] == 0

    summary = summarize_and_write_outputs(
        dropout_rows,
        clean_quotes,
        tmp_path,
        "13:30",
        quote_decision_policy="slate_or_tminus",
    )
    assert summary["decision"] == "WARN"
    assert "row-level decision time" in summary["reason"]


def test_slate_or_tminus_fails_when_selected_snapshot_after_decision_time(tmp_path: Path) -> None:
    dropout_rows = pd.DataFrame(
        [
            {
                "player_id": 10,
                "game_id": 1001,
                "stat": "batter_hits",
                "game_date": "2026-04-13",
                "dropout_bucket": "clean_quote_available",
            }
        ]
    )
    clean_quotes = pd.DataFrame(
        [
            {
                "selected_snapshot_time": "2026-04-13T20:00:00Z",
                "decision_time": "2026-04-13T19:59:00Z",
                "commence_time": "2026-04-13T23:00:00Z",
                "bookmaker": "draftkings",
            }
        ]
    )

    summary = summarize_and_write_outputs(
        dropout_rows,
        clean_quotes,
        tmp_path,
        "13:30",
        quote_decision_policy="slate_or_tminus",
    )
    assert summary["decision"] == "FAIL"
    assert summary["decision_time_violations"] == 1
