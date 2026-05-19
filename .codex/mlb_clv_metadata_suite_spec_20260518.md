# Implementation Spec: MLB CLV Metadata and Timing Suite Hardening

## Goal
Prepare GameFlowData to rerun MLB `batter_hits` quote-clean backtests with CLV-decision-grade artifacts and diagnostics. Add richer bet artifact metadata to the backtesting harness, then upgrade the CLV suite to use actual selected quote timestamps, support +15/+30/+60 timing horizons, emit detailed unmatched reasons, and expose measurement-quality gates.

## Target Files
- `src/backtesting/bet_simulator.py`
- `scripts/analyze_mlb_batter_hits_clv.py`
- `scripts/diagnose_mlb_clv_failure_modes.py`
- `tests/test_bet_simulator.py`
- `tests/test_analyze_mlb_batter_hits_clv.py`
- `tests/test_diagnose_mlb_clv_failure_modes.py` if needed

Do not edit unrelated files.

## GameFlow Invariants
- Do not change model probabilities, calibration, CDF logic, or betting edge math.
- Do not introduce global conformal recalibration.
- Preserve temporal ordering: `bet_quote_time <= bet_time < clv_quote_time <= commence_or_close_cutoff`.
- Main code must work with historical/future frequent odds snapshots, but do not add DB writes or run DB mutations.
- No Supabase MCP usage.

## Requirements

### 1. Backtesting artifact metadata
Extend `Bet` and `BetSimulator.place_bet/evaluate_predictions/to_dataframe` so future `bets.csv` can carry selected quote audit metadata.

Required new fields in the `Bet` dataclass and `to_dataframe()` output:
- `selected_market_last_update`
- `selected_bookmaker_last_update`
- `selected_line`
- `selected_price`
- `selected_side`
- `selected_bookmaker`
- `over_market_last_update`
- `under_market_last_update`
- `over_bookmaker_last_update`
- `under_bookmaker_last_update`
- `over_bookmaker`
- `under_bookmaker`

Existing fields must remain:
- `bookmaker`
- `selected_snapshot_time`
- `over_snapshot_time`
- `under_snapshot_time`

Rules:
- Keep backwards compatibility: all new parameters optional and default `None`.
- In `evaluate_predictions`, populate side-specific selected fields from the prediction row when available.
- For OVER bets, selected side should be `over`, selected price should default to `over_odds`, selected line to `line`, selected bookmaker to `over_bookmaker` if present else `bookmaker`.
- For UNDER bets, selected side should be `under`, selected price should default to `under_odds`, selected line to `line`, selected bookmaker to `under_bookmaker` if present else `bookmaker`.
- Use row fields when present: `selected_market_last_update`, side-specific `over_market_last_update` / `under_market_last_update`, `selected_bookmaker_last_update`, side-specific bookmaker update fields, etc. Do not fail if absent.
- Ensure `bookmaker` still behaves as before and is filled with selected bookmaker when side-specific selected bookmaker exists.

### 2. CLV normalize_bets improvements
In `scripts/analyze_mlb_batter_hits_clv.py`:
- Prefer existing `bet_snapshot_time` if present.
- Else use `selected_snapshot_time`.
- Else side-specific fallback: for under use `under_snapshot_time`, for over use `over_snapshot_time`.
- Else generic `snapshot_time`.
- Normalize `selected_bookmaker` and use it to fill/override `bookmaker` if `bookmaker` missing/blank/NaN.
- Preserve all selected metadata through `clv_matches.csv`.

### 3. Assumed bet-time safety
Modify `apply_assumed_bet_time_et` so it does not overwrite real `bet_snapshot_time` values by default. It should only fill missing/null `bet_snapshot_time` rows. Keep CLI behavior backwards-compatible but safer.

In `build_clv_matches`, if bet time is missing and no assumed time was used, unmatched reason should be `missing_bet_snapshot_time` where a timestamp is required to compute close after bet. If an assumed time causes `bet_time >= commence`, reason should be `invalid_assumed_time_early_game` when possible, otherwise preserve `bet_time_at_or_after_commence` for real artifact timestamps.

Implement this by marking rows filled by the assumption with a helper column like `bet_time_source` = `artifact`/`assumed`/`missing`.

### 4. Timing horizons +15/+30/+60
Replace the single +15 helper with generic horizon matching.

For horizons 15, 30, 60 minutes:
- Find same-book/same-line quote at or after `bet_snapshot_time` and at or before target time, preferring latest <= target. This matches current +15 semantics.
- Do not use post-commence quote.
- Store columns:
  - `plus15_odds`, `plus15_snapshot_time`, `plus15_clv_implied_prob`, `plus15_match_source`
  - `plus30_odds`, `plus30_snapshot_time`, `plus30_clv_implied_prob`, `plus30_match_source`
  - `plus60_odds`, `plus60_snapshot_time`, `plus60_clv_implied_prob`, `plus60_match_source`
- Match source values should include at least: `same_book_same_line`, `missing_bet_time`, `no_same_book_same_line_match`, `past_commence` / `unavailable`.

### 5. Detailed unmatched / coverage outputs
Add output `clv_unmatched_reasons.csv` from `run()` with counts and percentages by `unmatched_reason`.

Update `write_markdown_summary` to include:
- unmatched reason table
- timing horizon availability summary for +15/+30/+60
- note whether real artifact bet timestamps were used vs assumed timestamps when columns exist

### 6. Timing stability output shape
Update `clv_timing_stability.csv` to long or wide format that `diagnose_mlb_clv_failure_modes.py` can detect +15/+30/+60.
Preferred: long rows with `horizon` column (`+15m`, `+30m`, `+60m`) and columns: `bet_id`, `game_date`, `player_id`, `game_id`, `bookmaker_at_bet`, `line_at_bet`, `odds_at_bet`, `horizon_odds`, `horizon_snapshot_time`, `horizon_clv_implied_prob`, `horizon_match_source`, `final_clv_implied_prob`.

### 7. Diagnostics measurement gates
Update `diagnose_mlb_clv_failure_modes.py` to:
- Include timing horizon coverage percentages in JSON output.
- Treat missing +15/+30/+60 as `timing_stability_missing`.
- Keep decision label `fail_data_or_timing` when measurement quality fails.
- If `clv_unmatched_reasons.csv` exists, include top unmatched reasons in JSON and markdown.
- Use same-book coverage target >= 50% or existing threshold if not changing current behavior; do not make pass criteria looser except adding clearer reporting.

### 8. Tests
Add/adjust tests before implementation where practical:
- `test_bet_simulator.py`: verify `evaluate_predictions` propagates side-specific selected bookmaker/timestamps/market updates and `to_dataframe()` includes required columns.
- `test_analyze_mlb_batter_hits_clv.py`: verify assumed bet time fills missing only and does not overwrite artifact timestamp.
- `test_analyze_mlb_batter_hits_clv.py`: verify +15/+30/+60 horizon columns are populated when snapshots exist.
- `test_analyze_mlb_batter_hits_clv.py`: verify assumption-caused early-game invalid gets `invalid_assumed_time_early_game`.
- `test_analyze_mlb_batter_hits_clv.py`: verify `clv_timing_stability.csv` contains horizon rows if invoking `run()` is easy; otherwise test helper directly.

## Validation Commands
Run from repo root using Windows venv if needed:

```bash
venv/Scripts/python.exe -m pytest tests/test_bet_simulator.py tests/test_analyze_mlb_batter_hits_clv.py tests/test_diagnose_mlb_clv_failure_modes.py -q
venv/Scripts/python.exe -m py_compile src/backtesting/bet_simulator.py scripts/analyze_mlb_batter_hits_clv.py scripts/diagnose_mlb_clv_failure_modes.py
```

If `venv/Scripts/python.exe` is not executable from WSL, use `python -m pytest ...` only if dependencies are present; otherwise report the blocker.

## Review Criteria
- Scoped diff only touches target files unless justified.
- Backwards compatible: old bets without new columns still work.
- No model probability/calibration changes.
- CLV uses actual selected timestamps when present.
- Timing stability supports +15/+30/+60.
- New outputs are explicit enough to tell data gaps from model failures.
