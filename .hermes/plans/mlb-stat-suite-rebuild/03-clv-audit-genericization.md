# 03 — CLV / Audit Genericization

## Goal

Make CLV and audit tooling explicitly stat-generic so pitcher_strikeouts does not rely on a batter-named analyzer or forked copy.

## Current files

- `scripts/analyze_mlb_batter_hits_clv.py`
- `scripts/run_mlb_quote_clean_audit_suite.py`
- `scripts/diagnose_mlb_clv_failure_modes.py`
- `scripts/analyze_mlb_clv_ranking_diagnostics.py`
- `scripts/analyze_mlb_clv_book_sensitivity.py`
- `tests/test_analyze_mlb_batter_hits_clv.py`
- `tests/test_run_mlb_quote_clean_audit_suite.py`

## New / modified files

Status: implemented locally on 2026-06-07; pending review/commit.

- [x] Create: `scripts/analyze_mlb_clv.py`
- [x] Modify: `scripts/analyze_mlb_batter_hits_clv.py` into compatibility wrapper or alias.
- [x] Modify: `scripts/run_mlb_quote_clean_audit_suite.py` to call generic analyzer.
- [x] Add/modify tests:
  - [x] `tests/test_analyze_mlb_clv.py`
  - [x] keep `tests/test_analyze_mlb_batter_hits_clv.py` compatibility coverage
  - [x] `tests/test_run_mlb_quote_clean_audit_suite.py`

## Required behavior

Generic analyzer must support:

- [x] `--bets-csv`
- [x] `--snapshots-csv`
- [x] `--output-dir`
- [x] `--local`
- [x] `--bootstrap-samples`
- [x] `--ci-level`
- [x] `--batch-size`
- [x] `--snapshots-table`
- [x] `--assume-bet-time-et`
- [x] `--min-mean-clv`
- [x] new optional `--stat-label` or infer labels from bets.

It must preserve output files:

- [x] `clv_matches.csv`
- [x] `clv_summary.csv`
- [x] `clv_by_bookmaker.csv`
- [x] `clv_by_edge_bin.csv`
- [x] `clv_timing_stability.csv`
- [x] `phase1b_decision.csv`
- [x] `phase1b_clv_summary.md`

It must preserve semantics:

- [x] same-book close first;
- [x] consensus fallback;
- [x] changed-line classification;
- [x] +15/+30/+60 timing if bet timestamps exist;
- [x] block bootstrap CI;
- [x] Spearman edge/CLV diagnostics;
- [x] unmatched reason summaries.

## Refactor approach

1. [x] Copy/import existing implementation into `analyze_mlb_clv.py` without behavior change.
2. [x] Rename user-facing headings from “batter hits” to “MLB CLV” or profile-specific label.
3. [x] Keep `STAT_TO_MARKET_KEY = {"batter_hrr": "batter_hits_runs_rbis"}` or move it to shared stat mapping; pitcher_strikeouts passthrough is okay.
4. [x] Update audit suite line 574-ish behavior to call `analyze_mlb_clv.py`.
5. [x] Keep `analyze_mlb_batter_hits_clv.py` as a thin wrapper that imports and calls generic `main()` for backwards compatibility.

## Tests

### Characterization tests

- [x] Move broad tests from `tests/test_analyze_mlb_batter_hits_clv.py` to `tests/test_analyze_mlb_clv.py`.

Add pitcher-shaped tests:

- [x] `stat = pitcher_strikeouts`
- [x] line values like `5.5`, `6.5`
- [x] side `under`
- [x] same-book close CLV works;
- [x] consensus fallback works;
- [x] line movement classification works via carried generic characterization coverage;
- [x] output report no longer hardcodes batter_hits in generic path.

### Audit-suite tests

- [x] Modify `tests/test_run_mlb_quote_clean_audit_suite.py` to verify dry-run/command assembly references `analyze_mlb_clv.py`, not `analyze_mlb_batter_hits_clv.py`.

## Done criteria

- [x] Generic analyzer tests pass.
- [x] Backward compatibility wrapper tests pass.
- [x] Audit suite calls generic analyzer.
- [x] No pitcher-specific CLV fork exists.
- [x] Existing artifact output filenames remain compatible with ranker/book scripts.

## Validation evidence

- `./venv/Scripts/python.exe -m pytest tests/test_analyze_mlb_clv.py tests/test_analyze_mlb_batter_hits_clv.py tests/test_run_mlb_quote_clean_audit_suite.py -q` → 36 passed, 1 warning.
- `./venv/Scripts/python.exe -m py_compile scripts/analyze_mlb_clv.py scripts/analyze_mlb_batter_hits_clv.py scripts/run_mlb_quote_clean_audit_suite.py tests/test_analyze_mlb_clv.py tests/test_analyze_mlb_batter_hits_clv.py tests/test_run_mlb_quote_clean_audit_suite.py` → passed.
- `git diff --check -- .hermes/plans/mlb-stat-suite-rebuild scripts/analyze_mlb_clv.py scripts/analyze_mlb_batter_hits_clv.py scripts/run_mlb_quote_clean_audit_suite.py tests/test_analyze_mlb_clv.py tests/test_analyze_mlb_batter_hits_clv.py tests/test_run_mlb_quote_clean_audit_suite.py` → passed.
- Offline pitcher_strikeouts smoke with `--snapshots-csv` produced expected CLV output files and `# MLB Pitcher Strikeouts Phase 1B CLV Summary`.
