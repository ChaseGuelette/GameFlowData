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

- Create: `scripts/analyze_mlb_clv.py`
- Modify: `scripts/analyze_mlb_batter_hits_clv.py` into compatibility wrapper or alias.
- Modify: `scripts/run_mlb_quote_clean_audit_suite.py` to call generic analyzer.
- Add/modify tests:
  - `tests/test_analyze_mlb_clv.py`
  - keep `tests/test_analyze_mlb_batter_hits_clv.py` compatibility coverage
  - `tests/test_run_mlb_quote_clean_audit_suite.py`

## Required behavior

Generic analyzer must support:

- `--bets-csv`
- `--snapshots-csv`
- `--output-dir`
- `--local`
- `--bootstrap-samples`
- `--ci-level`
- `--batch-size`
- `--snapshots-table`
- `--assume-bet-time-et`
- `--min-mean-clv`
- new optional `--stat-label` or infer labels from bets.

It must preserve output files:

- `clv_matches.csv`
- `clv_summary.csv`
- `clv_by_bookmaker.csv`
- `clv_by_edge_bin.csv`
- `clv_timing_stability.csv`
- `phase1b_decision.csv`
- `phase1b_clv_summary.md`

It must preserve semantics:

- same-book close first;
- consensus fallback;
- changed-line classification;
- +15/+30/+60 timing if bet timestamps exist;
- block bootstrap CI;
- Spearman edge/CLV diagnostics;
- unmatched reason summaries.

## Refactor approach

1. Copy/import existing implementation into `analyze_mlb_clv.py` without behavior change.
2. Rename user-facing headings from “batter hits” to “MLB CLV” or profile-specific label.
3. Keep `STAT_TO_MARKET_KEY = {"batter_hrr": "batter_hits_runs_rbis"}` or move it to shared stat mapping; pitcher_strikeouts passthrough is okay.
4. Update audit suite line 574-ish behavior to call `analyze_mlb_clv.py`.
5. Keep `analyze_mlb_batter_hits_clv.py` as a thin wrapper that imports and calls generic `main()` for backwards compatibility.

## Tests

### Characterization tests

Move broad tests from `tests/test_analyze_mlb_batter_hits_clv.py` to `tests/test_analyze_mlb_clv.py`.

Add pitcher-shaped tests:

- `stat = pitcher_strikeouts`
- line values like `5.5`, `6.5`
- side `under`
- same-book close CLV works;
- consensus fallback works;
- line movement classification works;
- output report no longer hardcodes batter_hits in generic path.

### Audit-suite tests

Modify `tests/test_run_mlb_quote_clean_audit_suite.py` to verify dry-run/command assembly references `analyze_mlb_clv.py`, not `analyze_mlb_batter_hits_clv.py`.

## Done criteria

- Generic analyzer tests pass.
- Backward compatibility wrapper tests pass.
- Audit suite calls generic analyzer.
- No pitcher-specific CLV fork exists.
- Existing artifact output filenames remain compatible with ranker/book scripts.
