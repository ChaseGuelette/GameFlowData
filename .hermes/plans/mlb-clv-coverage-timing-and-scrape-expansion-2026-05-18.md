# MLB CLV Coverage, Timing, and Odds Snapshot Expansion Plan

> **For Hermes:** Use subagent-driven-development or gameflow-implementation-worker for implementation. Keep SQL isolated via the GameFlow SQL runner pattern. Do not use Supabase MCP directly from main context.

**Goal:** Make MLB `batter_hits` quote-clean CLV diagnostics decision-grade by fixing artifact metadata, improving CLV matching diagnostics, and defining targeted Odds API scrape/capture schedules for entry, intraday, and close quotes.

**Architecture:** Treat CLV readiness as two layers: measurement quality first, model quality second. The implementation should preserve exact selected quote metadata in backtest artifacts, compute +15/+30/+60 timing horizons from actual selected quote time, and separate true data gaps from script/matching failures. Historical rescrape and forward capture should be targeted around decision times and commence-relative close windows, not generic bulk rescrapes.

**Critical Invariants:**
- Probabilities use empirical CDF only, never Gaussian CDF.
- Never deploy global conformal recalibration.
- Preserve temporal ordering: `bet_quote_time <= bet_time < clv_quote_time <= commence_or_close_cutoff`.
- `prop_line_*` / market-derived features require strict as-of validation before promotion evidence is trusted.
- No feature expansion or production promotion until CLV measurement quality passes.

---

## Current Problem Summary

Recent Phase 1B CLV diagnostics for MLB `batter_hits` show weak-to-moderate positive CLV but fail decision-grade gates:

- Unmatched rate: ~59-66%, threshold should be <=20%.
- Same-book coverage: ~27-37%, target should be >=60% before promotion evidence is trusted.
- +15/+30/+60 timing horizons: missing everywhere.
- Edge ranking: mostly inconclusive; only `with_prop_line_edge005` had confirmed positive Spearman CI low.
- Bookmaker concentration: ESPNBet often dominates ~60-66% of rows.

Interpretation: do not kill the model; do not promote or expand features. Fix data/timing/matching evidence first.

---

## Required Timestamp Model

Every future CLV-capable bet artifact should preserve four timestamp concepts:

1. **Selected quote time**
   - Exact quote the strategy would have bet.
   - Must be saved in `bets.csv`.

2. **Timing stability quote times**
   - +15, +30, +60 minutes after selected quote time.
   - Measures whether the edge moves in favor soon after selection.

3. **Close quote time**
   - Best available same-book quote before commence, or explicitly labeled consensus fallback.
   - Used for final CLV.

4. **Commence cutoff**
   - Prevents any post-start quote from entering selection, timing stability, or close CLV.

Minimum future `bets.csv` / CLV artifact columns:

```text
selected_snapshot_time
selected_market_last_update
selected_bookmaker
selected_line
selected_price
selected_side
close_snapshot_time
close_market_last_update
close_bookmaker
close_line
close_price
plus15_snapshot_time
plus30_snapshot_time
plus60_snapshot_time
clv_source
unmatched_reason
```

Strongly preferred extra diagnostic columns:

```text
selected_bookmaker_last_update
close_bookmaker_last_update
plus15_price
plus30_price
plus60_price
plus15_line
plus30_line
plus60_line
plus15_match_source
plus30_match_source
plus60_match_source
same_book_available_at_close
same_line_available_at_close
line_movement_class
bookmaker_rank_at_selection
best_price_bookmaker_at_selection
```

---

## Forward Odds API Capture Schedule

For every MLB game with supported markets:

```text
T-6h to T-2h:       scrape every 15 minutes
T-2h to T-30m:      scrape every 5 minutes
T-30m to T-5m:      scrape every 2-5 minutes
T-5m to commence:   scrape every 1-2 minutes if quota allows
```

Quota-constrained minimum viable schedule:

```text
T-4h to T-60m:      every 15 minutes
T-60m to T-10m:     every 5 minutes
T-10m to T-1m:      every 2 minutes
```

Why:
- Entry/selected quotes may occur hours before first pitch.
- +15/+30/+60 requires actual snapshots after entry.
- Close CLV requires dense near-commence snapshots.
- Early games cannot use a universal 13:30 ET assumed bet time.

---

## Historical Rescrape Grid

If Odds API historical requests allow chosen timestamps, do not scrape only near close. Use a grid that supports entry, timing stability, and close.

Per game/date, decision-time candidates:

```text
09:30 ET
10:30 ET
11:30 ET
12:30 ET
13:30 ET
15:30 ET
17:30 ET
```

Per game/date, commence-relative candidates:

```text
T-180m
T-120m
T-60m
T-30m
T-15m
T-5m or closest legal pre-commence
```

If quota/cost forces prioritization:

1. Close grid: `T-60m`, `T-30m`, `T-15m`, `T-5m`.
2. Selected quote + `+15/+30/+60` from actual selected quote time.
3. Morning/noon snapshots for early market movement.

Minimum historical CLV set per game:

```text
entry quote: selected decision time or nearest valid pre-decision quote
+15 quote:   selected quote time + 15m
+30 quote:   selected quote time + 30m
+60 quote:   selected quote time + 60m
close quote: last available quote before commence
```

Important: if old artifacts do not contain selected quote time, historical rescrape alone is insufficient. Either rerun the backtest with selected quote metadata saved, or reconstruct selected quote metadata from predictions/dropout/selected-clean-quote artifacts.

---

## Implementation Tasks

### Task 1: Audit Existing Artifact Columns

**Objective:** Determine whether current sweep artifacts already include selected quote metadata.

**Files:**
- Read: `backtest_results/**/bets.csv`
- Read: `backtest_results/**/predictions.csv` if present
- Read: dropout audit outputs if present

**Steps:**
1. Write a small script or notebook-free helper to list columns for relevant `bets.csv` and `predictions.csv` files.
2. Report which artifacts have `selected_snapshot_time`, selected bookmaker, selected line, and selected price.
3. Report whether old May 15 sweeps can be reconstructed or must be rerun.

**Validation:**
- Output must explicitly classify each sweep as `ready`, `reconstructable`, or `rerun_required`.

---

### Task 2: Preserve Selected Quote Metadata in Backtest Artifacts

**Objective:** Ensure every future quote-clean backtest saves exact selected quote metadata to `bets.csv`.

**Likely Files:**
- Modify: `src/backtesting/bet_simulator.py`
- Modify: any MLB quote-clean runner that builds bet rows
- Test: relevant backtesting tests under `tests/`

**Requirements:**
- Add selected quote metadata without changing bet selection semantics.
- Preserve bookmaker, line, price, side, snapshot time, market_last_update, and bookmaker_last_update if available.
- Ensure no post-commence selected quote can be emitted.

**Validation:**
- Run existing quote-clean dropout audit.
- Inspect a generated `bets.csv` and confirm required columns are populated.

---

### Task 3: Upgrade CLV Matching to Use Actual Selected Quote Time

**Objective:** Stop relying on universal `--assume-bet-time-et 13:30` when artifact timestamps exist.

**Likely Files:**
- Modify: `scripts/analyze_mlb_batter_hits_clv.py`
- Test: add/update tests for CLV timestamp ordering if test harness exists

**Requirements:**
- Prefer `selected_snapshot_time` / `bet_snapshot_time` from artifact.
- Use `--assume-bet-time-et` only as explicit fallback.
- If fallback time is at/after commence, label as `invalid_assumed_time_early_game`, not generic unmatched.
- Enforce `selected_time <= bet_time < clv_time <= commence_or_close_cutoff`.

**Validation:**
- Rerun CLV on a timestamp-rich artifact.
- Confirm `bet_time_at_or_after_commence` decreases or is clearly separated as fallback-only.

---

### Task 4: Add +15/+30/+60 Timing Stability Matching

**Objective:** Compute timing stability horizons from actual selected quote time.

**Likely Files:**
- Modify: `scripts/analyze_mlb_batter_hits_clv.py`
- Modify: diagnostics writer / markdown summary code

**Requirements:**
- For each bet, match same-book/same-line quote nearest to selected time +15/+30/+60 within a configured tolerance.
- If same-book unavailable, optionally compute consensus fallback but label it separately.
- Save horizon match source and unmatched reason.

**Validation:**
- `clv_timing_stability.csv` or equivalent has non-empty horizon columns on timestamp-rich data.
- Diagnostic script reports `timing_horizons_present`.

---

### Task 5: Expand CLV Unmatched Reason Reporting

**Objective:** Make CLV failures self-explanatory in markdown and CSV outputs.

**Likely Files:**
- Modify: `scripts/analyze_mlb_batter_hits_clv.py`
- Modify: `scripts/diagnose_mlb_clv_failure_modes.py`

**Required unmatched categories:**

```text
invalid_assumed_time_early_game
bet_time_at_or_after_commence
no_close_match
no_same_book_close
no_same_line_close
line_moved_no_same_line
no_plus15_match
no_plus30_match
no_plus60_match
bookmaker_missing
market_missing
```

**Validation:**
- Markdown summary includes a count/percentage table for unmatched reasons.
- Diagnostic output distinguishes measurement failure from model-quality failure.

---

### Task 6: Diagnose ESPNBet / Bookmaker Concentration

**Objective:** Determine whether ESPNBet dominates because it is truly best price, because selection logic over-prefers it, or because other books lack coverage.

**Likely Files:**
- Create: `scripts/diagnose_mlb_bookmaker_selection.py`
- Read: selected quote artifacts / raw odds snapshots

**Questions to answer:**
1. At selected quote time, was ESPNBet the best available price/line?
2. How often were other books available for the same player/stat/side/line?
3. Is ESPNBet concentration caused by best-line selection or missing competing quotes?
4. What happens if ESPNBet is excluded?
5. What happens if selection is constrained to DraftKings/FanDuel/BetMGM/etc.?

**Required outputs:**

```text
bookmaker_selection_summary.csv
bookmaker_availability_summary.csv
bookmaker_exclusion_sensitivity.csv
bookmaker_selection_summary.md
```

**Validation:**
- Report classifies concentration as one of:
  - `best_price_explains_concentration`
  - `coverage_gap_explains_concentration`
  - `selection_bias_or_bug_possible`
  - `inconclusive`

---

### Task 7: Add Optional Bookmaker Policy Variants

**Objective:** Support controlled experiments that avoid or constrain ESPNBet without silently changing production policy.

**Likely Files:**
- Modify quote-clean selection runner / config
- Modify sweep scripts or add CLI flags

**Policy variants:**

```text
all_books_best_price
exclude_espnbet
major_books_only
same_book_required
bookmaker_cap_<max_share>
```

**Validation:**
- Rerun at least one small quote-clean replay for each policy variant.
- Confirm artifact records the policy name.
- CLV diagnostics compare ROI, mean CLV, coverage, and bookmaker concentration by policy.

---

### Task 8: Measurement-Quality Gate Before Model-Quality Gate

**Objective:** Prevent model decisions when CLV data quality is inadequate.

**Modify:**
- `scripts/diagnose_mlb_clv_failure_modes.py`

**Measurement-quality gate:**

```text
unmatched_rate <= 20%
same_book_share >= 60%
+15 horizon coverage >= 70% of scored bets
+30 horizon coverage >= 60% of scored bets
+60 horizon coverage >= 50% of scored bets
top_bookmaker_share <= 45% OR bookmaker-specific report passes
```

**Model-quality gate:**

```text
mean CLV implied probability >= +1.5 percentage points
mean CLV CI low > 0
Spearman(edge, CLV) CI low > 0
n_scored >= 200
n_blocks >= 25
```

**Validation:**
- Current May 15 artifacts should fail with `measurement_quality_failure`, not be overinterpreted as model failure.

---

## Decision Rules After Implementation

If measurement-quality fails:
- Decision: `data_or_timing_unresolved`.
- Do not promote, expand features, or conclude model is bad.
- Fix scrape/capture/matching.

If measurement-quality passes but model-quality fails:
- Decision: `model_or_policy_not_validated`.
- Tune selection policy/thresholds before feature expansion.

If both pass:
- Consider paper/live promotion with conservative sizing and continued CLV monitoring.

---

## Initial Candidate Arms To Continue Monitoring

- Control: `no_prop_line_edge005`.
- Ranking candidate: `with_prop_line_edge005`.
- High-CLV candidates: `with_prop_line_edge008`, `with_prop_line_edge010`.

Do not promote any arm until measurement-quality and model-quality gates pass.
