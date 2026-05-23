# MLB Dense CLV Validation Suite Architecture Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Turn the current MLB batter_hits dense-CLV loop from ad hoc inspection into a repeatable decision suite that can actually answer: sync/local readiness, dense table validity, dropout timing correctness, ESPNBet policy, staking policy, and feature-expansion readiness.

**Architecture:** Add first-class dense CLV local sync support to the existing sync architecture instead of one-off copy workarounds. Then make the audit suite decision-grade: policy-aware dropout timing, richer suite_summary outputs, and explicit pass/fail gates that map to next decisions. Keep model math unchanged.

**Tech Stack:** Python, pandas, SQLAlchemy/psycopg2, pytest, existing GameFlow CLI scripts.

---

## Non-goals / safety constraints

- Do not add indexes, keys, or long DDL on remote Supabase tables without Chase approval.
- Do not run broad full-table remote copies of `mlb_player_props_clv_snapshots` by default.
- Do not change model probabilities, empirical CDF behavior, Black-Litterman math, or selected bet logic while building validation plumbing.
- Do not promote, retrain, deploy, or feature-expand from this work alone. This work creates the gates that decide those actions.

---

## Acceptance gates

### Gate A — Local dense CLV sync is first-class

Command should exist and be safe:

```powershell
.\venv\Scripts\python.exe scripts\sync_local_db.py --tables mlb_player_props_clv_snapshots --start-date 2026-04-13 --end-date 2026-05-17
```

Expected behavior:
- Uses `snapshot_time` as incremental/date-window column for `mlb_player_props_clv_snapshots`.
- Does not treat the table as unknown/full refresh when date bounds are provided.
- Creates local schema if missing.
- Copies bounded rows from remote to local using COPY/batches.
- Provides row counts and min/max snapshot_time.
- Refuses unbounded full refresh of this table unless explicit `--allow-large-full-refresh` is provided.

### Gate B — Dropout timing audit respects decision policy

For `--quote-decision-policy slate_or_tminus`, the dropout audit must not compare all selected quote rows to fixed `--quote-cutoff-time-et 13:30`.

Expected behavior:
- Fixed policy: selected_snapshot_time <= fixed cutoff timestamp.
- Relative/slate policy: selected_snapshot_time <= selected/actual decision time when present, and selected_snapshot_time < commence_time.
- If selected decision time metadata is unavailable, output `WARN: decision_time_unavailable`, not false `FAIL`.
- Violation counts are computed on selected quote/bet rows relevant to replay, not every candidate quote row.

### Gate C — Suite summary is decision-grade

`suite_summary.md` must include:
- Dropout audit decision and bucket table.
- Sweep ROI/hit-rate/bet count/profit per config.
- CLV mean with CI, Spearman with CI, scored/same-book/unmatched counts.
- Diagnosis failure modes and reasons.
- Top bookmaker share plus ESPNBet share/CLV when present.
- Explicit policy recommendation per config: `reject`, `candidate_flat_only`, `needs_book_sensitivity`, `needs_more_data`, `candidate_for_next_gate`.

### Gate D — Validation decisions become concrete

For each completed suite, the summary must answer:
- Is dense table adequate for this replay? yes/no/needs-linking-audit.
- Is mean CLV confirmed? yes/no/underpowered.
- Is edge ranking confirmed? yes/no.
- Is ESPNBet concentration blocking? yes/no/sensitivity-required.
- Is flat staking allowed for paper? yes/no.
- Is edge/Kelly sizing allowed? yes/no.
- Is feature expansion allowed? yes/no.
- Is retraining indicated? yes/no/after-data-fix.

---

## Task 1: Add tests for dense CLV table registration in sync_local_db

**Objective:** Prove `mlb_player_props_clv_snapshots` is registered with an incremental/date-window strategy instead of falling through to unknown full-refresh behavior.

**Files:**
- Modify: `scripts/sync_local_db.py`
- Create: `tests/test_sync_local_db_dense_clv.py`

**Step 1: Write failing tests**

Add tests that import the registry/helper functions and assert:

```python
def test_dense_clv_snapshots_registered_incremental():
    from scripts import sync_local_db
    assert sync_local_db.MLB_TABLES["mlb_player_props_clv_snapshots"] == ("snapshot_time", "incremental")


def test_unknown_table_requires_explicit_full_strategy_or_error():
    from scripts import sync_local_db
    # Desired behavior: unknown --tables should not silently become full refresh for giant tables.
    # Implement via a build_table_plan helper that raises for unknown tables unless --allow-unknown-full-refresh.
```

**Step 2: Run RED**

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_sync_local_db_dense_clv.py -q
```

Expected: FAIL because the table is not registered and helper may not exist.

**Step 3: Implement minimal registry and planning helper**

- Add `"mlb_player_props_clv_snapshots": ("snapshot_time", "incremental")` to `MLB_TABLES`.
- Extract current table-building logic into a `build_table_plan(args)` helper.
- Make unknown explicit `--tables` fail by default, or require a clearly named escape hatch.

**Step 4: Run GREEN**

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_sync_local_db_dense_clv.py -q
```

---

## Task 2: Add date-window sync support to sync_local_db

**Objective:** Let Chase sync only dense CLV rows for a requested date range instead of full table.

**Files:**
- Modify: `scripts/sync_local_db.py`
- Modify: `tests/test_sync_local_db_dense_clv.py`

**Step 1: Write failing tests**

Test that a date-bounded where clause is generated:

```python
def test_date_window_where_clause_is_inclusive_start_exclusive_end():
    from scripts.sync_local_db import build_where_clause
    where, params = build_where_clause("snapshot_time", start_date="2026-04-13", end_date="2026-05-17", incremental_max=None)
    assert '"snapshot_time" >= %(start_date)s' in where
    assert '"snapshot_time" < %(end_exclusive)s' in where
    assert params["start_date"].startswith("2026-04-13")
    assert params["end_exclusive"].startswith("2026-05-18")
```

Also test incremental max combines with date lower bound safely:

```python
def test_incremental_max_and_start_date_use_later_bound():
    # If local max is inside requested range, resume from max; if before range, start from start-date.
```

**Step 2: Run RED**

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_sync_local_db_dense_clv.py -q
```

**Step 3: Implement CLI flags**

Add:
- `--start-date YYYY-MM-DD`
- `--end-date YYYY-MM-DD`
- `--allow-large-full-refresh`

Implementation details:
- End date should be inclusive from CLI but converted to exclusive `end + 1 day` internally.
- For incremental sync: lower bound = max(local_max, start_date) when both exist.
- For dense CLV table: if no date bounds and not `--allow-large-full-refresh`, exit with a clear error.

**Step 4: Run GREEN**

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_sync_local_db_dense_clv.py -q
```

**Step 5: Smoke dry command**

```powershell
.\venv\Scripts\python.exe scripts\sync_local_db.py --tables mlb_player_props_clv_snapshots --start-date 2026-04-13 --end-date 2026-05-17 --dry-run
```

If `--dry-run` does not exist yet, add it so this command reports the generated table plan/count query without copying rows.

---

## Task 3: Fix dropout audit decision-policy timing semantics

**Objective:** Stop false failures when auditing `slate_or_tminus` replays.

**Files:**
- Modify: `scripts/audit_mlb_quote_clean_dropout.py`
- Modify: `tests/test_audit_mlb_quote_clean_dropout.py`

**Step 1: Write failing tests**

Add a test for slate policy:

```python
def test_slate_or_tminus_does_not_fail_against_fixed_1330_cutoff_when_decision_time_later():
    clean_quotes = pd.DataFrame([
        {
            "selected_snapshot_time": "2026-04-13T21:30:00Z",
            "selected_decision_time": "2026-04-13T21:30:00Z",
            "game_date": "2026-04-13",
            "commence_time": "2026-04-13T23:00:00Z",
        }
    ])
    # Desired helper: count_selected_quote_timing_violations(..., quote_decision_policy="slate_or_tminus")
    # Should return cutoff/decision violations = 0 and commence violations = 0.
```

Add a test for actual violation:

```python
def test_slate_or_tminus_fails_when_selected_snapshot_after_decision_time():
    # selected_snapshot_time > selected_decision_time should count as a decision violation.
```

**Step 2: Run RED**

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_audit_mlb_quote_clean_dropout.py -q
```

**Step 3: Implement helper**

Add a function like:

```python
def count_selected_quote_timing_violations(clean_quotes, *, quote_decision_policy, cutoff_time_et):
    ...
```

Rules:
- `fixed_et`: compare selected_snapshot_time to fixed cutoff.
- `skip_early_fixed_et`: compare to fixed cutoff and require pre-commence.
- `relative_to_commence` / `slate_or_tminus`: if selected_decision_time exists, compare selected_snapshot_time <= selected_decision_time. If missing, return a warning flag and do not hard-fail solely on fixed cutoff.
- Always count commence violations separately.

**Step 4: Wire CLI args**

`summarize_and_write_outputs` needs `quote_decision_policy`, not just `cutoff_time_et`.

**Step 5: Run GREEN**

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_audit_mlb_quote_clean_dropout.py -q
```

---

## Task 4: Make audit suite summary decision-grade

**Objective:** Stop forcing Chase to manually open 30 CSV/JSON files to know what happened.

**Files:**
- Modify: `scripts/run_mlb_quote_clean_audit_suite.py`
- Modify: `tests/test_run_mlb_quote_clean_audit_suite.py`

**Step 1: Write failing tests**

Add tests for a pure summary builder function:

```python
def test_suite_summary_includes_failure_modes_reasons_and_dropout_buckets(tmp_path):
    # Given fake manifest items, fake diagnosis JSON, fake CLV by bookmaker CSV, fake dropout summary JSON,
    # when write_manifest/write_suite_summary runs,
    # then suite_summary.md contains failure modes, reasons, ESPNBet share, dropout buckets, and ROI fields.
```

**Step 2: Run RED**

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_run_mlb_quote_clean_audit_suite.py -q
```

**Step 3: Extend SuiteItem**

Add fields:
- `total_bets`
- `roi`
- `hit_rate`
- `total_profit`
- `n_scored`
- `n_same_book`
- `n_unmatched`
- `failure_modes`
- `failure_reasons`
- `top_bookmaker`
- `top_bookmaker_share`
- `espnbet_share`
- `espnbet_mean_clv`
- `policy_recommendation`

**Step 4: Load source artifacts**

In the wrapper after each config:
- Read matching row from `sweep_summary.csv` by config order/label.
- Read `diagnosis/.../clv_failure_modes.json`.
- Read `clv/.../clv_by_bookmaker.csv`.
- Read `dropout_audit/audit_summary.json` once.

**Step 5: Implement policy recommendation helper**

Suggested deterministic rules:
- `reject`: negative ROI with mean CLV CI crossing zero, or data quality failure.
- `candidate_flat_only`: positive mean CLV CI low > 0, ROI >= 0, edge ranking not confirmed, no data quality failure.
- `needs_book_sensitivity`: top bookmaker share > 50% or ESPNBet share > 40%.
- `needs_more_data`: n_scored < 200 or blocks < 20.
- `candidate_for_next_gate`: mean CLV CI low > 0, edge Spearman CI low > 0, unmatched <= 10%, top book share <= 50%.

Allow multiple labels if useful.

**Step 6: Run GREEN**

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_run_mlb_quote_clean_audit_suite.py -q
```

---

## Task 5: Add an explicit validation gate report

**Objective:** Produce one report whose headings match the decisions Chase is trying to make.

**Files:**
- Create: `scripts/summarize_mlb_validation_gates.py` or integrate into `run_mlb_quote_clean_audit_suite.py`
- Create: `tests/test_summarize_mlb_validation_gates.py`

**Report sections:**

```text
Dense table gate
- pass/fail/needs-audit
- scored/same-book/unmatched
- timing horizon coverage

Model/selection gate
- ROI and CLV together
- positive mean CLV confirmed? yes/no
- edge ranking confirmed? yes/no

ESPNBet/book policy gate
- top book share
- ESPNBet share
- ESPNBet CLV vs non-ESPNBet CLV if available
- recommendation: allow / cap / exclude sensitivity required

Staking gate
- flat paper allowed? yes/no
- edge-sized allowed? yes/no
- Kelly allowed? yes/no

Feature expansion gate
- allowed? yes/no
- reason

Retraining gate
- retrain now? yes/no
- reason
```

**Step 1: Write failing test**

Test a fake summary with config_03-like numbers yields:
- `flat paper: allowed cautiously`
- `edge-sized: no`
- `feature expansion: no`
- `ESPNBet sensitivity: required`

**Step 2: Implement report writer**

**Step 3: Run tests**

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_summarize_mlb_validation_gates.py -q
```

---

## Task 6: End-to-end validation command

**Objective:** One command runs/re-runs the full validation suite after a sweep and outputs a decision-grade report.

Command:

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_quote_clean_audit_suite.py --sweep-output-dir backtest_results\mlb_batter_hits_dense_quote_clean_slate_tminus60_20260413_20260517 --output-dir backtest_results\mlb_batter_hits_dense_quote_clean_slate_tminus60_20260413_20260517_audit_suite_v2 --model-dir src\models\mlb\artifacts\production --start 2026-04-13 --end 2026-05-17 --stats batter_hits --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_player_props_clv_snapshots --snapshots-table mlb_player_props_clv_snapshots --local --batch-size 50
```

Expected output files:
- `suite_summary.md` decision-grade, not thin.
- `suite_manifest.json` with all enriched fields.
- `validation_gate_report.md` with the gate decisions.
- Existing dropout/CLV/diagnosis artifacts preserved.

---

## Validation commands for implementation PR

Run focused tests:

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_sync_local_db_dense_clv.py tests\test_audit_mlb_quote_clean_dropout.py tests\test_run_mlb_quote_clean_audit_suite.py tests\test_diagnose_mlb_clv_failure_modes.py tests\test_analyze_mlb_batter_hits_clv.py -q
```

Run compile checks:

```powershell
.\venv\Scripts\python.exe -m py_compile scripts\sync_local_db.py scripts\audit_mlb_quote_clean_dropout.py scripts\run_mlb_quote_clean_audit_suite.py scripts\analyze_mlb_batter_hits_clv.py scripts\diagnose_mlb_clv_failure_modes.py
```

Run dry local sync plan:

```powershell
.\venv\Scripts\python.exe scripts\sync_local_db.py --tables mlb_player_props_clv_snapshots --start-date 2026-04-13 --end-date 2026-05-17 --dry-run
```

Run audit suite v2 on existing sweep:

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_quote_clean_audit_suite.py --sweep-output-dir backtest_results\mlb_batter_hits_dense_quote_clean_slate_tminus60_20260413_20260517 --output-dir backtest_results\mlb_batter_hits_dense_quote_clean_slate_tminus60_20260413_20260517_audit_suite_v2 --model-dir src\models\mlb\artifacts\production --start 2026-04-13 --end 2026-05-17 --stats batter_hits --quote-decision-policy slate_or_tminus --quote-relative-minutes 60 --line-source mlb_player_props_clv_snapshots --snapshots-table mlb_player_props_clv_snapshots --local --batch-size 50
```

---

## Implementation order recommendation

1. `sync_local_db.py` dense table registration + bounded date-window sync.
2. Dropout policy timing fix.
3. Rich suite summary.
4. Validation gate report.
5. Rerun suite v2 on existing sweep and make the actual decisions.

This gets us out of the loop of “do nothing, everything is broken” by making every blocker map to a specific gate and command.
