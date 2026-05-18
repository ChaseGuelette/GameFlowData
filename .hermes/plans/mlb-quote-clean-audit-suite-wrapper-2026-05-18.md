# MLB Quote-Clean Audit Suite Wrapper Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves. Keep SQL isolated through existing script entrypoints and `--local`; do not add direct Supabase MCP calls. Do not run broad repo-root searches.

**Goal:** Build one manifest-driven post-sweep audit wrapper that can compare any number of named MLB quote-clean sweep variants and produce a single promotion-gate report.

**Architecture:** Keep existing audit scripts as independent, testable components. Add a thin orchestrator that reads a YAML/JSON manifest, runs dropout audit, CLV analysis, and CLV failure-mode diagnosis per variant/config, then aggregates results into one candidate comparison and decision report. The wrapper must be variant-generic: `no_prop_line` and `with_prop_line` are examples, not hardcoded concepts.

**Tech Stack:** Python, argparse, pathlib, subprocess or direct function calls where safe, pandas, PyYAML or stdlib JSON, pytest, existing scripts under `scripts/`, existing backtest outputs under `backtest_results/`.

---

## Context and Decisions

### Why build this

The current audit workflow requires many separate commands after quote-clean sweeps:

1. Run dropout audit on each sweep.
2. Run CLV analysis on selected candidate configs.
3. Run CLV failure-mode diagnosis on each CLV output.
4. Manually compare variants and configs.

That is correct diagnostically but too easy to run inconsistently. The wrapper should make the post-sweep workflow repeatable and less error-prone.

### Core design decision

Do **not** merge all diagnostics into one giant script.

Instead:

- Keep `scripts/audit_mlb_quote_clean_dropout.py` as the source of truth for quote availability/dropout/timing checks.
- Keep `scripts/analyze_mlb_batter_hits_clv.py` as the source of truth for CLV math.
- Keep `scripts/diagnose_mlb_clv_failure_modes.py` as the source of truth for CLV interpretation.
- Add `scripts/run_mlb_quote_clean_audit_suite.py` as the orchestration/reporting layer.

This preserves testability and lets individual diagnostics be used directly when needed.

### Variant-generic requirement

The wrapper must accept N named variants, not a fixed pair.

Examples:

- `no_prop_line` vs `with_prop_line`
- `baseline_static` vs `hook_deep_start`
- `production_current` vs `candidate_new_features`
- `quote_clean_current_window` vs `quote_clean_independent_window`

`no_prop_line` and `with_prop_line` should be handled by manifest configuration only. Do not hardcode special logic for those names beyond optional role labels in reports.

### Prop-line / no-prop-line policy

Do not require every future run to include both prop-line and no-prop-line variants.

Require or strongly recommend both variants when:

- the model includes market-derived features such as `prop_line_*`, implied odds, consensus line, book price, or open/close movement;
- the feature store recently changed as-of/timestamp handling;
- the backtest ROI is suspiciously high;
- the result is being used for promotion/paper/live policy;
- there is a plausible market-feature leakage pathway.

Do not force both variants for models with no market-derived inputs or simple smoke/regression runs.

The wrapper should support metadata fields such as `role: leakage_control` and `role: candidate` so reports can explain why variants exist without assuming all comparisons are prop-line ablations.

### Prior lessons/invariants to preserve

- Quote-clean/CLV must precede feature work and promotion.
- Implausibly profitable backtests are methodology red flags.
- Raw timestamps do not guarantee temporal integrity.
- Large odds-table audits must be keyed and chunked.
- Feature selector output is not an ablation; use true include/exclude variant comparisons.
- Probabilities must remain empirical CDF based.
- Do not add DB mutations to this wrapper.

---

## Proposed Manifest Format

Create manifests under `configs/audit_suites/`.

Example file:

`configs/audit_suites/mlb_batter_hits_quote_clean_20260515.yaml`

```yaml
suite_name: mlb_batter_hits_quote_clean_20260515
stat: batter_hits
start: 2026-04-13
end: 2026-05-10
quote_cutoff_time_et: "13:30"
direction: under
local: true
output_dir: backtest_results/audits/mlb_batter_hits_20260515_suite

variants:
  no_prop_line:
    role: leakage_control
    model_dir: src/models/mlb/artifacts/mlb_run_batter_hits_20260515_131020_no_prop_line
    sweep_dir: backtest_results/mlb_sweep_20260515_131745
    candidate_configs:
      - name: edge005
        config_dir: config_02_no_BL_edge0.05_kelly0.125
      - name: edge008
        config_dir: config_03_no_BL_edge0.08_kelly0.125
      - name: edge012
        config_dir: config_05_no_BL_edge0.12_kelly0.125

  with_prop_line:
    role: candidate
    model_dir: src/models/mlb/artifacts/mlb_run_batter_hits_20260515_131017
    sweep_dir: backtest_results/mlb_sweep_20260515_131753
    candidate_configs:
      - name: edge005
        config_dir: config_02_no_BL_edge0.05_kelly0.125
      - name: edge008
        config_dir: config_03_no_BL_edge0.08_kelly0.125
      - name: edge010
        config_dir: config_04_no_BL_edge0.1_kelly0.125
```

Optional future fields:

```yaml
clv:
  bootstrap_samples: 1000
  min_mean_clv: 0.0
  assume_bet_time_et: "13:30"

execution:
  continue_on_error: true
  overwrite: false
  dry_run: false
```

---

## Output Contract

For a suite output directory, create:

```text
backtest_results/audits/<suite_name>/
  audit_suite_summary.md
  audit_suite_summary.json
  candidate_comparison.csv
  command_log.jsonl
  variants/
    <variant_name>/
      dropout/
        audit_summary.md
        audit_summary.json
        dropout_summary_by_bucket.csv
        dropout_by_date.csv
        dropout_by_bookmaker.csv
        dropout_rows.csv
      clv/
        <candidate_name>/
          ... existing analyze_mlb_batter_hits_clv.py outputs ...
      clv_diagnosis/
        <candidate_name>/
          ... existing diagnose_mlb_clv_failure_modes.py outputs ...
```

`candidate_comparison.csv` should include at minimum:

```text
variant,role,candidate,config_dir,bets_csv,total_bets,roi,hit_rate,total_profit,dropout_decision,dropout_cutoff_violations,dropout_commence_violations,clv_decision,clv_mean,clv_ci_low,clv_ci_high,edge_clv_spearman,edge_clv_ci_low,edge_clv_ci_high,failure_mode,promotion_status,notes
```

If a metric is unavailable because an underlying script output format does not expose it yet, leave it blank and add a note. Do not invent metrics.

`audit_suite_summary.md` should include:

1. Suite metadata.
2. Variant table.
3. Candidate comparison table.
4. Promotion-gate interpretation:
   - safest candidate
   - raw ROI winner
   - CLV winner if known
   - blocked candidates and reasons
   - inconclusive candidates and missing evidence
5. Required next steps.

---

## CLI Contract

Create:

`scripts/run_mlb_quote_clean_audit_suite.py`

Support both manifest and direct args, but manifest is the primary workflow.

Primary command:

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_quote_clean_audit_suite.py --manifest configs\audit_suites\mlb_batter_hits_quote_clean_20260515.yaml
```

Useful flags:

```text
--manifest <path>          Required for normal use.
--dry-run                  Print planned underlying commands without running them.
--overwrite                Allow replacing existing output dirs.
--continue-on-error        Run remaining configs after a failure and report failures.
--only dropout|clv|diagnosis|aggregate
--variant <name>           Limit to one variant.
--candidate <name>         Limit to one candidate name across variants.
```

The first implementation may require `--manifest` and may omit direct multi-arg mode. Do not overbuild.

---

## Task 1: Add Manifest Loader and Validation

**Objective:** Parse the YAML/JSON manifest into validated dataclasses or typed dictionaries.

**Files:**
- Create: `scripts/run_mlb_quote_clean_audit_suite.py`
- Create: `tests/test_run_mlb_quote_clean_audit_suite.py`
- Create: `configs/audit_suites/mlb_batter_hits_quote_clean_20260515.yaml`

**Implementation notes:**

- Prefer PyYAML if already available. If not available, support JSON first and make YAML optional, or add a clear error message.
- Resolve relative paths against the project root / current working directory.
- Validate:
  - `stat`, `start`, `end`, `quote_cutoff_time_et`, `output_dir` exist in manifest.
  - At least one variant exists.
  - Each variant has `model_dir`, `sweep_dir`, and `candidate_configs`.
  - Each candidate has `name` and `config_dir`.
  - Each `sweep_dir/config_dir/bets.csv` exists for CLV candidates.
  - Model dir and sweep dir exist.

**Test cases:**

- valid manifest loads two variants and six candidates.
- missing `model_dir` raises a useful validation error.
- missing `bets.csv` raises a useful validation error.
- duplicate candidate names within one variant raise a useful validation error.

**Validation command:**

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_run_mlb_quote_clean_audit_suite.py -q
```

---

## Task 2: Add Dry-Run Command Planning

**Objective:** Convert a validated manifest into the exact underlying commands without executing them.

**Files:**
- Modify: `scripts/run_mlb_quote_clean_audit_suite.py`
- Modify: `tests/test_run_mlb_quote_clean_audit_suite.py`

**Planned commands per variant:**

1. Dropout audit:

```text
python scripts/audit_mlb_quote_clean_dropout.py --local --model-dir <model_dir> --start <start> --end <end> --stats <stat> --direction <direction> --quote-cutoff-time-et <quote_cutoff_time_et> --sweep-output-dir <sweep_dir> --output-dir <suite_output>/variants/<variant>/dropout
```

2. CLV analysis per candidate:

```text
python scripts/analyze_mlb_batter_hits_clv.py --local --bets-csv <sweep_dir>/<config_dir>/bets.csv --output-dir <suite_output>/variants/<variant>/clv/<candidate> --assume-bet-time-et <quote_cutoff_time_et>
```

3. CLV diagnosis per candidate:

```text
python scripts/diagnose_mlb_clv_failure_modes.py --clv-output-dir <suite_output>/variants/<variant>/clv/<candidate> --output-dir <suite_output>/variants/<variant>/clv_diagnosis/<candidate>
```

**Important:** Use `sys.executable` internally instead of hardcoding `python`.

**Test cases:**

- dry-run emits expected number of commands.
- paths are placed under the expected suite output directory.
- `--only dropout` plans only dropout commands.
- `--variant no_prop_line` filters to one variant.

**Validation command:**

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_run_mlb_quote_clean_audit_suite.py -q
```

---

## Task 3: Add Execution Engine and Command Log

**Objective:** Execute planned commands in order, capture status, and write a machine-readable command log.

**Files:**
- Modify: `scripts/run_mlb_quote_clean_audit_suite.py`
- Modify: `tests/test_run_mlb_quote_clean_audit_suite.py`

**Implementation notes:**

- Use `subprocess.run(..., text=True, capture_output=True)`.
- Print command before running.
- Write `command_log.jsonl` with:
  - command type: `dropout`, `clv`, `diagnosis`
  - variant
  - candidate if applicable
  - command list
  - started_at / finished_at
  - returncode
  - stdout tail
  - stderr tail
- Default behavior: fail fast on first non-zero exit.
- With `--continue-on-error`, continue and mark suite as incomplete/failed in aggregation.
- Do not swallow failures.

**Test cases:**

- dry-run does not call subprocess.
- successful fake subprocess writes command log.
- failure without `--continue-on-error` stops.
- failure with `--continue-on-error` continues and records failure.

**Validation command:**

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_run_mlb_quote_clean_audit_suite.py -q
```

---

## Task 4: Aggregate Existing Output Files

**Objective:** Read outputs from dropout, CLV, diagnosis, and sweep metrics into a normalized candidate table.

**Files:**
- Modify: `scripts/run_mlb_quote_clean_audit_suite.py`
- Modify: `tests/test_run_mlb_quote_clean_audit_suite.py`

**Implementation notes:**

- Read sweep metrics from `<sweep_dir>/<config_dir>/metrics.json`.
- Read dropout summary from `<suite_output>/variants/<variant>/dropout/audit_summary.json`.
- Read CLV summary from whichever stable file `analyze_mlb_batter_hits_clv.py` emits. If exact file shape is not stable, implement best-effort extraction and leave blanks with notes.
- Read diagnosis summary from whichever stable file `diagnose_mlb_clv_failure_modes.py` emits. If exact file shape is not stable, implement best-effort extraction and leave blanks with notes.
- Write `candidate_comparison.csv` and `audit_suite_summary.json`.

**Do not invent metrics.** Missing file or missing key should become a clear note in the row.

**Test cases:**

- aggregation reads ROI/bets/hit rate from sample `metrics.json`.
- aggregation reads dropout decision/violations from sample `audit_summary.json`.
- missing CLV outputs produce blank CLV fields and note, not crash, if aggregation is run after partial suite.

**Validation command:**

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_run_mlb_quote_clean_audit_suite.py -q
```

---

## Task 5: Write Markdown Promotion-Gate Summary

**Objective:** Produce a human-readable final report that is suitable to paste into a handoff or review discussion.

**Files:**
- Modify: `scripts/run_mlb_quote_clean_audit_suite.py`
- Modify: `tests/test_run_mlb_quote_clean_audit_suite.py`

**Report sections:**

1. `# MLB Quote-Clean Audit Suite Summary`
2. Suite metadata.
3. Variants and roles.
4. Candidate comparison table.
5. Interpretation:
   - raw ROI winner
   - highest-volume viable candidate
   - CLV winner if available
   - candidates blocked by dropout
   - candidates blocked by CLV/failure-mode diagnosis
   - candidates inconclusive due to missing evidence
6. Recommendation:
   - `PROMOTE_CANDIDATE`, `PAPER_ONLY`, `BLOCKED`, or `INCONCLUSIVE`
   - reason bullets
7. Caveats and next steps.

**Promotion status logic for first version:**

Keep this conservative and transparent:

- `BLOCKED_DROPOUT` if dropout decision is `FAIL` or timing violations > 0.
- `BLOCKED_CLV` if diagnosis says fail, or CLV decision is explicitly fail.
- `INCONCLUSIVE` if CLV or diagnosis outputs are missing.
- `CANDIDATE` if dropout passes and CLV/diagnosis do not fail.

Do not auto-declare live deployment. Use `CANDIDATE` or `PAPER_ONLY` language unless Chase explicitly approves production/live promotion gates.

**Validation command:**

```powershell
.\venv\Scripts\python.exe -m pytest tests\test_run_mlb_quote_clean_audit_suite.py -q
```

---

## Task 6: Add Fixture Manifest and Smoke Test

**Objective:** Ensure the wrapper can run in dry-run mode against the real May 15 batter_hits paths.

**Files:**
- Create: `configs/audit_suites/mlb_batter_hits_quote_clean_20260515.yaml`
- Modify: `tests/test_run_mlb_quote_clean_audit_suite.py`

**Smoke command:**

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_quote_clean_audit_suite.py --manifest configs\audit_suites\mlb_batter_hits_quote_clean_20260515.yaml --dry-run
```

**Expected:**

- Plans 2 dropout commands.
- Plans 6 CLV commands.
- Plans 6 diagnosis commands.
- Does not execute anything.
- Prints final output directory.

---

## Task 7: Full Validation on Existing Outputs

**Objective:** Run the wrapper against the existing May 15 batter_hits sweeps and verify it produces the combined report.

**Files:**
- Runtime outputs only under `backtest_results/audits/mlb_batter_hits_20260515_suite/`.

**Command:**

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_quote_clean_audit_suite.py --manifest configs\audit_suites\mlb_batter_hits_quote_clean_20260515.yaml --continue-on-error
```

**Expected:**

- Dropout outputs exist for both variants.
- CLV outputs exist for six candidates, unless a data/DB issue is explicitly logged.
- Diagnosis outputs exist for completed CLV candidates.
- `candidate_comparison.csv` exists.
- `audit_suite_summary.md` exists.
- `command_log.jsonl` exists.

**Important:** If any CLV/DB query fails, do not silently retry with a different method. Report the specific failure and preserve partial outputs.

---

## Task 8: Documentation Update

**Objective:** Document how to use the audit suite wrapper after future sweeps.

**Files:**
- Create: `docs/development_docs/mlb_quote_clean_audit_suite.md`
- Optional Modify: `.hermes/plans/mlb-quote-clean-backtest-audit-hardening-2026-05-15.md` to reference this wrapper plan once implemented.

**Docs should include:**

- When to run the wrapper.
- How to create a manifest.
- Why variants are named/generic.
- When prop-line vs no-prop-line comparison is required.
- What outputs to inspect first.
- How to interpret `CANDIDATE`, `BLOCKED`, and `INCONCLUSIVE`.

---

## Non-Goals

- Do not retrain models inside the audit wrapper.
- Do not run backtest sweeps inside the first version of the wrapper.
- Do not mutate the database.
- Do not replace the underlying diagnostic scripts.
- Do not hardcode `no_prop_line` / `with_prop_line` as the only supported comparison.
- Do not auto-promote a model or edit production config.
- Do not add direct Supabase MCP usage.

---

## Acceptance Criteria

- One manifest-driven command can run the full post-sweep audit workflow.
- Wrapper supports any number of named variants.
- Wrapper supports multiple candidate configs per variant.
- Wrapper can dry-run and show exact underlying commands.
- Wrapper writes command logs and preserves partial failures.
- Wrapper emits `candidate_comparison.csv` and `audit_suite_summary.md`.
- Existing component scripts remain usable directly.
- The May 15 batter_hits no-prop-line vs with-prop-line audit can be represented entirely as manifest data.

---

## Suggested Commit Sequence

1. `test: add quote-clean audit suite manifest validation tests`
2. `feat: add manifest loader for MLB quote-clean audit suite`
3. `feat: plan audit suite subprocess commands`
4. `feat: execute audit suite commands with logging`
5. `feat: aggregate quote-clean audit suite outputs`
6. `docs: add MLB quote-clean audit suite usage guide`

---

## Open Questions for Chase

1. Should the first wrapper version support YAML only, JSON only, or both?
2. Should the wrapper default to fail-fast or continue-on-error?
3. Should CLV analysis default to `--assume-bet-time-et` equal to `quote_cutoff_time_et`, or require explicit CLV bet-time config?
4. Should the first report use conservative `CANDIDATE/PAPER_ONLY/BLOCKED/INCONCLUSIVE` language only, with no `PROMOTE` status?
5. Should the wrapper eventually be allowed to launch the sweeps too, or should it remain strictly post-sweep?
