# Batter Hits Broad BL Volume Sweep Implementation Plan

> **Partially superseded (2026-07-27):** The broad BL grid remains useful, but only after raw/no-BL
> feature discovery has selected a small baseline/model-finalist pool. Do not use this plan as the
> feature-family iteration loop. Tasks for the custom neighbor-stability analyzer, cached volume
> scanner, mandatory CLV ranker, edge-bucket monotonicity, and Kelly-readiness are removed from the
> active flat-staking path. Select adequately powered BL cells using flat profit, ROI, Sharpe,
> drawdown, bet count, and ordinary parameter-neighborhood review; then run independent-window
> dropout/timing certification and frozen flat forward paper. Mean CLV is optional supporting
> evidence. See `.hermes/plans/2026-07-27_204057-flat-first-model-selection-lifecycle.md`.

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Efficiently discover adequately powered Black–Litterman parameter regions for the newly trained Batter Hits platoon/contact-quality artifact, then certify only preregistered survivors on a fresh evaluation window.

**Architecture:** Reuse the completed model artifact and the sweep engine's existing shared Phase 0–1 prediction pass. Run a deliberately structured 100-cell quote-clean discovery grid, select candidates using volume and parameter-neighborhood stability rather than observed ROI, and reserve full CLV/ranker certification for a small set of preregistered configurations on a fresh untouched window. Add a cached summary-only volume-scan path later only if repeated sweeps justify the code change.

**Tech Stack:** Python 3, pandas, NumPy, YAML lifecycle configs, local PostgreSQL, Windows virtual environment launched from WSL/tmux, pytest.

---

## Evidence and Constraints

- Completed artifact:
  `backtest_results/lifecycle/batter_hits_platoon_contact_ablation_from_scratch/artifacts/mlb_run_batter_hits_20260722_083314_no_prop_line`
- Artifact identity:
  `2a48ccc5d14f46b1ea33c007981c6a8cf96326ed7915b7d45fc709f5a560eb41`
- Existing quote-clean evaluation window: `2026-05-18` through `2026-06-21`.
- Existing sweep generated 8,226 predictions over 35 game dates.
- Measured shared Phase 0–1 runtime: 660.9 seconds.
- Measured average per-cell runtime: 5.477 seconds.
- Estimated 100-cell compute runtime: approximately 20.1 minutes before serialization overhead; expected wall time is approximately 20–25 minutes.
- Existing output size: 479 MiB for nine cells; projected 100-cell output is approximately 5.2 GiB.
- Available C: drive space at planning time: approximately 946 GiB.
- Keep empirical-CDF probabilities unchanged: `(samples > line).mean()`.
- Keep quote-clean `slate_or_tminus`, T-minus 60, dense CLV snapshot source, preferred-book-first routing, both directions, and flat $100 report-only staking.
- Do not retrain the model for this discovery sweep.
- Do not pool bets across distinct parameter cells to satisfy `minimum_bets`.
- A cell below 100 bets is `UNDERPOWERED`, not necessarily bad.
- May 18–June 21 has already been inspected. It may be used for volume discovery, but newly selected BL parameters require a fresh untouched window for performance certification.
- No deployment, promotion, live betting, or Kelly action is authorized by this plan.

---

### Task 1: Create the 100-cell BL discovery lifecycle config

**Objective:** Define a broad but bounded BL grid that spends combinations on posterior behavior rather than irrelevant staking variants.

**Files:**
- Create: `configs/mlb/batter_hits/platoon_contact_bl_volume_discovery.yaml`
- Reference: `configs/mlb/batter_hits/platoon_contact_ablation_resume_no_bl_audit.yaml`
- Reference: `configs/mlb/examples/USAGE_GUIDE.md`

**Step 1: Write the config**

Use the completed artifact but do not attach the previous narrow sweep. Configure the discovery grid as:

```yaml
evaluation:
  start: 2026-05-18
  end: 2026-06-21
  direction: both
  edge_thresholds: [0.05, 0.07, 0.08, 0.10]
  flat_bet: 100
  tau: [null, 0.10, 0.25, 0.50, 0.90]
  z_max: [0.25, 0.50, 1.00]
  max_weight: [0.50, 0.65]
  kelly_values: [0.0]
```

Required surrounding settings:

```yaml
experiment:
  profile: batter_hits
  purpose: discovery

model:
  base: no_prop_line
  artifact_dir: backtest_results/lifecycle/batter_hits_platoon_contact_ablation_from_scratch/artifacts/mlb_run_batter_hits_20260722_083314_no_prop_line
  sweep_artifact_identity_sha256: 2a48ccc5d14f46b1ea33c007981c6a8cf96326ed7915b7d45fc709f5a560eb41
  tune: false
  feature_controls:
    mode: include
    families: [platoon, contact_quality]
    features: []

quotes:
  clean: true
  line_source: mlb_player_props_clv_snapshots
  decision_policy: slate_or_tminus
  relative_minutes: 60
  routing: preferred_book_first
```

Use `audit.mode: clv_only` for discovery. Do not configure the broad grid as `finalist_certification`.

**Step 2: Verify the grid count without running DB/model work**

Run the config parser or lifecycle dry run through Windows Python.

Expected:

```text
96 BL cells + 4 no-BL controls = 100 total configurations
```

The grid builder must deduplicate no-BL across `z_max` and `max_weight`.

**Step 3: Verify identity and quote policy**

Run:

```bash
powershell.exe -NoProfile -Command "Set-Location 'C:\Users\Chase\Projects\GameFlowData'; .\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\batter_hits\platoon_contact_bl_volume_discovery.yaml --dry-run"
```

Expected:

- attached artifact identity passes;
- training is not scheduled;
- a new 100-cell sweep is scheduled;
- quote-clean source is `mlb_player_props_clv_snapshots`;
- routing is `preferred_book_first`;
- staking remains report-only and live-blocked.

**Step 4: Validate repository hygiene**

Run:

```bash
git diff --check -- configs/mlb/batter_hits/platoon_contact_bl_volume_discovery.yaml
git status --short -- configs/mlb/batter_hits/platoon_contact_bl_volume_discovery.yaml
```

Expected: no whitespace errors; only the intended config is new or modified.

**Step 5: Commit after review approval**

```bash
git add configs/mlb/batter_hits/platoon_contact_bl_volume_discovery.yaml
git commit -m "config: add batter hits BL volume discovery sweep"
```

---

### Task 2: Run the discovery sweep in persistent tmux

**Objective:** Execute the 100-cell quote-clean sweep without risking termination from SSH disconnection.

**Files:**
- Input: `configs/mlb/batter_hits/platoon_contact_bl_volume_discovery.yaml`
- Expected output: `backtest_results/lifecycle/batter_hits_platoon_contact_bl_volume_discovery/`

**Step 1: Confirm no conflicting heavy run is active**

Run:

```bash
tmux list-sessions
```

Also inspect Windows Python lifecycle/sweep processes. Do not start another large sweep if concurrent model work would create avoidable CPU, RAM, disk, or local-Postgres contention.

**Step 2: Create a dedicated tmux session**

```bash
tmux new-session -s batter-hits-bl-volume
```

**Step 3: Launch the lifecycle using Windows Python**

```bash
powershell.exe -NoProfile -Command "Set-Location 'C:\Users\Chase\Projects\GameFlowData'; .\venv\Scripts\python.exe scripts\run_mlb_model_lifecycle.py --config configs\mlb\batter_hits\platoon_contact_bl_volume_discovery.yaml"
```

**Step 4: Detach safely**

Use `Ctrl-b`, then `d`.

**Step 5: Verify completion**

Inspect:

- `stage_status.json`
- `sweep/sweep_summary.csv`
- `sweep/sweep_results.json`

Expected:

- artifact attached, not retrained;
- sweep exit code is zero;
- exactly 100 one-to-one config directories/results exist;
- no silent date-level failures;
- game dates and total predictions remain consistent with the intended window;
- no deployment or live action occurred.

---

### Task 3: Produce a volume-and-stability candidate report

**Objective:** Identify robust, adequately powered BL regions without selecting on observed ROI.

**Files:**
- Create: `scripts/analyze_mlb_bl_volume_surface.py`
- Test: `tests/test_analyze_mlb_bl_volume_surface.py`
- Input: discovery `sweep_results.json` and `sweep_summary.csv`
- Output: discovery `bl_volume_surface.csv`, `bl_volume_candidates.json`, and `bl_volume_report.md`

**Step 1: Write failing tests**

Tests must prove that the analyzer:

1. marks cells with fewer than 100 bets as `UNDERPOWERED`;
2. never pools bets across different cells;
3. groups neighbors by one-step changes in `tau`, `z_max`, `max_weight`, or edge threshold;
4. marks an isolated eligible cell as fragile;
5. marks a multi-cell eligible neighborhood as stable;
6. does not use ROI, profit, hit rate, or Sharpe in candidate eligibility or ranking;
7. preserves the no-BL controls separately.

Run:

```bash
powershell.exe -NoProfile -Command "Set-Location 'C:\Users\Chase\Projects\GameFlowData'; .\venv\Scripts\python.exe -m pytest tests\test_analyze_mlb_bl_volume_surface.py -v"
```

Expected: FAIL before implementation.

**Step 2: Implement the minimal analyzer**

Required CLI:

```text
--sweep-results-json
--sweep-summary-csv
--output-dir
--minimum-bets 100
--minimum-stable-neighbors 2
```

Candidate fields must include:

```text
tau
z_max
max_weight
edge_threshold
total_bets
power_status
eligible_neighbor_count
stability_status
selection_basis
```

`selection_basis` must always state that candidates were chosen by volume and parameter-neighborhood stability, not outcome performance.

**Step 3: Run tests to verify pass**

Expected: all targeted tests pass.

**Step 4: Run the analyzer against the real discovery sweep**

Expected report sections:

- adequately powered no-BL controls;
- adequately powered BL cells;
- stable BL neighborhoods;
- isolated/fragile cells;
- underpowered cells retained as evidence;
- explicit statement that performance metrics were not used for candidate selection.

**Step 5: Review candidate count**

Target two to five representative BL candidates. Do not select dozens of near-duplicates for full auditing.

**Step 6: Commit after review approval**

```bash
git add scripts/analyze_mlb_bl_volume_surface.py tests/test_analyze_mlb_bl_volume_surface.py
git commit -m "feat: analyze MLB BL volume stability"
```

---

### Task 4: Define and verify a fresh certification window

**Objective:** Establish an untouched quote-clean window with sufficient linked snapshot coverage before evaluating newly selected BL parameters.

**Files:**
- Create: `.hermes/audits/mlb/batter_hits_bl_fresh_window_coverage.md`
- Reference: `scripts/audit_mlb_clv_snapshot_coverage.py` or the current bounded coverage-audit entry point

**Step 1: Propose candidate dates**

Choose dates strictly after `2026-06-21`. Do not assume data completeness from calendar availability.

**Step 2: Run bounded coverage checks**

Check only the proposed date range and required MLB tables. Report:

- game dates;
- linked game/player snapshot coverage;
- usable two-sided quote coverage;
- decision-time availability;
- +15/+30/+60 timing horizon coverage;
- expected candidate count by stat and direction.

Use the local Windows PostgreSQL environment. Keep queries bounded and SELECT-only.

**Step 3: Precommit the fresh window**

Record the exact start/end dates and coverage evidence before running performance evaluation.

**Step 4: Gate execution**

Do not proceed if linked or timing coverage is insufficient. Extend or shift the window rather than weakening the evidence gates.

---

### Task 5: Create the finalist-certification lifecycle config

**Objective:** Audit a small preregistered set of stable BL candidates plus one no-BL control on the fresh window.

**Files:**
- Create: `configs/mlb/batter_hits/platoon_contact_bl_fresh_certification.yaml`
- Reference: discovery `bl_volume_candidates.json`
- Reference: `.hermes/audits/mlb/batter_hits_bl_fresh_window_coverage.md`

**Step 1: Write explicit selectors**

Use:

```yaml
experiment:
  purpose: finalist_certification

audit:
  minimum_bets: 100
  bootstrap_samples: 1000
  mode: full
  selection:
    policy: explicit
    include_no_bl_control: true
```

Include one no-BL control and two to five stable BL candidates. Do not pick candidates based on May 18–June 21 ROI.

**Step 2: Keep hard decision gates**

```yaml
decision:
  max_drawdown: 0.25
  require_positive_roi: true
  require_positive_mean_clv_ci_low: true
  require_positive_ranker_ci_low: true
  require_edge_bucket_monotonicity: true
  require_independent_window: true
```

**Step 3: Dry-run and verify identities**

Expected:

- artifact identity passes;
- fresh dates match the precommit document;
- explicit selectors resolve one-to-one;
- full audit, timing diagnostics, ranker, and report-only decision are scheduled;
- no live action is possible.

**Step 4: Commit after review approval**

```bash
git add configs/mlb/batter_hits/platoon_contact_bl_fresh_certification.yaml .hermes/audits/mlb/batter_hits_bl_fresh_window_coverage.md
git commit -m "config: preregister batter hits BL fresh certification"
```

---

### Task 6: Run and adjudicate fresh-window certification

**Objective:** Determine whether any broad-grid BL candidate beats the no-BL control under leak-free, quote-clean evidence gates.

**Files:**
- Expected output: `backtest_results/lifecycle/batter_hits_platoon_contact_bl_fresh_certification/`

**Step 1: Run inside a dedicated tmux session**

Use Windows Python and the local database.

**Step 2: Verify every stage**

Required PASS/complete artifacts:

- artifact identity;
- fresh-window sweep;
- audit selection;
- CLV matching and block-bootstrap confidence intervals;
- dropout diagnostics;
- +15/+30/+60 timing stability;
- ranker/Spearman confidence intervals;
- promotion decision;
- report-only staking recommendation.

**Step 3: State the decision clearly**

The final report must state:

- purpose;
- candidate winner, if any;
- no-BL comparison;
- data audit status;
- quote-clean replay status;
- CLV status;
- leak-free baseline status;
- deployment status;
- explicit reasons for `Confirm`, `Watch`, or `Shelf`.

**Step 4: Preserve underpowered and failed candidates**

Do not delete them or call them bad without evidence. Distinguish:

- `UNDERPOWERED`;
- `FAILED_GATE`;
- `ELIGIBLE_BUT_INFERIOR`;
- `CONFIRMED`.

---

### Task 7: Add cached summary-only volume-scan mode if repeated use justifies it

**Objective:** Reduce future broad-sweep runtime and disk duplication without changing promotion-critical probability or routing semantics.

**Files:**
- Modify: `src/backtesting/mlb/run_mlb_sweep.py`
- Modify: `src/backtesting/mlb/sweep_config.py`
- Modify: `src/backtesting/mlb/sweep_execution.py`
- Modify: `src/backtesting/mlb/edge_engine.py`
- Modify: `src/backtesting/mlb/sweep_results.py`
- Test: `tests/test_mlb_sweep_config.py`
- Test: `tests/test_mlb_sweep_fast_path.py`
- Test: `tests/test_mlb_sweep_results.py`

**Step 1: Write failing tests for `--volume-scan`**

Required behavior:

- no full per-cell `bets.csv` or candidate-edge CSV for underpowered cells;
- compact summary includes total bets per cell;
- probabilities and edge counts match the existing full sweep exactly;
- empirical-CDF probability remains unchanged;
- preferred-book routing remains unchanged;
- no outcomes are required for volume eligibility.

**Step 2: Add a persisted precomputed-frame cache**

Key it by:

- artifact identity;
- evaluation start/end;
- stats;
- direction;
- quote-clean policy;
- relative minutes;
- line source;
- routing policy.

Refuse cache reuse when any key component differs.

**Step 3: Group work by unique BL transformation**

Compute posterior edges once per `(tau, z_max, max_weight)`, then evaluate all edge thresholds from that frame.

**Step 4: Persist full evidence only for survivors**

Add an explicit follow-up command or mode that materializes full outputs for chosen cells.

**Step 5: Run differential verification**

Compare old full-sweep versus new volume-scan outputs on a small fixture and a bounded real-date slice. Require exact equality for:

- posterior probabilities;
- selected side;
- selected bookmaker;
- edge threshold pass/fail;
- total bet count.

**Step 6: Run the relevant suite**

```bash
powershell.exe -NoProfile -Command "Set-Location 'C:\Users\Chase\Projects\GameFlowData'; .\venv\Scripts\python.exe -m pytest tests\test_mlb_sweep_config.py tests\test_mlb_sweep_fast_path.py tests\test_mlb_sweep_results.py -v"
```

**Step 7: Commit after review approval**

```bash
git add src/backtesting/mlb tests
git commit -m "feat: add cached MLB BL volume scan"
```

---

## Acceptance Criteria

- The discovery grid contains exactly 100 cells: 96 BL and four no-BL controls.
- Training is not repeated.
- Shared Phase 0–1 executes only once per sweep.
- Discovery remains quote-clean and uses the dense linked CLV snapshot source.
- Every cell retains its own bet count; no cross-cell pooling occurs.
- Cells under 100 bets are retained and labeled `UNDERPOWERED`.
- Candidate selection uses only volume and parameter-neighborhood stability.
- No candidate is promoted from inspected May 18–June 21 ROI.
- A fresh evaluation window is coverage-audited and preregistered before certification.
- Full CLV/timing/dropout/ranker work runs only for a small explicit finalist set.
- Final output states purpose, winner, evidence status, and deployment status.
- All staking output remains report-only; deployment and live betting remain blocked.

## Approval Gate

Do not implement Tasks 1–7 automatically from this document. Obtain user approval before creating the discovery config, starting the broad sweep, adding analyzer code, or modifying the sweep engine. Task 7 is optional and should be deferred unless repeated broad sweeps make the optimization worthwhile.
