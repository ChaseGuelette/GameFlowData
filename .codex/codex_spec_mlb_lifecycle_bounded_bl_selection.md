# Implementation Spec: Bounded BL Grid and Selected-Config Certification

## Goal
Make the YAML MLB lifecycle support a bounded Black-Litterman sweep while auditing, running expanded ranker diagnostics, and performing dropout/timing analysis only for a small, recorded set of selected configurations.

## Approved Selection Policy
- Discovery runs may use automatic risk-filtered top-N selection.
- Independent-validation and finalist-certification runs must use explicit preregistered sweep cells.
- The selected set, metrics, and selection reasons must be persisted and included in downstream identity/provenance.
- Fail closed when no valid selected configs exist. Never silently omit `--bets-csv` and thereby fall back to auditing every config.

## Allowed Files
Implementation/tests/docs may touch only:
- `src/models/mlb/lifecycle/config.py`
- `src/models/mlb/lifecycle/adapters.py`
- `src/models/mlb/lifecycle/runner.py`
- `scripts/run_mlb_quote_clean_audit_suite.py`
- `scripts/audit_mlb_quote_clean_dropout.py`
- `tests/test_mlb_lifecycle_config.py`
- `tests/test_mlb_lifecycle_adapters.py`
- `tests/test_mlb_lifecycle_runner.py`
- `tests/test_run_mlb_quote_clean_audit_suite.py`
- `tests/test_audit_mlb_quote_clean_dropout.py`
- YAML files under `configs/mlb/`
- `configs/mlb/examples/USAGE_GUIDE.md`
- `docs/development_docs/mlb_model_lifecycle_usage_guide.md`
- `docs/development_docs/mlb_model_lifecycle_runner.md`

Do not edit model math, training algorithms, probability generation, database schema, production code, or deployment/live-trading behavior.

## Requirements

### 1. Expose BL controls in lifecycle YAML
Add typed evaluation fields:
- `z_max: list[float]`, default `[1.0]`
- `max_weight: list[float]`, default `[0.50]`

Validate useful numeric bounds consistent with the sweep CLI. Thread both lists to `run_mlb_sweep.py` as `--z-max ...` and `--max-weight ...`.

A small BL example should be expressible as:
```yaml
evaluation:
  tau: [null, 0.50, 0.90]
  z_max: [0.25]
  max_weight: [0.50]
  edge_thresholds: [0.10, 0.12, 0.15]
```

### 2. Add typed audit selection configuration
Add a nested selection config under `audit`, with these supported policies:
- `all_decision_grade`: backward-compatible only for discovery; prohibited for independent/finalist.
- `risk_filtered_top_n`: discovery only.
- `explicit`: required for independent-validation/finalist-certification.

Recommended shape (minor naming adjustments are allowed if clearer and consistently documented/tested):
```yaml
audit:
  minimum_bets: 100
  bootstrap_samples: 1000
  mode: full
  selection:
    policy: explicit
    max_configs: 3
    include_no_bl_control: true
    rank_by: sharpe_ratio
    configs:
      - tau: null
        z_max: 0.25
        max_weight: 0.50
        edge_threshold: 0.12
        kelly_fraction: 0.0
      - tau: 0.50
        z_max: 0.25
        max_weight: 0.50
        edge_threshold: 0.12
        kelly_fraction: 0.0
```

Explicit selectors are parameter-based; users must not depend on generated config directory numbering. Match against the config payloads in `sweep_results.json` and their corresponding sorted `config_*` directories. Float matching should be robust to normal serialization (e.g. tolerance or normalized decimal comparison).

Validation rules:
- `risk_filtered_top_n` is valid only for `purpose: discovery`.
- `all_decision_grade` is valid only for `purpose: discovery`.
- `explicit` is required for independent/finalist.
- `max_configs >= 1`.
- Explicit selectors must be nonempty and no larger than `max_configs`.
- Duplicate selectors fail validation.

### 3. Deterministic selection behavior
Replace the current uncapped `_decision_grade_bets` behavior with a selection result that includes config directory, bets CSV, candidate-edge CSV, sweep config parameters, metrics, and reason.

For `risk_filtered_top_n`:
- require `total_bets >= audit.minimum_bets`;
- require drawdown <= `decision.max_drawdown`;
- honor `decision.require_positive_roi` as a filter;
- rank deterministically by configured `rank_by` (`sharpe_ratio` or `roi`), with stable tie breakers;
- if `include_no_bl_control` is true, reserve one slot for the best eligible `tau: null` control;
- fill remaining slots with the best eligible BL configs (`tau != null`) up to `max_configs`;
- never exceed `max_configs` and never select duplicates.

For `explicit`:
- match exactly the preregistered parameter cells;
- each matched config must have `total_bets >= minimum_bets`;
- missing, ambiguous, duplicate, or underpowered selections fail closed before audit execution.

For `all_decision_grade` discovery compatibility:
- still cap to `max_configs`; do not allow unbounded fan-out.

Persist `audit_selection.json` (and optionally a concise Markdown companion) under the lifecycle run root. Include the selection manifest and selected source files in audit/ranker/decision input identity hashing. Dry-run should persist the planned selection policy; attached sweeps should resolve real selected configs during dry-run when files exist.

### 4. Restrict CLV, ranker, and dropout to selected configs
- Pass repeated `--bets-csv` only for selected configs to the audit suite.
- If selection is empty, raise before building/running the audit command. Never allow audit-suite auto-discovery to broaden to all configs.
- The audit suite must pass the same selected bets files into the dropout tool.
- Add repeated `--bets-csv` support to `audit_mlb_quote_clean_dropout.py`; when supplied, `collect_saved_bet_keys` and relevant saved-output validation must use only those paths rather than globbing every `config_*/bets.csv`.
- Preserve old auto-discovery behavior only when the dropout tool is invoked directly without explicit bets paths.

### 5. Expanded ranker diagnostics
For every selected config only:
- invoke `analyze_mlb_clv_ranking_diagnostics.py` with `--score-set all`;
- pass the selected sweep config's actual `bookmaker_candidate_edges.csv` via `--candidate-edges-csv`;
- fail closed if that candidate-edge file is missing for lifecycle-selected certification;
- keep bootstrap/min-N values from YAML;
- preserve exact config identity from sweep selection through CLV/ranker/decision.

### 6. Update presets/examples
- `configs/mlb/examples/start_from_scratch.yaml`: demonstrate a small BL grid and `risk_filtered_top_n`, max 3, include no-BL control, `audit.mode: clv_only`.
- `configs/mlb/examples/resume_existing.yaml`: explicit selected cells matching the attached no-BL sweep.
- Existing `independent_validation` and `finalist_certification` presets under `configs/mlb/` must add explicit selectors so they remain valid.
- The end-to-end batter-hits finalist preset should demonstrate bounded BL controls and explicit selection, but do not change its model feature family or temporal window.
- Keep flat staking. BL/edge/Kelly values remain sweep-grid compatibility inputs; no Kelly action is performed.

### 7. Documentation
Update both lifecycle guides to explain:
- sweep grid vs certification subset;
- discovery automatic selection vs independent/finalist preregistration;
- no-BL control retention;
- `z_max` and `max_weight` fields;
- selection manifest path;
- only selected configs flow through CLV/ranker/dropout;
- full audit is required for strict dropout/+15/+30/+60 timing certification.

## TDD Requirements
Use strict RED-GREEN-REFACTOR vertical slices:
1. Add one focused failing test for z-max/max-weight command propagation; run and observe expected failure; implement; rerun green.
2. Add failing config validation tests for purpose/policy rules; run red; implement; rerun green.
3. Add failing runner tests for top-N control+BL selection, explicit matching, cap, and fail-closed empty/missing selection; run red; implement; rerun green.
4. Add failing adapter/ranker tests for `--score-set all` and exact candidate-edge path; run red; implement; rerun green.
5. Add failing audit/dropout tests proving only explicitly selected bets paths are consumed; run red; implement; rerun green.
6. Update YAML/docs only after behavior is green; validate all presets resolve.

## GameFlow Invariants
- Empirical probabilities remain `(samples > line).mean()`; do not touch probability math.
- Never add global calibration/conformal offsets; Q10 behavior is unchanged.
- Preserve quote-clean temporal integrity and exact artifact/sweep provenance.
- Do not deploy, promote, trade live, or execute Kelly.
- No DB writes, schema changes, broad queries, training, sweeps, or audits during implementation validation.
- Tests and CLI `--dry-run` only.

## Validation
Run:
```text
.\venv\Scripts\python.exe -m pytest tests/test_mlb_lifecycle_config.py tests/test_mlb_lifecycle_adapters.py tests/test_mlb_lifecycle_runner.py tests/test_run_mlb_quote_clean_audit_suite.py tests/test_audit_mlb_quote_clean_dropout.py -q
.\venv\Scripts\python.exe -m ruff check <modified Python files>
git diff --check -- <allowed files>
.\venv\Scripts\python.exe scripts/run_mlb_model_lifecycle.py --config configs/mlb/examples/start_from_scratch.yaml --dry-run
.\venv\Scripts\python.exe scripts/run_mlb_model_lifecycle.py --config configs/mlb/examples/resume_existing.yaml --dry-run
```

## Review Criteria
- Small BL grids are fully controlled by YAML.
- Audit/ranker/dropout never fan out across every BL cell when selection is configured.
- Discovery auto-selection is deterministic, capped, risk-filtered, and retains a raw control when requested.
- Independent/finalist selection is explicit and preregistered.
- Expanded ranker receives `score-set all` and the exact selected config candidate-edge artifact.
- Selection/provenance is persisted and hashed.
- Existing attached/resume behavior remains fail-closed and compatible after presets are updated.
- No expensive or DB-dependent job was executed.
- Do not commit.
