# MLB lifecycle evidence hardening spec

## Goal

Make the YAML lifecycle fail closed on missing subprocess outputs and require persisted full-audit/dropout and timing-stability evidence before finalist Confirm/live-ready decisions.

## Allowed edit scope

- `src/models/mlb/lifecycle/runner.py`
- `src/models/mlb/lifecycle/decision.py`
- `scripts/run_mlb_quote_clean_audit_suite.py`
- `scripts/diagnose_mlb_clv_failure_modes.py`
- focused lifecycle/audit/diagnosis tests
- `docs/development_docs/mlb_model_lifecycle_runner.md`
- `docs/development_docs/mlb_model_lifecycle_usage_guide.md`

Do not alter model probability math, training behavior, feature-family definitions, quote routing, sweep math, or production deployment.

## Required behavior

### Subprocess output contracts

A zero exit code is necessary but not sufficient. Before marking a stage completed:

- Training must resolve a finalized non-`_incomplete` artifact containing every profile-required model artifact file. Artifact identity validation remains the next independent gate.
- Sweep must contain parseable non-empty `sweep_summary.csv`, parseable `sweep_results.json`, one `config_*` directory per result/config, and `metrics.json` in each config directory. A config with positive `total_bets` must contain `bets.csv`.
- Audit must contain parseable non-empty `suite_manifest.json`, `suite_manifest.csv`, and `suite_summary.md`. Every requested bets label must have `clv/<label>/clv_matches.csv`, `clv/<label>/clv_timing_stability.csv`, and `diagnosis/<label>/clv_failure_modes.json`.
- Full audit must additionally contain `dropout_audit/audit_summary.json`, `dropout_summary_by_bucket.csv`, `dropout_rows.csv`, and `selected_clean_quotes.csv`, with a persisted complete full-audit attestation in the suite manifest metadata.
- Each ranker subprocess must create `ranking_score_summary.csv` with the required decision columns.

Missing/malformed required outputs set the stage to `failed` and raise. Dry-run never requires generated outputs.

### Persisted full-audit evidence

The audit suite manifest metadata must persist:

- `audit_mode`: `clv_only` or `full`
- whether dropout audit ran
- dropout subprocess return code
- dropout summary path
- dropout decision
- `full_audit_complete`
- `full_audit_passed`

`full_audit_complete` requires full mode, dropout return code zero, and a parseable dropout summary. `full_audit_passed` additionally requires dropout decision `PASS`.

### Persisted timing-stability evidence

CLV diagnosis must persist structured timing evidence:

- required horizons `+15m`, `+30m`, `+60m`
- observed horizons
- per-horizon availability/coverage
- status `PASS` or `FAIL`

PASS requires all required horizons and at least one scored/available observation at each horizon. The audit suite must copy this structured evidence/status into each suite item.

### Decision gate

For `finalist_certification`:

- missing/incomplete full-audit evidence => Shelf/live-blocked
- completed dropout decision WARN or missing PASS => Shelf/live-blocked
- dropout decision FAIL => Exclude/live-blocked
- missing timing evidence => Shelf/live-blocked
- timing status FAIL => Exclude/live-blocked
- Confirm/live-ready requires `full_audit_passed` and timing PASS for the same candidate in addition to all existing gates

Discovery and independent-validation behavior remains unchanged except that persisted evidence appears in reports.

## TDD and validation

Add failing tests first and record RED. Then implement.

Run:

- focused lifecycle, audit suite, dropout, CLV analyzer/diagnosis, ranker, and profile tests
- Ruff on every changed Python file
- all three lifecycle dry-runs
- the repository test suite if focused tests pass and runtime is practical

Do not run real training, sweep, audit, ranker, DB, scraping, promotion, Kelly, or live workloads.
