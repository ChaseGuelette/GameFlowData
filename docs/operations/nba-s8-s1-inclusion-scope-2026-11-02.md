# Slice 8 prerequisite: narrow S1 fail-closed inclusion

Chase initially authorized isolated inclusion, then explicitly authorized committing and merging all available S1 work. This supersedes the earlier no-commit/no-main-merge boundary for the reviewed S1 allowlist only. Push and deployment remain unauthorized.

## Exact scope

- `src/models/features/nba/date_range_loader.py`: sanitized failed-chunk metadata; raise before concatenating/returning any partial result.
- `src/backtesting/backtest_harness.py`, `BacktestHarness.run`: sanitized date-specific error and immediate failure rather than continuing into partial simulation/results.
- Same file, `_run_date`: allow predictor exceptions to propagate; preserve legitimate empty inputs/results and all numerical/quote/combo behavior.

No whole-file S1 copying. Its predecessor harness lacks integrated `_derive_replay_combo_samples` and other accepted Slice 3 behavior. Preserve current integrated source outside these three exception sites. No tests/fixture edits, DB/network, source fixes, architecture or lifecycle semantics changes.

## Review and validation

Read-only independent S1 review recommended these three semantic hand-ports only. Coordinator applies these localized safety-sensitive edits directly to avoid a broad stale-source writer.

Run affected existing feature-mode/harness/backtest suites with dotenv disabled and DB/network guards; scoped Ruff/compile and whitespace checks. Use a small disposable actual-class probe for prediction failure propagation, sanitized date failure, and partial chunk rejection, with real external boundaries mocked. Controlled results are not historical-data/model acceptance.

State: implemented; independent specification/quality review and controlled execution PASS. Parent verified current bytes against the independent hashes. Existing affected suites: 31 passed. Scoped Ruff, compilation and whitespace: passed. No new tests or fixtures, no real-data/model/DB execution. Local commit/main integration is now authorized; no push or deployment is authorized.

## Accepted evidence

Reviewed source SHA-256 values, rechecked by the coordinator:

- `src/models/features/nba/date_range_loader.py`: `4d6a411646eeca2744eea3eda47c67767288bfa3f94b04657503b7951f196a5e`
- `src/backtesting/backtest_harness.py`: `e051dba1bbe77d46dadcf0a62dd9445676216e8f413ddac683741ceb1030b956`

Affected existing suites were `test_nba_feature_store_inventory.py`, `test_nba_mode_loaders.py`, `test_backtest_harness.py` and `test_run_backtest.py`. They ran through the existing Windows project venv, with dotenv disabled, DB URL environment variables removed and network access guarded.

The independent actual-class controlled probe verified:

- A failed second feature chunk caused zero concatenations or partial returned features.
- Predictor exceptions propagated before combo derivation.
- Date failure caused zero actuals/voids fetches, simulation, metrics or returned results.
- Successful one-row loading and legitimate empty-input no-ops remained valid.
- Failure logging was enabled, captured, nonempty and sanitized.
- No network or credential-file-open attempts occurred.
- SQL constants and protected combo/sample helpers matched the unchanged base.

This is source/mechanics acceptance, not historical source readiness, model acceptance, training permission or production activation. The original S1 worktree remains preserved; its stale whole-file harness and unrelated logging changes are intentionally not integrated. Remaining Slice 8 policy, historical-data and baseline-launch gates remain open.
