# Fix plan 02 — Prediction rows / Kalshi model_prob linkage need verification

## Verdict

Production has no recent MLB prediction/sample rows in the last 7 days. Kalshi model/edge columns exist historically, but recent trading candidate tables are empty. This blocks any claim that successful inference jobs are actually producing tradable MLB edges.

## Evidence from remote production DB

SELECT-only SQL-runner audit on `DATABASE_URL`, statement timeout 15s:

- `mlb_daily_predictions`: 0 rows in the last 7 days.
- `mlb_daily_prediction_samples`: 0 rows in the last 7 days.
- Kalshi-related tables:
  - `kalshi_markets`: has `model_prob`, no generic `edge`, has `sport`; estimated ~26.7M rows, so broad grouped scan was intentionally skipped.
  - `kalshi_paper_bets`: total 5,421; all have model/edge; max `game_date = 2026-05-17`; no recent rows.
  - `kalshi_trade_queue`: total 1,256; all have model/edge; max `game_date = 2026-04-26`; no recent rows.
  - `kalshi_live_orders`: total 96; all have model/edge; max `game_date = 2026-04-26`; no recent rows.
  - `paper_bets`: total 905; all have model/edge; max `game_date = 2026-05-17`; no recent rows.

Interpretation:

- Historical Kalshi linkage worked at some point.
- Current inference/storage/output linkage is broken or producing zero-row days.
- Recent job-success markers are insufficient and may represent wrapper success with no rows, dry-run-like behavior, no games, stale derived inputs, missing model load, or storage failure.

## Code findings

Relevant files:

- `src/orchestration/mlb_inference_job.py`
  - Stores predictions with `MLBPredictionStore.store_predictions(preds, target_date)` and samples with `store_samples(samples, target_date)`.
  - Logs `MLB INFERENCE JOB COMPLETED SUCCESSFULLY` after store/export/paper bet attempts.
  - Paper bet placement failures are non-fatal.
- `src/models/mlb/mlb_daily_runner.py`
  - Returns empty DataFrame if no games or no predictions.
  - Batter predictions depend on loaded suite models and `batter_feature_store`.
  - Line/edge calculation depends on `fetch_lines_at_decision_time(... allow_latest_without_as_of=True)` returning lines.
  - Probability calculation correctly uses empirical CDF when samples exist.
- `src/models/mlb/mlb_prediction_store.py`
  - Upserts into `mlb_daily_predictions` with conflict key `(prediction_date, player_id, game_id, stat)`.
  - Stores samples into `mlb_daily_prediction_samples`.
- `src/models/kalshi_edge.py`
  - Loads samples from daily prediction sample tables, computes model probabilities/edges, and updates `kalshi_markets` by `id`.
  - For MLB, it uses `mlb_daily_predictions` / `mlb_daily_prediction_samples` through `_load_samples` and `_find_sportsbook_odds` paths.
- `src/trading/kalshi/selection_loader.py`
  - Live candidate selection reads latest `kalshi_markets` rows with `model_prob IS NOT NULL`.

## Likely root causes to test

1. Inference job runs but generates zero predictions.
   - Causes: no games found, probable starters missing, no active batters after lineup filter, feature build failures, suite has no loaded models, stale/missing derived feature inputs.

2. Inference job generates predictions but storage is pointed at a different DB or failing before commit.
   - DB audit says remote has 0 recent rows; compare Railway env target vs expected `DATABASE_URL` after Railway auth is restored.

3. Predictions are generated for unexpected dates.
   - Need grouped max/min query beyond 7 days and compare to scheduler run dates.

4. Kalshi refresh has no samples to match.
   - `KalshiEdgeCalculator.compute_edges` logs and returns early when `_load_samples` returns empty.
   - That can make Kalshi refresh “complete” while no markets are updated.

## Implementation status

Implemented 2026-05-26:

- Added `scripts/verify_mlb_prediction_outputs.py` read-only verifier.
- Patched `src/orchestration/mlb_inference_job.py` to fail if scheduled games exist but no predictions are generated, if predictions have no samples, or if non-dry-run storage readback finds no committed prediction/sample rows.
- Patched `src/models/kalshi_edge.py` and `src/orchestration/kalshi_refresh_job.py` so MLB open markets with missing samples are surfaced as a blocking output gap instead of a healthy no-op edge refresh.

Validation run:

- `./venv/Scripts/python.exe -m ruff check src/orchestration/mlb_inference_job.py src/models/kalshi_edge.py src/orchestration/kalshi_refresh_job.py scripts/verify_mlb_prediction_outputs.py` — passed.
- `./venv/Scripts/python.exe -m py_compile src/orchestration/mlb_inference_job.py src/models/kalshi_edge.py src/orchestration/kalshi_refresh_job.py scripts/verify_mlb_prediction_outputs.py` — passed.
- `./venv/Scripts/python.exe -m pytest tests/test_kalshi_refresh_job_direct_services.py -q` — 3 passed, 1 warning.
- `./venv/Scripts/python.exe -m pytest tests/test_kalshi_sargable_queries.py tests/test_time_windows.py -q` — 5 passed, 1 warning.

## Fix proposal

### Phase A — add an explicit output verifier

Create a small read-only script, e.g. `scripts/verify_mlb_prediction_outputs.py`, that checks:

- schedule games for target date;
- `mlb_daily_predictions` count by stat;
- sample count by stat;
- counts with `line`, `over_edge`, `under_edge`, `bl_*`, and `is_recommended`;
- prediction rows with null critical fields;
- recent `kalshi_markets` rows for that sport/date with `model_prob`, `raw_edge`, `bl_model_prob`, `bl_edge` populated, using a sargable UTC window, not ET date cast;
- queue/paper/live rows for target date.

It should exit non-zero if:

- games exist but predictions are zero;
- predictions exist but samples are zero;
- predictions exist but line/edge coverage is unexpectedly low;
- Kalshi markets exist but model_prob update count is zero after edge refresh.

### Phase B — make inference job fail on zero outputs when games exist

Patch `mlb_inference_job.py` or `MLBDailyPredictionRunner` wrapper:

1. After `runner.run_for_date`, if schedule games exist and `preds.empty`, exit non-zero with a clear reason.
2. After storage, re-query `mlb_daily_predictions` count for target date/stat and assert rows were committed.
3. Log model suite loaded stats and feature-store availability explicitly.
4. Keep paper bet placement non-fatal, but not prediction generation/storage.

### Phase C — gate Kalshi refresh on prediction/sample availability

Patch `kalshi_refresh_job.py` / `KalshiEdgeCalculator` behavior for MLB:

- If open supported MLB markets exist but samples are missing, classify as a warning/blocking output gap in summary.
- Emit a metric/Discord warning for “no samples; edges skipped”.
- Do not let this masquerade as a healthy edge refresh.

## Near-future required run

After the next approved non-dry-run MLB inference / Kalshi refresh cycle, run the remote verifier against production to prove rows actually landed and Kalshi model/edge columns are being populated. This is intentionally not required before committing the code gate, because it can fail on a day where inference has not yet run or there are no eligible current outputs.

PowerShell repo-root command:

```powershell
.\venv\Scripts\python.exe scripts\verify_mlb_prediction_outputs.py --remote --date 2026-05-26 --sport mlb
```

Replace `2026-05-26` with the target inference date being validated.

## Verification commands

After code change or manual rerun, from PowerShell repo root:

```powershell
.\venv\Scripts\python.exe src\orchestration\mlb_inference_job.py --date 2026-05-26 --dry-run --skip-bets --skip-discord
```

Then, after approved non-dry-run:

```powershell
.\venv\Scripts\python.exe scripts\verify_mlb_prediction_outputs.py --remote --date 2026-05-26 --sport mlb
```

If the verifier script does not exist yet, implement it before trusting logs.

## Non-goals

- Do not flip live trading flags.
- Do not treat historical `kalshi_paper_bets` as current readiness.
- Do not solve NBA prediction rows in this lane.
