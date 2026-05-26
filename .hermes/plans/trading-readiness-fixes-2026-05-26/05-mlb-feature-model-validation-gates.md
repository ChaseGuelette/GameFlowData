# Fix plan 05 — MLB feature/model path needs validation and promotion gates

## Verdict

Current MLB model/trading path is not promotion-ready. Existing roadmap already says batter_hits is paper-only/caution. The immediate fix is not another feature push; it is a gate sequence: verify functional production artifacts, output rows, quote-clean/dense CLV, edge ranking, and paper-live stability.

## Relevant prior lessons/invariants

Retrieved from GBrain/source-all fallback:

- `operations/hard-facts`
  - Never deploy global conformal recalibration offsets.
  - Q10 miscalibration can be the edge; do not blindly correct it.
  - Probabilities must use empirical sample CDF, not Gaussian CDF.
- `operations/critical-invariants`
  - Do not trade unsupported Kalshi stat types.
  - Kalshi YES side defaults disabled; only controlled experiments should change it.
- `lessons/quote-clean-clv-before-feature-work`
  - Quote-clean CLV must come before feature expansion or promotion.
  - Legacy line mode can radically overstate strategy quality.
  - Positive mean CLV is not enough if edge does not rank CLV.
  - Intraday stability is production-readiness evidence, not a nice-to-have.
- `lessons/empirical-cdf-for-probabilities`
  - Use `(samples > line).mean()` or the intentional Kalshi integer-line variant, never Gaussian approximations.
- Existing doc: `docs/development_docs/mlb_batter_hits_model_functionality_roadmap.md`
  - Current posture: caution / paper-only.
  - Gates: audit/temporal safety, dense CLV, book concentration, edge ranking/staking, feature-expansion decision, paper/live readiness.

## Evidence from current investigation

1. Feature inputs are stale.
   - Rolling averages, bullpen status, and active roster are stale on remote production.
   - Model validation before fixing inputs would be misleading.

2. Prediction outputs are absent.
   - Remote `mlb_daily_predictions` and `mlb_daily_prediction_samples` have zero rows in the last 7 days.
   - Promotion cannot proceed until actual production prediction rows and samples exist.

3. Production model suite is structurally present but must be audited.
   - `src/models/mlb/mlb_model_suite.py` loads whatever models exist from a production directory and skips missing ones gracefully.
   - This is useful operationally, but promotion docs need explicit loaded-stat verification so missing model artifacts do not silently reduce coverage.

4. Batter probability/edge math mostly follows invariants in current code.
   - `src/models/mlb/mlb_daily_runner.py` uses empirical CDF from samples for `over_prob` when samples exist.
   - `src/models/kalshi_edge.py` uses empirical CDF and intentional `>=` semantics for integer Kalshi lines.

5. `batter_hrr` is in some config/runner mappings but historically flagged as unsupported/no trained model in invariants.
   - Do not trade it unless current artifact + validation proves support.


## Implementation status

Implemented 2026-05-26 as the artifact/functionality gate for this hotfix lane:

- Added `scripts/audit_mlb_model_artifacts.py` as a read-only model artifact audit.
- Added `tests/test_audit_mlb_model_artifacts.py` coverage for loaded-stat failure, batter feature-count metadata, and explicit validation required before treating `batter_hrr` as supported.
- The audit verifies the production model directory resolution, loaded suite stats, predictor classes, required production stats, core artifact presence, feature counts, and metadata visibility.
- Required stats default to `pitcher_strikeouts` and `batter_hits`. Optional/unsupported loaded artifacts are reported as warnings and do not become live-trading support by existing merely in the directory.

Validation run:

- `./venv/Scripts/python.exe -m ruff check scripts/audit_mlb_model_artifacts.py tests/test_audit_mlb_model_artifacts.py` — passed.
- `./venv/Scripts/python.exe -m py_compile scripts/audit_mlb_model_artifacts.py tests/test_audit_mlb_model_artifacts.py` — passed.
- `./venv/Scripts/python.exe -m pytest tests/test_audit_mlb_model_artifacts.py -q` — 3 passed, 1 warning.
- `./venv/Scripts/python.exe scripts/audit_mlb_model_artifacts.py --model-dir src/models/mlb/artifacts` — passed with `Status: OK`.

Latest audit result:

- Resolved model dir: `src/models/mlb/artifacts/production`.
- Required stats loaded: `pitcher_strikeouts`, `batter_hits`.
- Extra loaded stats: `batter_total_bases`, `batter_rbis`, `batter_runs_scored`, `batter_hrr`.
- Warnings remain by design: extra loaded stats are not live-trading support; `batter_hrr` is present but not validated for live support; known non-live/unsupported stats must stay out of live trading; artifact metadata does not expose train seasons/calibration cutoff for the required stats.

Relevant prior lessons/invariants preserved:

- Quote-clean CLV and edge-ranking gates still come before feature expansion or live-money promotion.
- Probabilities must remain empirical sample CDF where applicable.
- Q10 miscalibration is a known edge and should not be blindly corrected.
- This gate confirms artifact/functionality presence only; it is not permission to enable live money.

Remaining operational follow-up before live money:

- Run the remote prediction/Kalshi output verifier after the next approved non-dry-run cycle.
- Complete quote-clean CLV, edge-ranking, dense intraday stability, and paper/live output gates before promotion.

## Fix proposal

### Phase A — model artifact/functionality audit

Create or run a small artifact audit that prints:

- production model directory used by Railway/local command;
- loaded suite stats;
- artifact metadata for each stat:
  - train seasons;
  - calibration cutoff;
  - variant/feature flags;
  - model type;
  - feature manifest count;
- whether each supported Kalshi stat has a loaded model:
  - `pitcher_strikeouts`;
  - `batter_hits`;
  - only include `batter_hrr` if a current validated model exists.

Fail if intended production stats are missing. Do not rely on “gracefully skips missing ones” for production readiness.

### Phase B — feature source coverage gate

Before interpreting any new feature-family validation:

1. Fix stale derived tables from `01-mlb-derived-feature-freshness.md`.
2. Run feature-path coverage checks across training/calibration/inference seasons:
   - new feature columns are present;
   - non-default/non-null variation exists in the seasons used for training;
   - train/serve paths use the same semantics.
3. For recent force-feature-family work, verify the artifact metadata/manifest actually reflects the intended feature family.

Do not treat “feature selector did/did not select X” as proof. Use force-include / force-exclude family ablations if feature families matter.

### Phase C — quote-clean and CLV gates

Use the existing roadmap/audit scripts:

- `docs/development_docs/mlb_batter_hits_model_functionality_roadmap.md`
- `scripts/run_mlb_quote_clean_audit_suite.py`
- `scripts/audit_mlb_quote_clean_dropout.py`
- `scripts/diagnose_mlb_clv_failure_modes.py`
- `scripts/analyze_mlb_clv_ranking_diagnostics.py`
- `scripts/analyze_mlb_clv_book_sensitivity.py`
- `scripts/analyze_mlb_clv_alt_book_candidates.py`

Gate requirements before live money:

1. Quote-clean replay uses production model dir and correct cutoff/decision policy.
2. Dropout audit has explained missing quote buckets.
3. CLV mean is positive with block bootstrap by game/date.
4. Edge/ranker quality has positive Spearman CI low or a predeclared alternative ranker/filter; otherwise use flat/threshold-only paper at most.
5. Book concentration survives preferred-book/candidate-book checks, not just selected-book ROI.
6. +15/+30/+60 minute intraday stability exists from dense snapshots.

### Phase D — paper/live readiness gate

Only after Phases A-C:

1. Run production-like paper mode with current data/features fixed.
2. Verify rows in:
   - `mlb_daily_predictions`;
   - `mlb_daily_prediction_samples`;
   - `kalshi_markets` model/edge fields;
   - `kalshi_paper_bets` or queue tables.
3. Ensure `KALSHI_ALLOW_YES_BETS=false` unless explicitly experimenting.
4. Keep `KALSHI_LIVE_TRADING_ENABLED=false` until Chase explicitly approves live execution.

## Suggested validation command shape

Artifact/functionality gate:

```powershell
.\venv\Scripts\python.exe scripts\audit_mlb_model_artifacts.py --model-dir src\models\mlb\artifacts
```

After fixing derived data and prediction verifier:

```powershell
.\venv\Scripts\python.exe scripts\verify_mlb_prediction_outputs.py --remote --date 2026-05-26 --sport mlb
```

For saved-artifact audit/CLV work, follow `docs/development_docs/mlb_batter_hits_model_functionality_roadmap.md` and use production model dir:

```powershell
.\venv\Scripts\python.exe scripts\run_mlb_quote_clean_audit_suite.py --local --sweep-output-dir backtest_results\mlb_batter_hits_dense_slate_t60_promoted_20260413_20260517 --output-dir backtest_results\audits\suite_selected5_mlb_batter_hits_dense_slate_t60_20260413_20260517_prodmodel --model-dir src\models\mlb\artifacts\production --start 2026-04-13 --end 2026-05-17 --stats batter_hits --quote-decision-policy slate_or_tminus
```

Do not launch long retraining/backtest sweeps automatically; Chase prefers to run those manually.

## Non-goals

- Do not promote live staking from one short-window ROI result.
- Do not start feature expansion before quote-clean CLV/ranking gates.
- Do not trade unsupported MLB stat types.
- Do not use Gaussian CDF probability shortcuts.
