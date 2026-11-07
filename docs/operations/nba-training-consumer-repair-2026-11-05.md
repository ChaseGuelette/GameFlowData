# NBA training consumer repair — 2026-11-05

Status: implemented locally, focused verification passed; not committed or deployed. Training launch is not certified.

## Corrections to earlier readiness assessment

Ordinary experimental training does not require the strict historical replay attachment. The earlier `reports/nba-pretraining-acceptance-steps1-2-2026-11-05.md` conflated these contracts. Its blanket strict-injury training blocker is superseded by this distinction. Strict replay requirements themselves remain unchanged.

Zero exact linked-prop game matches do not prove absence of physical raw season data. Current local/remote table probes found only 2026 clocks, but physical 2024–25 coverage remains unknown without exhaustive evidence. Do not prescribe archive recovery or rescraping from that result. A date-incoherent exact link was verified (staging_id 246329485); matching game IDs alone does not certify quote eligibility.

## Implemented

- Training-specific injury selection preserves unsupported timestamps, missing cutoffs and omitted players as unknown, not healthy.
- Nested source observations are evaluated independently of overwritten flattened report labels. Provider clock meanings require explicit mapping; no timezone is invented.
- Incomplete reports do not produce complete-team absence totals. Existing legacy bulk query/facade contracts remain intact.
- Missing injury values survive feature selection, full/partial tuning, fitting preparation, calibration, quantile prediction, and single/batch Monte Carlo preparation. Other feature imputation and ordering remain unchanged.
- Empty fitting populations fail explicitly. The owner-set linked-props exclusion remains unchanged.

Source scope: `injury_context.py`, `training_loader.py`, `quantile_trainer.py`, `train_pipeline.py`, `monte_carlo.py`, and `feature_selection.py` in their existing source locations. Unrelated orchestration changes are not part of this repair.

## Verification

Coordinator independently reviewed scoped trainer/pipeline/selector/Monte Carlo diffs and reran:

- Existing injury-context and feature-store tests: 25 passed.
- Controlled actual public TrainingFeatureLoader probe: 1,000 supplied fixture rows; nested time, omission, conflict, unknown aggregates and legacy compatibility passed. This was not a real season load.
- Controlled calibration, full/partial tuning and single/batch Monte Carlo input probes: injury NaN preserved, non-injury zero imputation preserved, order/dimensions stable. Model calls were stubbed; no fitting occurred.
- Offline guard confirmed no live DB/network/model-load/model-fit attempts in these checks. Scoped diff check passed.

Separate real local read-only smoke exercised public injury-row and quantile-input helpers with game 0022600160 and fit-season game 0022400875. It proved unknown handling without crashes, not populated historical injury signal. No supported report-clock mapping was supplied; current-season observations lacked usable captured source records and the fit sample lacked a real cutoff.

## Remaining actual limitations

- Historical injuries are not established as usable predictive signal. Current unsupported evidence stays missing.
- Intended fitting seasons still have zero games satisfying the existing exact linked-props predicate in the inspected local population. Resolve physical-date source coverage and correct date-coherent linkage before declaring the requested run ready; do not silently remove the fitting rule.
- Configuration-only `--dry-run` reports data readiness unchecked and is not a training acceptance test.
- No training, data repair, archive scan, new index, commit, push or deployment was performed by this repair.

Receipts are in Hermes scratch: `nba_injury_real_local_smoke_receipt.json`, `nba_injury_repair_probe.py`, `nba_injury_imputation_probe.py`, and `nba-raw-season-factcheck-20261105-v2/factcheck_report.md`.

## Subsequent timing MVP

The later [injury timing reconciliation wiki](../understanding/nba-injury-timing-reconciliation.md) records the applied remote/local timing migration and a real official-PDF recovery pilot. Twelve observations for one historical game now pass the public injury consumer, with after-cutoff and missing-cutoff rejection verified. This supersedes the blanket absence of populated historical injury evidence for that pilot only; whole-season coverage, the props fitting blocker and production deployment remain separate. Original repair status above is a dated checkpoint, not current publication state.
