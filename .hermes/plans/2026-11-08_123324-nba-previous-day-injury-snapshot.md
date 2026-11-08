# Previous-Day NBA Injury Snapshot Implementation Scope

> **For Hermes:** After approval, execute bounded slices through the GameFlow implementation-worker lane, with scoped diff review and real public-path verification. This is scoping only: do not launch training, captures, backfills, migrations, deployments or commits from this document.

**Goal:** Add an opt-in `previous_day_v1` injury-feature variant that uses the same explicitly defined information policy in fitting, calibration, ordinary replay and live inference, without delaying or changing the delivered no-injury baseline.

**Architecture:** Reuse existing `rapidapi_injuries.source_observations` and capture checkpoints. Introduce one small policy/selector module, feed its selected observations into shared injury-feature aggregation, and bind that policy to the saved model artifact. Preserve current behavior for existing artifacts; reject unsupported policy/mode combinations rather than silently falling back.

**Tech stack:** Existing Python/pandas/PostgreSQL feature stack and capture CLI. No new provider, PDF archive, database table or generic ingestion framework in the initial scope.

**Scope date:** November 8, 2026. Source inspection is against the dirty main checkout, not a stable merged API. Relevant paths must be rechecked after concurrent owners finish.

## 1. Decisions and boundaries

### Owner direction

- Scope a previous-day injury-feature experiment.
- Keep the injury-excluded first baseline moving independently.
- Do not require complete reconstruction of every historical report edition before this experiment.

### Recommended policy to approve with implementation

For an authoritative target game date `D`:

1. Require nested RapidAPI `requested_date == D-1` and raw payload `date == D`. The two dates answer different questions.
2. Use the authoritative NBA game date, not the UTC date portion of tipoff. Freeze the injury information set at the start of `D` in `America/New_York`, with DST handled by the timezone database. This is a calendar-day boundary, not subtraction of an arbitrary 24 hours from tipoff.
3. Ignore `reportTime` as a publication clock. Preserve it as raw evidence; do not feed a fabricated midnight timestamp into the existing publication parser.
4. Do not admit game-day updates, even if they arrive before T-60. This variant deliberately uses older information, not the latest T-60 report.
5. Validate that the freeze boundary is no later than the actual decision cutoff. A forecast made before the freeze cannot claim the full previous-day snapshot; do not silently borrow later information.
6. No fallback to the current day's report, to a current roster's inferred injury status, or to eventual participation. Missing/ambiguous evidence stays unknown.
7. Freeze only injury-report selection. Existing pregame rules for rolling minutes, usage, position, quotes and all non-injury features remain unchanged.

### Historical versus live evidence

These must be distinguishable in provenance, not disguised as identical timestamps:

- **Historical exploratory mode:** select the provider's archived response requested for `D-1` containing game `D`. Explicitly record the sample-backed assumption that the dated response represents that publication day's information. Name it, for example, `archive_day_assumption_v1`; do not claim we historically downloaded it then or that every intraday edition is reconstructed.
- **Live mode:** select an actual retained capture of that request day retrieved before the freeze boundary. A refetch on `D` or later must not be backdated into a supposedly live `D-1` capture.
- Keep actual retrieval timestamps unchanged. A source-day availability bound is not an exact publication timestamp.
- Historical datasets must pin their selected capture IDs in a receipt/manifest. Repeated historical API fetches or future DB updates must not silently change an existing artifact's training inputs.
- For multiple live captures, choose the latest valid complete capture before the freeze, not per-player latest rows from mixed captures. For archived captures, collapse identical bodies; where materially different captures exist, pin a reviewed choice or mark ambiguity rather than assuming later retrieval means later historical edition.

This is a practical, explicit source assumption for an experiment, supported by the prior small comparisons. It is not a claim of promotion-grade historical source certification. A short forward frozen-versus-archived comparison tests that assumption before operational use.

## 2. The feature contract: what the model actually receives

The implementation must produce useful injury features, not just return more rows while leaving every aggregate NaN.

Use existing feature names/families from `src/models/features/nba/contracts.py`, with versioned semantics in the artifact:

- Player Questionable/Probable flags from an explicit selected status. An explicitly listed other supported status can give a zero flag; an unlisted player remains unknown in this first version. Do not invent Healthy rows.
- Team/opponent/same-position features describe **reported Out players in the accepted previous-day provider snapshot**, with the existing pregame contribution weights. They are not a promise to enumerate every true injury or all roster members.
- A represented, valid team section with explicit statuses but no Out rows can have zero **reported-Out** count/sum. A missing team section, partial persisted capture or invalid relevant identity is not the same thing and must remain unknown for affected features.
- Unknown weighted contributions must not silently become zero. Preserve a known count separately from unknown weighted sums where possible.
- Expose capture presence, selected listed statuses, unresolved identities and per-feature missingness in diagnostics. Do not call one listed player proof of complete medical coverage.
- Do not drop games solely because the previous-day report is absent, including back-to-backs. Keep the existing full eligible cohort and honest missing values.

This is an explicit observed-report interpretation for the new experimental variant. It does not weaken the existing strict full-roster historical interpreter or assert healthy-by-omission. Approval of this scope should include the interpretation above.

## 3. Verified existing seams and gaps

| Area | Observed implementation | Needed change |
|---|---|---|
| Existing baseline | `.hermes/plans/nba-first-no-injury-training.md` records delivered `--exclude-injury-features`, fixed incumbent minutes/pts/reb/ast config and offline checks | Preserve it; do not add gates to that launch |
| Training selection | `training_loader.py:72-101` calls `load_training_injury_rows`, then deliberately empties team aggregation inputs | Dispatch the opt-in policy and populate supported observed-report aggregates instead of inheriting the blanket-empty branch |
| Current selector | `injury_context.py:524-615` depends on publication clocks and selects latest accepted team timestamp | Add separate day-snapshot selection; do not map unknown labels to invented clocks |
| FeatureStore | `feature_store.py:52-145` has no injury policy; constructs separate training/date/range/player requests | Thread one resolved policy through all modes |
| Date and range | `date_batch_loader.py` and `date_range_loader.py` use direct same-day injury SQL with zero defaults | Bypass/replace only the injury portion for the new policy, retaining non-injury SQL and legacy defaults |
| Single-player mode | `inference_loader.py` calls `FeatureStore._get_injury_context` | Route the new variant through the same selector/aggregator as bulk modes |
| Artifact reload | `quantile_trainer.py:506-553` saves/restores feature lists in `feature_config.joblib`, but not an injury policy | Persist, validate and bind the policy when loading the model |
| Runtime | `inference_job.py:159-165` creates a default store and then calls `load_all`; `sweep_runner.py:285-288` follows the same shape | A central load-time binding/compatibility check can cover these callers; verify both actual paths |
| Storage | `nba_injury_db.py:59-146` retains request date, raw payload date, capture hash, actual retrieval time and multiple changed captures | Reuse it; no initial table migration |
| Capture resume | `rapidapi_injury_backfill.py:315-324` refetches today's request date, but skips completed past dates by default | Capture on the actual preceding day; do not mistake a skipped/refetched past date for a timely live snapshot |

Research evidence: `docs/operations/nba-rapidapi-clock-validation-2026-11-08.md`. The retained sample includes 197 previous-day records across six target dates, but only two previous-day subsets were compared to official reports. This is not whole-cohort coverage certification.

## 4. Implementation slices

### Slice A — Shared policy, selector and feature assembly

**New proposed file:** `src/models/features/nba/injury_snapshot_policy.py`.

**Modify:** `src/models/features/nba/injury_context.py` and, as required for the explicit request contract, `src/models/features/nba/requests.py`.

1. Define a small immutable policy/config with policy ID/version, timezone, prior-day offset, live/archive evidence mode and explicit unknown semantics. No provider-pluggable framework.
2. Implement pure selection over retained raw observations and authoritative game/team contexts. Retain capture IDs/hashes, original dates and rejection reasons.
3. Query/index by payload game date `D`, then filter nested request date `D-1`. Do not incorrectly change the table predicate to `report_date=D-1`: the current table column holds the applicable game date.
4. Reconstruct a coherent selected capture for the target context; do not select each player's latest table ID. Use existing successful capture checkpoints/counts/ordinals where available to detect partial persistence. Do not pretend a partial date-filtered subset is the full provider body.
5. Keep raw/flat identity consistency, supported-status validation and known team/player mapping. Preserve malformed-name/status cases as diagnostics; this slice does not invent aliases or rescrape data.
6. Return selected statuses plus observed-report aggregates, unknown masks and accounting. Reuse existing rolling/position weighting helpers and target-game historical team policy.
7. Reject archive/live mode confusion and freeze-after-decision contexts. Keep legacy selection unchanged.

**Acceptance:** Controlled tests with retained real records prove previous-day selection, same-day rejection, coherent capture selection and unknown preservation. Huerter's May 4 Questionable record must win for the May 5 target; the May 5 Out record must never enter this variant's features. Status agreement does not by itself certify completeness.

### Slice B — Full training/calibration and artifact identity

**Modify:** `src/models/train_pipeline.py`, `src/models/feature_store.py`, `src/models/features/nba/requests.py`, `src/models/features/nba/training_loader.py`, `src/models/quantile_trainer.py`.

**Proposed CLI:** `--injury-snapshot-policy previous-day` (new, not currently runnable). The default retains current behavior. Saved identity is `previous_day_v1`, not a mutable CLI alias alone.

1. Thread the resolved policy from `TrainingOrchestrator.__init__` through `_load_population` into the training request for both fit and calibration.
2. Enable Slice A's observed-report assembly only for this policy. Preserve existing unknown behavior elsewhere and avoid any global feature-list mutation.
3. Reject `previous-day` combined with `--exclude-injury-features`; do not silently ignore either. Initially reject unsupported partial-training/calibration-only/strict-historical combinations with a clear error.
4. Save the policy and source-assumption version in run configuration, artifact settings and the serialized feature configuration. Pin selected training capture IDs/digests or their immutable manifest reference. Include new source modules in applicable implementation fingerprints after inspecting the current registry.
5. In `PlayerPropsModelPipeline.load_all`, validate the saved policy before features are loaded and bind it to the supplied FeatureStore, or fail on an explicit conflict. Feature-name equality alone is not enough for compatibility.
6. Use the existing incomplete-directory/finalization path. Older artifacts without this new setting keep their current interpretation; the no-injury artifact continues using its injury-free selected lists.

**Acceptance:** Dry-run records the intended variant; fit and calibration choose the same policy; conflicting modes fail explicitly. Saved/reloaded policy survives the real public artifact boundary with model fitting disabled during verification. The approved no-injury dry-run remains unchanged.

### Slice C — Date/range/single-player inference and ordinary replay parity

**Modify:** `src/models/features/nba/date_batch_loader.py`, `src/models/features/nba/date_range_loader.py`, `src/models/features/nba/inference_loader.py`, and narrow facade/helper wiring in `feature_store.py` / `injury_context.py`.

**Verify callers:** `src/orchestration/inference_job.py`, `src/backtesting/nba/sweep_runner.py`, `src/models/quantile_trainer.py`. Edit caller files only if central policy binding is insufficient and the extra scope is approved.

1. For the new policy, bypass existing raw same-day injury SQL/zero-filling and apply the shared selected snapshot. Do not execute unsafe injury SQL merely to overwrite the values afterward when it can be avoided.
2. Keep non-injury feature acquisition and defaults unchanged. Batch contexts to avoid an injury query per player.
3. Ensure missing injury values survive downstream preparation; do not add a final blanket fillna(0).
4. Assert that feature policy agrees with the exact artifact. Do not let a previous-day model load into a default same-day live store without binding/checking it.
5. Ordinary local replay/date-range features should use the same archived capture policy as training. Preserve existing quote-clean, artifact and chronology gates; this does not grant promotion-grade source status.
6. Explicitly reject `previous_day_v1` with the current strict historical full-roster/local-observed attachment until a separate compatible interpreter is approved. Do not expand this first slice into the entire strict historical framework or produce a false PASS.

**Acceptance:** On the same bounded game/capture inputs, training, calibration, single-date, date-range and single-player paths agree on injury values and unknown masks. Later day-of records do not change the result. Confirm ordinary replay can consume the new artifact without silently reverting to legacy injury SQL.

### Slice D — Pre-boundary live capture and bounded end-to-end validation

**Reuse:** `src/scrapers/rapidapi_injury_backfill.py`, `src/scrapers/nba_injury_db.py`, existing scraper/linker chain.

**Protected until coordinated:** `src/orchestration/scheduler.py`, `src/orchestration/lines_job.py`, and the concurrently modified smoke/training tooling.

1. On calendar day `D-1`, collect the API request for `D-1` and retain rows whose raw game date is `D`. Existing ingestion already supports that distinction.
2. Verify whether the current daily cadence supplies a sufficiently late, successful pre-boundary capture. If not, propose one bounded late-evening injury-only capture with retries ending before the freeze. Confirm the precise schedule with the job owner; do not re-run the full props pipeline for this purpose.
3. Do not implement the misleading shortcut of changing a day-D lines run to request `D-1` with `--no-resume` and calling it a contemporaneous previous-day capture. That is an after-boundary archived fetch, which must remain labelled as such.
4. Enforce shared writer/overlap protection with existing injury collection/linking. Preserve actual retrieval timestamps and existing immutable observations.
5. Freeze the selected capture identity for each target date/context. Runtime feature calculation must select the frozen/pre-boundary capture even if later retries add more source rows.
6. Compare a few actual pre-boundary captures to later queries for the same dated archive. Material status drift is evidence the archive-day assumption needs revision; do not overwrite the frozen live input.
7. Run a bounded real-data feature smoke covering back-to-back, missing prior-day team, changed status, conflicting captures, DST and an early game. Use the existing smoke tool only after its owner hands it off; do not create a duplicate seasonal validator.

**Acceptance:** A genuine capture -> persistence -> selected snapshot -> actual inference-feature path works and is read back. A successful scraper or SQL link count alone is insufficient. No predictions/bets need to be placed to prove feature parity.

## 5. Scope exclusions and protected work

- No changes to the completed injury-free baseline's launch policy or incumbent hyperparameters.
- No automatic training, tuning, broad historical recovery, DB migration, deployment, paper/live betting, commit or push.
- No change to final participation/DNP/settlement policy; no target-game box scores as pregame injury labels.
- No obligation to reconstruct every PDF or obtain 100% report coverage before this explicitly scoped experiment.
- No automatic conversion of previous-day incomplete reports into Healthy/Available rows.
- No source-policy replacement for old artifacts; no hidden change to strict historical attachments.
- Do not overwrite the dirty training/smoke/scheduler work. Re-read the delivered parser, configuration and metadata contracts after integration. If overlap remains, use an isolated worktree and a precisely bounded writer allowlist.

The exact artifact load seam and date/range SQL changes make this a multi-file feature-path change, not a one-line date adjustment. It is still substantially smaller than a new historical injury data platform.

## 6. Validation and completion criteria

Preserve the current no-new/no-modified-unit-tests convention unless Chase changes it. Reuse existing scoped tests and controlled real-record probes. No synthetic probe should be described as live data acceptance.

Required checks:

- Date D-1 versus payload D; reject D, D-2, wrong target game/team and malformed status.
- Freeze calendar boundary across DST; reject a freeze later than the actual decision.
- Same-day Out does not replace previous-day Questionable.
- Multiple captures never produce a mixed per-player snapshot; pinned historical inputs remain stable after later DB additions.
- Missing team/report and invalid weighted contributions remain unknown; represented valid sections produce the documented reported-Out features.
- Injury columns show meaningful variation where source evidence exists, not a new all-NaN or all-zero variant.
- All four feature-store modes and both training roles produce equivalent values from identical evidence.
- Save/reload carries policy identity; older and injury-free artifacts retain prior behavior.
- Unsupported partial/strict modes fail clearly rather than return success with legacy features.
- Approved no-injury dry-run and existing scoped tests still pass; scoped Ruff/compile/diff checks pass.
- Any approved live capture/DB write is verified independently after execution. Broad runs remain Chase-launched.

Completion levels must be stated separately:

1. **Code complete:** policy/selector/feature assembly/artifact binding/mode parity implemented and verified.
2. **Historical experiment ready:** existing previous-day cohort coverage/missingness is measured, the archived-day assumption is explicit, and a bounded actual-input smoke passes. This does not reopen the no-injury baseline gate.
3. **Live feature ready:** an actual timely capture and forward frozen/archived comparison pass.
4. **Model accepted:** owner launches a separate fixed-parameter previous-day artifact and compares it with the no-injury baseline on the same splits/evaluation window. Training, economics and promotion are later gates, not part of this coding scope.

## 7. Recommended order

Approve the policy/observed-report interpretation, then implement A+B together as the first coherent slice; C must finish before claiming a train/live-compatible variant. D's forward scheduling/activation is separately coordinated with the production owner. Do not start a new training run until the intended artifact's code/input gates pass; do not delay the independent no-injury run.

## Relevant prior lessons/invariants

- `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: the field name `reportTime` is not a clock contract; preserve actual dates and source evidence.
- `lessons/cheap-baseline-before-architecture`: reuse the existing capture/storage/feature paths and keep the simple baseline independent.
- Preserve empirical sample probabilities, no global conformal offsets, and no blind Q10 correction. This scope changes injury information policy, not model architecture or calibration.
- SQL remains isolated, writes approved/scoped/read back, and long jobs owner-launched.

## Read first at implementation handoff

- `.hermes/plans/nba-first-no-injury-training.md`
- `docs/operations/nba-rapidapi-clock-validation-2026-11-08.md`
- `docs/operations/nba-injury-baseline-direction-2026-11-08.md`
- The latest scoped diffs for `requests.py`, `training_loader.py`, `train_pipeline.py`, `feature_selection.py` and `scheduler.py`.

No implementation changes were made while writing this scope.
