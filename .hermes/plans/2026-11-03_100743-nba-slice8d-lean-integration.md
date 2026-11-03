# NBA Slice 8D — Lean Operator Wiring Plan

**Status:** Proposed scope; planning only. Source implementation awaits approval.
**Goal:** Make the accepted historical feature/replay path reachable through the existing replay, shared-input sweep and attach-mode lifecycle commands, with honest policy/source identity and one controlled end-to-end acceptance.
**Architecture:** Reuse the existing HistoricalPolicy, PostgresHistoricalSources, artifact validator, BacktestHarness, sweep evidence format, auditor and lifecycle runner. Add attachment and identity plumbing, not a second verifier/simulator. Preserve the existing data-validity and report-only posture.
**Tech stack:** Existing Python, JSON, argparse and lifecycle Pydantic/YAML; no new dependencies.

## Scope meaning

Original 8D combines operator tooling with real-data acceptance (D0–D4). This proposal completes the **8D code-integration portion**, not the owner-run real-data gates. Chase continues to own data validity. Do not label controlled execution, metadata hashes or an attached policy as historical acceptance.

8B is accepted as a prerequisite; 8C code mechanics are closed. Do not reopen them except for a demonstrated indispensable boundary defect and an explicit scope decision.

## Verified starting point

- Actual accepted consumer source is uncommitted in `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-slice8-consumers`, HEAD `b24a69f3`, not main.
- Historical consumers already accept `HistoricalPolicy` and `FeatureStore(engine, historical_sources=PostgresHistoricalSources(...))`. Optional metadata sidecars are JSON; they assert source facts but do not replace DB feature values.
- `historical_policy.py` already supplies `policy_from_json`, `policy_payload` and `require_execution_ready`.
- `run_backtest.py` validates artifacts and supports DB/model-free planning, but does not attach historical inputs to its runtime constructors.
- `run_nba_sweep.py` -> `SweepConfig` -> `bootstrap_runtime` also lacks historical attachment. Crucially, `acquire_shared_inputs` currently calls the feature range loader without historical kwargs and prefetch without its effective feature population. Adding flags alone would still run the wrong acquisition path.
- `sweep_config.py` already hashes `source_policy` into acquisition identity but fills it with unknowns.
- `evidence.py` explicitly requires the exact unknown source-policy object (lines 548–549 at inspection). It also has strict fixed-config/acquisition schemas. Therefore auditor compatibility is indispensable; silently changing producer metadata alone will break auditing.
- Lifecycle already has validate -> attach/train -> artifact_identity -> sweep -> audit -> ranker -> decision stages, immutable resolved config, predecessor/output identity and reuse invalidation. Reuse these rather than inventing another state machine.
- No existing `scripts/verify_nba_historical_readiness.py` was found. Creating that proposed umbrella wrapper is deferred: the existing lifecycle can be the durable operator command.

## Implementation ownership and subagents

- Two bounded file-only explorers were used for planning: one traced replay/sweep, one lifecycle. Their key claims were checked against source, including the missing sweep kwargs and strict audit placeholder.
- Coordinator implements sequentially and owns final scoped verification. No writer/reviewer fan-out by default, matching the latest accepted workflow.
- Only an explicitly approved localized repair may be delegated if a concrete defect can be isolated to a disjoint file/method. No SQL/data/model agents.
- No new parallel source writer while the same files are being inspected or validated.

## Narrow deliverable

An explicit historical configuration can travel through:

`replay CLI or lifecycle attach config -> sweep plan -> configured native FeatureStore/harness -> one shared acquisition -> existing sweep exporter -> existing evidence audit -> report-only lifecycle decision`

The planned controlled acceptance uses one date, one stat, one raw/no-BL flat-stake cell and a tiny existing producer-built artifact fixture. No training, BL grid, ranker implementation or live DB replay.

## Step 1 — Shared input attachment

Add one small DB/model-free attachment helper, proposed new file:

- `src/backtesting/nba/historical_execution.py`

Responsibilities only:

1. Read explicit policy JSON through the existing parser; enforce execution readiness using the existing canonical family contract.
2. Resolve optional source-metadata JSON and capture its file/content identity. Embedded metadata remains an explicitly represented mode when no sidecar is supplied.
3. Expose a detached canonical receipt and validated policy for both CLI paths; no second policy schema, inferred authority, arbitrary defaults or environment secrets.
4. Recheck attachments against the frozen receipt immediately before acquisition and before publishing completion. A same-path file replacement must not silently change the run.
5. Reuse exact artifact validation. Evaluation attachment does not retroactively certify or rewrite the artifact's training policy, training windows or data provenance. Reject genuine feature/date incompatibility through current owners; report unverified historical provenance honestly.

Proposed user-facing options (not existing commands yet): `--historical-policy <json>` and `--historical-source-metadata <json>`. Reject metadata without policy. Preserve legacy behavior when both are absent. Require explicit local target for this new historical operator route. Keep existing diagnostic acknowledgement; do not expose controlled-evidence flags as a normal operator bypass.

No production policy file or made-up cohort/window values will be created.

## Step 2 — Replay and shared-input sweep wiring

Modify:

- `src/backtesting/run_backtest.py`
- `src/backtesting/nba/run_nba_sweep.py`
- `src/backtesting/nba/sweep_config.py`
- `src/backtesting/nba/sweep_runner.py`

Contracts:

- Preflight validates explicit historical attachments before engine/model construction; dry-run creates no output directories and performs no DB access/model deserialization.
- Configure `PostgresHistoricalSources` once, give it to FeatureStore, and pass the same policy to the acquisition harness.
- `acquire_shared_inputs` must pass historical feature kwargs and the returned feature mapping into historical quote prefetch, retaining the 8C effective-game-population fix.
- Preserve feature-frame readiness attrs, missing-source errors and complete date accounting. A missing required historical date cannot be converted into generic no_features/no_quotes success.
- Keep one acquisition shared by all cells; per-cell evaluation must not reacquire data or silently change policy.
- Bind policy payload/content identity, explicit metadata mode/content identity and historical implementation source hashes into existing request/acquisition evidence. File identity is not DB freshness: leave unknown watermark/coverage unknown.
- Carry quote rejection ledgers through the existing shared-input/export evidence, not just accepted pairs. If the envelope needs an extension, include it in integrity/accounting rather than placing unsigned evidence beside the bundle.
- Keep current mathematical owners, simulations, RNG behavior and output row-selection logic untouched.

## Step 3 — Evidence compatibility and lifecycle threading

Modify narrowly:

- `src/backtesting/nba/evidence.py`: historical attachment/ledger envelope validation only.
- `src/models/nba/lifecycle/config.py`
- `src/models/nba/lifecycle/adapters.py`
- `src/models/nba/lifecycle/runner.py`

Evidence contract:

- Retain validation of the legacy unknown-policy envelope.
- Define an explicit, version-discriminated historical binding extension if new envelope keys are required. Never relax exact-key checks into accepting arbitrary metadata.
- Auditor checks saved canonical policy/content IDs, metadata receipt, acquisition commitments and ledger integrity without resolving source paths or connecting to a database.
- Do not change existing ROI, quote-timing, matching, coverage or audit PASS/FAIL/BLOCKED semantics merely to make a controlled run green.
- A completed report containing FAIL/BLOCKED remains negative evidence.

Lifecycle contract:

- Optional historical configuration is included in the immutable resolved snapshot, with file contents/identities rather than filenames alone.
- Forward the resolved inputs through the actual sweep parser; attach mode only for this new historical route. Reject unsupported historical training rather than launching a trainer that ignores the policy.
- Bind current attachment identities and implementation hashes into stage inputs/verification. Policy/sidecar/artifact drift must refuse stale reuse or invalidate affected downstream stages through existing behavior; no new resume engine.
- Preserve pure recorded-state `status`; no model/DB operations to show status. Existing config-conflict refusal is an acceptable safe result, not a reason to rebuild in place.
- Keep decision report-only and `decision_grade=False`. Report policy attachment separately from source authenticity; do not relabel audit/lifecycle historical readiness as verified.

Do not modify lifecycle decision math, train execution, ranker or artifact-manifest schema.

## Step 4 — One lean validation pass

1. Existing affected suites, verified filenames: `tests/test_run_backtest.py`, `tests/test_backtest_harness.py`, `tests/test_nba_line_sources.py`, `tests/test_run_sweep.py`. Inspect relevance of the legacy sweep suite before using it as evidence for the new NBA sweep. No NBA lifecycle test filenames surfaced; do not claim MLB lifecycle tests validate NBA behavior.
2. Reuse the established offline runner/fixture producers. Install dotenv, network, database-driver and model-load/training guards before project imports. Do not rely on environment-variable removal alone.
3. Parse real generated lifecycle argv with the real replay/sweep parsers; malformed/missing policy or metadata-without-policy must fail before bootstrap. Verify dry-run creates no outputs.
4. Run one tiny populated public path through CLI/sweep/native reader/harness/export/audit/lifecycle wiring. Only external DB results and model loading/prediction boundaries may be controlled; do not mock away the changed orchestration or fabricate final producer output files.
5. Check policy/sidecar identity propagation, retained rejection ledger and effective game filtering. Run one file-drift/reuse check and one invalid-evidence check; no exhaustive mutation matrix.
6. Scoped Ruff/compile/whitespace and protected numerical diff review. One consolidated repair pass, then rerun only affected checks.

Use one-date/one-stat/one-cell controlled inputs for bounded execution. Existing reader batching is NOT a total player cap and quote caps are NOT feature-source acquisition caps. This tranche does not implement or claim the original proposed verifier's full pre-acquisition max-player/resource controls. Before live D2/D3 execution, those bounds must be established or separately implemented; do not add pretend caps that merely truncate after broad reads.

No permanent tests/fixtures, broad source audit, live query, expensive scratch framework, real model load or long run is authorized by this proposed scope.

## Stop conditions / effort budget

This is larger than 8C: it crosses strict producer/auditor/lifecycle contracts. Target one implementation pass plus one repair/validation pass, approximately 60–90 minutes if compatibility stays localized; this is an estimate, not a guarantee.

Stop and present the exact narrower choice if any of these becomes necessary:

- redesigning 8B acquisition or inventing a new data/provenance policy;
- modifying numerical/audit semantics to force success;
- broad artifact schema migration;
- new general-purpose verifier/lifecycle framework;
- additional review cycles without a concrete reproduced defect.

Do not silently omit a named seam to meet the time target. Report implemented versus blocked stages explicitly.

## Completion definition

8D **code integration** is complete when all public attachment paths reach the accepted historical reader/replay, the real producer and auditor agree on the evidence contract, lifecycle cannot reuse stale attachments, and the controlled chain passes with honest negative outcomes preserved.

Original 8D **real-data acceptance** remains owner-run: approved source bounds, exact real artifact, one-day smoke, broader bounded replay and real lifecycle evidence. These are not waived by code completion. 8E remains frozen baseline configuration and owner launch after applicable acceptance.

## Protected / excluded

- Existing dirty 8B/C sources, source readers and harness numerical code; main's position/opponent/sync work.
- Data repair/validity, SQL audits, DB schema changes, scraping/backfills, automatic synchronization.
- Production policy freeze, new model artifacts, retraining, promotion, baseline launch.
- Engineering OS and GBrain repair.
- Commit, merge, push or deployment without separate authorization.

## Documentation / working base

After approval, continue in the current consumer worktree after collision/status checks and baseline fingerprints. Do not transplant uncommitted dependencies or create an unrequested commit to obtain a clean base.

Documentation allowlist for implementation:

- `docs/operations/nba-slice8-completion-status.md`
- New `docs/understanding/nba-s8-operator-workflow.md`

Document exact implemented CLI/YAML fields and separate planned, code-verified, real-data-verified and launch states. Save executable commands only after parser verification. Preserve the original umbrella plan as historical scope rather than overwriting its owner-run acceptance gates.
