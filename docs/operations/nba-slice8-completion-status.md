# NBA Slice 8 completion lane

Current cross-lane assessment: [training-readiness reconciliation](nba-training-readiness-reconciliation-2026-11-03.md). Recovery completion, policy approval, real-source acceptance and the training launch route are distinct gates.

## Local integration — November 3, 2026

Chase authorized committing and merging all outstanding Slice 8 work. Consumer implementation/docs were committed as `8a1b21f2` and merged into main as `df87cd3a`. Remaining completion-decision/family-proposal docs were committed as `72345fb3` and merged as `0d11cca3`. Earlier historical-policy and S1 commits were already ancestors of main. This supersedes the uncommitted/unmerged status in historical checkpoints below; it does not grant data acceptance or approve the proposed family policies.

All incoming consumer bytes matched the reviewed branch in the merge index. Actual main-checkout imports were verified; the same four existing scoped suites passed **55 tests** with DB/network/model-load guards and zero external calls. Main's unrelated position/opponent-repair work was excluded. No push, deployment, data job or training launch was authorized or performed.

Remaining work is not merely reading old models: approve cohort/split/family/source semantics, verify an exact real artifact and dataset with explicit acquisition bounds, then freeze/dry-run the reproducible baseline for Chase to launch. The original full pre-acquisition player/resource caps remain a possible bounded code task; reader batching is not that guarantee. Existing models are reference/attachment candidates, not automatic evidence of historical validity.

## 8D code integration closeout — November 3, 2026

**Approved operator-wiring scope complete; original real-data acceptance remains owner-run.** The accepted 8B/8C source was preserved. Coordinator implemented directly, with no additional agents, new unit tests, framework, production policy/configuration, live queries or long jobs.

### Delivered

- Shared DB/model-free attachment helper: `src/backtesting/nba/historical_execution.py`. Explicit policy JSON and optional metadata-sidecar JSON are validated, frozen and rechecked; local-only targeting, exact manifested artifact/calibration boundary and declared evaluation windows are enforced.
- Replay CLI and sweep CLI/config/runtime now thread those inputs through actual native source/feature/replay owners. Sweep uses historical feature kwargs and 8C's effective feature-population prefetch. Missing required date/feature receipts fail closed; no per-cell reacquisition.
- Historical sweep manifest/plan v2 binds attachment, historical source hashes and a complete quote-occurrence ledger. The strict auditor accepts explicit v1/v2 branches, validates ledger accounting/hash/input binding and retains all existing PASS/FAIL/BLOCKED semantics. Policy attachment is not authentication or DB-freshness proof.
- Attach-mode lifecycle schema/argv/stage identities carry frozen historical inputs. Same-path file drift refuses stale reuse and withdraws old success. Historical training is explicitly unsupported; default nonhistorical behavior remains available.
- Historical dataclasses/enums are projected to inert JSON at the sweep evidence boundary. This repaired a reproduced shared-input serialization failure without changing 8B or numerical owners.

### Verified results

- **55 existing tests passed**: `test_run_backtest.py`, `test_backtest_harness.py`, `test_nba_line_sources.py`, `test_run_sweep.py`. The last is legacy compatibility coverage, not the new sweep's proof.
- One-date/one-stat/raw-flat controlled chain passed actual replay CLI plus actual sweep CLI -> native reader/assembler/harness -> exporter -> evidence audit -> lifecycle report-only decision. External DB reads and model loading/prediction were controlled; the changed orchestration and source consumers were not replaced. The actual manifest producer created an explicitly dummy artifact; no trained artifact was deserialized.
- Replay/sweep dry-run blocked engine/model entrypoints and created no output directories. Metadata-only, malformed policy, remote historical target and historical-training configuration were rejected.
- Rejection ledger included a valid pair and two rejected late outcomes. Verified resume launched no children; metadata file drift invalidated success; corrupted ledger bytes failed audit; requested historical readiness remained BLOCKED.
- A saved v1 producer bundle still passed the actual auditor. Scoped Ruff, compilation and whitespace passed; 8B/8C file fingerprints and protected sweep numerical/identity ASTs stayed unchanged.
- All guarded execution reported zero external connection/model-load attempts. No real SQL/schema/source coverage, profitability or production acceptance follows from these controlled checks.

Scope is one new helper plus eight existing source files: replay CLI; sweep CLI/config/runner; evidence auditor; lifecycle config/adapters/runner. Source remains uncommitted, unmerged and unpushed in the current worktree. Main's data-repair work was not touched.

Operator documentation: [NBA S8 operator workflow](../understanding/nba-s8-operator-workflow.md). Existing lifecycle dry-run still writes its isolated planning files; only replay/sweep dry-run are zero-output. No new standalone verifier was built.

Disposable validation: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-s8d/acceptance.py`, `receipt.json`, `final-source-hashes.json`, `implementation.diff`. Final controlled receipt root: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-s8d/check-391acc5a788c497ea1fc4b509d6a6ff7`. Scratch can expire; outcomes and limitations are preserved here.

Remaining: owner-approved real artifact/data and true pre-acquisition bounds for D2/D3 (reader batching and quote caps are not global player/resource caps), real lifecycle acceptance, then 8E frozen configuration and owner launch. Do not label the full original 8D real-data workflow or Slice 8 complete from this code delivery.

## 8C closeout — November 3, 2026

**8C code mechanics complete within the owner-approved lean scope.** Chase accepts 8B as the prerequisite and owns data validity. This checkpoint supersedes older 8C pending/native-integration wording below. No live-data, PostgreSQL/schema, model or independent-review acceptance is claimed.

The existing adapter/native-reader/harness path was retained. Only `src/backtesting/backtest_harness.py` changed during closeout:

- Reproduced direct success versus prefetch game-cap failure when a configured context was outside the replay feature population. Historical `run()` now supplies prefetched features to quote prefetch, which applies the same minutes filter as direct replay before selecting game IDs. Standalone prefetch without that optional argument still requests the full configured context population.
- A missing historical prefetched date now raises `HistoricalReplayBlockedError` instead of becoming an empty quote frame. A present empty frame remains a legitimate no-eligible-quote result.
- No 8B source, pure adapter, replay owner, default legacy path or numerical owner was changed. Fingerprint comparison covered 25 source files; only the harness changed. Protected probability/combo/selection/edge AST checks passed.

### Actual closeout verification

- Existing selected suites: **21 passed** (`test_backtest_harness.py`, `test_run_backtest.py`, `test_nba_line_sources.py`). This is a new narrow closeout result, not a rerun of the older 135/216-test receipts.
- Reused actual-harness probe: direct/prefetch pair, ledger, prediction and edge parity; empirical probabilities; metadata retention; sanitized query-error propagation; protected AST parity passed.
- Reused native-reader probe: actual reader -> public training/date/range -> harness executed successfully with controlled external DB/model boundaries. No live source validation.
- One bounded regression probe: excluded/minutes-filtered game parity, public `run()` and `to_csv()` execution, missing-prefetch-date blocking, strict-cutoff rejection/ledger parity, candidate-cap propagation and exact Decimal/source-ID serialization passed.
- Driver/network/model-load guards reported zero external attempts. Scoped Ruff, py_compile and whitespace checks passed. No permanent tests/fixtures or dependencies were added.

Validation used `C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe` and the disposable guarded runner `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-s8c-closeout/run_offline.py`, invoked with `pytest`, `subset_probe.py`, or `nba-s8-native-path-probe.py` (absolute scratch paths). `subset_probe.py` reuses `nba-s8-harness-acceptance-probe.py`; it does not replace project owners. Scratch can expire; this record preserves the decisive result and boundaries.

Final harness SHA-256: `c6083b0f8df491bdd46931b7c86c62e8f4f1ccfdeda9dcea13753f7e48af8b7e`.

Source remains uncommitted/unmerged/unpushed in `nba-slice8-consumers`, base `b24a69f3`. No changes were made to main's data-repair work. CLI/artifact-policy attachment, lifecycle/real-data acceptance (8D) and frozen baseline/owner launch (8E) remain outside this closeout. No additional subagents or review fan-out were used for implementation.

## Current checkpoint — native minimal path

Session wrap-up is saved in [the pending handoff](handoff-pending-nba-s8-native-sources-2026-11-03.md). Canonical remote promotion/sync is blocked by Tailscale SSH reauthentication; no GBrain publication is claimed. Main advanced concurrently to `04cdb08a`; this implementation remains uncommitted on base `b24a69f3` in its isolated worktree.

This checkpoint supersedes the earlier implementation history below. Chase assigned data repair to himself and requested direct coordinator implementation with only the two localized review fixes delegated. Those fixes are released and incorporated. No additional writer/reviewer fan-out was launched.

Normal FeatureStore training/date/range now use `PostgresHistoricalSources` and the shared assembler. The unavailable-adapter gate is removed; real DB values and explicit source metadata feed the existing typed adapters. The same source reader supplies native raw quotes and contexts/routes to direct/prefetch replay. A populated normal-path execution passed with controlled DB/model boundaries, including an OUT player with no target-game box score and a precise missing-metadata failure. Existing affected suites: 135 passed; scoped Ruff/whitespace passed; protected math AST parity passed.

The minimal source architecture is implemented and exercised, not live-data accepted or independently reapproved after these changes. See [exact source contract](nba-s8-native-source-contract.md). No source repair, DB/network execution, long model job or Git publication was performed. CLI/artifact-policy attachment, real-data/lifecycle acceptance, frozen owner configuration and owner launch remain open. All outputs remain non-decision-grade.

## Scope and owner authority

Chase renewed the request to complete all remaining Slice 8 work after S1 integration. This authorizes implementation and bounded verification, not invented domain policy/source evidence, long model jobs, source repairs, deployment or Git publication.

Worktree: `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-slice8-consumers`
Base: `b24a69f36dc7f7d51aef3a78381af209e6a9530a` (reviewed S1 integrated).

Confirmed policy: T-60, strict source/injury/commence blocking, no diagnostic fallback, target/rolling thresholds 5/8. Exact family/cold-start/transition choices and cohort/splits remain explicit caller inputs awaiting project freeze; the earlier family approval form timed out. Parameterized implementation does not manufacture those approvals.

## Sequence

- 8B: actual shared historical feature/injury consumer integration first; preserve legacy/default production paths and accepted S1 abort behavior.
- 8C: shared direct/prefetch quote acquisition and eligibility after 8B interfaces are stable; existing primitive-only discovery may run concurrently.
- 8D: bounded verifier and lifecycle threading using real owners; distinguish controlled execution from real source/model acceptance.
- 8E: freeze/dry-run exact configuration only with actual approved windows/artifact/readiness; Chase launches the long baseline. Missing prerequisites are explicit blockers, not passing placeholders.

[Executable 8B scope](../../.hermes/plans/nba-slice8b-consumer-implementation.md).

An independent 8C section now implements only the new quote adapter and its wiki against already-integrated immutable policy/observation/quote APIs. Its [exact scope](../../.hermes/plans/nba-slice8c-quote-adapter.md) excludes the harness, unfinished 8B APIs and feature files. Harness integration remains sequential after 8B review; no lifecycle consumer is launched against unfinished interfaces.

## Established baseline and worker route

Coordinator independently ran all eight existing NBA feature suites on the new base: 118 passed, one existing warning. Dotenv disabled, DB URL environment variables removed, network guarded. No tests or fixtures edited.

Codex Spark availability canary failed deterministically: the configured `gpt-5.3-codex-spark` is not supported by the current ChatGPT account. No implementation was attempted with that model. An explicit `gpt-6.1-sol-900k` / `openai-codex` / high one-shot canary returned `WORKER_READY`; that per-launch override is used for the 8B writer. No global or other-profile config is changed.

## Current source/artifact acceptance limits

Prior remote SQL metadata checks showed no explicit ingestion-route column in current props/game-line tables and no report-edition/completeness marker in the injury table. That alone does not rule out recoverable external producer receipts; a bounded read-only producer/receipt audit is underway. Never infer route authenticity from timestamps alone or absence of an injury row as healthy.

The bounded [producer audit](nba-s8-producer-evidence-audit.md) is now complete: inspected historical props progress tracks attempts including empty/422 responses, not source-row response receipts; inspected game-line producer retains no payload receipt; injury conflict handling collapses editions and any represented date may be treated as complete. External retained evidence remains unexamined rather than ruled out. Current sources cannot be promoted solely from these producer records.

Coordinator inspected only immediate NBA artifact directories under main's `src/models/artifacts`, without recursion or deserialization. The only directories returned were `production` and `production_playoffs`; each has `run_config.json` and neither has `artifact_manifest.json`. These paths are not a valid exact nonlegacy artifact attachment for 8D. No artifacts are modified or fabricated; other external artifact locations remain unexamined.

## Safety and integration state

### Interrupted implementation checkpoint

Both implementation subprocesses exited 130 before final delivery/acceptance. The quote worker recorded a provider stream timeout; no successful completion is inferred from saved files. Partial source exists: request/facade/mode threading, shared historical loader and line/injury helpers, plus the independent quote adapter. The quote wiki and final 8B execution-evidence document were not delivered.

Coordinator recovery checks on the saved bytes: scoped Ruff and compilation passed; all 135 selected existing feature/harness/backtest tests passed under an offline network guard with dotenv disabled. These prove legacy compatibility, not the new historical paths. Read-only independent reviews of both partial sections are underway; no repair writer is changing their review targets. Do not call these bytes accepted or Slice 8 complete.

### Coordinator repairs and bounded execution

Initial independent reviews rejected both partial sections. Coordinator repaired quote policy-reference binding, pandas-null handling, snapshot/source conflicts and deterministic metadata; one actual public-constructor probe passed coherent pairing, conflict/duplicate/cutoff and container parity checks. Quote output explicitly remains non-decision-grade with unverified upstream authority. Independent post-repair quote review is pending.

The delegated 8B repair failed with provider HTTP 400 before editing. Coordinator applied bounded repairs directly: normal historical calls now block before acquisition; controlled caller evidence requires an explicit opt-in and cannot become real source readiness. Exact roster membership, market-route guards, legacy-default guards, policy-owned target construction, source/context conflicts and per-date accounting were added. The controlled actual-consumer probe passed training/date/range parity with 74 ordered features and safety checks. All 135 selected existing tests, scoped Ruff, final compilation and whitespace passed after these repairs. A separate short read-only review is running; these results do not establish full native source integration or real acceptance.

### Real recovery candidate

Exactly two publisher-hosted April 12 injury-report PDFs were downloaded, hashed and their actual headers inspected. See [source recovery evidence](nba-s8-source-recovery-2026-11-03.md). This corrects any impression that the collapsed RapidAPI rows prove external editions unavailable. The captured bytes are real raw source candidates, not parsed/mapped/ingested or accepted historical features. No source-policy approval or full-window coverage follows from two files.

### Controlled direct/prefetch replay integration

The independently accepted pure quote adapter's current bytes were verified against the reviewer SHA-256. A [narrow replay seam](../../.hermes/plans/nba-slice8c-replay-seam.md) now wires one bounded source owner into the harness's explicit historical direct/prefetch branches, preserving default legacy paths and protected numerical owners. Native source rows remain explicitly unknown/blocked. Controlled route/market/context/cap checks, side clock/pair metadata and structured ledger export are implemented; every controlled result remains non-decision-grade.

The coordinator's short actual-FeatureStore/harness controlled probe passed direct/prefetch pair/ledger and prediction/edge parity, metadata retention, empirical-probability behavior, sanitized query failure propagation and protected AST equality. Final selected existing suite run: 216 passed, one existing warning; scoped Ruff/compile/whitespace passed. An independent read-only replay-seam review is pending. This is not full native 8C acceptance, and no CLI/lifecycle or real-data/baseline gate is claimed complete.

No runtime source repairs, DB writes, archive queries, sync/backfills, model loading/training, broad sweeps or baseline launch have occurred in this lane. Main's unrelated untracked sync-readiness document and earlier completion worktree records remain untouched. Commit/merge/push is not inferred from implementation authorization; the prior Git permission applied to S1 only.
