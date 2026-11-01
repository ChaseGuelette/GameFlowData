# Batch D — pure NBA observation and quote primitives

## Kick off immediately

This slice implements mechanical eligibility and pairing, not historical source policy.

- This chat: Batch D / coding slice 4 implementation and bounded verification complete; perform the owner-approved branch merge while preserving newer planning docs and unrelated dirty files.
- You: pairing/duplicate contract, implementation, branch publication and merge into main approved. Historical policy and consumer activation remain separate gates.
- Other chat: retain ownership of sync/S1/CLI work; no overlapping writer is needed.
- Read: this contract, [rebuild wiki](../understanding/nba-rebuild.md), then [Batch D in the coding plan](nba-coding-readiness-plan-2026-11-01.md#batch-d--policy-ready-shared-quoteobservation-primitives).

## Status and scope

Implemented and coordinator-verified with controlled inputs. Independent review findings were repaired and the coordinator reran their cases against the final source. Chase approved merging branch commit `b2fb86f` into main. No historical-data or consumer-path certification.

Worktree: `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-observation-primitives`.
Branch: `repair/nba-observation-primitives`; base `d4784584b089bd94c6b5f3ce2d87d6a3fc632969`.

Allowed implementation files (new):

- `src/models/features/nba/observation_policy.py`
- `src/models/features/nba/quote_selection.py`

Documentation: this record plus the rebuild wiki and coding-plan status. The latter two were copied from main's uncommitted documents as explicit planning context; they are not inherited from the base commit. During the approved merge, main's newer Batch B status is retained alongside Batch D's additions. Other pre-existing document links remain dependent on separately owned documentation commits. S1/CLI worktrees remain untouched.

## Approved decision record

### Explicit time inputs, not clock policy

The caller supplies aware decision and commence instants and an identified clock interpreter. Decision must precede commence. Interpreted availability and every required cutoff-evidence timestamp must be at/before decision and strictly before commence. UTC-normalized comparisons preserve DST fold semantics. No current clock, implicit timezone, fixed UTC decision time, inferred commence or T-minus default.

The interpreter retains raw source evidence, supplies interpreted availability and identifies its interpretation. Unknown clocks, missing history and ambiguous history remain structured rejections. Unexpected interpreter errors propagate instead of becoming ordinary missing-data outcomes. An injected interpretation is a caller assertion, not certified source truth.

### Atomic pairing

Pair only the same game, player, market including period/variant, bookmaker, line, source namespace and explicit snapshot compatibility key. Both Over and Under must independently qualify. Equal timestamps alone cannot establish compatible observations. Never borrow a side from another group or synthesize its price.

### Duplicates and conflicts

Separate source-record identity from semantic quote identity. Exact semantic duplicates agree on normalized content, prices/formats, clock evidence and interpretation; only source record references may differ. Collapse them and retain every distinct reference plus occurrence accounting.

Different material evidence within the same group/side is a conflict, not an exact duplicate. Reject the entire ambiguous group; never pick by ID, input order, better price or later timestamp. A contradictory reuse of `(source namespace, source record ID)` invalidates all affected groups. Detect conflicts before dropping ineligible candidates so eligibility filtering cannot hide contradictions.

### Pair inventory versus selected pair

Return all eligible complete pairs deterministically. An incomplete later snapshot does not remove an older complete pair, but neither does it authorize an older-snapshot fallback selection. Selection requires an explicit caller rank rule; equal best ranks on distinct pairs are ambiguous. Stable content IDs order evidence and never settle a betting-policy tie.

### Provenance and accounting

Return both sides' prices, raw timestamps, interpreted clocks, source references, interpretation ID, context and stable content IDs. Every input occurrence is selected, eligible-not-selected, duplicate or rejected. Preserve duplicate-collapse/group-rejection evidence. Semantic results are input-order independent; occurrence indices may follow the supplied input order.

## Non-goals and remaining gates

No historical source mapping, stale-age rule, cohort/split policy, injury parser/fallback, source joins, production SQL or consumer integration. No edits to `train_pipeline.py`, `run_backtest.py`, `contracts.py`, package exports or S1 files. No new dependencies or unit tests. No model math/calibration changes, data sync, training or deployment. Branch commit/push and merge were subsequently approved explicitly.

S2 still owns evidence-backed source-clock interpretation and historical policy approval. S3/S4 and Batch H own consumer wiring and real historical equality/coverage checks. Passing controlled primitives cannot retroactively clean old artifacts or certify backtest returns.

## Bounded verification contract

Disposable controlled records execute the actual APIs: good pair; decision equality; after-decision/at-tip; missing/naive timestamps; DST folds/transitions; unknown/missing/ambiguous history; required-update bounds; incompatible pairing fields; incomplete pairs; exact and conflicting duplicates; contradictory source ID reuse; shuffled inputs; explicit/no/tied ranking; nonfinite values; provenance, immutability and occurrence accounting; propagated adapter errors.

Scoped Ruff, compile/import checks and source-isolation review complement execution. No DB/network/API data is used. Actual commands/results and review resolutions follow below.

## Executed evidence and review resolution

The implementation passes bounded offline validation. This establishes contract mechanics only, not real historical availability or consumer correctness.

Public entrypoints:

- `observation_policy.evaluate_observation(context=..., raw_evidence=..., interpreter=..., interpreter_id=...)` returns an immutable `ObservationDecision` with typed reasons. `validate_observation_decision` independently rechecks supplied result consistency at the selector boundary.
- `quote_selection.select_quote_pairs(quotes, ranker=None)` returns immutable eligible pairs, optional selected pair, selection status/top rank and an occurrence ledger retaining each raw input. Rank values must be finite numbers; maximum wins, tied maximum remains ambiguous.
- Quote inputs identify period and variant explicitly. American (`abs(price) >= 100`) and decimal (`price > 1`) formats are supported without conversion. Opaque identifiers retain exact spelling/case/whitespace; blank identities are rejected. Only side/odds-format enums and numeric representations are normalized.
- Each side must share normalized decision/commence instants and interpretation identity. Raw source clocks and interpreted cutoff evidence remain distinct; available interpretations require nonempty cutoff evidence. Missing context is structured rejection; malformed caller/interpreter contracts raise. Arbitrary interpreter/ranker exceptions propagate.

Coordinator-rerun checks:

| Check | Actual result |
| --- | --- |
| Worker controlled API probe | 77 assertions passed |
| Coordinator adversarial probe | All six cases passed after repairs |
| Independent-review resolution probe | Full pair equality across six provenance-order permutations and six scalar-representation permutations; empty-ID isolation; independent rejection reasons; rejected-duplicate raw evidence all passed |
| Existing `tests/test_nba_feature_store_inventory.py` | 10 passed; no tests created or edited |
| Scoped Ruff / compile | Passed |
| Scoped Pyright | Zero errors/warnings/information; tool emitted version notice and missing worktree-local venv diagnostic, existing main venv was used for execution |
| Git / source isolation | No tracked/staged changes; exactly the two new modules and three approved documentation paths untracked; new-file whitespace checks passed |
| Pure-module static review | Imports restricted to stdlib and the sibling observation module; no dynamic file/execution calls or consumer imports |

Initial acceptance was withheld despite the worker probe passing. Coordinator reproduced and repaired opaque-ID case folding, price-invalid conflicts being filtered out, mixed-context pairing, caller-forged acceptance flags, duplicate provenance keys and context-rounded American price validation. The independent reviewer additionally found raw-provenance representative ordering, empty source IDs falsely poisoning other groups, and validation reasons lost on source-ID reuse. Those cases now pass the resolution probe. Duplicate ledger entries retain canonical rejection reasons and every raw occurrence, not only a digest/reference.

Reproducible runner (PowerShell, one line; sets its own working directory, PYTHONPATH and dotenv-disable flag):

```powershell
& 'C:\Users\Chase\Projects\GameFlowData\venv\Scripts\python.exe' 'C:\Users\Chase\AppData\Local\hermes\cache\scratch\nba-batch-d\final_verify.py'
```

Actual per-command arguments, stdout/stderr, return codes and source SHA-256 values are in `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-batch-d/verification-results.json`. The runner executes the three probe files, scoped Ruff/Pyright/compile, the existing inventory suite and exact file/whitespace guards. Probes are disposable scratch evidence, not durable repository unit tests; they may be pruned. The initial direct scratch-script launch lacked PYTHONPATH and failed to import `src`; reruns use the isolated worktree explicitly. The runner also distinguishes expected `git diff --no-index` difference exit status from actual whitespace diagnostics.

## Review handoff and documentation integration

Chase authorized wrap-up, commit/push and subsequently merge into main on November 1, 2026. Branch commit: `b2fb86f359eb3b812b00badc868f1f57970a2f7a`, `feat(nba): add observation and quote-selection primitives`. Consumer activation remains unapproved.

Main continued receiving other-chat documentation during this task. Merge only this slice's wiki/plan additions into the then-current versions; do not overwrite them with these copied snapshots. No main or S1 source was edited by this lane. Worktree source/tests/config outside the two new modules remain identical to base. No DB, training, sync or model operation was run.

The reusable procedure is saved in the default profile's `gameflow-implementation-worker/references/nba-observation-quote-primitives.md`. The Tailscale authentication blocker was resolved. Canonical `handoffs/handoff-129` was committed/pushed in GameFlowBrain (`8143209`), synced/embedded and verified through exact readback, search, hub backlink and timeline backfill. Orphan count was zero and embedding coverage 100%; unrelated code-index/automation health debt remains. See the [delivery handoff](nba-batch-d-handoff-2026-11-01.md).

## Relevant prior lesson

`lessons/raw-timestamps-do-not-guarantee-temporal-integrity`: timestamps and passing local rules do not prove historical availability or downstream query correctness. The active temporal-integrity invariant requires source evidence and real consumer-path validation before readiness claims.
