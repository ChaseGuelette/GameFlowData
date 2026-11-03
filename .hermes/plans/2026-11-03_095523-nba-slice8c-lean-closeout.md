# NBA Slice 8C Lean Closeout Plan

**Status:** Proposed scope; planning only. No implementation or publication authorized by this plan.
**Goal:** Close the existing shared historical quote replay implementation with a small, reproducible code-mechanics acceptance check, without reopening 8B or taking over Chase's data-validity work.
**Architecture:** Reuse the existing `PostgresHistoricalSources` -> `HistoricalReplayQuotes` -> `adapt_historical_quotes` -> BacktestHarness path. Direct and prefetched replay must consume equivalent game populations and preserve the same paired quotes and evidence; numerical owners and default production behavior remain unchanged.
**Tech stack:** Existing Python, pandas, SQLAlchemy, pytest and Ruff; no new dependencies.

## Current evidence and scope decision

- Owner accepts 8B to an adequate degree and owns data validity. Minor unrelated 8B defects are deferred, not a reason to restart certification.
- Actual source is uncommitted/untracked in `C:/Users/Chase/Projects/GameFlowData-worktrees/nba-slice8-consumers`, branch `repair/nba-slice8-consumers`, HEAD `b24a69f3`. Main at inspection is `04cdb08a`; it does not contain this worktree's consumer source.
- The November 3 pending handoff records a populated native-reader/public-consumer path, 135 selected existing tests, and controlled direct/prefetch checks. These are prior receipts, not tests rerun by this planning session.
- 8C is already substantially implemented: `historical_quote_adapter.py`, `historical_replay.py` and historical branches in `backtest_harness.py`. This is a closeout, not a greenfield implementation.
- Earlier independent adapter/replay approval predates native-reader changes. Do not claim final-byte independent acceptance from old receipts.
- Current owner choices recorded in the local handoff: T-60; strict missing/ambiguous source/injury/commence blocking; no diagnostic fallback; target minutes 5 and rolling contribution minutes 8. No new cohort or family policy is selected here.

## People and agents

1. Coordinator: owns the closeout, minimal code repairs, validation and final documentation. This honors the latest direct-implementation/lean-validation workflow instead of starting another writer/reviewer fan-out.
2. One file-only explorer: already used during planning, bounded to three document reads; found the newer local handoff and current 8C contracts. No further discovery agent is planned.
3. Optional localized repair subagent: only if Chase explicitly authorizes it and a reproduced defect is separable into one exact file/method. No parallel writes to shared harness/source files. Otherwise coordinator fixes it directly.
4. No broad reviewer agents, SQL runners, data auditors, model workers or lifecycle workers for this scope. Coordinator checks spec compliance before code quality, using the same frozen diff and concrete execution evidence. Do not call this independent review.

## Step 1 — Freeze the starting point (bounded)

- Recheck worktree status/HEAD and confirm no other writer is active before edits; do not reset, clean, copy stale snapshots or create a commit.
- Keep work in the existing consumer worktree rather than transplanting uncommitted 8B source into a new tree.
- Record scoped starting diffs and hashes, including the untracked modules. Preserve all inherited 8B code and S1 abort behavior.
- Read the existing scratch probes before execution. Reuse them if still available and safe; confirm dotenv/network/database-driver guards precede imports. Do not treat absence of a worktree `.env` as isolation.
- No rerunning all predecessor validation as a separate baseline ceremony.

## Step 2 — One focused replay acceptance pass

Use the existing public path and tiny controlled source-boundary inputs. Database results and model predictions may be controlled; the reader, assembler, adapter, harness and exporter must execute real project code.

Acceptance criteria:

1. Direct and prefetched paths use the same `HistoricalReplayQuotes`/adapter contract, policy, contexts and effective game population. Specifically inspect direct `for_date(day, game_ids)` versus prefetch `for_date(day)` so subset filtering does not introduce different decisions or blocking behavior.
2. Equivalent inputs yield equal selected pairs, rejection accounting and prediction/edge results. Compare semantic rows with stable ordering; do not invent source identities to force equality.
3. A single coherent Over/Under pair retains player/game/market/book/line/snapshot identity and side clocks through edge merge and result export. Never independently select newest sides.
4. Missing/ambiguous evidence, query failure and acquisition-cap overflow remain explicit blocked/errors; neither mode returns healthy empties or partial success for these cases.
5. Strict decision/commence cutoff and existing route checks remain intact. Reuse prior adapter coverage rather than rerunning an exhaustive timestamp/duplicate matrix.
6. Exact ledger values serialize without lossy identity/Decimal conversion. Controlled outputs stay `controlled_only`; normal reader outputs stay `source_integrated`; neither claims `decision_grade=True`.
7. Legacy/default behavior and empirical probability, BL, sample/combo, selection/sizing/settlement owners are unchanged.

If these pass against current bytes, make no source changes just to make the slice look substantial.

## Step 3 — Repair only reproduced 8C defects

Writable source allowlist:

- `src/backtesting/nba/historical_replay.py`
- `src/backtesting/backtest_harness.py`: only historical owner/acquisition/prefetch/date-consumer branches, metadata projection and ledger/config export.
- `src/backtesting/nba/historical_quote_adapter.py`: only if the focused check reproduces an adapter defect indispensable to 8C; otherwise read-only.

Protected prerequisites:

- `src/models/features/nba/historical_sources.py`, `historical_loader.py`, all 8B mode loaders, feature facade, injury and line-source contracts.
- Existing math, probability, prediction/sample/combo and settlement owners.
- S1 fail-closed behavior.
- All main-checkout position/opponent/sync work and other worktrees.

A prerequisite API defect that genuinely prevents replay gets one minimal reproducer and an explicit scope decision, not a broad 8B cleanup. Cosmetic/style-only findings and unrelated edge cases are deferred. No source-reader redesign, new generic validators, new test files or permanent fixtures.

## Step 4 — Lean validation and stopping rule

Verified existing test targets for the narrow closeout:

- `tests/test_backtest_harness.py`
- `tests/test_run_backtest.py`
- `tests/test_nba_line_sources.py`

Run through the existing guarded pytest mechanism. Inner command, from the consumer worktree using the main venv:

`C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe -m pytest tests/test_backtest_harness.py tests/test_run_backtest.py tests/test_nba_line_sources.py -q`

The command above is a test selection, NOT a substitute for installing the driver/network guards before imports. Reuse the established guarded runner after inspection. Add an already-existing affected suite only if the actual diff crosses that boundary.

Reuse these existing short probes after inspecting safety and applicability:

- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-s8-native-path-probe.py`
- `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-s8-harness-acceptance-probe.py`

Use their relevant checks to cover acceptance criteria once, not repeated full source certification. If a probe expired, replace only its tiny required check, not its entire historical test matrix. Explicit controlled input is test evidence, never represented as real database output.

Run Ruff, py_compile and scoped whitespace checks on changed files. Review exact untracked new modules as well as tracked diffs. After repairs rerun only the affected reproducer and narrow regression selection; freeze final bytes for the receipt.

Effort target: one acceptance pass, one consolidated repair pass, one final check, roughly 30–45 minutes if only localized defects remain (estimate, not guarantee). At that limit, report the exact remaining blocker/scope rather than launching another open-ended review loop. During execution provide a short progress update about every five minutes.

## Step 5 — Deliver and stop

Update only slice-owned documentation in the consumer worktree:

- `docs/operations/nba-slice8-completion-status.md`
- `docs/understanding/nba-s8-quote-adapter.md`

Record: source fingerprint, exact commands/results, 8C fixes or no-change acceptance, remaining owner-data conditions and code-integration state. Mark 8C code mechanics complete only if all listed criteria pass. Explicitly state controlled execution is not live PostgreSQL/schema, historical authenticity, ROI or production acceptance.

No commit, merge, push or deployment without separate approval. Source remains in the consumer worktree for later scoped integration.

## Explicitly out of scope

- General 8B hardening or its accepted minor errors.
- Source validity, historical coverage, position repair, injuries acquisition, metadata repair, live SQL, data copies, scraping and backfills: Chase owns these.
- Live historical replay, model loading/training, sweeps or baseline execution.
- CLI/artifact-policy attachment, lifecycle threading, verifier expansion and real-data acceptance (8D), frozen baseline/policy launch (8E).
- New policy YAML, chosen family/cohort/split values, calibration/model changes, new framework or exhaustive scratch tests.
- GBrain sync repair, Engineering OS, Git publication and deployment.

## Relevant invariants

Preserve temporal/strict source checks without fabricating missing evidence; keep empirical probabilities and Q10/no-global-offset behavior unchanged. Keep long runs owner-launched and database operations outside main context. This plan changes replay plumbing only, not model architecture or evaluation policy.

## Evidence inspected for this plan

- Consumer worktree `docs/operations/handoff-pending-nba-s8-native-sources-2026-11-03.md`.
- Consumer worktree `.hermes/plans/nba-slice8c-quote-adapter.md` and `nba-slice8c-replay-seam.md`.
- Current `HistoricalReplayQuotes` implementation and harness historical owner/prefetch/feature receipt branches.
- Current Git status/worktree inventory and existing test filenames.
