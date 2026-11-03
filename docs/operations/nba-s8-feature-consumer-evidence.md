# Slice 8B feature-consumer implementation evidence

## Scope and posture

Source is uncommitted in `repair/nba-slice8-consumers`, based on `b24a69f3`. The first implementation and a later delegated repair were interrupted by provider failures. Coordinator repaired the saved bytes rather than treating worker output as acceptance.

Native source integration now exists: normal historical requests read PostgreSQL sources through `PostgresHistoricalSources`; the unconditional unavailable-adapter gate has been removed. Missing metadata is a specific data error. Caller-provided evidence remains controlled-only. Both normal and controlled paths remain non-decision-grade pending real-data acceptance. Default production paths remain unchanged. Chase explicitly owns data repair; the coordinator owns code integration and delegates only the localized repair findings.

## Source changes

- Exact policy and controlled-evidence propagation through existing requests/facade/training/date/date-range owners.
- One shared controlled historical loader using existing transforms and ordered feature contracts.
- Explicit immutable market-route and exact injury-roster membership requirements.
- Guards against legacy ratio/schedule fallbacks, unsupported carryover, contradictory source reuse, inconsistent game contexts and missing/nonfinite targets.
- Static cohort classification before target validation; rate targets independent of mutable store configuration.
- Empty/excluded date accounting and unused-evidence accounting.

The accepted S1 failure behavior and all production numerical owners remain protected. No tests/fixtures or policy configuration were added.

## Executed checks

Coordinator executed one disposable probe through actual `FeatureStore`, policy/context/evidence constructors and the real source helpers; only the external DB read boundary was mocked. All values were explicitly controlled, not invented historical receipts or model outputs.

Probe: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-s8b-acceptance-probe.py`
Receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba-s8b-acceptance-receipt.json`
Interpreter: `C:/Users/Chase/Projects/GameFlowData/venv/Scripts/python.exe`

Actual result: `PASS_CONTROLLED_ONLY`.

- Normal historical attestations blocked before an engine connection.
- Training/date/range frames matched, with 74 ordered canonical features.
- An empty second date remained present with zero population accounting.
- Rate targets obeyed the immutable target policy even with a deliberately incompatible mutable store threshold.
- Missing roster members and unapproved legacy default-triggering inputs blocked.

Coordinator also ran ten affected existing feature/harness/backtest suites: **135 passed**, one existing deprecation warning. Scoped Ruff and whitespace checks passed after the repairs; the prior interrupted code had compiled, and final changed-source compile verification must accompany any integration review. No real DB/network/model calls occurred during the controlled checks.

## Remaining gates

- Both localized review findings were repaired: injury contribution column/target-season guards and static season-type exclusion ordering. Coordinator inspected the released changes before native integration.
- The new native path executed populated date/training/range APIs and actual direct/prefetch replay, with only DB/model boundaries controlled. OUT-player weights work without a target-game box score; missing source metadata blocks explicitly. Latest scoped existing-suite run: 135 passed; Ruff/whitespace passed. This is execution evidence, not independent approval of final native bytes or live database validation.
- Actual reader/metadata contract and limitations: [native source contract](nba-s8-native-source-contract.md). Data coverage/repair and actual live-source acceptance are deliberately separate from code architecture.
- Per-family/cohort/source policy freeze remains owner-controlled.
- Direct/prefetch native-reader integration exists. CLI/artifact-policy threading, bounded real-data/lifecycle acceptance and the frozen owner-launched baseline remain unfinished.
- Two official injury-report PDFs were captured separately as a real recovery candidate, not ingested or accepted; see [source-recovery evidence](nba-s8-source-recovery-2026-11-03.md).
