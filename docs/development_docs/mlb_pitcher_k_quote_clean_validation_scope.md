# MLB Pitcher K Quote-Clean Validation Scope

> **Gate completed for Slice 7:** The requirements in this scope were satisfied for the
> 2026-04-13 through 2026-06-21 baseline. Use
> [`mlb_pitcher_k_ablation_roadmap.md`](mlb_pitcher_k_ablation_roadmap.md) for current work;
> retain this file as the original validation contract.

Date: 2026-05-15
Status: implementation-ready / validation-gated

## Purpose

Pitcher strikeouts must be validated through the same quote-clean line-selection and dropout-audit workflow as batter_hits before any production/promotion decision. Legacy latest-row backtests remain useful for debugging and hypothesis generation only; they are not equivalent evidence.

## Scope

This document exists to make the pitcher K validation gate explicit while the generic audit tooling is introduced:

- `src/backtesting/mlb/run_mlb_sweep.py --quote-clean` is the only promotion-grade MLB replay path.
- `scripts/audit_mlb_quote_clean_dropout.py` must support `--stats pitcher_strikeouts` without batter-only assumptions.
- CLV/dropout diagnostics must be reviewed before treating quote-clean ROI as deployable.

## Why pitcher K is included

Pitcher K uses `mlb_raw_player_props`, so it shares the same quote-source risk class as batter markets:

- latest-row selection can pick rows not available at decision time;
- rows can arrive after the quote cutoff;
- rows can be post-commence;
- same-book Over/Under pairs can be accidentally synthesized if snapshot identity is ignored.

Known source-audit facts from the 2026-04-13 to 2026-05-10 window:

- `pitcher_strikeouts` rows: 142,216
- timestamp coverage: 100% for existing rows
- post-start rows: 1,288 (~0.91%)

A lower post-start rate than batter markets does not make latest-row selection promotion-grade.

## Validation checklist

1. Run quote-clean replay with explicit cutoff:

```text
.\venv\Scripts\python.exe src\backtesting\mlb\run_mlb_sweep.py --local --quote-clean --quote-cutoff-time-et <HH:MM> --stats pitcher_strikeouts ...
```

2. Run dropout audit:

```text
.\venv\Scripts\python.exe scripts\audit_mlb_quote_clean_dropout.py --local --model-dir <artifact_dir> --start <YYYY-MM-DD> --end <YYYY-MM-DD> --stats pitcher_strikeouts --quote-cutoff-time-et <HH:MM> --sweep-output-dir <quote_clean_sweep_or_config_dir> --output-dir <audit_output_dir>
```

3. Confirm:

- zero selected quote cutoff violations;
- zero selected quote post-commence violations;
- post-start source rows are filtered out;
- same-book, same-line, same-snapshot Over/Under pairing is enforced;
- quote-clean pairing/dropout is stable and plausible by date/game;
- over/under side splits are reviewed before recommendations.

4. If CLV is available, run CLV diagnostics and then the failure-mode classifier:

```text
.\venv\Scripts\python.exe scripts\diagnose_mlb_clv_failure_modes.py --clv-output-dir <clv_output_dir> --output-dir <clv_output_dir>\failure_modes
```

## Acceptance criteria

- Generic dropout audit supports `pitcher_strikeouts`.
- Quote-clean selected lines have no cutoff/commence violations.
- Legacy latest-row ROI is not presented as production evidence.
- Any future Phase 3B pitcher-side feature work compares against a quote-clean Phase 2 baseline, not against inflated legacy-line results.

## Non-goals

- Do not rerun training from the audit script.
- Do not mutate the database.
- Do not add more pitcher K feature families until the quote-clean baseline gate is accepted.
