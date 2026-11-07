# NBA training readiness checkpoint — 2026-11-06

## Verdict

Full injury-aware training is not yet cleared. The one-game repair, remote/local mirror, no-op rerun and public consumer work; full-cohort source acceptance remains incomplete. This is not a demand for perfect historical coverage: a first exploratory run can tolerate explicitly measured unknown values, but its actual local fitting population and per-season feature coverage must be checked first.

## Fresh evidence

- `logs/receipts/injury_history_batches/` currently contains only `2023-10-24_2023-10-30_execute_mirror.json`: 51 game records, 13 verified, 38 blocked, 174 verified observation entries.
- A completion-accounting defect was reproduced from that saved state. `scripts/recover_nba_injury_history.py:725` treats previously visited blocked games outside the current batch as remaining budget work. With 38 blocked games and a 10-game batch, the reproduced output is `accounted=True`, `remaining_due_to_budget=True`; the launcher therefore cannot advance normally and may exhaust its batch ceiling. Earlier advice that it would automatically advance with gaps was incomplete. Do not rerun or edit active execution until the owner pauses it; preserve checkpoints and committed repairs.
- `C:/Users/Chase/.gameflow/nba-props-t60/alias_repair_execute.json` records 647 successful tasks, two unresolved, and 29,855 feature rows. Alias repair is no longer merely prepared.
- The specific documented local compact-props sync journal `C:/Users/Chase/.gameflow/sync/nba_player_prop_feature_lines_2023-10-24_2026-11-04.json` was absent. This does NOT prove the local table is absent or unsynced; verify actual local state or another journal before concluding.

## Minimal next gate

1. Repair the small injury completion-accounting defect; verify more blocked games than batch size advances with explicit gaps, while genuinely unvisited/report-budget-deferred work remains resumable. No provider clock, omission or matching-policy changes.
2. Owner resumes the recovery with existing checkpoints; do not start a broad job automatically.
3. Run a bounded local training-input acceptance check: nonempty fit/calibration population, compact T-60 props availability, per-season usable/missing injury coverage and unchanged chronology. Check actual consumer output, not just stored timestamps or a successful scraper.
4. Then launch a small owner-run exploratory baseline with explicitly documented limitations before committing to the full training matrix.

Relevant prior invariant/lesson: missing injury evidence stays unknown, not healthy; temporal integrity is an end-to-end consumer property (`operations/critical-invariants`, `lessons/raw-timestamps-do-not-guarantee-temporal-integrity`). No model run, DB query/write or source repair was performed during this readiness check.
