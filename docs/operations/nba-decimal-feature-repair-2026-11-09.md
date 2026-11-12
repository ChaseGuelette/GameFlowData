# NBA Decimal NaN feature repair — 2026-11-09

## Confirmed cause

Canonical read-only queries for players 201599, 204456 and 1626172 reproduced `decimal.InvalidOperation` in `safe_ratio`: PostgreSQL NUMERIC NaN arrives as Decimal('NaN'), and `den <= 0` raises. Actual fields include rolling L3/L5 values; player 204456 also has missing L15/advanced/season fields. These are unavailable values, not zero performance.

## Repair and evidence

Only `src/models/features/nba/transforms.py` changed: float conversion before the denominator comparison and rest-day cap. Missingness remains NaN; finite/None/zero defaults and no-row policies are preserved. No database corrections, global decimal-trap changes, model changes or betting execution.

- Exact captured canonical row replay: all three player mappings pass and match float-input semantics.
- Five existing transform tests pass with offline guards; Decimal NaN/finite/None/zero probes pass; Ruff and scoped diff checks pass.
- Railway deployment `9384f8f6-57aa-4cc3-8b21-deb65e1064f8`: SUCCESS. Isolated archive of active production commit `5c304cf0da1b8a59add48703cd1f8a2ee0439ea9` plus this single-file repair; unrelated dirty diagnostic work excluded.
- Native deployed helper probes pass. Deployed/local SHA256 both `3cf8a31eb4c14665de1732bfa447957bca74304b477ed27768efb814468d9525`.
- Independent full-feature replay passed for all three affected players using guarded canonical source reads and exact November 9 game/team/opponent contexts. To enforce the query budget, the runner batch-loaded canonical rows and replayed them through the current FeatureStore helpers; this is not three unmodified live per-player DB executions. All returned 86 keys without exceptions/None. Numeric finiteness remains limited: 201599 has 59/82 finite values, 204456 has 46/82, and 1626172 has 58/82; missing rolling and injury-aggregate inputs remain missing. The exception is repaired, not the underlying source completeness. No whole-slate inference or prediction-output recovery is claimed. Receipt: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/gameflow_nba_feature_gate_2026-11-09.json`.

Source repair is local and deployed but uncommitted/unpushed. Preserve it in the next Git publication. Canonical GBrain sync is not claimed.

Receipts: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/gameflow_player_rolling_decimal_diag_receipt.json`; offline replay `C:/Users/Chase/AppData/Local/hermes/cache/scratch/check_nba_decimal_rolling.py`.
