# Slice 8C replay seam: executable bounded scope

User's remaining implementation authorization applies. The pure quote adapter is independently accepted at SHA256 `1c84d179ff922dff410da906ab4c1f8bad4daf1741f7f76032d7ab3e708bd7a2`; do not modify it in this section. Existing 8B keyword interfaces are stable but native source acquisition remains blocked; no real source acceptance is inferred.

Writable scope:
- New `src/backtesting/nba/historical_replay.py`: bounded raw candidate acquisition plus one per-date call to the accepted adapter; unknown current-table provenance remains blocked, never inferred from snapshot clocks. Explicit controlled rows may exercise mechanics only. Use indexed exact game aliases, per-game row limits and read-only/timeout-bounded PostgreSQL sessions. No actual live queries launched in this implementation step.
- `src/backtesting/backtest_harness.py`: explicit opt-in policy/context/controlled-source fields; narrow acquisition branches in `_get_lines_for_date`, `_prefetch_all_lines`, `run`, `_run_date`; ledger/config export; metadata projection only in the edge merge. Protect probability, BL, combo, selection/sizing/settlement and S1 abort behavior. Legacy default paths unchanged.
- Narrow additions to `docs/understanding/nba-s8-quote-adapter.md` and coordinator status/evidence.

Direct and prefetch paths must share the exact acquisition/adapter owner, produce equivalent per-date pair/ledger frames, and propagate blocked/query errors instead of returning healthy empties or partial results. Current native rows lack route/period/variant/provenance authority: record that truth, do not invent those fields. Preserve exact paired metadata while converting numeric values only at the existing calculator boundary, with loss/nonfinite guards; American-odds-only harness must reject unsupported formats rather than reinterpret them. Both modes remain non-decision-grade pending real source acceptance.

No CLI/policy schema redesign, new tests/fixtures, DB writes, source repair, artifact creation, model loading/training, long runs, Git publication or deployment. Existing affected suites and one short controlled actual-harness probe, plus scoped Ruff/compile/whitespace. This implements replay mechanics, not full Slice8C native acceptance or 8D/8E.
