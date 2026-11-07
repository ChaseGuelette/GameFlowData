# Separate NBA official game-time fixer — approved scope

## Owner direction

Use NBA game-level scheduled start times from the authoritative games/schedule table. Do not use injury PDF clocks for game starts or require odds timestamps to agree. If an odds line is needed by a downstream consumer, select the latest eligible observation before the authoritative decision cutoff; odds do not define the official start. Keep the currently running injury recovery and its checkpoints untouched. Build a small independent fixer, not another full injury crawler.

## Implementation boundary

- New standalone entry point: `scripts/fix_nba_game_start_times.py`.
- First verify the actual table/columns and historical coverage. Existing source references `public.nba_game_schedule.commence_time_utc`; do not invent a `games` table or turn a date-only value into a start timestamp.
- Reuse the existing official NBA schedule normalization/upsert path where it genuinely supports the source. If historical official starts are absent, obtain them from a verified official NBA game-level endpoint and store them in the schedule authority table with source provenance; do not relabel odds-derived times as official.
- Use explicit date/game scope, bounded batch and network limits, dry-run default, and idempotent writes. Official game ID/date/home-away identities must agree with canonical game records. Separate scheduled and actual start fields.
- Optional exact-scope remote-to-local mirror; no full sync, truncate or schema overhaul. Report absent/invalid official evidence honestly.
- Do not edit `scripts/recover_nba_injury_history.py`, its PowerShell launcher or checkpoints while active. Avoid shared-helper edits where possible; a valid row in the existing authoritative table is already preferred by both the running resolver and training loader.
- No PDF requests and no odds-table scan in this fixer. No need to touch quote selection if official schedule population alone supplies the existing start-minus-60 injury cutoff. The instruction about latest pre-cutoff odds does not authorize unrelated props-lane edits or changing T-60 policy.

## Verification and stopping rule

1. Bounded isolated SQL discovery establishes the real source authority and historical gaps.
2. Verify the real official input on previously conflicted games, plus a known current-season control; no guessed endpoint payloads.
3. Implement only the required vertical path. Scoped compile/Ruff, existing short checks and a genuine tiny dry-run; no new unit-test suite or broad historical launch.
4. Preserve the running job's source bytes and state. Code completion, real data preview and any owner-run writes remain distinct claims.
5. Provide exact standalone dry-run/execute commands and limitations. No training, commit, push or deployment.

## Discovery and implementation checkpoint

- Live authority is `public.nba_game_schedule.commence_time_utc`; no separate `games` table exists. Both databases currently have 1,206 2026–27 schedule rows and lack the sampled historical targets. Evidence: `logs/receipts/nba_official_schedule_fixer/discovery.json`.
- The official `stats.nba.com/stats/scheduleleaguev2?LeagueID=00&Season=YYYY-YY` endpoint works through the existing workstation-style Chrome-compatible curl transport. Plain requests timed out. One response supplies an entire season, including exact official game IDs, home/away teams and `gameDateTimeUTC`.
- Real cached sources parsed to 1,318 games for 2023–24, 1,320 for 2024–25 and 1,321 for 2025–26: 3,959 valid scheduled games, no PDF/odds requests. These include postseason and are source population, not approval of a model fitting population.
- Implemented one new file, `scripts/fix_nba_game_start_times.py`. It fills missing/null authoritative remote starts, preserves populated official table starts, reports table/source differences, and optionally mirrors exact canonical rows locally. It does not write `nba_injury_game_timing`: the existing training and recovery readers already prefer the schedule table, so the running job's helper code stays untouched.
- Historical metadata is not forced through the live CDN 48-hour freshness gate. Raw response metadata remains in the cache; `observed_at` is retrieval time, not historical availability, and `source_updated_at` remains unknown rather than borrowing the CDN-specific clock interpretation. Provenance is `nba_stats_schedule`.
- The 2023–24 response carries `postponedStatus='A'` on every completed game. Normalize the current postponed flag to `N` only where official `gameStatus=3` and status text is Final; preserve raw flags in the cached source. Other unresolved/postponed statuses remain rejected. No AM/PM inference or PDF-clock use.
- Scoped Ruff, compile, guarded help, seven existing parser tests and a real-input insert/no-op/identity-conflict planning exercise passed. The actual standalone CLI preview passed against remote and local on `0022300064`, `0022300068` and `0022300093`: three planned rows per target, matching canonical date/team identities and official starts. PostgreSQL EXPLAIN (without ANALYZE) accepted the actual UPSERT against the existing primary key; no mutation occurred. Receipts: `logs/receipts/nba_official_schedule_fixer/tiny_preview.json` and `validation.json`. No schedule DB writes or broad launch have run in this implementation; live write/no-op verification is not claimed.

## Operator commands

From the repository root, preview all three historical seasons and the exact local mirror scope:

`.\venv\Scripts\python.exe scripts\fix_nba_game_start_times.py --season 2023-24 --season 2024-25 --season 2025-26 --max-games 6000 --mirror-local --output logs\receipts\nba_official_schedule_fixer\historical_preview.json`

Owner execution uses the same inputs plus `--execute` and a distinct `--output logs\receipts\nba_official_schedule_fixer\historical_execute.json`. Sources are cached; reruns do not fetch them again unless `--refresh` is explicit. Every write chunk is bounded and freshly read back. No migration, general sync, PDF download or odds query is included.

Existing game-start cutoffs remain start minus 60 minutes. No downstream odds line is needed for this repair; quote selection is not changed. Future injury-recovery batches will see populated official table rows. Previously processed injury gaps are not magically supplied with missing publication observations by filling starts; a later replay of cached injury evidence may still be required for those games. Do not claim training acceptance from this schedule-only repair.
