## Kick off immediately

This receipt preserves the bounded, SELECT-only Slice 8A evidence collected on November 2, 2026. It is a concise readiness record, not a raw SQL transcript and not an authorization to repair, backfill, archive-scan, train, replay, or trade.

## Scope and query safety — PASS

- Compared the remote canonical database with the local mirror using read-only transactions and a 15-second statement timeout.
- Restricted every game-scoped route to one fixed cohort of exactly 19 games; no broad historical scan was used.
- The cohort contains 18 regular-season games sampled across the opening, middle, and end of seasons `22023`, `22024`, and `22025`, plus playoff game `0042500106`.
- No archive table or archive recovery path was queried. Current-table absence is therefore not evidence of archive absence.
- Results below describe only the selected cohort and bounded supporting rows. They must not be generalized to complete NBA history.

## Outcome summary

| Area | Outcome | Durable conclusion |
|---|---|---|
| Query safety | **PASS** | Remote canonical and local mirror were inspected read-only, with a 15-second timeout, an exact 19-game cohort, and no archive access. |
| Game identity | **PASS** | All 19 selected games are present, and selected-window remote/local identity results match. |
| Game/team mapping | **BLOCKED** | Only 13/19 games are mapped, and only 11/19 have both team IDs. |
| Game lines | **BLOCKED** | The 11 represented games match exactly across remote/local, but 8/19 selected games have no current game-line rows. |
| Current props coverage | **BLOCKED** | Current props exist only for `0042500106`; the other 18 selected games have zero current rows. |
| Props timing semantics | **BLOCKED** | Available timestamps do not establish clean pre-commence availability for the full row set. |
| Props pair integrity | **BLOCKED** | One-sided, non-over/under, conflicting-price, and duplicate groups remain material. |
| Remote props duplicate count | **UNKNOWN** | The bounded remote duplicate-count query timed out; no value is inferred. |
| Injury history semantics | **BLOCKED** | The selected rows do not support edition history or omission claims across the full period. |
| Raw feature-source coverage | **BLOCKED** | Several families are incomplete or stale; 2025-10-21 advanced coverage is zero. |
| Assembled feature parity | **UNKNOWN** | This slice inspected raw/source readiness only; it did not assemble or compare model-ready feature rows. |
| Archive contribution | **UNKNOWN** | Archive data was intentionally excluded from this bounded receipt. |

## Identity and mapping

### Game identity — PASS

- All 19 selected game IDs are present.
- Remote canonical and local mirror results match for the selected identity window.

### Mapping — BLOCKED

- `game_id_map` coverage: **13/19** selected games.
- Games with both team IDs resolved: **11/19**.
- Missing mapping coverage is concentrated in these exact selected IDs:
  - `0022301199`
  - `0022500590`
  - `0022500591`
  - `0022501199`
  - `0022501200`
  - `0042500106`
- Mapping presence alone is not proof that every player, event, or quote row is linked to the correct game and teams.

## Game lines — parity PASS, cohort completeness BLOCKED

- Current game-line rows cover **11/19** selected games and total **1,492 rows**.
- Observed markets are `h2h`, `spreads`, and `totals`.
- Remote and local rows have exact selected-window parity.
- The bounded pair checks found **251 totals pairs** and **251 spread pairs**.
- Quote clocks are pre-commence for the represented line evidence.
- `inserted_at` is later backfill metadata and must not be interpreted as quote availability time.
- Eight selected games have no current game-line rows, so parity on represented games does not establish cohort-complete line readiness.

## Current player props — BLOCKED

### Coverage

- Only playoff game `0042500106` has current props rows.
- That game has **1,147,195 rows**, **12 markets**, **22 local bookmakers**, and **3 commence-time candidates**.
- Each of the other 18 selected games has **zero** current props rows.
- The current table therefore cannot support a cross-season 19-game replay or parity claim by itself.

### Timing

- Distinct non-null timing values observed:
  - `snapshot_time`: **355**
  - market update time: **6,053**
  - bookmaker update time: **entirely null**
- **45,058 rows** have `snapshot_time` at or after commence.
- **82,052 rows** have `snapshot_time` earlier than market update time.
- These counts are evidence of mixed or insufficient timestamp semantics. They do not prove when a quote became available to a historical consumer.
- Commence ambiguity must be resolved before timing-sensitive use; three commence candidates were observed for the sole represented game.

### Pair and duplicate integrity

- Paired keys: **514,125**.
- One-sided keys: **62,507**.
- Non-over/under keys: **41,560**.
- Conflicting-price groups: **191**.
- Local exact-duplicate groups: **4,578**.
- Remote exact-duplicate count: **UNKNOWN** because the bounded query exceeded the 15-second timeout.
- A paired key is not automatically a valid two-sided market: direction, line, bookmaker, timing, price conflict, and duplication still require explicit policy.

## Injuries — BLOCKED

- Selected injury rows: **1,277**.
- `report_time` contains compact, timezone-free values only; those values are not self-sufficient instants.
- Histories before **2026-04-12** are January 29 backfilled snapshots, not a sequence of contemporaneous report editions.
- The later sample contains **106 multirow player-dates** and **51 status changes**.
- Omission cannot be proven from the selected current rows: absence may mean no report, no player row, a collapsed repeat, or missing history.
- The unique key collapses identical repeats, so identical successive reports cannot be reconstructed as separate editions.
- This evidence supports bounded row-shape inspection only, not point-in-time injury availability or complete report-history replay.

## Feature sources — raw/source readiness only

| Feature family | Selected coverage | Outcome and caveat |
|---|---:|---|
| Player-game base | **1,824/1,824** rows in cohort | **PASS** as the denominator for these bounded checks only. |
| Advanced player | **1,613/1,824** | **BLOCKED**; coverage is incomplete and is **0** for `2025-10-21`. |
| Player rolling | **1,618/1,824** | **BLOCKED**; 206 selected player-game rows lack coverage. |
| Position as-of | **1,774/1,824** | **BLOCKED**; 50 selected rows lack as-of coverage. |
| Team rolling | **146/146** | **PASS** for selected raw team rows. |
| League priors | Present | **BLOCKED** for readiness: stale carryover semantics remain unresolved. |

- Remote and local selected-window results match for the inspected feature-source routes.
- Local rolling-feature watermarks end at **2026-06-13**. Selected-window parity does not establish current-season freshness beyond that date.
- No model-ready feature assembly, join-loss audit, default-value audit, transform parity, or train/serve parity check was performed.
- These results authorize only raw/source readiness classification; they do not establish assembled feature parity or prediction readiness.

## Exact caveats that survive this receipt

1. The cohort is exactly 19 selected games, not a census of NBA history.
2. Archive tables were not queried; current-table misses cannot be labeled permanent historical absence.
3. Exact remote/local parity can mean both sides share the same source gap.
4. `inserted_at` is later backfill metadata, not historical quote availability.
5. Props timing columns have conflicting/incomplete semantics, and bookmaker update time is entirely null.
6. Pre-2026-04-12 injury rows are backfilled snapshots rather than proven contemporaneous editions.
7. Injury omission is unprovable, and identical repeats can be collapsed by the unique key.
8. Pair counts do not certify valid, executable two-sided quotes.
9. The remote exact-duplicate count is unknown because its bounded query timed out.
10. League-prior presence does not resolve stale carryover policy.
11. Local rolling watermarks ending 2026-06-13 block a current-freshness claim.
12. Raw/source coverage is not assembled-model-input parity.

## Compact SQL route appendix

The executed evidence used bounded variants of the following routes. This appendix records purpose and safety predicates, not every full query.

| Route | Purpose | Core bounded predicates / grouping |
|---|---|---|
| Selected-game identity | Prove game presence and compare canonical/local identity | `game_id = ANY(:selected_game_ids)` with exactly 19 text IDs |
| Game mapping | Count external mappings and resolved team IDs | selected IDs only; conditional counts for non-null home/away team IDs |
| Game lines | Count rows/games/markets and compare row content | selected IDs only; market in `h2h`, `spreads`, `totals`; deterministic key ordering |
| Totals/spread pairs | Count represented two-sided line pairs | selected IDs and market-specific grouping by game/bookmaker/line/quote clock |
| Current props coverage | Count rows, games, markets, bookmakers, and commence candidates | selected IDs only; grouped by game ID and bounded distinct counts |
| Props timing | Inspect non-null clocks and temporal ordering | `game_id = '0042500106'`; conditional counts around commence and market update |
| Props pairs | Classify paired, one-sided, non-O/U, and conflicting-price keys | sole represented game; grouped by stable quote/player/market/line/bookmaker keys |
| Exact duplicates | Count identical local groups and attempt the same remotely | sole represented game; full equality grouping; 15-second timeout retained |
| Injuries | Inspect selected player-date history and status changes | selected cohort/player-date linkage and bounded report-date windows |
| Player features | Measure family coverage against player-game denominator | selected game IDs; left joins on player/game and as-of predicates where applicable |
| Team features | Measure team rolling coverage | selected game/team keys only |
| League priors | Confirm presence and inspect applicable prior dates | selected season/date window; no claim beyond observed as-of rows |
| Watermarks | Compare latest rolling dates | aggregate `max(date)` on the inspected rolling sources; no archive route |

All routes ran read-only with a 15-second statement timeout. A timeout remains `UNKNOWN`; it is never converted to zero or pass.

## What this receipt authorizes

- Use the fixed 19-game cohort as a durable diagnostic fixture for subsequent bounded repair and acceptance work.
- Treat selected-window remote/local identity and represented-row parity as established evidence.
- Treat mapping, current props history, temporal semantics, injury editions, and several feature families as explicit blockers requiring separate repair or policy decisions.
- Design follow-up SELECT-only probes around the listed gaps while preserving exact IDs, read-only mode, timeout bounds, and archive separation.
- Cite these counts and caveats as the November 2, 2026 Slice 8A source-readiness baseline.

## What it does not authorize

- No database writes, backfills, deduplication, archive transfer, index creation, pruning, or schema changes.
- No inference, training, backtest, replay, model promotion, calibration change, betting decision, or live/paper trading action.
- No claim of complete historical quote availability, quote-clean replay readiness, injury point-in-time correctness, or omission completeness.
- No claim that local parity proves canonical correctness or that current-table absence proves archive absence.
- No claim of assembled feature parity, train/serve parity, current rolling freshness, prediction readiness, or decision-grade historical evaluation.
