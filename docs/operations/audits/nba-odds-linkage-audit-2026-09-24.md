# NBA Odds, Prop-Linking, and Inference-to-Edge Audit — 2026-09-24

**Lane:** A3 — odds and linking  
**Audit mode:** Read-only production SQL plus targeted source/test inspection  
**Evidence window:** 2026-09-24 11:44–11:52 EDT (15:44–15:52 UTC)  
**Production database role:** `postgres`, forced `SET TRANSACTION READ ONLY` for audit queries  
**Code state:** Dirty tree preserved; this report is the only repository file created by this lane.

## Executive verdict

**NOT READY for a dependable customer-facing NBA pick path.**

The live Odds API ingestion path is active and discovering future NBA events. In the bounded newest 500,169 prop rows, all player IDs were linked, all 107 latest quote groups had paired Over/Under prices, and timestamps were pre-commence. The latest completed inference date also had complete prediction-to-sample linkage (364/364 arrays, all 10,000 samples).

The end-to-end product path still fails readiness for three reasons:

1. **Current future props are not linked to NBA `game_id` at all**: 0/500,169 newest rows, across three October 20 opening-night events. Inference and edge selection join on `game_id`, so the currently ingested props cannot become picks yet.
2. **Quote selection is not decision-time or commence-safe**: inference selects the latest raw row without a decision cutoff; edge refresh selects the latest row in the last 24 hours without `snapshot_time < game_time/commence_time`. Edge refresh continues throughout the day and can overwrite predictions and place paper bets.
3. **The frontend directly queries the append-only raw table without a server-side latest-snapshot/cutoff contract**. Analysis fetches all matching history and deduplicates client-side; sportsbook availability has no snapshot/date cutoff. These query shapes are unsafe against a table whose current high ID is 266,227,419 and whose newest bounded window alone contains 500,169 rows.

Because September 24, 2026 is preseason and the first observed prop events commence October 20, absence of same-day predictions is expected. It is **not** evidence that the October daily path will work.

## Readiness table

| Layer | Status | Evidence | Verdict |
|---|---|---|---|
| Event discovery | **Active** | Latest game-line rows reached 2026-09-23; bounded game-line window contains 77 API events including October–December games. | Ready for discovery; NBA-ID linkage is not ready. |
| Player-prop ingestion | **Active but wasteful** | Latest prop snapshot at 2026-09-24 15:45 UTC; 168 rows for 3 events every 10 minutes; 500,169 rows accumulated since 2026-08-28 for only 3 opening-night events. | Data arrives, but high-frequency far-future append-only capture is excessive. |
| Player linking | **Ready in sampled current window** | 500,169/500,169 newest rows have `player_id`. Distinct API names equal distinct linked players for each of the three events sampled. | Pass for bounded window. |
| Game linking | **Blocked** | 0/500,169 newest prop rows have `game_id`; 0/100,001 newest game-line rows have `nba_game_id`. | **P0 blocker.** |
| Team linking | **Blocked/unsafe** | 0/500,169 newest prop rows have `team_id`. Incremental linker code assigns `home_team` ID as a placeholder when both game/player match, not the player's actual team. | **P0 blocker.** |
| Paired O/U | **Pass in bounded current sample** | 107/107 latest player/event/book/market/line groups were paired; FanDuel 49/49, PrizePicks 40/40, Fanatics 18/18. | Pass for observed three-event window only. |
| Bookmaker coverage | **Thin** | Only 3 books in newest 500,169 rows: FanDuel, PrizePicks, Fanatics. | P1: insufficient sportsbook breadth for a customer product. |
| Market coverage | **Partial** | 11 markets observed; core points/rebounds/assists/threes present, but no steals or turnovers in the bounded current window and blocks appeared for only 1 event/1 book. | P1. |
| Selected line/price persistence | **Partial** | `daily_predictions` stores line, both prices, bookmaker, probabilities and edges. It does not store selected raw row IDs, quote timestamps, market/book update timestamps, or decision time. | **P1 reproducibility gap.** |
| Prediction-to-sample linkage | **Ready on latest historical run** | Latest date 2026-05-17: 364/364 predictions matched sample rows; all had 10,000 samples. | Pass historically; no current-season proof. |
| Inference-to-quote-to-edge linkage | **Incomplete** | Latest date had priced edges for only 51/364 rows (14.0% overall; 51/156 base-stat rows, 32.7%). Combo rows had no lines/edges. Exact quote reconstruction query timed out because persisted quote identity is incomplete. | P1. |
| Raw-table query safety | **Mixed / unsafe consumers** | Backend game-key queries have useful indexes. Audit quote reconstruction timed out at 30 seconds. Dashboard raw queries omit timestamp cutoffs and latest-row RPCs. | **P0 for frontend raw query path; P1 backend hardening.** |
| Current-date outputs | **Expected empty** | 0 predictions and 0 samples for 2026-09-24; latest stored date is 2026-05-17. Inference job on 2026-09-24 completed in 4.75 seconds with no stored output. | Expected offseason/preseason no-op, not launch proof. |
| Automation | **Running but overloaded** | Lines jobs run every 10 minutes and take roughly 5.5–6.3 minutes; edge refreshes take roughly 6.5–13.7 minutes despite no current NBA samples. | P1 capacity/cadence issue. |
| Frontend DB compatibility | **Schema/RLS compatible; query contract unsafe** | Required RPCs exist; authenticated users can read predictions/samples. Raw props are readable by both `anon` and `authenticated`. | P1 security/exposure; P0 correctness/performance path. |

## Findings by audit question

### 1. Event discovery

The live scraper uses The Odds API `/v4/sports/basketball_nba/events` endpoint and requests `us,us2,us_ex,us_dfs`. Production evidence shows event discovery is active:

- newest prop row: snapshot `2026-09-24 15:35 UTC`, commence `2026-10-21 01:30 UTC`;
- newest bounded prop window: 3 API events, all on October 20 ET;
- newest bounded game-line window: 77 API events, with future events through December 25 ET;
- latest job history shows repeated successful `lines_job.py` runs through 15:45 UTC.

However, event discovery currently produces Odds API event IDs only. The downstream NBA ID columns are empty in the bounded windows:

- props `game_id`: **0/500,169**;
- game lines `nba_game_id`: **0/100,001**.

### 2. Props ingestion and coverage

The newest 500,169 rows cover only three API events and five players:

| Matchup | Linked players | Books | Markets | Linked game rows |
|---|---:|---:|---:|---:|
| Boston Celtics @ Detroit Pistons | 2 | 3 | 10 | 0 |
| Philadelphia 76ers @ New York Knicks | 1 | 3 | 9 | 0 |
| Oklahoma City Thunder @ San Antonio Spurs | 2 | 3 | 11 | 0 |

Recent snapshots contain 168 rows every ten minutes. This proves the scheduled scrape is live, but the retention/cadence is collecting hundreds of thousands of identical/far-future snapshots before a game is actionable. The latest 500,169 rows span August 28–September 24 for games on October 20.

Observed books:

- `fanduel`: 265,225 rows, 3 events, 11 markets;
- `prizepicks`: 187,320 rows, 3 events, 8 markets;
- `fanatics`: 47,624 rows, 3 events, 4 markets.

This is not broad enough to support the frontend's configured major-book list (`draftkings`, `fanduel`, `betmgm`, `caesars`, `williamhill_us`, `betrivers`, `espnbet`, `fanatics`) for the sampled future slate.

### 3. Player, game, and stat linkage

**Player linkage passed** in the sampled newest window: every row had `player_id`.

**Game linkage failed** in both props and game lines. The incremental linker has structural limitations:

- it first calls `nba_api.stats.endpoints.ScoreboardV2` and only falls back to CDN after no games return; this violates the Railway CDN-only invariant by attempting `stats.nba.com` from Railway;
- upcoming lookup covers only today plus two days, so September 24 jobs cannot link October 20 props by design;
- the incremental query counts/loads rows through `commence_time >= NOW() - interval '30 days'`, which includes all future dates and repeatedly revisits far-future unlinked data;
- its separate game-ID pass uses exact/±1-day matching, which is safer than the historical ±90-day linker, but cannot work until the game enters the three-day lookup;
- its `team_id` helper returns `home_team` ID first (`team_lookup.get(home_norm) or ...`), not the player's actual team. If used, this can assign away players to the home team.

The backend edge path correctly keys joins by `(player_id, game_id, stat)`, so missing `game_id` is a hard dependency blocker rather than a cosmetic field gap.

### 4. Paired Over/Under availability

In the bounded current window, latest quotes were fully paired:

- 107 quote groups;
- 107 paired groups;
- 0 unpaired groups.

This is a positive ingestion signal, but it covers only three far-future events and three books. It is not sufficient to certify regular-season coverage or each supported market.

### 5. Selected line and price persistence

`daily_predictions` persists:

- selected `line`;
- `over_odds` and `under_odds`;
- `bookmaker`;
- empirical `over_prob`/`under_prob`;
- implied probabilities and edges;
- inference-time `prop_line`.

It does **not** persist the quote's:

- `staging_id` or other raw quote identity;
- `snapshot_time`;
- `market_last_update`;
- `bookmaker_last_update`;
- edge-refresh/decision timestamp;
- selected side-specific source rows.

Therefore, a displayed or bet edge cannot be deterministically reproduced after the raw table changes. A bounded reconstruction attempt for the latest prediction date timed out at 30 seconds.

### 6. Raw-table query safety

Production indexes exist on:

- primary key `staging_id`;
- `game_id`;
- `(game_id, market_key)`;
- `(player_id, game_id)`.

These support keyed backend queries. Risks remain:

- `DailyPredictionRunner._get_current_lines()` has no snapshot/date/commence cutoff. It scans all rows for the selected game IDs and chooses latest per book/line/side.
- `edge_refresh_job.fetch_fresh_lines()` adds a 24-hour snapshot window but no pre-commence or decision-time guard.
- neither path uses `market_last_update` as a quote-validity cutoff.
- `AnalysisModal` directly reads all raw rows for player/game/market, orders only by bookmaker server-side, and deduplicates/sorts client-side. PostgREST row limits can return an incomplete subset before client sorting.
- dashboard sportsbook availability reads raw rows by game IDs/books with no snapshot or market cutoff.
- raw props are exposed to `anon` as well as authenticated users by a `SELECT true` RLS policy.

One audit query joining 51 persisted priced predictions back to raw quotes timed out at 30 seconds despite keyed predicates. This is evidence that arbitrary reconstruction/query shapes are not safe on the production raw table.

### 7. Inference/prediction/sample linkage to quotes and edges

Latest stored prediction date: **2026-05-17**.

- 364 predictions, 52 players, 2 games;
- 364/364 predictions had a matching `daily_prediction_samples` row;
- all 364 sample rows had 10,000 samples;
- points/rebounds/assists each had 17/52 rows with selected line, paired prices, bookmaker, model probabilities, and edges;
- combo stats (`pra`, `pr`, `pa`, `ra`) had 0/52 priced rows each;
- only one row was recommended.

The probability implementation uses empirical MC CDF when samples are available: `(samples > line).mean()`. That invariant is preserved.

The incomplete priced coverage may reflect the limited slate/book/market state on May 17, but the stored schema cannot prove which exact quote snapshot generated each edge.

### 8. Current-date readiness and automation

On September 24, 2026:

- `daily_predictions`: 0 rows;
- `daily_prediction_samples`: 0 rows;
- latest persisted prediction/sample date: May 17, 2026;
- one `inference_job.py` execution at 14:15 UTC reported success in 4.75 seconds, consistent with a no-games/no-output path;
- repeated lines jobs succeeded but commonly required 331–376 seconds;
- repeated edge-refresh jobs reported success but commonly required 390–482 seconds, with one 821-second run.

A successful wrapper status is not output readiness. Today has no NBA regular-season slate, and edge refresh is doing substantial work despite zero NBA samples for today.

### 9. Frontend compatibility

Compatible surfaces:

- `daily_predictions` has the fields the current dashboard maps into `Prediction`;
- `get_prediction_dates` and `get_game_commence_times` functions exist;
- authenticated users have read policies on predictions and samples.

Blocking or misleading surfaces:

- dashboard cards depend on predictions with non-null `line`; current season has no tested output;
- analysis/book selection recomputes line edges using quantile interpolation, while backend production edge uses empirical MC samples. The displayed per-book edge can differ from backend probability semantics;
- raw quote queries lack a server-side latest paired-quote contract and temporal cutoff;
- public/anonymous raw-table read access is broader than the stated authenticated-client boundary;
- game-time fallback can mask missing persisted game-time/quote identity rather than proving a coherent selected quote.

## P0 findings

### P0-1 — Current props cannot join to NBA games

**Evidence:** 0/500,169 newest props have `game_id`; 0/100,001 newest game-line rows have `nba_game_id`.

**Impact:** no reliable prop → NBA game → inference join for the observed opening-night props.

**Smallest safe repair:** make the production linker CDN-only, load a wider bounded future schedule window appropriate for available props, and verify exact team/date linkage on a one-event dry run before any broad update. Do not use the historical ±90-day fuzzy rule for current production linking.

### P0-2 — Edge selection can use post-decision/post-commence quotes

**Evidence:** inference has no snapshot cutoff; edge refresh has only `snapshot_time > now() - 24 hours`; jobs continue during game hours and upsert predictions/place paper bets.

**Impact:** customer picks and recorded edges can change using live/post-start information.

**Smallest safe repair:** require an explicit decision timestamp and `COALESCE(market_last_update, snapshot_time) <= decision_time`, `snapshot_time <= decision_time`, and quote time `< commence_time`. Stop refreshing a game once its cutoff is reached.

### P0-3 — Frontend raw quote queries are not production-safe

**Evidence:** client-side history fetch/dedupe without timestamp cutoff or server-side latest pairing; sportsbook availability has no snapshot cutoff; reconstruction query timed out.

**Impact:** stale/incomplete lines, row-limit truncation, slow pages, and inconsistent prices.

**Smallest safe repair:** route the dashboard through a bounded RPC/view/API that returns latest paired pregame quotes for explicit game/player/market keys. No DDL/index work is authorized by this audit.

### P0-4 — Incremental team linkage can assign the wrong team

**Evidence:** `get_team_id()` returns the home team's ID first for every matched player/game.

**Impact:** away-player props can receive the home team's ID, corrupting joins and presentation.

**Smallest safe repair:** derive team from current roster/schedule/player context or leave `team_id` null when ambiguous; never use home-first fallback.

## P1 findings

1. **Selected quote identity is not persisted.** Add decision/quote timestamps and raw source identity to the prediction/edge contract before claiming reproducibility.
2. **Book coverage is thin in current data.** Only FanDuel, PrizePicks, and Fanatics appeared in the newest bounded prop window.
3. **Market coverage is uneven.** Blocks appeared for one event/book; steals and turnovers were absent in the sampled window.
4. **Combo predictions are not quote-linked in the latest stored run.** All 208 combo rows on May 17 lacked line/price/edge fields.
5. **High-frequency far-future scraping creates raw-table bloat.** Roughly 500k rows were added for three October 20 events before September 24.
6. **Scheduler work is close to/over cadence.** Lines jobs consume about 57% of their 10-minute interval; edge refreshes often consume 65%+ and can exceed the interval.
7. **Production linker attempts `stats.nba.com` before CDN fallback.** This violates the Railway CDN-only invariant even if exceptions are caught.
8. **Frontend per-book probability semantics diverge from backend.** `AnalysisModal` uses five-quantile interpolation instead of stored MC samples/empirical CDF.
9. **Raw props have anonymous read access.** This conflicts with the authenticated-client boundary and increases exposure of a massive operational table.
10. **Edge sample-age check references `mc_samples`, not `daily_prediction_samples`.** It fails non-fatally, so sample staleness is not actually verified.

## P2 findings

1. Replace deprecated `datetime.utcnow()` usage with timezone-aware UTC timestamps.
2. Add targeted tests for pre-commence quote cutoffs, persisted selected quote identity, away-player team linkage, PostgREST row-limit behavior, and combo quote linkage.
3. Make job metrics distinguish no-op success from output-producing success (`events`, inserted rows, linked rows, predictions, priced edges).
4. Reconcile misleading comments/docs that refer to a 26M-row raw table; production high ID is above 266M.
5. Use one canonical stat↔market mapping in backend and frontend rather than duplicated maps.

## Unknowns and caveats

- This lane did not call paid APIs, mutate data, run backfills, deploy, or add indexes.
- Counts on `raw_player_props_combined` were ID-windowed; no whole-table count was run.
- The newest 500,169-row window is not a full historical coverage statement.
- Exact latest-prediction quote reconstruction was not completed because two bounded attempts hit the 30-second statement timeout. The failure itself is recorded as query-safety evidence.
- No October 2026 inference output exists yet, so current-season end-to-end edge quality remains unverified.
- Railway deployment identity/config was not separately queried; production database job records demonstrate active executions but do not prove deployed commit identity.
- RLS policy existence was checked, but this audit did not perform a real signed-in browser session.

## Safe bounded next work package

1. **Fix/test the NBA linker contract**: CDN-only schedule lookup, bounded future dates based on observed prop commence dates, exact matchup/date joins, no guessed team ID.
2. **Add quote-cutoff contract** to daily inference and edge refresh, with focused tests and explicit decision time.
3. **Persist selected quote identity/timestamps** with predictions/edges.
4. **Replace frontend raw-table reads** with a bounded latest-paired-quote consumer.
5. Run a one-game read-only/preflight plus dry-run linkage/inference shadow check before any approved production update/backfill.

Any production updates, backfills, schema changes, or index changes require separate Chase approval.

## Commands executed

```text
date --iso-8601=seconds
venv/Scripts/python.exe C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_odds_audit_probe.py
venv/Scripts/python.exe C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_odds_audit_probe2.py
venv/Scripts/python.exe C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_odds_audit_probe3.py
venv/Scripts/python.exe C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_odds_jobs_probe.py
venv/Scripts/python.exe C:/Users/Chase/AppData/Local/hermes/cache/scratch/nba_frontend_db_probe.py
venv/Scripts/python.exe -m pytest tests/test_daily_player_props_scraper.py tests/test_daily_runner.py tests/test_nba_linker_local.py -q
```

Focused tests: **34 passed**, 4 deprecation/config warnings, 0 failures.

## SQL evidence

All SQL below ran inside an explicit read-only transaction with bounded statement/lock timeouts.

### Session and schema

```sql
SELECT current_database() AS database_name, current_user AS db_user,
       current_setting('transaction_read_only') AS transaction_read_only,
       now() AS database_now;

SELECT table_name, ordinal_position, column_name, data_type
FROM information_schema.columns
WHERE table_schema='public'
  AND table_name IN ('raw_player_props_combined','raw_game_lines_staging',
                     'daily_predictions','daily_prediction_samples')
ORDER BY table_name, ordinal_position;

SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname='public' AND tablename='raw_player_props_combined'
ORDER BY indexname;
```

### Bounded newest prop window

```sql
SELECT COUNT(*) AS rows,
       MIN(staging_id) AS min_id, MAX(staging_id) AS max_id,
       MIN(snapshot_time) AS min_snapshot, MAX(snapshot_time) AS max_snapshot,
       MIN(commence_time) AS min_commence, MAX(commence_time) AS max_commence,
       COUNT(DISTINCT api_game_id) AS api_events,
       COUNT(DISTINCT bookmaker) AS bookmakers,
       COUNT(DISTINCT market_key) AS markets,
       COUNT(*) FILTER (WHERE player_id IS NOT NULL) AS player_linked,
       COUNT(*) FILTER (WHERE game_id IS NOT NULL) AS game_linked,
       COUNT(*) FILTER (WHERE team_id IS NOT NULL) AS team_linked,
       COUNT(*) FILTER (WHERE snapshot_time >= commence_time) AS snapshot_at_or_after_commence,
       COUNT(*) FILTER (WHERE market_last_update >= commence_time) AS market_update_at_or_after_commence
FROM raw_player_props_combined
WHERE staging_id >= 265727251;

SELECT market_key, COUNT(*) AS rows,
       COUNT(DISTINCT api_game_id) AS events,
       COUNT(DISTINCT bookmaker) AS books,
       COUNT(*) FILTER (WHERE player_id IS NOT NULL) AS player_linked,
       COUNT(*) FILTER (WHERE game_id IS NOT NULL) AS game_linked
FROM raw_player_props_combined
WHERE staging_id >= 265727251
GROUP BY market_key
ORDER BY rows DESC;

SELECT bookmaker, COUNT(*) AS rows, COUNT(DISTINCT api_game_id) AS events,
       COUNT(DISTINCT market_key) AS markets
FROM raw_player_props_combined
WHERE staging_id >= 265727251
GROUP BY bookmaker
ORDER BY rows DESC
LIMIT 30;
```

### Latest paired quotes

```sql
WITH side_latest AS (
  SELECT player_id, api_game_id, game_id, bookmaker, market_key, line, outcome_label,
         odds_american, snapshot_time,
         ROW_NUMBER() OVER (
           PARTITION BY player_id, api_game_id, bookmaker, market_key, line, outcome_label
           ORDER BY snapshot_time DESC, staging_id DESC
         ) AS rn
  FROM raw_player_props_combined
  WHERE staging_id >= 265727251
    AND player_id IS NOT NULL
    AND outcome_label IN ('Over','Under')
), groups AS (
  SELECT player_id, api_game_id, game_id, bookmaker, market_key, line,
         MAX(odds_american) FILTER (WHERE outcome_label='Over' AND rn=1) AS over_odds,
         MAX(odds_american) FILTER (WHERE outcome_label='Under' AND rn=1) AS under_odds
  FROM side_latest
  WHERE rn=1
  GROUP BY player_id, api_game_id, game_id, bookmaker, market_key, line
)
SELECT COUNT(*) AS quote_groups,
       COUNT(*) FILTER (WHERE over_odds IS NOT NULL AND under_odds IS NOT NULL) AS paired_groups,
       COUNT(*) FILTER (WHERE over_odds IS NULL OR under_odds IS NULL) AS unpaired_groups,
       COUNT(*) FILTER (WHERE game_id IS NOT NULL) AS game_linked_groups
FROM groups;
```

### Bounded game-line window

```sql
SELECT COUNT(*) AS rows, MIN(staging_id) AS min_id, MAX(staging_id) AS max_id,
       MIN(snapshot_time) AS min_snapshot, MAX(snapshot_time) AS max_snapshot,
       MIN(commence_time) AS min_commence, MAX(commence_time) AS max_commence,
       COUNT(DISTINCT api_game_id) AS api_events,
       COUNT(DISTINCT bookmaker) AS books,
       COUNT(*) FILTER (WHERE nba_game_id IS NOT NULL) AS game_linked
FROM raw_game_lines_staging
WHERE staging_id >= 3173714;
```

### Predictions, samples, and current date

```sql
SELECT MAX(prediction_date) AS latest_prediction_date FROM daily_predictions;
SELECT MAX(prediction_date) AS latest_sample_date FROM daily_prediction_samples;

SELECT COUNT(*) AS rows,
       COUNT(*) FILTER (WHERE line IS NOT NULL) AS with_line,
       COUNT(*) FILTER (WHERE over_odds IS NOT NULL AND under_odds IS NOT NULL) AS with_paired_prices,
       COUNT(*) FILTER (WHERE over_edge IS NOT NULL OR under_edge IS NOT NULL) AS with_edge,
       COUNT(*) FILTER (WHERE bookmaker IS NOT NULL) AS with_bookmaker
FROM daily_predictions
WHERE prediction_date = DATE '2026-09-24';

WITH d AS (SELECT MAX(prediction_date) AS dt FROM daily_predictions)
SELECT prediction_date, stat, COUNT(*) AS rows,
       COUNT(*) FILTER (WHERE line IS NOT NULL) AS with_line,
       COUNT(*) FILTER (WHERE over_odds IS NOT NULL AND under_odds IS NOT NULL) AS paired_prices,
       COUNT(*) FILTER (WHERE bookmaker IS NOT NULL) AS with_bookmaker,
       COUNT(*) FILTER (WHERE over_prob IS NOT NULL AND under_prob IS NOT NULL) AS with_model_probs,
       COUNT(*) FILTER (WHERE over_edge IS NOT NULL AND under_edge IS NOT NULL) AS with_edges,
       COUNT(*) FILTER (WHERE prop_line IS NOT NULL) AS with_inference_prop_line,
       COUNT(*) FILTER (WHERE game_time IS NOT NULL) AS with_game_time,
       COUNT(*) FILTER (WHERE is_recommended) AS recommended
FROM daily_predictions, d
WHERE prediction_date=d.dt
GROUP BY prediction_date, stat
ORDER BY stat;

WITH d AS (SELECT MAX(prediction_date) AS dt FROM daily_predictions)
SELECT COUNT(*) AS predictions,
       COUNT(s.id) AS matched_samples,
       COUNT(*) FILTER (WHERE s.id IS NULL) AS missing_samples,
       COUNT(*) FILTER (WHERE s.n_samples=10000) AS samples_10k
FROM daily_predictions p
CROSS JOIN d
LEFT JOIN daily_prediction_samples s
  ON s.prediction_date=p.prediction_date AND s.player_id=p.player_id
 AND s.game_id=p.game_id AND s.stat=p.stat
WHERE p.prediction_date=d.dt;
```

### Job history, RLS, and frontend RPCs

```sql
WITH recent AS (
  SELECT id,job_name,status,started_at,ended_at,duration_seconds,error_message,metrics
  FROM job_executions ORDER BY id DESC LIMIT 1000
)
SELECT id,job_name,status,started_at,ended_at,duration_seconds,
       LEFT(COALESCE(error_message,''),300) error_excerpt,metrics
FROM recent
WHERE job_name ILIKE '%line%' OR job_name ILIKE '%infer%' OR job_name ILIKE '%edge%'
ORDER BY id DESC LIMIT 50;

SELECT tablename,policyname,roles,cmd,qual
FROM pg_policies
WHERE schemaname='public'
  AND tablename IN ('daily_predictions','raw_player_props_combined','daily_prediction_samples')
ORDER BY tablename,policyname;

SELECT routine_name,routine_type,data_type
FROM information_schema.routines
WHERE routine_schema='public'
  AND routine_name IN ('get_prediction_dates','get_game_commence_times')
ORDER BY routine_name;
```
