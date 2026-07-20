# 03 — Database, schema, and data-lifecycle audit

**Audit date:** 2026-07-18
**Mode:** file-only, read-only audit; this report is the only file written
**Scope:** tracked schema/migration/function files, database clients and repositories/query-bearing code, ingestion and archival writers, dashboard Supabase access, prior tech-debt reports `00-02`, `04-09`, `12-13`, and canonical remote GameFlowBrain schema guidance
**Excluded:** Supabase/Postgres/SQL-runner access, live schema/migration/count/index/policy inspection, secrets, migrations, destructive commands, code/config/plan/register/card edits

## Executive assessment

GameFlow does not currently have a reproducible tracked database authority. Canonical GameFlowBrain guidance correctly defines three distinct layers—live database truth, hand-written migration intent, and non-deployable generated snapshots—but the repository does not yet implement that separation as an enforceable contract. `database/schema.sql` is a stale and internally invalid snapshot, migrations are split across three roots and manual SQL-editor paths, multiple current code-owned tables and constraints have no tracked DDL, and the same RPC names have divergent definitions.

The highest-risk consequences are:

1. a clean environment cannot be reconstructed confidently from tracked files;
2. dashboard/authenticated access, service-role API access, and Python/postgres access are not represented by one auditable role/RLS/function-grant matrix;
3. high-volume prop ingestion is not idempotent, while archive/deletion behavior depends on untracked positional schema compatibility and conflicting retention defaults;
4. relational provenance and finality are weak for predictions, paper/live trades, queues, and derived logs;
5. Kalshi sports decommissioning cannot safely remove schema because sports, non-sports, Polymarket/arb, exposure history, and aggregate history have unresolved ownership and retention decisions;
6. no tracked backup/restore contract demonstrates recoverability.

Two invariants are non-negotiable throughout remediation:

- **Python backend code uses the `postgres` role; dashboard/client code uses `authenticated` with RLS.** Do not “simplify” the architecture by giving browser/dashboard clients backend credentials or by routing Python jobs through user-scoped RLS.
- **Never run non-concurrent `CREATE INDEX` on `raw_player_props_combined`.** Any future index work on that table requires a separately approved, non-transactional `CREATE INDEX CONCURRENTLY` procedure after read-only size/index/lock preflight. This report does not authorize or propose a live index operation.

## Evidence boundary and authority model

Canonical remote guidance was read over read-only SSH from `/home/chase/GameFlowBrain/schema.md` and `/home/chase/GameFlowBrain/RESOLVER.md`.

The canonical contract says:

- live production PostgreSQL is Layer 1 truth; `information_schema`, `pg_constraint`, `pg_indexes`, `pg_policies`, `pg_proc`, and migration history establish current state (`GameFlowBrain/schema.md:3-17`);
- hand-written SQL migrations are Layer 2 intent and the deployable contract; generated output is not authoritative (`GameFlowBrain/schema.md:20-28`);
- generated snapshots are Layer 3 diagnostics only, must be generated after successful deployment, and must never be deployed directly (`GameFlowBrain/schema.md:31-44`);
- GameFlow currently needs a migration baseline, integrity check, contract test, and generated schema snapshot (`GameFlowBrain/schema.md:47-53`);
- destructive operations are outside automated resolver scope, and DDL may only be proposed after live schema/index/constraint inspection (`GameFlowBrain/schema.md:70-89`);
- resolver states are `unresolved`, `resolved`, `deferred`, and `abandoned`; evidence must be appended rather than overwritten (`GameFlowBrain/RESOLVER.md:3-8,36-45`).

This audit therefore distinguishes:

- **Confirmed tracked-code finding:** directly visible in versioned files and current call paths.
- **Needs live SQL evidence:** current production object existence, applied migration order, grants/policies, row counts, index validity/usage, orphan counts, table size, and retention volume. No such evidence was collected here.

## Tracked authority inventory

| Surface | Confirmed tracked state | Authority implication |
|---|---|---|
| `database/schema.sql` | A 747-line snapshot that defines `raw_player_props_combined` three times and repeats two non-concurrent indexes (`:385-482`) | Not executable and not authoritative |
| `database/migrations/` | 31 SQL files, including duplicate migration number `003`, no single baseline, and function rewrites through `022` plus newer tables through `033` | Partial intent history, not a proven replay chain |
| `migrations/` | Four SQL files; two explicitly say “Run in Supabase SQL Editor (NOT via apply_migration)” (`kalshi_live_trading.sql:1-3`; `user_dfs_entries.sql:1-3`) | Separate manual deployment channel |
| `sql/functions/` | Two standalone RPC definitions, including a stale `get_sportsbook_lines` definition (`get_sportsbook_lines.sql:1-43`) | Competing function authority |
| Database client | One shared SQLAlchemy engine factory with statement/idle-transaction timeouts (`src/db/client.py:51-77,80-110`) | Useful runtime primitive, not schema authority |
| Dashboard clients | Browser/server anon-key clients are untyped; admin client uses service-role key (`dashboard/src/lib/supabase/client.ts:1-9`; `server.ts:1-31`; `admin.ts:1-17`) | Access correctness depends on live RLS/grants not represented by generated types |

## Findings

### DB-01 — CRITICAL — Schema authority and migration replay are fragmented and non-reproducible

**Confirmed tracked-code evidence**

- Canonical guidance explicitly separates live truth, hand-written migrations, and generated diagnostic snapshots (`GameFlowBrain/schema.md:3-44`), but the repository supplies no tracked manifest saying which files form the deployable chain or which files are snapshots/manual history.
- `database/schema.sql` creates `raw_player_props_combined` at `:385-406`, again at `:418-439`, and a third time at `:451-472`; it also repeats `idx_combined_dedupe` and `idx_props_api_game_id` (`:408-416,441-449,474-482`). The second `CREATE TABLE` would fail, so this cannot bootstrap a database.
- `database/migrations/` contains two unrelated `003` files (`003_dfs_paper_trading.sql`, `003_mlb_statcast_averages.sql`), then a non-contiguous sequence through `033`; filenames alone cannot establish one total application order.
- `migrations/kalshi_live_trading.sql:1-3` and `migrations/user_dfs_entries.sql:1-3` require manual SQL-editor execution outside the main migration root.
- `database/migrations/032_mlb_player_season_advanced_history.sql:6-10` says the original copy lived at `migrations/create_mlb_player_season_advanced_history.sql`; both remain tracked with separate text (`database/...:11-52`; `migrations/...:6-47`).
- `sql/functions/get_sportsbook_lines.sql:6-43` defines an older cast-based RPC, while `database/migrations/022_optimize_dfs_rpc.sql:70-120` defines the same RPC with ET bounds, a 48-hour snapshot cutoff, SQL/STABLE, and a 30-second timeout.
- Report 12 independently found that current Kalshi code relies on market, orderbook, paper, queue, cancellation, arb, Polymarket, and verified-link tables absent from the only tracked Kalshi DDL (`12-kalshi-deprecation-and-project-pruning.md:322-337`).

**Failure mode**

A new environment, disaster recovery, or decommission migration starts from the wrong file set. Bootstrap fails on duplicate DDL, RPC behavior regresses to an older full-scan definition, manual-only objects are omitted, or an operator assumes code-referenced constraints/policies exist when they were only applied interactively. Schema drift remains discoverable only after runtime failures.

**Severity:** Critical
**Confidence:** High for tracked fragmentation and replay failure; current production state needs live SQL evidence.

**Mitigation**

Adopt the canonical three-layer model explicitly: one immutable ordered migration ledger as deployable intent, one clearly labeled non-deployable generated snapshot, and one object/owner/consumer manifest. Quarantine standalone/manual SQL as historical or promote it into new forward-only migrations after an approved live preflight; never rewrite migration history to pretend it was always ordered. Add an empty-database replay test and post-replay schema-contract checks in CI using a disposable database.

**Existing-report interaction**

- Closes the database-authority coverage gap identified by report 00 (`00-existing-inventory-reconciliation.md:383-396`).
- Confirms report 12’s incomplete Kalshi DDL finding (`12:322-337`).
- Extends report 02: build/unit CI cannot verify a migration chain that is not defined.
- Uses report 08’s recovery finding rather than treating `sync_local_db.py` as schema authority (`08-infra-security-dependencies.md:68-106`).

**Needs live SQL evidence**

Applied migration ledger; object definitions and owners; table/view/function/policy/grant inventory; extensions; actual constraints/indexes; which manual files were applied; and drift between live objects and proposed baseline.

**Safe evidence step**

First create a file-only candidate manifest mapping every tracked DDL file to `deployable`, `historical/manual`, `superseded`, or `diagnostic`. Separately, in an approved isolated SQL-runner lane, collect read-only object fingerprints and migration history—no DDL and no counts beyond what is needed to classify objects.

**Done condition**

A disposable empty database replays one ordered chain successfully; schema-contract tests verify tables, columns, constraints, policies, grants, functions, and indexes; generated snapshots are reproducible and marked non-deployable; every runtime-owned object has one migration owner; and live drift is reported without automatic destructive repair.

---

### DB-02 — CRITICAL — Role, RLS, service-role, and `SECURITY DEFINER` boundaries are not one enforceable contract

**Confirmed tracked-code evidence**

- The intended role split is explicit: migration 018 says Python uses `postgres` and clients are blocked/default-scoped by RLS (`database/migrations/018_rls_lockdown.sql:1-4`). Browser/server dashboard clients use anon credentials and authenticated sessions (`dashboard/src/lib/supabase/client.ts:3-8`; `server.ts:4-30`).
- Migration 018 enables RLS on a fixed list of 18 tables (`:6-35`) and adds selected dashboard policies (`:37-50`), but later tables such as `mlb_player_props_clv_snapshots` are created without any RLS statement (`database/migrations/030_mlb_clv_snapshot_table.sql:10-97`; `031_mlb_clv_snapshot_linking.sql:5-27`).
- Early Statcast migrations create policies named “Allow public read” with unqualified roles and `USING (true)` (`database/migrations/003_mlb_statcast_averages.sql:64-65,130-131`). Migration 029 later describes similarly broad public-read policies as the retained policy owner (`database/migrations/029_phase1_io_cleanup.sql:24-37`).
- The browser bot tracker directly selects full rows from `kalshi_live_orders`, `kalshi_paper_bets`, and both daily-log tables (`dashboard/src/lib/hooks/useBotTracker.ts:29-97`), yet tracked Kalshi DDL says “No RLS needed — accessed by Python backend via postgres role” (`migrations/kalshi_live_trading.sql:1-3`). Those two contracts cannot both be sufficient.
- Kalshi API routes check only that a user exists, then use the service-role client. Any authenticated user can reach queue reads (`dashboard/src/app/api/kalshi/queue/route.ts:5-18`) and privileged mutations such as trade approval (`approve/route.ts:10-30,63-87`) or circuit-breaker resume (`resume/route.ts:5-21`). The page middleware’s admin check (`dashboard/src/lib/supabase/middleware.ts:77-84`) is not an authorization check inside these API handlers.
- `rebuild_user_daily_log(target_user_id)` is `SECURITY DEFINER`, deletes and rebuilds rows for the caller-supplied user, and is granted to `authenticated` without verifying `auth.uid() = target_user_id` (`database/migrations/026_track_record.sql:53-63,69-83,95-123`).
- All tracked `SECURITY DEFINER` functions inspected omit an explicit safe `search_path`; examples include the DFS/sportsbook functions (`database/migrations/022_optimize_dfs_rpc.sql:26-30,82-86`) and track-record functions (`026_track_record.sql:58-63,131-147`). Target objects are often unqualified.
- `get_sportsbook_lines_by_games` is granted to `anon`, `authenticated`, and `service_role` (`database/migrations/008_fix_rpc_game_id_dedup.sql:96-139`) and reads raw props through a definer function.

**Failure mode**

An authenticated dashboard user receives service-role effects through an API route; a definer RPC mutates another user’s derived history or resolves unqualified objects under an unsafe path; a newly created table falls outside the fixed RLS lockdown list; or a direct browser query is either unexpectedly blocked or broadly readable depending on untracked live grants/policies. The role invariant exists in prose but not as a complete, testable object matrix.

**Severity:** Critical
**Confidence:** High for route/RPC source behavior and missing tracked declarations; exploitability and current live policy/grant state need live SQL evidence.

**Mitigation**

Preserve the role invariant exactly:

- Python batch/backend connections remain `postgres` with bounded timeouts and no browser exposure.
- Dashboard/browser access remains `authenticated` and must be authorized by row-scoped RLS or narrowly authorized RPCs.
- Service-role route handlers must perform their own server-side admin/ownership authorization before constructing the admin client; middleware is defense-in-depth only.
- Every `SECURITY DEFINER` function needs an explicit safe `search_path`, schema-qualified references, least-privilege execute grants, and caller identity/authorization checks where user data or mutation is involved.
- Replace fixed “lockdown migration” thinking with a schema contract that fails when a client-reachable table lacks an explicit RLS/grant disposition.

**Existing-report interaction**

- Report 07 owns the dashboard authorization failure; this finding adds the database role/RLS/function side and must not create a competing fix (`07-dashboard-product.md:84-107`).
- Report 12 reclassifies Kalshi privileged routes for removal rather than hardening (`12:90-98,360-368`). Removal is preferred for retired sports paths, but surviving generic/user RPCs still need the contract above.
- Report 08 owns service-role configuration and secret handling; no secret values were read here.

**Needs live SQL evidence**

`relrowsecurity`/`relforcerowsecurity`; policies; table/function grants including PUBLIC defaults; function owner, `prosecdef`, `proconfig`, and definition; view security mode; API-exposed schemas; service-role use; and effective privileges for `anon`, `authenticated`, `service_role`, and `postgres`.

**Safe evidence step**

Add static tests that inventory tracked client `.from()`/`.rpc()` callsites and require a declared access owner. Characterize route authorization with mocked Supabase clients. In a separately approved read-only SQL lane, export only policy/grant/function metadata and compare it to the static matrix.

**Done condition**

Every dashboard table/RPC has an authenticated/RLS authorization contract; every backend-only object is inaccessible to browser roles; no authenticated-only API route can obtain service-role effects without explicit server-side authorization; definer functions have safe paths and caller checks; and CI fails on a new unclassified table/function/client callsite.

---

### DB-03 — HIGH — High-volume quote ingestion, idempotency, and query/index assumptions diverge

**Confirmed tracked-code evidence**

- `raw_player_props_combined` has only a primary key on `staging_id`; the index named `idx_combined_dedupe` is non-unique and covers only `(api_game_id, api_player_name, bookmaker, market_key, line)` (`database/schema.sql:385-416`). It omits side/outcome, price, and snapshot identity.
- NBA historical/live writers use bulk `INSERT` with no conflict handling (`src/scrapers/player_prop_scraper.py:219-236`; `daily_player_props_scraper.py:172-186,216-245`). Retrying the same API snapshot can append duplicate quote rows.
- MLB raw-prop writers have the same append-only behavior (`src/scrapers/mlb/mlb_player_props_scraper.py:183-203`; `mlb_daily_player_props_scraper.py:195-210`). By contrast, dense CLV snapshots use `ON CONFLICT DO NOTHING` (`mlb_daily_player_props_scraper.py:252-269`) backed by a tracked quote-identity unique index (`database/migrations/030_mlb_clv_snapshot_table.sql:66-79`).
- `kalshi_markets` uses an idempotent `(ticker, snapshot_time)` upsert in code (`src/scrapers/kalshi/kalshi_market_scraper.py:438-475`), but the required unique constraint/index has no tracked DDL in the inspected migration roots.
- `KalshiQueueService.propose_trades` inserts queue rows with no `ON CONFLICT` clause (`src/trading/kalshi/queue_service.py:65-112`), while paper bets do use `(game_date, ticker, side)` conflict handling (`src/paper_trading/kalshi_paper_trader.py:601-639`). This is current-code drift from report 06’s prior queue-idempotency description.
- Migration 022 assumes historical `game_id` values are normalized and removes `LPAD` from partition keys (`database/migrations/022_optimize_dfs_rpc.sql:1-10,42-56,97-110`), while current daily-runner code explicitly handles mixed 8- and 10-digit values to avoid index-breaking `LPAD` on the large table (`src/models/daily_runner.py:797-823`).
- The standalone RPC still casts `commence_time::date` (`sql/functions/get_sportsbook_lines.sql:20-33`), while the migration chain uses range predicates and snapshot cutoffs (`database/migrations/022_optimize_dfs_rpc.sql:46-51,101-106`).

**Failure mode**

Retries inflate storage and produce multiple logically identical rows; “latest” window queries can select nondeterministically among duplicates; queue retries create duplicate proposals or fail an entire transaction depending on an untracked live constraint; replayed stale RPC SQL reintroduces large scans; and mixed game IDs defeat the partition/index assumptions used by production queries.

**Severity:** High
**Confidence:** High for writer/query behavior; actual duplicate volume, constraints, and query plans need live SQL evidence.

**Mitigation**

Define an explicit identity/finality contract per dataset:

- raw quote identity must distinguish provider event, player, book, market, outcome, line/price semantics, and source snapshot/request identity;
- retries must either update a mutable observation or no-op on an immutable observation, never append accidentally;
- queue identity must be durable and enforced before side effects;
- normalized game IDs must be enforced at ingestion with characterization tests, not merely assumed by RPCs;
- each production query must name its expected predicate and supporting index in the schema contract.

Do not retrofit a non-concurrent index onto `raw_player_props_combined`. Prefer application-level characterization and future-write idempotency first. If a new index/constraint on that table is eventually justified, it must use a separately approved concurrent build/validation flow after live duplicate and lock preflight.

**Existing-report interaction**

- Extends report 04’s unverified per-target idempotency gap (`04-scheduler-ingestion-operations.md:460-465`).
- Report 06 owns exchange exactly-once semantics; this finding covers persistence identities and confirms current queue insert drift (`06-trading-market-safety.md:101-120`).
- Report 08’s local sync correctly stages and upserts by actual primary key (`08:87-92`), but that cannot repair weak production natural keys.

**Needs live SQL evidence**

Duplicate groups by candidate identity; null/format distributions; actual unique constraints; invalid/unused indexes; `EXPLAIN` plans; table/index size; queue duplicates; and whether live RPC definitions match migration 022.

**Safe evidence step**

Build file-only/pure fixtures that replay the same provider payload twice through each writer and state the expected row identity. Later, use isolated read-only aggregate queries to compare candidate keys without returning raw rows. Any destructive duplicate scope must be independently counted before an approved cleanup plan.

**Done condition**

Every scheduled writer has a tested retry contract backed by a tracked key/constraint where appropriate; duplicate replay is harmless; queue proposals are durable and idempotent; game IDs are normalized before persistence; current RPC definitions and query predicates have one owner; and high-volume index changes are concurrent, preflighted, and separately approved.

---

### DB-04 — HIGH — Retention, archival, and destructive-operation gates are inconsistent and schema-coupled

**Confirmed tracked-code evidence**

- The archive script’s module contract says rows older than seven days are moved (`src/orchestration/archive_old_props_job.py:3-15`), and `DEFAULT_RETENTION_DAYS = 7` (`:41-44`). The scheduler wrapper says it archives rows older than 30 days but calls the script without a retention argument (`src/orchestration/scheduler.py:719-721`). Actual default behavior is therefore seven days, not the scheduler’s stated 30.
- `archive_batch` selects by nullable `snapshot_time`, copies `SELECT r.*, now()` into an archive table, then deletes source rows in the same transaction (`archive_old_props_job.py:46-70`). No tracked DDL for `raw_player_props_archive` was found in `database/`, `migrations/`, or `sql/`; positional compatibility and destination uniqueness are not reproducible.
- The scheduled job has row/batch bounds but no dry-run or operator confirmation (`archive_old_props_job.py:73-113`). The one-time `scripts/run_full_archival.py` loops until empty, defaults to seven days, and has no max-row/approval gate (`:3-12,33-63,75-110`).
- Rows with `snapshot_time IS NULL` never satisfy the cutoff, while the tracked source schema allows `snapshot_time` to be null (`database/schema.sql:385-405`).
- Migration 028 performs two destructive duplicate-deletion passes before replacing the MLB paper-bet unique constraint (`database/migrations/028_mlb_paper_bets_dedup_constraint.sql:12-44`) without a tracked preflight count, backup artifact, lock timeout, or independent verification gate.
- Migration 033 demonstrates a safer pattern: explicit read-only preflight, short lock/statement timeouts, and a concurrent alternative for a large table (`database/migrations/033_rapidapi_injuries_nullable_team_unique_index.sql:1-32`).
- Local analytical full sync commits `TRUNCATE TABLE ... CASCADE` before import (`scripts/sync_local_db.py:411-480`); only dense MLB CLV has a special unbounded-full-refresh guard (`:500-549`).

**Failure mode**

A documentation mismatch silently shortens hot-data retention; positional archive inserts fail or mis-map after schema drift; nullable timestamps accumulate forever; an unbounded archival loop or migration deletes more rows than intended; or a local refresh leaves an analytical table empty after a committed truncate and later failure. “Archive” is mistaken for backup despite residing in the same database.

**Severity:** High
**Confidence:** High for tracked behavior; active schedules, archive schema, affected counts, and provider backup state need live evidence.

**Mitigation**

Create a data-class lifecycle registry that names owner, purpose, hot retention, archive retention, finality, deletion authority, legal/accounting/research hold, and restore class. Make retention values single-owned and passed explicitly by scheduler configuration. Replace positional archive copies with named columns and a tracked archive schema/key contract. Require dry-run/count preview, maximum scope, transaction/lock/time budgets, approval tokens for destructive/manual tools, and independent count verification before DB-adjacent deletion. Keep archive, analytical replica, logical backup, and provider PITR distinct.

**Existing-report interaction**

- Report 04 owns schedule/telemetry behavior and records the archive schedule (`04:75-102`); this report identifies the retention/schema semantics.
- Report 08 already establishes that archive and local sync are not backups (`08:68-106`); DB-07 adopts that finding.
- Report 12 forbids first-slice Kalshi deletion and requires independent destructive scope verification (`12:322-337`).

**Needs live SQL evidence**

Archive table definition/key; source/archive counts by age and null timestamp; active scheduler arguments; lock/index support for cutoff batches; orphan/duplicate risk; local replica use; provider retention/PITR; and whether migration 028 was applied.

**Safe evidence step**

Add pure SQL-shape/unit tests for named archive columns, max-row limits, explicit retention arguments, and failure rollback using fake connections. In a future approved read-only lane, independently count candidate source and destination scopes; do not archive or delete.

**Done condition**

Retention is explicit and consistent from policy through scheduler to query; every archive destination has tracked DDL and an idempotent key; nullable/unclassifiable rows have a defined disposition; destructive tools fail closed without bounded scope and approval; independent pre/post evidence is required; and recovery documentation never labels same-database archive or local sync as backup.

---

### DB-05 — HIGH — Data ownership, foreign keys, derived-state provenance, and finality are incomplete

**Confirmed tracked-code evidence**

- `mlb_daily_predictions` declares `id BIGSERIAL` but makes `(prediction_date, player_id, game_id, stat)` the primary key; `id` is not declared unique (`database/migrations/015_mlb_prediction_tables.sql:5-50`). `mlb_paper_bets.prediction_id` has no foreign key (`:71-93`), so it cannot safely identify that non-unique column.
- `mlb_daily_prediction_samples` duplicates prediction identity but has no foreign key to `mlb_daily_predictions` (`:58-69`). Prediction and sample lifecycles can diverge.
- `user_bets.prediction_id` is also not a foreign key (`database/migrations/012_user_bet_tracking.sql:20-41`) and is later made nullable (`026_track_record.sql:5-20`), weakening provenance further.
- `user_dfs_entries.user_id` references `auth.users` without `ON DELETE CASCADE` (`migrations/user_dfs_entries.sql:5-29`), unlike `user_profiles`, `user_bets`, and `user_bets_daily_log`, which cascade on account deletion (`database/migrations/012_user_bet_tracking.sql:7-22`; `026_track_record.sql:26-42`). User deletion semantics are inconsistent by product.
- MLB prediction/paper status and direction fields are comments rather than checked contracts (`database/migrations/015_mlb_prediction_tables.sql:71-93`). Kalshi live status has a check but omits exchange lifecycle states such as unknown/partial/settlement-pending (`migrations/kalshi_live_trading.sql:5-37`), consistent with report 06’s finality findings.
- Kalshi live-order uniqueness includes `placed_at` (`migrations/kalshi_live_trading.sql:33-40`), so it does not identify one logical request/queue proposal. The tracked live-order DDL has no FK/correlation to `kalshi_trade_queue` or an exchange request identity.
- Track-record daily logs are delete-and-rebuild derived state (`database/migrations/026_track_record.sql:53-120`) with no source-version or rebuild-run identity.
- Raw prop links (`game_id`, `player_id`, `team_id`) intentionally remain nullable and unconstrained in the tracked snapshot (`database/schema.sql:385-405`), but no tracked ownership contract distinguishes unresolved staging links from durable referential truth.

**Failure mode**

Paper bets or samples outlive/mismatch their generating prediction; account deletion is blocked by one product while silently cascading another; derived logs cannot prove which source snapshot produced them; status labels imply terminal certainty that the provider has not established; and queue/order records cannot prove one-to-one execution lineage. Cleanup code cannot determine authoritative owner versus disposable derivative.

**Severity:** High
**Confidence:** High for tracked constraints and source semantics; actual orphan counts/constraints and provider finality need live evidence.

**Mitigation**

Define ownership for each table as source-of-truth, immutable observation, mutable operational state, derived cache/log, or archive. Use stable run/request/source IDs and tracked foreign keys where lifecycle coupling is real; where staging performance requires deferred/no FKs, document the resolver, unresolved state, validation cadence, and deletion order. Make account deletion semantics explicit per user-owned table. Model provider finality separately from local workflow status. Derived aggregates must carry source range/version and rebuild identity or be safely reproducible from retained source rows.

**Existing-report interaction**

- Report 05 owns model artifact/promotion identity; database prediction/sample lineage is the persistence side of that contract.
- Report 06 owns Kalshi execution/finality semantics (`06:101-168` and later findings); report 12 converts those to archival/closeout requirements rather than new sports feature work.
- Report 07 owns user-facing account/history behavior; this finding supplies the FK/deletion contract.

**Needs live SQL evidence**

Orphan counts; actual unique/FK/check constraints; auth-user deletion blockers; prediction/sample/paper lineage coverage; pending/terminal status distributions; queue/order correlation; and raw-link unresolved rates.

**Safe evidence step**

Create a file-only ownership matrix and pure contract fixtures for deletion order, derived rebuild identity, and status transitions. Later collect read-only orphan/finality aggregates in the isolated SQL lane. Before any destructive cleanup, independently verify every candidate count as required by `AGENTS.md`.

**Done condition**

Every table has one lifecycle owner/class; provenance IDs are stable and constrained where appropriate; user deletion behavior is intentional and tested; derivatives are reproducible and traceable; unresolved staging links are explicit; and no local terminal label is treated as authoritative exchange finality without provider evidence.

---

### DB-06 — HIGH — Kalshi sports schema removal is blocked by unresolved mixed-domain and retention decisions

**Confirmed tracked-code evidence**

- Report 12 correctly says sports Kalshi is a decommission target, while non-sports Kalshi remains a separate product decision (`12-kalshi-deprecation-and-project-pruning.md:5-26,56-66`).
- The non-sports job writes economics/macro markets into the same Kalshi storage with `sport=NULL` for Polymarket matching (`src/orchestration/kalshi_nonsports_refresh_job.py:1-16,42-69`).
- `store_markets` writes sports/non-sports through one `kalshi_markets` upsert keyed by `(ticker, snapshot_time)` (`src/scrapers/kalshi/kalshi_market_scraper.py:438-505`).
- Sports orderbook history is stored by ticker only; `kalshi_orderbook_snapshots` insert rows do not carry `sport`, so classification depends on joining to market history (`src/orchestration/kalshi_refresh_job.py:441-469,490-519`).
- The tracked live daily log is keyed only by `game_date` and has no sport or source dimension (`migrations/kalshi_live_trading.sql:42-57`). Singleton halt/config is global (`:59-74`). These rows cannot be assumed to belong cleanly to one sport from DDL alone.
- Arb records are provider-paired by design: selection joins `arb_opportunities` to `arb_paper_bets` on Kalshi ticker, Polymarket condition, and Kalshi side (`src/paper_trading/arb_paper_trader.py:53-123`). Polymarket/arb data is therefore not safely removable merely because Kalshi sports is retired.
- Report 12’s data disposition retains live orders/fills/fees/queues/cancellations/settlement/config as read-only evidence; archives paper and analysis data; and leaves market/orderbook, arb, verified links, and Polymarket to product/retention decisions (`12:322-337`).

**Exact schema/data decisions blocking removal**

1. **Non-sports product decision:** retire non-sports Kalshi, retain it read-only for research, or retain an active read-only collector. `sport IS NULL` cannot be deleted as sports.
2. **Polymarket decision:** retire all prediction-market integrations or establish a standalone Polymarket owner after decoupling Kalshi-derived linking/contracts.
3. **Arb decision:** archive paired-provider evidence or retain a newly scoped research lane; current arb rows/links are not provider-independent.
4. **Exposure/finality decision:** establish whether any resting, partial, accepted-but-unrecorded, queued, cancellation-pending, or unresolved positions exist. Local statuses are non-authoritative per report 06/12.
5. **Retention decision:** set legal/accounting/tax, incident, research, and model-evidence retention periods for live orders/fills/fees and paper/market snapshots.
6. **Mixed-history classification:** define how ticker-only orderbook rows, date-only daily aggregates, singleton config/halt state, and any NULL/unknown sport rows are assigned or retained.
7. **Schema dependency decision:** identify live RPCs, views, policies, grants, indexes, triggers, functions, and consumers absent from tracked DDL before proposing object removal.
8. **Archive format/owner decision:** choose a read-only, restorable, access-controlled archival representation with integrity metadata; do not equate leaving mutable production tables in place with archival completion.
9. **Deletion gate:** after all above, independently verify row/object scope and dependency order. No table, row, index, policy, RPC, or secret deletion belongs in the first containment slice.

**Failure mode**

A “drop Kalshi” migration destroys retained non-sports or Polymarket/arb evidence, removes accounting/incident records before retention expires, strands orderbook/history rows without classification, or removes lifecycle objects while exposure remains. Conversely, indefinite retention leaves privileged controls and large mixed history active without an owner.

**Severity:** High
**Confidence:** High for tracked co-location/coupling and unresolved decisions; current exposure, volume, live objects, and legal requirements need external/live evidence.

**Mitigation**

Follow report 12’s dependency order: contain sports capability first without DB deletion; perform separately approved exposure closeout; remove consumers; adjudicate non-sports/Polymarket/arb; then inventory schema and implement retention/archive decisions in a dedicated DB-safe plan. Do not invest in new Kalshi sports schema hardening except what is strictly needed for evidence preservation/closeout.

**Existing-report interaction**

This finding reconciles and adopts report 12 rather than superseding it (`12:322-411`). Report 06’s live finality defects mean DB statuses cannot prove zero exposure. Report 07’s privileged Kalshi routes should be removed/contained before schema cleanup. Report 08’s missing backup contract means archival claims need independent recovery evidence.

**Needs live SQL evidence**

Counts/sizes by sport/NULL/ticker/time/status; object dependencies; pending/unknown exposure records; queue/order/fill relationships; policy/grant state; and provider-authoritative exposure. Legal/accounting retention is a human decision, not a SQL fact.

**Safe evidence step**

Produce a file-only table/column/consumer classification from tracked code. After explicit approval, use isolated read-only SQL to collect bounded aggregates and dependency metadata; verify destructive candidate counts independently. Exposure finality requires a separate operations/provider closeout lane, not inference from DB rows.

**Done condition**

Sports, non-sports, Polymarket, and arb ownership is explicit; zero exposure is independently established from authoritative provider evidence; retention periods and archive owners are approved; mixed/NULL history is classified; all consumers and object dependencies are inventoried; preserved evidence is restorable and read-only; and any later removal migration is bounded, reviewed, reversible where possible, and independently verified.

---

### DB-07 — HIGH — Backup/restore and disaster-recovery evidence is absent

**Confirmed tracked-code evidence**

- Targeted tracked searches found no `pg_dump`, `pg_restore`, provider PITR, restore drill, RPO, RTO, or disaster-recovery procedure. Report 08 independently recorded the same bounded absence (`08-infra-security-dependencies.md:50-55,68-106`).
- `sync_local_db.py` is an analytical replica utility, reflects only a registered subset, strips foreign keys when creating local tables, and can commit truncate before a later import failure (`scripts/sync_local_db.py:411-480,500-622`; report 08 exact inventory at `:72-79`).
- The utility logs failed tables and continues, then exits normally (`scripts/sync_local_db.py:586-622`).
- Production archive moves rows within the same database transaction/project (`src/orchestration/archive_old_props_job.py:50-70`); it is retention, not independent recovery.
- CSV prediction exports preserve selected outputs, not roles, policies, functions, schema, or complete source data (`docs/daily_pipeline_automation.md:239-242,313-315`).

**Failure mode**

Production loss/corruption occurs and no tested process can restore schema, security objects, functions, all data classes, and point-in-time state. A partial local analytical copy or same-project archive is accepted as backup, masking missing tables/security and successful-exit partial failures.

**Severity:** High
**Confidence:** High for tracked evidence absence; provider-managed backups/PITR and untracked organizational procedures need live/operational evidence.

**Mitigation**

Adopt report 08 I-01: a tracked recovery contract must distinguish logical backup, provider PITR, local analytical replica, same-database archive, and exported artifacts; enumerate protected assets; define owner, encryption/access, retention, RPO/RTO, restore order, integrity checks, and a disposable restore drill. The local sync tool must fail nonzero on partial failure and must not be treated as recovery.

**Existing-report interaction**

This is **confirmed unchanged / adopted from report 08 I-01**, not a duplicate competing recommendation (`08:68-106`). DB-01 adds schema/policy/function replay requirements; DB-04 adds retention/archive distinctions; report 12 adds Kalshi evidence-retention requirements.

**Needs live SQL evidence**

Provider backup/PITR settings and retention; database size/restore duration; protected extensions/storage/auth assets; last successful backup; and any existing external backup ownership. No live inspection occurred.

**Safe evidence step**

Create a paper recovery inventory using tracked schema plus provider documentation, then design a disposable non-production restore drill. Do not use production credentials/data or run a restore under this audit.

**Done condition**

A tracked runbook defines RPO/RTO and all protected assets; a disposable restore proves schema, data, policies, grants, functions, triggers, and integrity; backup evidence is monitored; and local sync/archive are explicitly excluded from backup claims unless independently upgraded and tested.

---

### DB-08 — MEDIUM — Generated schema/types and database contract verification do not protect consumers from drift

**Confirmed tracked-code evidence**

- Canonical guidance calls for a generated schema snapshot after deploy and forbids deploying that snapshot directly (`GameFlowBrain/schema.md:31-53`). The current `database/schema.sql` is neither reproducibly generated nor executable (`:385-482`).
- Dashboard Supabase clients do not pass a generated `Database` generic (`dashboard/src/lib/supabase/client.ts:1-9`; `server.ts:1-31`; `admin.ts:1-17`). Searches found no tracked generated database type file. Domain types such as Kalshi bot-tracker types are handwritten (`dashboard/src/types/bot-tracker.ts`), while query results are cast (`dashboard/src/lib/hooks/useBotTracker.ts:29-97`).
- `get_sportsbook_lines` has divergent standalone and migration definitions (`sql/functions/get_sportsbook_lines.sql:1-43`; `database/migrations/022_optimize_dfs_rpc.sql:70-120`).
- `get_games_for_date` documents a required concurrent index but does not own a migration that guarantees it (`sql/functions/get_games_for_date.sql:1-22`).
- Migration 023 creates dashboard views without a tracked generated row contract or explicit view security mode (`database/migrations/023_mlb_stats_vault_views.sql:21-120`).
- Current CI/report 02 has no database migration replay/schema-contract stage and no dashboard tests; dashboard verification is build-only (`02-testing-ci-verification.md:109-136`).

**Failure mode**

A column/RPC/view changes and TypeScript casts continue compiling; a standalone function is replayed over the optimized version; a required index is absent or invalid; or a view’s effective security/shape differs from consumer assumptions. Drift is detected through production errors rather than generation/replay checks.

**Severity:** Medium
**Confidence:** High for tracked verification gaps; current live generated/openapi types, function definitions, and indexes need live SQL evidence.

**Mitigation**

After DB-01 establishes one migration chain, generate two non-deployable artifacts in CI from a disposable replay: a normalized schema fingerprint and Supabase TypeScript types. Compile dashboard clients against generated types instead of unchecked casts. Add static contracts for RPC signatures, view columns/security disposition, table access owners, and required query indexes. Generated artifacts diagnose drift; hand-written migrations remain the deployable authority.

**Existing-report interaction**

- Extends report 02’s dashboard and contract-test gaps without replacing its broader test strategy.
- Implements the generated-snapshot requirement from canonical `schema.md`.
- Supports report 07 by making dashboard data contracts visible; authorization still requires runtime/RLS tests.

**Needs live SQL evidence**

Current RPC signatures/definitions/grants; view columns/security options; PostgREST-exposed schema; actual generated types; and required index existence/validity.

**Safe evidence step**

Replay candidate migrations only in a disposable local database once DB-01’s manifest exists, generate types/fingerprint there, and compare to tracked consumer calls. Do not generate from or mutate production during this audit.

**Done condition**

Disposable replay produces deterministic schema/type artifacts; dashboard builds against generated database types; RPC/view/table contracts and access owners are checked; required index contracts are testable; and CI reports drift without deploying generated SQL.

## Confirmed findings versus live-evidence queue

| Topic | Confirmed from tracked files | Needs live SQL / external evidence |
|---|---|---|
| Schema authority | Fragmented roots, invalid snapshot, duplicate/stale definitions, missing tracked Kalshi DDL | Applied migration ledger and live object fingerprint |
| Roles/RLS | Intended postgres/authenticated split; missing/inconsistent tracked RLS; under-authorized service-role routes; unsafe definer patterns | Effective policies, grants, owners, search paths, exposed schemas |
| Raw props | Append-only non-idempotent writers; non-unique “dedupe” index; mixed-ID assumption conflict | Duplicate counts, formats, query plans, index validity/size |
| Retention/deletion | 7-day versus 30-day drift; positional archive; unbounded/manual destructive paths | Archive DDL, age/null volumes, active schedule args, lock impact |
| Ownership/FKs/finality | Missing lineage FKs/unique IDs; inconsistent user deletion; weak terminal/provenance semantics | Orphan/status counts, actual constraints, provider finality |
| Kalshi removal | Sports/non-sports co-location, ticker/date-only history, paired arb/Polymarket dependencies | Exposure, counts/sizes, object dependencies, retention decisions |
| Recovery | No tracked backup/restore contract; sync/archive are not backups | Provider PITR/backups, RPO/RTO, restore evidence |
| Generated contracts | No reproducible schema snapshot/types; divergent RPC definitions | Live RPC/view/index fingerprints |

## Priority order

1. **Contain retired Kalshi sports control paths per report 12 without DB deletion.** This is a product/security removal action owned by existing reports, not a schema mutation from this audit.
2. **Define the migration/object/access manifest (DB-01/DB-02) using files only.** Do not baseline by guessing from stale SQL.
3. **Collect separately approved read-only live metadata through the isolated SQL-runner lane.** No main-context Supabase access, no mutations, no broad row export.
4. **Establish disposable migration replay plus schema/RLS/RPC/type contracts.** This creates a safe place to test forward migrations.
5. **Characterize idempotency, provenance, retention, and archive contracts with pure/fake-DB tests.** Avoid large-table DDL.
6. **Approve data ownership/retention/recovery decisions, especially Kalshi mixed history.** Human/legal/accounting/product decisions precede cleanup.
7. **Only then plan forward-only schema remediation.** Destructive scopes require independent counts; `raw_player_props_combined` index work is concurrent-only and separately approved.

## Rejected or deferred interpretations

- **Rejected:** `database/schema.sql` is a deployable baseline. Its repeated table/index DDL proves otherwise.
- **Rejected:** migration filenames prove production application order. Multiple roots/manual channels and duplicate numbers prevent that inference.
- **Rejected:** RLS-enabled means correctly authorized. Policies, grants, definer functions, views, and service-role routes jointly determine access.
- **Rejected:** a “dedupe” index means writes are idempotent. The raw-prop index is non-unique and writers append without conflict handling.
- **Rejected:** local database sync or same-project archive is a backup. Neither demonstrates independent recoverability.
- **Rejected:** local `won`/`lost`/`filled`/`cancelled` proves exchange finality. Reports 06 and 12 establish contrary failure modes.
- **Rejected:** all Kalshi-named tables can be dropped with sports decommission. Non-sports, Polymarket/arb, mixed history, exposure, and retention decisions block that conclusion.
- **Deferred:** exact live drift, duplicate/orphan counts, index recommendations, object drops, and policy changes. They require approved live read-only evidence first.

## Validation record

Validation for this audit is intentionally limited to:

- report-path/content checks for `.hermes/audits/tech-debt/03-database-schema-data-lifecycle.md`;
- `git diff --check -- .hermes/audits/tech-debt/03-database-schema-data-lifecycle.md`;
- scoped diff inspection of this report only.

No database connection, migration, SQL runner, count query, secret read, code/config/plan/register/card edit, or live mutation was performed.
