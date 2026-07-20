# Performance, Storage, and Cost Audit

**Audit date:** 2026-07-18

**Mode:** read-only tracked-repository audit; this report is the only file written

**Scope:** repository and deploy weight; tracked/generated residue; model-artifact duplication; cache and retention ownership; dashboard/API query fan-out and unbounded reads; scheduler frequency and duplicate compute; Python process/import startup; storage growth; logging retention; package/build context; and cleanup ownership.

**Prohibited and not performed:** no DB, network, live-service, secret, billing, Railway/Vercel runtime, package install, build, workload, backtest, training, scrape, model load, or source/config/plan/register/card edit.

## Executive verdict

The largest measured cost is already a pruning problem, not a refactor problem. The tracked checkout is **220,023,222 bytes across 1,432 files**. Named non-production NBA/MLB artifact groups account for **168,994,105 bytes (76.81%)**. SHA-256 inventory found **74,754,388 bytes (71.29 MiB)** of extra byte-identical model copies across 23 duplicate groups. Nixpacks explicitly copies the checkout into `/app`, so these artifacts inflate Railway source/image transfer and storage even though runtime references only the current NBA production/playoff targets and the MLB production target.

The second no-refactor cut is Kalshi decommissioning. Report 12's static total of **1,532 configured triggers/day** reconciles exactly to current scheduler cron expressions. Of those, **1,442/day** are sports refresh, summary/resolution, or sports live-order lifecycle triggers; the remaining **90/day** are the unresolved non-sports refresh. These cuts must be credited before sizing a scheduler-registry migration.

The strongest supported-runtime performance issue is the dashboard performance page. Its default mount starts four query hooks and can issue at least **eight Supabase requests** before user interaction: props log/bets plus recommendation enrichment, DFS entries/log, My Bets, and a duplicate track-record log/bets pair. All histories are unbounded, two hooks independently read the same resolved `user_bets`, and React Query can retain the full payloads for 30 minutes. This combines request fan-out, duplicate reads, browser aggregation, and growth without a bounded contract.

Other confirmed risks are exact MLB lines schedule collisions, a redundant NBA 10:00 trigger, an archive process with a daily 500,000-row ceiling but no archive-table retention owner, and 20 orchestration modules that append to unrotated local log files while also emitting stdout. Import/startup overhead is structurally plausible—high-frequency scheduler triggers launch fresh Python processes, and the NBA edge job imports NumPy/Pandas and a large policy graph at module import—but actual startup milliseconds/CPU billing remain runtime evidence gaps.

No report 03 exists in the working tree. Database size, query plans, live row rates, archive growth, and index behavior therefore remain unmeasured and are not presented as repository facts.

## Required reconciliation before findings

### Report 13 artifact accounting

The report-13 accounting was independently reproduced from `git ls-files` plus filesystem sizes without traversing ignored heavy directories:

| Tracked group | Files | Bytes | Disposition interaction |
|---|---:|---:|---|
| Entire tracked checkout | 1,432 | 220,023,222 | Measured baseline |
| `src/` | 379 | 209,449,975 | 95.19% of checkout; model binaries dominate |
| NBA `production/` | 5 | 13,520,158 | Retain until manifest/rollback replacement |
| NBA `production_playoffs/` | 7 | 7,870,605 | Retain; active runtime references exist |
| NBA old/bad/archived backups | 28 | 70,841,349 | BP-02 archive candidate |
| Tracked NBA `nba_run_*` | 22 | 39,588,968 | BP-02 archive after resolver/promotion fix |
| NBA `hybrid_pts_test/` | 6 | 20,887,809 | BP-02 archive candidate |
| Old NBA run zip | 1 | 3,746,096 | BP-02 archive candidate |
| MLB production | 20 | 15,842,116 | Retain required-stat subset pending manifest |
| MLB non-production experiments | 24 | 33,929,883 | BP-02 archive candidate |
| Named non-production total | 81 | 168,994,105 | 76.81% of tracked checkout |

This audit credits BP-02/BP-03 rather than proposing a new artifact-store refactor. Archive/remove only after report 05's manifest, explicit identity, and rollback gates exist. Current production artifacts and applied migration history remain explicit retains.

Content hashing adds one new measured dimension: 113 tracked model-artifact files contain 23 duplicate-content groups with 74,754,388 extra bytes. Examples include:

- the same 8,576,248-byte `threes_multiclass_model.joblib` in `hybrid_pts_test/`, `production_archived_20260305/`, `production_bad_20260305/`, and `production_old_20260210/` (three redundant copies; SHA-256 prefix `065532e264f1e1e1`);
- the same 6,668,645-byte `reb_rate_model.joblib` in `hybrid_pts_test/`, `production_archived_20260305/`, and `production_old_20260210/` (two redundant copies; prefix `ff2d98a5ebefb70c`);
- production NBA models duplicated in named run directories, including `production/reb_rate_model.joblib` and `nba_run_20260323_212931/reb_rate_model.joblib` (7,728,084 bytes; prefix `ff5cc9ba480f0cdb`).

Hashes establish byte identity only. They do not prove that an artifact can be deleted safely or that separate provenance records are interchangeable.

### Report 12 Kalshi trigger and surface removal

Current scheduler expressions at `src/orchestration/scheduler.py:1171-1262` reproduce report 12's 1,532 daily trigger total:

| Trigger family | Triggers/day |
|---|---:|
| resolution + summary | 2 |
| MLB + NBA sports refresh | 180 |
| non-sports refresh | 90 |
| approved execution | 450 |
| repricing | 225 |
| pending-fill polling | 180 |
| stale-fill polling | 180 |
| cancellation execution | 225 |
| **Total** | **1,532** |

The approved sports decommission boundary can remove **1,442/day** before any scheduler registry refactor. The 90/day non-sports schedule remains a Chase product decision. Closeout-dependent fill/cancel/settlement jobs must not be removed until report 12 Phase 2 establishes authoritative zero exposure. This audit does not recommend optimizing or hardening retired Kalshi jobs.

Dashboard polling cuts are also credited. `dashboard/src/lib/hooks/useBotTracker.ts:102-124` has three 60-second polling hooks, `useArbScanner.ts:47-95` has three more, and `useTradeQueue.ts:41-44` polls every 15 seconds. Report 12 owns removal of the sports controls and product surfaces; do not optimize these retired pollers first.

## Findings

### PSC-01 — High — Non-production model binaries dominate the deployable checkout, with 71.29 MiB of proven duplicate copies

**Severity:** High
**Confidence:** High

**Exact evidence**

- Tracked-file accounting above: 168,994,105 bytes in named non-production artifact groups, 76.81% of the 220,023,222-byte tracked checkout.
- SHA-256 inventory: 23 duplicate groups and 74,754,388 extra duplicate bytes among 113 tracked model-artifact files.
- `nixpacks.toml:6-7` says the build performs `COPY . /app`; `:15-19` then creates/install dependencies under `/app`, and `:25-26` starts the scheduler there.
- `src/orchestration/scheduler.py:588-589` and the usage inventory reconciled in report 13 show active NBA production/playoff targets; report 05 MMP-02/MMP-08 shows artifact identity and packaging are not yet manifest-enforced.
- Report 13 BP-02 already owns archival sequencing; BP-15 explicitly retains current production targets.

**Concrete cost/performance failure mode**

Every Railway clean checkout/image context carries experiment, bad, old, archived, run-copy, and zip payloads that the scheduler does not need. This increases clone/upload/build-layer size and image storage; any source change can invalidate layers around a 220 MB checkout. Git also stores byte-identical binaries under multiple paths, increasing clone/history burden. Actual Railway transfer seconds and storage billing were not observed.

**Current mitigation**

Git provides history and current runtime paths are explicit. `.gitignore:77-82` ignores some future run naming families. Those controls do not remove already tracked binaries or cover all old/bad/hybrid names.

**Report interaction**

Confirm/extend report 13 BP-02 with content-hash evidence; depend on report 05 MMP-02/MMP-05/MMP-08 before moving artifacts. Do not create another artifact migration plan.

**Safe evidence step**

Create a values-free archive ledger from tracked paths: path, size, SHA-256, run/manifest identity, literal references, classification, and intended retention. Verify external immutable readback/checksums and a named rollback retrieval without loading binaries or building/deploying.

**Done condition**

Only manifest-declared deployable suites remain in the active checkout; historical artifacts have immutable IDs, checksums, owner, retention, and verified retrieval; promotion/backtest never discovers candidates lexically; clean-clone artifact validation passes; and Railway build context excludes archived runs without losing rollback.

### PSC-02 — High cheap win — Kalshi product removal eliminates up to 1,442 sports/lifecycle triggers per day before scheduler refactoring

**Severity:** High current waste / removal priority
**Confidence:** High for configured frequency; runtime work per no-op is unmeasured

**Exact evidence**

- `src/orchestration/scheduler.py:1171-1205` registers daily summary/resolution and two sports refreshes.
- `src/orchestration/scheduler.py:1217-1262` registers approved execution every two minutes, repricing/cancellation every four minutes, and two fill checks every five minutes across 15 hours.
- `src/orchestration/scheduler.py:1207-1215` separately registers the unresolved non-sports refresh (90/day).
- Report 12 lines 7-26 and 351-412 define containment, closeout, surface removal, and anti-regrowth order; report 04 E-09 shows disabled jobs can be recorded as successful no-ops.

**Concrete cost/performance failure mode**

Even when child gates prevent exchange actions, APScheduler invokes wrappers, fresh Python interpreters import modules, jobs inspect state/gates, logs are emitted, and execution records may be written. Polling a retired product consumes scheduler threads, CPU, DB/API checks where gates permit them, logs, and operator attention. Refactoring all jobs into a registry first would spend engineering effort preserving 1,442 triggers/day that should disappear.

**Current mitigation**

New-trade gates default off and report 12 requires lifecycle retention until exposure closeout. These mitigate trading risk, not trigger/process/log cost.

**Report interaction**

This is a quantified sequencing credit for report 12, not a new Kalshi repair. Refresh Lane 07's post-cut inventory only after decommission boundaries are approved.

**Safe evidence step**

Use a static scheduler inventory test to classify each registration as sports-remove, closeout-only, non-sports-decision, or retained. After approved code removal, verify registered IDs from scheduler construction without starting jobs or calling providers.

**Done condition**

No sports refresh, new-order, summary, approval, reprice, fill, cancel, or settlement schedule remains after authoritative closeout; non-sports is explicitly retained or removed; scheduler inventory mechanically enforces the decision; and deployed registration evidence shows the corresponding trigger count is zero.

### PSC-03 — High — Performance page mounts at least eight requests, duplicates resolved-bet reads, and retains unbounded histories in browser cache

**Severity:** High
**Confidence:** High for source fan-out/unboundedness; runtime rows/latency unknown

**Exact evidence**

- `dashboard/src/app/(protected)/performance/page.tsx:63-77` mounts props, DFS, My Bets, and track-record hooks regardless of `activeTab`.
- `dashboard/src/lib/hooks/usePerformanceData.ts:19-53` performs two full-history props reads and a conditional third prediction-enrichment read; none has date/range/limit.
- `usePerformanceData.ts:105-123` performs two unbounded DFS-history reads.
- `usePerformanceData.ts:133-160` reads all resolved `user_bets` for sport stats.
- `dashboard/src/lib/hooks/useTrackRecordData.ts:191-225` independently reads the same resolved `user_bets` plus its daily log for the default `my_bets` source; `:228-248` does the same for paper data; combined mode performs both pairs at `:253-278`.
- `QueryProvider.tsx:9-16` keeps query data for 30 minutes; domain hooks use 10-minute stale times. Different query keys mean My Bets and track record do not deduplicate each other.
- `performance/page.tsx:96-247` performs KPI, chart, and breakdown aggregation client-side over returned histories.

**Concrete cost/performance failure mode**

Opening the default props tab starts at least eight Supabase requests (three props, two DFS, one My Bets, two track record) and transfers histories for inactive tabs. The same resolved user-bet history is fetched twice under different query keys, while props/paper histories can also overlap by source. Payload, browser memory, aggregation/render CPU, query time, and egress rise with every retained bet/log row. Failures in several reads become empty arrays and plausible zero KPIs, as report 07 F-07 already records.

**Current mitigation**

React Query avoids repeated focus refetches and caches data for a session. Parallel requests reduce wall-clock latency. Neither bounds payload or prevents inactive/duplicate reads.

**Report interaction**

Confirms and quantifies report 07 F-07; reinforces report 02 TV-03/F-12 test-harness prerequisite. This should update Lane 10's performance slice, not create a parallel dashboard refactor.

**Safe evidence step**

With mocked Supabase only, count query calls and returned rows for each active tab/source. Assert inactive hooks are disabled and establish golden KPI parity for bounded fixtures. Separately propose server aggregate/range contracts; do not query production to choose limits.

**Done condition**

Only active/visible domains fetch; duplicate source histories share one query/repository contract; every list has an explicit date/range/page/aggregate bound; errors differ from valid empty data; cache keys and retention are intentional; and tests cap request counts and preserve KPI definitions.

### PSC-04 — High — Ask multiplies DB and LLM spend without durable entitlement or cross-instance usage ownership

**Severity:** High
**Confidence:** High for source behavior; actual provider/DB spend and latency unknown

**Exact evidence**

- `dashboard/src/app/api/ask/route.ts:563-590` authenticates, charges the process-local limit, then parses/validates the body; it does not enforce subscription entitlement.
- MLB performs four base queries (`:601-643`), one sequential team lookup (`:650-668`), and up to four more parallel lookups (`:679-712`) before the provider call: up to nine DB requests.
- NBA begins with five queries (`:802-847`), then two more (`:857-872`), an injury query (`:884-892`), and two enrichment queries (`:908-919`) before later rounds documented in report 07 F-08.
- Report 07 F-03/F-06 establishes that page subscription middleware excludes APIs and the 20/day `Map` is per process/cold start; malformed/provider-failed attempts consume quota.
- The route file is 1,301 lines and has no route-wide query/provider timeout budget (report 07 F-08).

**Concrete cost/performance failure mode**

One Ask request fans out to many database reads and one paid provider call. Unsubscribed authenticated users can invoke it directly; multiple Vercel instances/cold starts reset independent limits; and validation occurs after quota mutation but before no durable reservation. Slow sequential rounds amplify server duration, while local counters neither cap global spend nor yield authoritative remaining usage.

**Current mitigation**

Queries have bounded row limits in the inspected rounds, and parallel batches reduce latency. Authentication and a local 20/day bucket limit a single warm instance. These are not durable billing controls.

**Report interaction**

No duplicate Lane 09 refactor is proposed. Apply report 07 F-03/F-06 before F-08 decomposition: entitlement and durable usage are cheaper spend controls than route extraction.

**Safe evidence step**

Use mocked repositories/provider to record DB call count, query rounds, provider calls, and charge outcome for anonymous, unsubscribed, malformed, provider-failed, MLB, and NBA requests. Define attempt-versus-success charging and a hard latency/call budget without live calls.

**Done condition**

Paid entitlement is checked server-side before costly work; one atomic cross-instance usage owner reserves/finalizes usage under a documented policy; invalid requests do not trigger DB/provider work; query rounds and timeouts are bounded/observable; and contract tests enforce maximum call counts for representative NBA/MLB requests.

### PSC-05 — High — Non-Kalshi schedule collisions and restart-blind retries amplify duplicate work before any registry migration

**Severity:** High for MLB collisions; Medium for NBA skip/retry noise
**Confidence:** High for static schedules, Medium for realized production duplication

**Exact evidence**

- `src/orchestration/scheduler.py:883-889` registers a dedicated NBA props scrape at 10:00; `:925-930` also runs the same props wrapper every five minutes, including 10:00.
- The NBA script-level lock is limited to `lines_job.py` (`scheduler.py:116-120`), so the duplicate normally becomes one run plus one persisted skip (`:393-412`) rather than two concurrent children.
- `scheduler.py:1150-1157` runs MLB close snapshots every ten minutes. Report 04 E-02 proves exact collisions with fixed MLB jobs at 12:00, 13:00, 17:00, and 18:00; `LOCKABLE_JOB_SCRIPTS` excludes MLB.
- `lines_job.py:67-123` and `mlb_lines_job.py:65-111` spawn separate scraper/linker children; the MLB collisions can therefore duplicate Odds API and linker work.
- Report 04 E-05 shows scheduled retries consult only process memory, while persistent success exists; a Railway restart can repeat a successful full ingestion.

**Concrete cost/performance failure mode**

Four daily MLB collisions can run duplicate scrapers/linkers concurrently, doubling provider requests and DB/linker pressure at those instants. The NBA duplicate consumes scheduler/telemetry/log capacity even when the lock prevents duplicate scraping. Restarts before retry windows can rerun successful ingestion. These are bounded schedule-policy fixes; a broad registry extraction is not required to stop the waste.

**Current mitigation**

NBA has a nonblocking script lock and full jobs were offset to :01. Downstream writes are often incremental/upsert-like. MLB has no equivalent script/resource lock, and idempotency does not avoid API/compute cost.

**Report interaction**

Confirms report 04 E-02/E-05 and TD-006. Recalculate the scheduler baseline after report 12's Kalshi cuts; do not justify Lane 07 solely from pre-cut job count.

**Safe evidence step**

Static cron expansion test for script/resource collisions plus a mocked persistent-success retry test. Historical logs/job executions require separate approval and are not needed to prove the source collision.

**Done condition**

No enabled unsafe resource key has colliding triggers; the redundant NBA 10:00 trigger is removed or intentionally represented once; MLB close/fixed jobs are offset or share a tested lock/lease; retries consult a date/window-aware persistent result; and skips/retry decisions remain attributable.

### PSC-06 — High growth risk — Hot-table archival has conflicting ownership, a fixed daily ceiling, and no tracked archive-retention consumer

**Severity:** High conditional storage risk
**Confidence:** High for code policy; Low/unknown for current backlog and table sizes

**Exact evidence**

- `src/orchestration/archive_old_props_job.py:41-43` defines 50,000-row batches, 500,000 rows/run, and seven-day hot retention.
- `archive_old_props_job.py:50-70` copies then deletes each batch transactionally into `raw_player_props_archive`.
- Scheduler invokes it once daily without overrides and describes 30 days (`src/orchestration/scheduler.py:963-972`), conflicting with the effective seven-day default.
- Targeted tracked searches found archive-table writes only in this job and `scripts/run_full_archival.py`; no tracked purge/tier/export/restore consumer for `raw_player_props_archive` surfaced.
- Report 00 groups old 67M-row/performance claims as Needs-Evidence; report 08 I-07 establishes only the static 7-vs-30 mismatch. Report 03 is absent.

**Concrete cost/performance failure mode**

If eligible rows arrive faster than 500,000/day, the hot-table archive backlog grows despite a daily successful job. Regardless of hot-table movement, the archive table grows monotonically because no tracked terminal retention/tier owner surfaced. Conflicting 7/30-day policy can also move data earlier than consumers expect. Current ingestion rate, hot/archive sizes, and query impact are unknown and must not be inferred from old counts.

**Current mitigation**

Each batch is transactional and bounded; rows are moved rather than dropped. The one-time full archival script can drain backlog manually, but it is not a retention owner or backup.

**Report interaction**

Extends report 08 I-07 and report 00's DB lifecycle cluster. Final adjudication should reconcile with report 03 if it is later created; no index/DDL recommendation is made here.

**Safe evidence step**

First add no-DB policy tests for effective retention and maximum daily capacity. Then, only through the approved SQL-runner lane, measure daily eligible-row inflow, hot/archive date bounds and sizes, archive consumers, and oldest/newest rows. Independently verify counts before any destructive decision.

**Done condition**

One owner declares hot, warm/archive, and terminal-retention windows plus consumers; scheduler/job/docs/tests agree; daily capacity exceeds measured peak with headroom or backlog is observable; archive retrieval/restore is documented; and any purge/tiering policy has approved legal/accounting/model-retention scope and independently verified DB evidence.

### PSC-07 — Medium — Orchestration duplicates logs to stdout and unbounded local files with no rotation owner

**Severity:** Medium
**Confidence:** High for code behavior; runtime disk/platform retention unknown

**Exact evidence**

- Targeted `src/orchestration` inventory found 20 `logging.FileHandler` sites and no `RotatingFileHandler` or `TimedRotatingFileHandler` site.
- Representative high-frequency jobs configure both `StreamHandler` and append-only `FileHandler`: `lines_job.py:48-59`, `mlb_lines_job.py:47-58`, `edge_refresh_job.py:53-64`, and `mlb_edge_refresh_job.py:28-38`.
- `.gitignore:35-36` ignores `*.log`, so local file growth is invisible to tracked-file review.
- Scheduler captures child stdout/stderr and re-emits the final 50 lines (`src/orchestration/scheduler.py:415-435`), creating another copy in Railway logs.
- Report 08 I-08 separately identifies a tracked 231,306-byte Railway log export under `.hermes/tmp/`; report 13 BP-09 owns removing/promoting temp artifacts.

**Concrete cost/performance failure mode**

Each job writes the same operational messages to container/local disk and stdout; the scheduler captures and re-logs tails. Append-only files have no size/time cap, so a long-lived container/workstation can accumulate logs until restart/cleanup or disk pressure. Platform log retention and local file retention have no common owner. Actual disk bytes/day and Railway retention billing are unknown.

**Current mitigation**

Railway container replacement may discard local files, and Git ignores them. Those are incidental lifecycle effects, not retention policy. The tracked four-hour export is bounded but should not be the archive mechanism.

**Report interaction**

Credit report 12 removal of Kalshi log producers and report 13 BP-09 cleanup. Keep generic observability; do not delete logs without replacing actionable stdout/structured fields.

**Safe evidence step**

Static inventory handlers by job/frequency and define local versus Railway ownership. In a future harmless mocked logger test, verify one record reaches the selected sink and rotation/cap behavior without running jobs. Platform retention needs values-free settings inspection.

**Done condition**

Railway jobs have one primary structured stdout sink; local developer files are explicitly opt-in and rotated/capped; duplicate scheduler tail emission is bounded; retention/access/redaction owners are documented; `.hermes/tmp` contains no production log dump; and alerts/forensics retain required correlation fields without secrets.

### PSC-08 — Medium hypothesis — Fresh-process and import overhead is multiplied by high-frequency wrappers

**Severity:** Medium candidate
**Confidence:** High for process/import structure; Low for material CPU/billing impact

**Exact evidence**

- Every scheduler execution launches a fresh interpreter through `subprocess.run` (`src/orchestration/scheduler.py:371-421`).
- NBA props and edge refresh each run 180 times/day (`scheduler.py:923-937`) before considering the redundant 10:00 trigger.
- `edge_refresh_job.py:21-44` imports NumPy, Pandas, SQLAlchemy, Black-Litterman, daily-runner policy, Monte Carlo helpers, and prediction storage at module import; it is 1,252 lines.
- The edge job describes itself as lightweight/no inference (`edge_refresh_job.py:3-8`) but still owns a lazy model cache (`:49-51`) that cannot persist across scheduler-launched processes.
- Lines wrappers spawn additional fresh scraper/linker processes (`lines_job.py:67-91`; `mlb_lines_job.py:65-83`), creating nested startup overhead.
- MLB edge refresh moves heavier imports inside `main` (`mlb_edge_refresh_job.py:72-80`), but still starts a new process per trigger.

**Concrete cost/performance failure mode**

Interpreter startup, scientific-library imports, logger/file setup, dotenv/config, and nested child startup repeat per trigger. Process-local caches provide no reuse between five-minute runs. This can consume CPU and elongate short jobs enough to increase overlap risk, but repository evidence does not establish milliseconds, RSS, or billed compute.

**Current mitigation**

Subprocess isolation limits leaks, gives timeouts, and separates failures. A persistent worker could increase state/caching correctness risk; replacement is not automatically better.

**Report interaction**

Do not use this hypothesis to justify Lane 07 or merge jobs before report 12/PSC-05 cheap cuts. Report 04 E-03 process-tree ownership must be resolved if process architecture changes.

**Safe evidence step**

After approved trigger cuts, use a disposable no-DB import/startup harness on representative entrypoints with provider/DB calls mocked or bypassed. Record cold start, import profile, RSS, and total no-op duration. Do not run real scheduler jobs.

**Done condition**

Measured startup/import cost is either accepted with a documented isolation rationale or reduced through lazy imports/shared lightweight entrypoints; process-local caches are not advertised as cross-run savings; and any persistent-worker change preserves timeout, state isolation, and no-stale-config contracts with tests.

## Cache and retention ownership matrix

| Surface | Current owner/evidence | Risk | Audit disposition |
|---|---|---|---|
| Dashboard query cache | `QueryProvider.tsx:9-16`, 5m stale/30m GC | Retains full unbounded performance histories; duplicate keys retain duplicate data | Bound/shared queries before tuning cache duration |
| Performance domain cache | `usePerformanceData.ts:63-170`, 10m stale | Inactive tabs eagerly populate caches | Add `enabled`/active-domain ownership and bounded contracts |
| Ask usage cache | `ask/route.ts:8-32`, report 07 F-06 | Process-local counter resets/duplicates across instances | Durable atomic usage owner; not a larger in-memory cache |
| Edge model cache | `edge_refresh_job.py:49-51` | Fresh scheduler process prevents cross-trigger reuse | Measure before architectural change |
| Scheduler status cache | `scheduler.py:124-126` | Restart loses retry state; persistent history exists separately | Reuse one restart-aware policy (report 04 E-05) |
| Hot props retention | `archive_old_props_job.py:41-43` | 7-day effective policy, 500k/day cap | Reconcile policy and measure inflow/backlog |
| Archive retention | only insert/move references surfaced | Monotonic growth; no terminal owner found | Needs DB/data-retention decision |
| Local logs | 20 append-only `FileHandler` sites | Invisible unbounded disk growth | stdout on Railway; opt-in rotated local logs |
| Production log export | `.hermes/tmp/railway_logs_4h.jsonl`, 231,306 bytes | Tracked operational residue/future exposure path | Report 13 BP-09 / report 08 I-08 removal/archive |

## Package and build-context assessment

Measured facts:

- Railway uses a full checkout copy into `/app` and then `pip install -r requirements.txt` (`nixpacks.toml:6-19`). The tracked source context is already 220,023,222 bytes before dependencies.
- `requirements.txt:1-22` contains 22 direct requirements; six are lower bounds rather than exact pins. Report 08 I-02 establishes that this graph differs from `uv.lock`/pyproject and is not hash-enforced.
- Native libraries are selected from the first `/nix/store` match (`nixpacks.toml:8-12`), and the venv uses `--system-site-packages` (`:17-18`); report 08 I-09 owns reproducibility risk. This audit did not measure package/image size.
- Root `vercel.json:1-6` uses `npm install` from repository-root mode; `dashboard/vercel.json:1-4` represents dashboard-root mode. Report 08 I-06/report 13 BP-07 own consolidation after values-free Vercel root inspection.
- `dashboard/package.json:11-34` has a bounded app dependency set and a lockfile. No evidence supports pruning dashboard packages merely from manifest presence.

Hypotheses requiring build/platform evidence:

- exact Railway source upload, layer reuse, image size, install duration, and storage billing;
- whether Vercel's active project root uploads/processes the full 220 MB checkout or only `dashboard/`;
- dependency import/native ABI cost and whether root npm tooling enters any deploy path;
- actual cache hit rates and Vercel function duration/cost.

## Prioritized cheap wins before refactors

| Priority | Cheap win / decision | Why before refactor | Expected static cut | Gate / owner |
|---:|---|---|---:|---|
| 1 | Execute report 12 sports containment/decommission sequence | Avoid preserving retired jobs/UI in Lane 07/09/10 refactors | Up to 1,442 scheduler triggers/day plus 15s/60s dashboard pollers | Exposure closeout and Chase non-sports decision |
| 2 | Disable inactive performance-tab hooks and unify duplicate My Bets/track reads | Small query-policy change attacks immediate fan-out without JSX decomposition | Default mount from at least 8 requests toward active-domain-only | Mocked KPI/request-count tests; Lane 10 refresh |
| 3 | Remove/resolve redundant NBA 10:00 trigger and offset/lock four MLB collisions | Bounded schedule fixes stop known duplicate work | 1 NBA skip/day; up to 4 concurrent MLB duplicate launches/day | Static cron/resource test; report 04 |
| 4 | Enforce Ask subscription before context/provider work | Direct spend containment before 1,301-line route extraction | Blocks unentitled DB+LLM calls | Report 07 F-03 route contracts |
| 5 | Remove high-confidence generated residue and tracked temp/log copies | No architecture dependency for most residue | `.next`, egg-info, probes; 311,118-byte tracked `.hermes/tmp` after ownership review | Report 13 BP-08/BP-09 |
| 6 | Choose stdout as Railway log owner; rotate opt-in local files | Removes duplicate sink/growth without observability vendor work | 20 append-only file sinks, fewer after Kalshi removal | Static/mocked logging contract |
| 7 | Reconcile seven-versus-thirty-day retention prose | Prevents policy drift before changing DB/query architecture | No immediate byte claim | SQL-runner evidence needed before value change |
| 8 | Build artifact archive ledger and verify external readback | Prerequisite to the largest byte cut | 168,994,105 tracked bytes eligible for adjudication; not immediate deletion | MMP manifests/rollback + archive owner |
| 9 | Consolidate Vercel root mode and use `npm ci` | Avoids measuring/refactoring two deploy contracts | Unknown build-time reduction | Values-free Vercel metadata; report 08 I-06 |
| 10 | Measure import/startup only after trigger cuts | Prevents optimizing processes that will be removed | Unknown | Disposable no-DB harness |

## Rejected suspicions and confirmed mitigations

1. **Rejected: the repository is large mainly because of source code.** Tracked model artifacts dominate; named non-production groups alone are 76.81% of the checkout.
2. **Rejected: every duplicate hash is immediately deletable.** Byte identity does not replace provenance, manifest, rollback, or historical report dependencies. BP-02/MMP gates apply.
3. **Rejected: current production/playoff artifacts are old backups.** Runtime references exist; report 13 BP-15 retains them pending a replacement artifact contract.
4. **Rejected: Kalshi jobs should be optimized because they run frequently.** Sports is a decommission target. Remove/close out in report 12 order; non-sports remains a product decision.
5. **Rejected: all 1,532 Kalshi triggers can be removed immediately.** Ninety/day belong to unresolved non-sports, and fill/cancel/settlement lifecycle removal requires authoritative exposure closeout.
6. **Rejected: React Query caching is absent.** Shared 5-minute stale/30-minute GC defaults and domain 10-minute stale times exist. The defect is eager/unbounded/duplicate data ownership.
7. **Rejected: history pagination is still the performance problem.** The history-page blanket claim was resolved. PSC-03 concerns the separate performance/track-record hooks.
8. **Rejected: edge refresh always reloads NBA model binaries.** Its advertised normal path reuses stored samples; model loading is lazy for drift handling. PSC-08 concerns import/process setup and cache lifetime, not a claim of per-run inference.
9. **Rejected: the hot-table archive deletes rows without copying.** `archive_old_props_job.py:50-70` inserts and deletes in one transaction. The risks are policy/capacity/terminal retention, not a copyless delete.
10. **Rejected: every log line is retained in Git.** `*.log` is ignored. PSC-07 concerns local/container growth and duplicate sinks; the separate tracked JSONL export is report 08/13 residue.
11. **Rejected: root npm dependencies currently inflate Railway/Vercel installs.** Current deploy commands install Python requirements or `dashboard/`; report 13 BP-12 correctly keeps root Claude-flow tooling as Needs-Evidence.
12. **Rejected: import/startup overhead is already proven material.** Static structure supports a hypothesis only. No workload/import timing or billing data was collected.
13. **Rejected: model artifact age or size proves model quality debt.** No artifact was loaded or evaluated; storage identity is separate from predictive quality.
14. **Rejected: ignored untracked artifact directories were measured.** They were intentionally not recursively scanned. All byte totals in this report are tracked-file evidence.

## Coverage gaps and evidence requiring runtime, DB, or billing data

### Missing report dependency

- `03-database-data-lifecycle.md` does not exist in the current audit directory. No claim from that report was invented. If it appears later, PSC-06 and query/storage findings require reconciliation before final adjudication.

### DB evidence not gathered

- Current hot/archive table bytes, row counts, daily eligible-row inflow, archive backlog, index sizes/usage, query plans, TOAST/bloat, vacuum behavior, and storage billing.
- Current performance-hook table row counts, payload bytes, RLS/query latency, egress, and whether server-side aggregate RPCs already exist live.
- Ask query plans, per-request DB duration, usage/event tables, and provider-call cost attribution.
- Logging/job-execution retention, row growth, indexes, and purge policy.
- Kalshi/arb table sizes and sports/non-sports distributions; report 12 owns data retention decisions.

### Runtime/platform evidence not gathered

- Railway configured/deployed job gates, actual trigger/no-op counts, CPU/RSS, process startup/import time, overlap frequency, API request counts, image size, build duration, cache reuse, disk persistence, log volume/retention, and billing.
- Vercel active project root, source upload size, function duration/concurrency/cold starts, query/provider spend, cache hit rate, and deployment build timing.
- Git host clone/LFS/history pack size and whether binary history causes practical clone/CI cost; only current tracked working-tree bytes were measured.
- Browser transfer/memory/render timing for the performance page and real user navigation patterns.
- Provider/exchange account state or exposure; required before report 12 lifecycle removal.

### Repository surfaces intentionally not scanned/executed

- Ignored/untracked `backtest_results/`, generated MLB ablation directories, `node_modules/`, venv, `.git`, `.claude-flow`, and `tmpclaude-*`.
- Binary model contents were hashed but not deserialized.
- No package/build/test/import/scheduler/job command was run.
- No notebooks, historical archives, all scripts, or all dashboard routes were exhaustively audited for cost; report 13's bounded pruning inventory remains the authority for broader candidates.
- No secret values or credential files were inspected.

## Recommended adjudication order

1. Credit report 12's sports product cuts and decide non-sports/Polymarket/arb before estimating scheduler/dashboard refactors.
2. Confirm PSC-03 and PSC-04 as bounded cost controls inside refreshed Lane 10/Lane 09 work; do not create duplicate plans.
3. Confirm PSC-05 schedule fixes ahead of Lane 07 extraction.
4. Reconcile PSC-06 with a later report 03 and approval-gated SQL evidence.
5. Treat PSC-07 as a small observability-retention slice after Kalshi producer removal.
6. Keep PSC-08 as Needs-Evidence until a no-DB startup profile proves material impact.
7. Execute report 05 manifest/rollback prerequisites, then realize report 13 BP-02/BP-03 and PSC-01 storage cuts.
8. Consolidate package/build ownership through report 08 I-02/I-06/I-09; do not install/update dependencies in the audit lane.

## Validation record

- Read `AGENTS.md`, the audit README, and every present requested report (`00`, `01`, `02`, `04`, `05`, `06`, `07`, `08`, `09`, `12`, and `13`) completely. Requested report `03` was absent and is recorded as a coverage gap.
- Reproduced report 13's tracked-byte accounting from `git ls-files`; did not recursively scan ignored heavy directories.
- Hashed tracked model artifacts only; no binary was loaded.
- Reconciled report 12's 1,532 configured triggers/day and separated 1,442 sports/lifecycle from 90 unresolved non-sports triggers.
- Every promoted finding includes exact path/symbol/line evidence, failure mode, severity, confidence, mitigation, report interaction, safe evidence step, and done condition.
- Measured repository evidence is separated from DB/runtime/platform/billing hypotheses.
- No source, config, plan, register, card, DB, service, package, build, test, workload, model, or secret state was changed.
- Only `.hermes/audits/tech-debt/10-performance-storage-cost.md` was written.
