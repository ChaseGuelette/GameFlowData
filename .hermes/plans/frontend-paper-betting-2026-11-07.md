# Frontend Paper Betting Implementation Plan

> **For Hermes:** Use subagent-driven-development for the bounded frontend implementation and review. Owner authorized implementation on November 7, 2026. No commit, push, deploy, production writes, new unit tests, or backend repairs are authorized.

**Goal:** Make personally selected prop bets easy to save, find and inspect in Performance, while removing DFS UI and simplifying model views.

**Architecture:** Retain existing Supabase user_bets, automatic paper_bets, React Query and Next.js routes. Performance has three top-level tabs; Props contains a source switch between actual personal bets and model picks. Backend settlement is a separately deferred dependency, not implemented by this frontend slice.

**Tech Stack:** Next.js 16, React 19, TypeScript, TanStack Query, existing Supabase clients and Tailwind.

## Scope and settled interpretation

- Top-level Performance tabs: Props, Record, History.
- Props source buttons: Model Picks and My Bets. Default: Props / My Bets. My Bets means authenticated user_bets (including personally selected paper bets), NOT renaming the automatic paper_bets query.
- Retain manual paper/real visibility in History; do not silently exclude paper bets from My Bets analytics. Make the inclusion clear. Changing staking or model selection is out of scope.
- History tabs: My Bets and Model History; Model History has NBA/MLB selection, not a separate MLB tab. Keep existing pending/resolved fields, direction/status filters and pagination.
- Remove DFS from desktop/mobile navigation, Performance and History. Existing /dfs bookmarks redirect to /performance rather than preserving a hidden active DFS page. Do not delete backend jobs, tables, stored entries or shared types merely because the UI is removed.
- Save confirmation must follow successful persistence; failures must be visible and retryable. Do not change duplicate semantics or bypass RLS.
- My Bets analytics gets the same inclusive date-range controls/presets as History, applied to fetched rows, query keys, KPIs, chart and breakdown, with invalid/empty range handling.
- Dashboard schedule fallback is /api/games -> official NBA CDN, independent of Daily Stats. Do not present schedule fetch failures as proof of no scheduled games. Distinguish unavailable schedule and awaiting predictions from a verified empty date. No schedule ingestion or scraper repair in this slice.

## Allowed implementation files

- dashboard/src/app/(protected)/performance/page.tsx
- dashboard/src/components/performance/HistoryPanel.tsx
- dashboard/src/components/layout/Navbar.tsx
- dashboard/src/app/(protected)/dfs/page.tsx
- dashboard/src/components/shared/BetSourceFilter.tsx
- dashboard/src/lib/hooks/usePerformanceData.ts
- dashboard/src/lib/hooks/useHistoryData.ts (only if needed for selected-sport source or matching invalidation)
- dashboard/src/components/analysis/AnalysisModal.tsx
- dashboard/src/app/(protected)/dashboard/page.tsx
- dashboard/src/lib/hooks/useUserBets.ts
- Optional small NEW shared date-range component under dashboard/src/components/shared/ if reuse is materially simpler than existing controls.

Read-only prerequisites: dashboard/package.json, relevant types/contexts/history components, dashboard/src/app/api/games/route.ts, existing eslint/tsconfig/Next config. No changes to Python, DB schemas, authentication, backend settlement, model artifacts, dependencies, lockfiles, or unrelated injury work.

## Task 1: Performance navigation and data sources

1. Remove DFS imports, queries, calculations and rendering from Performance; remove separate My Bets Analytics top-level tab.
2. Replace source enum `model|all` with `model|my_bets` after tracing both consumers. Props source switch must choose the corresponding query/analytics, default My Bets.
3. Keep Record behavior. Preserve ?tab=history navigation; optionally support old my_bets/dfs parameters by routing to Props/My Bets.
4. Consolidate History's model views under a sport selector; enable only the selected model query. Remove DFS hooks/rendering there. Preserve its My Bets/Paper filtering and displayed settlement fields.

## Task 2: Date-range analytics

1. Reuse History's date-range presentation and defaults/presets. Avoid inventing a new framework.
2. Pass inclusive start/end into My Bets analytics query and React Query key; apply the same range to all displayed personal metrics.
3. Prevent reversed ranges from showing stale unrelated data; show actionable validation and a genuine zero-bets state.

## Task 3: DFS navigation removal

1. Remove desktop/mobile DFS links without affecting sport switch, account or other routes.
2. Replace DFS page content with a server redirect to /performance. Leave unused components/backend untouched unless imports must be removed from an allowed file.

## Task 4: Save/display lifecycle

1. Change AnalysisModal callback contract to await a success/failure result. While saving disable duplicate submissions; show success only on returned persisted ID; show a visible error otherwise and permit retry.
2. Dashboard handleTakeBet returns the result and invalidates existing user/history/performance/record query keys after success. Inspect actual keys rather than guessing.
3. Preserve selected book, odds, line, side, paper flag and existing stake policy. Keep existing authenticating user identity and insert semantics.
4. Confirm pending bets and settled status/actual/PnL are rendered through existing History components; do not claim this fixes backend resolution.
5. Display schedule lookup errors honestly rather than showing 'no games' for upstream failure; bound only client behavior, preserve real empty success handling.

## Validation

- Before writer: baseline TypeScript and scoped ESLint on current files; check installed tooling, no dependency installs by default.
- After writer: TypeScript noEmit, scoped ESLint, production Next build if environment permits. Classify existing unrelated diagnostics separately; no broad lint cleanup.
- Bounded controlled render/handler checks using installed tools, outside repository test files: default My Bets; Props/Record/History only; no DFS navigation; date filtering and NBA/MLB history routing; awaited save success/failure and exact paper payload; pending/won/lost/push/cancelled display.
- Browser smoke of local compiled UI when practical; if authenticated real writes are unavailable, use clearly labeled controlled data without changing production auth or submitting real bets. No fabricated live evidence.
- Independent spec review followed by code-quality review; parent reviews scoped diff and final validation on the integrated working checkout.

## Deferred backend acceptance checklist

- [ ] Restore current input/inference readiness so current props exist.
- [ ] Reconcile overlapping user-bet resolvers and DNP/missing-result semantics.
- [ ] One separately authorized authenticated paper placement -> exact DB readback -> Performance/History pending -> next-morning result ingestion/resolution -> displayed actual/status/PnL.
- [ ] Automatic ledger immutability/pregame/evaluation-identity repairs remain a separate lane.

## Delivery status

Implemented and integrated into the main working checkout, uncommitted. Independent specification review passed and final quality review approved the completed slice, including the coordinator's small corrections. The integrated production build (including TypeScript) passes. Scoped ESLint has zero errors and three pre-existing warnings. Controlled actual-code checks passed for the personal source/default view, inclusive date query/filter keys, invalid range, independent NBA/MLB history table routing, retained settlement fields, awaited paper save success/null/rejection, and schedule empty/populated/error/malformed/stale-response behavior.

Verification helper: `C:/Users/Chase/AppData/Local/hermes/cache/scratch/frontend-paper-smoke.cjs` (controlled adapters; no authenticated live writes). No new unit tests or dependencies were added. Backend source, jobs, tables and model artifacts were not changed. Existing unrelated work was preserved.

The schedule display fix reports upstream failure honestly; it does not repair CDN access or generate missing predictions. Frontend completion does not certify next-morning backend settlement or production deployment. The deferred backend acceptance checklist above remains open.
