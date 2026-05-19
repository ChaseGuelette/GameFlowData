# Dashboard God Components / Pages Migration Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves this lane. This is a migration plan, not approval to redesign the UI, change Supabase query semantics, alter subscription/auth gates, or change betting/DFS/performance calculations.

**Goal:** Rebuild the largest dashboard pages/components into focused data hooks, pure view models, reusable UI sections, and thin page shells while preserving current UI behavior and RLS-authenticated data access.

**Architecture:** Preserve existing routes/components as compatibility entry points. Extract pure formatting/filtering/view-model logic first, then move Supabase/client data fetching into hooks/repositories, then split large JSX sections into presentational components. Avoid visual redesign unless backed by explicit before/after snapshots and Chase approval.

**Tech Stack:** Next.js App Router, React client components, TypeScript, Supabase client, React Query, existing dashboard hooks, Tailwind classes, Vercel-hosted frontend.

---

## Relevant prior lessons/invariants

Retrieved before writing this plan:

- `operations/hard-facts`
- `operations/critical-invariants`
- GBrain query for dashboard component context returned no focused canonical page, so this doc is grounded primarily in live dashboard code plus global architecture facts.

Applied facts/invariants:

1. Dashboard is a Next.js app hosted on Vercel.
2. Dashboard/client code uses Supabase `authenticated` role with RLS; do not replace client queries with backend/admin access casually.
3. Dashboard rows reflect model outputs and paper/live trading evidence; UI refactors must not alter calculations silently.
4. Product/subscription/auth flows are user-facing; preserve route paths and interaction behavior first.
5. This lane should not mix with Ask API route migration except for shared `AnalysisModal`/`AskChat` boundaries.

---

## Executive diagnosis

Dashboard god components/pages are concentrated in a handful of high-LOC files:

- `dashboard/src/components/analysis/AnalysisModal.tsx`
  - 1,043 total lines
  - 913 non-comment LOC
  - player history fetching, bookmaker lines, Kelly/staking UI, AskChat embedding, modal rendering, sport-specific charting/context all in one component
- `dashboard/src/app/(protected)/dashboard/page.tsx`
  - 733 total lines
  - 643 non-comment LOC
  - prediction fetching/filtering/sorting, fallback games, live status, slate builder, taken-bet integration, modal orchestration, cards/grid rendering
- `dashboard/src/app/(protected)/performance/page.tsx`
  - 688 total lines
  - 629 non-comment LOC
  - props/DFS/my-bets/track-record tabs, KPI calculation, chart data, CSV/manual bet modals, multi-domain rendering
- `dashboard/src/components/dfs/DfsTable.tsx`
  - 552 total lines
  - 511 non-comment LOC
  - sorting, filtering-visible columns, slip-leg construction, edge display, table rendering, row selection
- `dashboard/src/app/(protected)/dfs/page.tsx`
  - 546 total lines
  - 466 non-comment LOC
  - DFS date/filter state, hook data, derived rows, slip builder integration, page rendering
- `dashboard/src/components/bot-tracker/TradeApprovalPanel.tsx`
  - 482 total lines
  - 451 non-comment LOC
  - trade queue polling/actions, analysis modal handoff, row rendering, approval/rejection state
- Additional large files likely in later slices:
  - `bot-tracker/BetAnalysisModal.tsx` 428 lines
  - `bot-tracker/BotOrdersTable.tsx` 386 lines
  - `history/page.tsx` 405 lines
  - `stats/page.tsx` 349 lines
  - `app/api/slate/route.tsx` 343 lines

No dashboard test files currently exist under `dashboard/`, so the first migration slice must establish a minimal component/view-model test harness or use static/inventory tests before refactoring.

---

## Current ownership problems

### 1. `AnalysisModal` owns data access, view models, betting controls, and chat

Current responsibilities in `AnalysisModal.tsx`:

- sport inference for MLB stat prefixes
- Supabase history query for NBA and MLB
- bookmaker line query against `raw_player_props_combined` and `mlb_raw_player_props`
- DFS book filtering and latest-snapshot dedupe
- bankroll/Kelly preference inputs
- take-bet payload construction
- stat-specific charts/history values
- model context and insights display
- embedded `AskChat`
- modal layout and close behavior

Target owners:

- `components/analysis/AnalysisModalShell.tsx`
- `components/analysis/AnalysisHeader.tsx`
- `components/analysis/PredictionSummary.tsx`
- `components/analysis/BookmakerLinesPanel.tsx`
- `components/analysis/KellyControls.tsx`
- `components/analysis/TakeBetPanel.tsx`
- `components/analysis/HistorySection.tsx`
- `lib/analysis/useAnalysisHistory.ts`
- `lib/analysis/useBookmakerLines.ts`
- `lib/analysis/buildTakeBetPayload.ts`
- `lib/analysis/viewModel.ts`

Safety:

- Do not change AskChat API contract in this lane.
- Do not change Kelly math/prefs behavior unless a failing test captures a bug and Chase approves.

---

### 2. Dashboard page owns both data orchestration and presentation

Current `dashboard/page.tsx` owns:

- prediction fetch from Supabase
- injury safety filter
- available-date selection
- team/book/direction/model-pick/live filters
- sorting and normalization of display values
- fallback games via `/api/games`
- live game status polling via hook
- taken-bet integration
- slate-builder image generation via `/api/slate`
- modal selection
- rendering of play of the day, filters, games, grid, slate UI

Some data fetching is already partly abstracted by hooks like `usePredictions`, but the page still includes significant custom fetching/filtering.

Target owners:

- `lib/dashboard/useDashboardPredictions.ts`
- `lib/dashboard/useDashboardFilters.ts`
- `lib/dashboard/useSlateBuilder.ts`
- `lib/dashboard/predictionViewModel.ts`
- `components/dashboard/DashboardToolbar.tsx`
- `components/dashboard/SlateBuilderBar.tsx`
- `components/dashboard/PredictionSections.tsx`

Compatibility:

- `app/(protected)/dashboard/page.tsx` remains the route shell.

---

### 3. Performance page mixes cross-domain data and derived metrics

Current `performance/page.tsx` owns:

- tab state across props/DFS/my-bets/track-record
- React Query hooks from `usePerformanceData`
- bet-source filtering
- aggregate KPI calculations
- stat breakdown calculations
- bankroll chart adaptation
- DFS KPI calculations
- modals for CSV upload/manual bet form
- rendering all tabs and child components

Target owners:

- `lib/performance/viewModels.ts`
- `lib/performance/usePerformanceTabs.ts`
- `components/performance/PerformanceTabs.tsx`
- `components/performance/PropsPerformancePanel.tsx`
- `components/performance/DfsPerformancePanel.tsx`
- `components/performance/TrackRecordPanel.tsx`

Safety:

- Preserve current calculation definitions for ROI/win rate/chart data.
- Do not change query hooks or status filters in first slice.

---

### 4. DFS page/table split is incomplete

Current files:

- `dfs/page.tsx`: date/filter state, hook data, row derivation, slip builder integration.
- `components/dfs/DfsTable.tsx`: sorting, table rendering, slip-leg construction, selected state, display decisions.

Target owners:

- `lib/dfs/useDfsPageState.ts`
- `lib/dfs/dfsViewModel.ts`
- `lib/dfs/sortRows.ts`
- `components/dfs/DfsTableHeader.tsx`
- `components/dfs/DfsTableRow.tsx`
- `components/dfs/DfsEmptyState.tsx`

Safety:

- Do not change edge mode semantics or slip builder leg payloads.
- Preserve selected-leg key behavior.

---

### 5. Bot tracker panels combine API actions and table rows

Current large components:

- `TradeApprovalPanel.tsx`: queue loading/actions, analyze modal state, row rendering, countdown.
- `BotOrdersTable.tsx` and `BetAnalysisModal.tsx` also large.

Target owners:

- `lib/bot-tracker/useTradeApprovalActions.ts`
- `components/bot-tracker/TradeApprovalTable.tsx`
- `components/bot-tracker/TradeApprovalRow.tsx`
- `components/bot-tracker/TradeActionButtons.tsx`
- `components/bot-tracker/OrderAnalysisPanel.tsx`

Safety:

- Do not change approval/cancellation endpoints or optimistic update behavior without tests.
- Keep AnalysisModal handoff compatible.

---

### 6. Types and view models are too component-local

Existing useful types/hooks:

- `dashboard/src/types/predictions.ts`
- `dashboard/src/types/dfs.ts`
- `dashboard/src/types/arb-scanner.ts`
- `dashboard/src/types/bot-tracker.ts`
- `dashboard/src/lib/hooks/usePredictions.ts`
- `dashboard/src/lib/hooks/usePerformanceData.ts`
- `dashboard/src/lib/hooks/useDfsLines.ts`
- `dashboard/src/lib/hooks/useArbScanner.ts`
- `dashboard/src/lib/hooks/useTradeQueue.ts`

Problem:

- Large components still compute display-specific data inline.
- UI calculations are hard to unit-test because they are embedded in React components.

Target owner:

- per-domain `viewModel.ts` modules with pure functions and typed outputs.

---

## Target design by responsibility

### A. Data hooks/repositories

- `lib/analysis/useAnalysisHistory.ts`
- `lib/analysis/useBookmakerLines.ts`
- `lib/dashboard/useDashboardPredictions.ts`
- `lib/dashboard/useDashboardFilters.ts`
- `lib/dfs/useDfsPageData.ts`
- `lib/bot-tracker/useTradeApprovalActions.ts`

### B. Pure view models

- `lib/analysis/viewModel.ts`
- `lib/dashboard/predictionViewModel.ts`
- `lib/performance/viewModels.ts`
- `lib/dfs/dfsViewModel.ts`
- `lib/bot-tracker/viewModels.ts`

### C. Presentational components

- focused sections under `components/analysis/`, `components/dashboard/`, `components/performance/`, `components/dfs/`, and `components/bot-tracker/`.

### D. Route/page shells

- protected page files should own route-level state and compose hooks/components only.

---

## Refactor phases

### Phase 0: Dashboard test harness and inventory

Objective: Establish minimal safety before splitting large UI files.

Files:

- Create: `dashboard/src/lib/__tests__/dashboardInventory.test.ts`
- Add test tooling only if missing, as a separate setup slice.

Tests/static checks:

- large files exist and export expected default/components.
- endpoint strings used by dashboard page/AskChat/slate remain unchanged.
- key route paths remain present.
- no production behavior changes.

Validation:

`cd dashboard && npm run typecheck`

If test runner exists/gets added:

`cd dashboard && npm test -- dashboardInventory`

---

### Phase 1: Extract pure formatting/view-model helpers from `AnalysisModal`

Objective: Move no-data-fetch logic first.

Files:

- Create: `dashboard/src/lib/analysis/viewModel.ts`
- Create: `dashboard/src/lib/analysis/buildTakeBetPayload.ts`
- Create tests for pure helpers.
- Modify: `AnalysisModal.tsx` to delegate.

Tests:

- MLB stat value extraction for batter/pitcher history preserved.
- take-bet payload fields preserved.
- displayed edge/probability/line formatting preserved.

---

### Phase 2: Extract AnalysisModal data hooks

Objective: Move Supabase reads out of the modal.

Files:

- Create: `dashboard/src/lib/analysis/useAnalysisHistory.ts`
- Create: `dashboard/src/lib/analysis/useBookmakerLines.ts`
- Modify: `AnalysisModal.tsx`

Tests:

- NBA history query table/columns preserved.
- MLB pitcher/batter history query table/columns preserved.
- props table selection preserved.
- latest bookmaker snapshot dedupe preserved.

---

### Phase 3: Split AnalysisModal presentational sections

Objective: Make modal composition obvious.

Files:

- Create section components listed above.
- Keep `AnalysisModal.tsx` as shell.

Validation:

- typecheck/lint.
- manual modal QA after implementation.

---

### Phase 4: Extract dashboard page view models

Objective: Move filtering/sorting/slate pick derivation out of route page.

Files:

- Create: `dashboard/src/lib/dashboard/predictionViewModel.ts`
- Create: `dashboard/src/lib/dashboard/slateViewModel.ts`
- Create tests.
- Modify: `dashboard/page.tsx`

Tests:

- model-pick/live/team/book/direction filters preserved.
- sorted prediction order preserved.
- slate pick payload preserved.
- out-player filter semantics preserved.

---

### Phase 5: Extract dashboard hooks and presentational sections

Objective: Shrink `dashboard/page.tsx` to route shell.

Files:

- Create: `useDashboardPredictions.ts`, `useSlateBuilder.ts`, section components.
- Modify: `dashboard/page.tsx`.

Safety:

- Preserve `/api/games` and `/api/slate` calls.
- Preserve `AnalysisModal` interaction.

---

### Phase 6: Extract performance page view models

Objective: Move KPI/stat/chart calculations into pure functions.

Files:

- Create: `dashboard/src/lib/performance/viewModels.ts`
- Create tests.
- Modify: `performance/page.tsx`

Tests:

- props KPI calculations preserved.
- model-only filtered chart behavior preserved.
- DFS KPI/chart adapter preserved.
- stat breakdown sort/order preserved.

---

### Phase 7: Split performance page panels

Objective: Make each tab a component.

Files:

- Create: `PerformanceTabs.tsx`, `PropsPerformancePanel.tsx`, `DfsPerformancePanel.tsx`, `TrackRecordPanel.tsx`.
- Modify `performance/page.tsx`.

---

### Phase 8: Extract DFS table/page view models and rows

Objective: Decouple DFS sorting/table/slip-leg creation.

Files:

- Create: `lib/dfs/dfsViewModel.ts`
- Create: `lib/dfs/sortRows.ts`
- Create table row/header components.
- Modify: `dfs/page.tsx`, `DfsTable.tsx`.

Tests:

- sort behavior preserved.
- edge mode display preserved.
- slip builder leg payload preserved.
- selected leg key behavior preserved.

---

### Phase 9: Extract bot-tracker approval panel pieces

Objective: Separate API actions and row rendering.

Files:

- Create: `lib/bot-tracker/useTradeApprovalActions.ts`
- Create: `TradeApprovalTable.tsx`, `TradeApprovalRow.tsx`, `TradeActionButtons.tsx`.
- Modify: `TradeApprovalPanel.tsx`.

Tests:

- approve/reject API payloads preserved.
- analyze modal handoff preserved.
- countdown display behavior preserved.

---

### Phase 10: Anti-regrowth guards

Recommended thresholds after extraction:

- `AnalysisModal.tsx` under 350 non-comment LOC.
- `dashboard/page.tsx` under 300 non-comment LOC.
- `performance/page.tsx` under 300 non-comment LOC.
- `DfsTable.tsx` under 280 non-comment LOC.
- `dfs/page.tsx` under 260 non-comment LOC.
- `TradeApprovalPanel.tsx` under 260 non-comment LOC.

Guards:

- no large Supabase query chains directly in modal/page components when a hook exists.
- no KPI math embedded in JSX-heavy components.
- no duplicate slip-leg construction logic.
- pages compose hooks and section components only.

---

## Files likely touched

Existing high-priority:

- `dashboard/src/components/analysis/AnalysisModal.tsx`
- `dashboard/src/app/(protected)/dashboard/page.tsx`
- `dashboard/src/app/(protected)/performance/page.tsx`
- `dashboard/src/components/dfs/DfsTable.tsx`
- `dashboard/src/app/(protected)/dfs/page.tsx`
- `dashboard/src/components/bot-tracker/TradeApprovalPanel.tsx`

Existing medium-priority later:

- `dashboard/src/components/bot-tracker/BetAnalysisModal.tsx`
- `dashboard/src/components/bot-tracker/BotOrdersTable.tsx`
- `dashboard/src/app/(protected)/history/page.tsx`
- `dashboard/src/app/(protected)/stats/page.tsx`
- `dashboard/src/app/(protected)/arb-scanner/page.tsx`

New packages/modules:

- `dashboard/src/lib/analysis/*`
- `dashboard/src/lib/dashboard/*`
- `dashboard/src/lib/performance/*`
- `dashboard/src/lib/dfs/*`
- `dashboard/src/lib/bot-tracker/*`
- focused section components under existing component folders
- tests under `dashboard/src/**/__tests__/*`

---

## Validation commands

Typecheck:

`cd dashboard && npm run typecheck`

Lint:

`cd dashboard && npm run lint`

Tests after harness exists:

`cd dashboard && npm test -- analysis dashboard performance dfs bot-tracker`

Diff hygiene:

`git diff --check -- dashboard/src/components dashboard/src/app/'(protected)' dashboard/src/lib dashboard/src/types .hermes/plans/god-class-migrations/10-dashboard-god-components-pages-migration.md`

Manual QA after implementation, not during doc creation:

- dashboard predictions load for NBA and MLB.
- filters/sorting/book exclusions behave as before.
- Analysis modal opens, history/chart/bookmaker lines load.
- AskChat still opens from modal.
- Take Bet still records and marks taken bet.
- Performance props/DFS/my-bets/track-record tabs render.
- DFS table sorting and slip builder selection work.
- Bot tracker approval/analyze workflow works.

---

## Risk controls / non-goals

Non-goals:

- Do not redesign UI/UX in this migration.
- Do not change Supabase RLS/authenticated client behavior.
- Do not change query table names or filters without focused tests.
- Do not change betting/Kelly/DFS calculations.
- Do not change subscription/auth gates.
- Do not change `/api/slate`, `/api/ask`, or Kalshi approval endpoints.
- Do not migrate all medium-priority large files in the first PR.
- Do not mix dashboard component extraction with backend/API route rewrites.

Hard rules:

- Pages/routes keep their existing paths.
- Existing props passed to child components remain compatible until callers migrate.
- Extract pure view models before moving data fetching.
- Add tests or static inventory before splitting render trees.
- Any visual/product behavior change requires explicit approval.

---

## Expansion checkpoints learned from Kalshi

Trigger a new named sub-slice if you discover:

1. A component owns a hidden workflow/state machine.
2. A hook has different sport-specific semantics than the page assumes.
3. A child component depends on incidental prop shape from a god page.
4. A Supabase query is constrained by RLS/user preferences in a non-obvious way.
5. A UI calculation is also used by paper-trading or backend code and needs shared ownership.
6. A modal embeds API calls that should be part of a separate API-route migration.
7. A visual snapshot/test harness is missing and must be established first.
8. A behavior-changing UX fix appears; split it from extraction.
9. A page-level provider/context dependency blocks isolated component tests.
10. A parity guard is needed before deleting old inline view-model logic.

Progress log entries must distinguish: view model extracted, hook extracted, section component created, shell delegates, old inline logic removed, behavior-changing issue deferred.

---

## First implementation PR recommendation

Start with test harness/inventory plus pure AnalysisModal helpers:

1. Add dashboard inventory/static tests if a runner exists; otherwise add the smallest test harness setup as its own slice.
2. Extract `lib/analysis/viewModel.ts` and `buildTakeBetPayload.ts` from `AnalysisModal`.
3. Keep Supabase queries and JSX layout in place.
4. Run `npm run typecheck` and `npm run lint`.
5. Do not touch dashboard page, performance page, DFS, or bot tracker in the first PR.

This creates a low-risk seam around the largest component without changing data access or UI structure.

---

## Progress log

### 2026-05-19 initial migration documentation

Created from bounded code/brain deep dive.

Evidence inspected:

- Size inventory across `dashboard/src/app` and `dashboard/src/components`.
- Targeted reads of `AnalysisModal.tsx`, `dashboard/page.tsx`, `performance/page.tsx`, `usePredictions.ts`, and `usePerformanceData.ts`.
- Structure inventory for large dashboard pages/components.
- Callsite scan for `AnalysisModal`, `AskChat`, `/api/ask`, and existing hooks.
- GBrain hard facts and critical invariants.

Current status:

- Documentation only.
- No dashboard code changed.
- No typecheck/lint/tests/browser QA run.

---

## Done when

- Large dashboard pages/components are thin shells over hooks, view models, and presentational sections.
- Core UI calculations are pure-tested.
- Supabase data access lives in hooks/repositories with route/page shells preserved.
- Existing dashboard behavior, route paths, endpoint calls, and RLS-authenticated access remain stable.
- Visual/product behavior changes are split into separate approved work.
