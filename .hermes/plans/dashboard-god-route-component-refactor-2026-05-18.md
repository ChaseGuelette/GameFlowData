# Dashboard God Route and Component Refactor Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves this lane.

**Goal:** Decompose dashboard god routes/components into focused hooks, services, pure domain functions, and thin UI/API entrypoints.

**Architecture:** Fix correctness bugs first, then extract pure logic. Avoid broad UI rewrites. Keep pages/routes as orchestration shells over tested helpers.

**Tech Stack:** Next.js, React, TypeScript, Supabase client, existing dashboard test setup if present.

---

## Problem summary

Current high-risk files:

- `dashboard/src/app/(protected)/dfs/page.tsx`
- `dashboard/src/app/api/ask/route.ts`
- `dashboard/src/components/analysis/AnalysisModal.tsx`
- `dashboard/src/app/(protected)/dashboard/page.tsx`
- `dashboard/src/app/(protected)/performance/page.tsx`
- `dashboard/src/components/dfs/DfsTable.tsx`
- `dashboard/src/app/api/kalshi/approve/route.ts`
- `dashboard/src/app/api/arb/verify/route.ts`

Core issue:

Routes/components own too many responsibilities:

- fetching
- auth/rate limiting
- prompt construction
- domain mapping
- edge/Kelly math
- formatting
- mutations
- rendering
- state-machine transitions

---

## Immediate correctness issue

`dashboard/src/app/(protected)/dfs/page.tsx` appears to return early on sport gating before later hooks are called. If the sport config changes between renders, this can violate React Rules of Hooks.

Fix this before any cosmetic refactor.

---

## Target boundaries

Suggested shared modules:

- `dashboard/src/lib/betting/kelly.ts`
- `dashboard/src/lib/markets/statMappings.ts`
- `dashboard/src/lib/formatting/odds.ts`
- `dashboard/src/lib/formatting/money.ts`
- `dashboard/src/lib/dfs/normalize.ts`
- `dashboard/src/lib/dfs/comparisons.ts`
- `dashboard/src/lib/performance/aggregateBets.ts`
- `dashboard/src/lib/ask/promptBuilders/nba.ts`
- `dashboard/src/lib/ask/promptBuilders/mlb.ts`
- `dashboard/src/lib/ask/contextLoaders/nba.ts`
- `dashboard/src/lib/ask/contextLoaders/mlb.ts`
- `dashboard/src/lib/kalshi/tradeApprovalService.ts`

---

## Bite-sized tasks

### Task 1: Fix DFS hook-order issue

**Objective:** Ensure all hooks are called unconditionally or move the gated content into a child component.

**Files:**
- Modify: `dashboard/src/app/(protected)/dfs/page.tsx`

**Preferred approach:**
- Keep `DfsPage` as shell.
- If sport is not NBA, return unavailable UI.
- Else render `DfsPageContent` where all DFS-specific hooks live.

**Validation:**
- Run dashboard typecheck/lint command used in repo.
- If no known command, inspect package scripts first.

---

### Task 2: Centralize duplicated market/stat constants

**Objective:** Remove duplicate `COMBO_COMPONENTS` and `STAT_TO_MARKET` definitions.

**Files:**
- Create: `dashboard/src/lib/markets/statMappings.ts`
- Modify:
  - `dashboard/src/app/api/ask/route.ts`
  - `dashboard/src/components/analysis/AnalysisModal.tsx`
  - `dashboard/src/components/bot-tracker/BetAnalysisModal.tsx`
  - `dashboard/src/types/dfs.ts` if appropriate.

**Acceptance:**
- One source of truth for combo components and stat-to-market mapping.

---

### Task 3: Centralize Kelly and odds/money formatting

**Objective:** Remove duplicated math/formatters.

**Files:**
- Create:
  - `dashboard/src/lib/betting/kelly.ts`
  - `dashboard/src/lib/formatting/odds.ts`
  - `dashboard/src/lib/formatting/money.ts`
- Modify:
  - `dashboard/src/components/analysis/AnalysisModal.tsx`
  - `dashboard/src/lib/hooks/useUserBets.ts`
  - `dashboard/src/components/history/BetCard.tsx`
  - summary card components as low-risk follow-up.

**Acceptance:**
- Shared helpers match previous output for representative inputs.

---

### Task 4: Extract DFS pure comparison logic

**Objective:** Move route-level DFS computation into testable pure functions.

**Files:**
- Create:
  - `dashboard/src/lib/dfs/normalize.ts`
  - `dashboard/src/lib/dfs/comparisons.ts`
- Modify: `dashboard/src/app/(protected)/dfs/page.tsx`

**Acceptance:**
- Page still renders same row counts for current data.
- Pure functions can be tested without React.

---

### Task 5: Split Ask API route into services

**Objective:** Make `api/ask/route.ts` a thin HTTP orchestration layer.

**Files:**
- Create:
  - `dashboard/src/lib/ask/rateLimit.ts`
  - `dashboard/src/lib/ask/promptBuilders/nba.ts`
  - `dashboard/src/lib/ask/promptBuilders/mlb.ts`
  - `dashboard/src/lib/ask/contextLoaders/nba.ts`
  - `dashboard/src/lib/ask/contextLoaders/mlb.ts`
  - `dashboard/src/lib/ask/conversationPersistence.ts`
- Modify: `dashboard/src/app/api/ask/route.ts`

**Acceptance:**
- Route still handles auth/request/response.
- Prompt/context logic is reusable and testable.

---

### Task 6: Extract AnalysisModal data/math hooks

**Objective:** Keep modal focused on rendering and user interactions.

**Files:**
- Create:
  - `dashboard/src/components/analysis/useAnalysisHistory.ts`
  - `dashboard/src/components/analysis/useBookmakerLines.ts`
  - `dashboard/src/components/analysis/buildTakeBetData.ts`
- Modify: `dashboard/src/components/analysis/AnalysisModal.tsx`

**Acceptance:**
- Modal no longer owns Supabase line/history fetching directly.
- Betting math uses shared helpers.

---

### Task 7: Move Kalshi approval transitions into a service

**Objective:** Avoid HTTP route as state machine.

**Files:**
- Create: `dashboard/src/lib/kalshi/tradeApprovalService.ts`
- Modify:
  - `dashboard/src/app/api/kalshi/approve/route.ts`
  - `dashboard/src/lib/hooks/useTradeQueue.ts` if shared types are introduced.

**Acceptance:**
- Route validates/auths and calls service.
- Action/status strings are centralized.

---

## Validation commands

Before changing, inspect scripts:

`cd dashboard && npm run`

Likely validation candidates:

`cd dashboard && npm run typecheck`

`cd dashboard && npm run lint`

Use whichever scripts actually exist.

---

## Done when

- DFS hook-order bug is fixed.
- Shared mapping/formatting/math helpers exist.
- Largest route/component files are reduced by extracting pure services/hooks.
- API routes no longer own large business state machines.
