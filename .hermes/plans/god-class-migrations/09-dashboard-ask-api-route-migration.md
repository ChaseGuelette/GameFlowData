# Dashboard Ask API Route Migration Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task after Chase approves this lane. This is a migration plan, not approval to change auth/RLS, model provider, prompt content, query semantics, chat persistence schema, rate limits, or product behavior.

**Goal:** Rebuild the Dashboard Ask API from one 1,300-line route into explicit services for auth/rate limiting, request validation, context loading, prompt composition, LLM invocation, and chat persistence while preserving current NBA/MLB answers and response shape.

**Architecture:** Keep `dashboard/src/app/api/ask/route.ts` and `dashboard/src/app/api/ask/history/route.ts` as Next.js compatibility entry points. Extract pure prompt builders and typed context loaders first, then move Supabase query groups behind repository modules, then isolate Anthropic invocation and persistence. Add parity/characterization tests before changing prompts or queries.

**Tech Stack:** Next.js App Router, TypeScript, Supabase SSR client, Supabase RLS/authenticated role, Anthropic SDK, React client caller `AskChat`, Vitest/Jest-style test target if introduced.

---

## Relevant prior lessons/invariants

Retrieved before writing this plan:

- `operations/hard-facts`
- `operations/critical-invariants`
- GBrain query for dashboard Ask/API context returned no focused canonical page, so this doc is grounded primarily in live dashboard code plus global architecture facts.

Applied facts/invariants:

1. GameFlow dashboard is a Next.js app hosted on Vercel.
2. Dashboard/client code uses Supabase `authenticated` role with RLS, unlike Python backend `postgres` access.
3. Do not move backend-only privileged DB behavior into dashboard routes casually.
4. Ask route must not alter model math, recommendation edges, or calibration; it explains existing predictions only.
5. Dashboard API route changes can affect user-facing paid product behavior, so preserve response schema first.

---

## Executive diagnosis

The Ask API route is the largest dashboard route and combines many unrelated responsibilities:

- `dashboard/src/app/api/ask/route.ts`
  - 1,301 total lines
  - 1,057 non-comment LOC
  - module-level helpers and constants
  - `buildSystemPrompt(...)`: NBA prompt/context builder, lines 76-394
  - `buildMlbSystemPrompt(...)`: MLB prompt/context builder, lines 406-553
  - `POST(...)`: auth, rate limit, request parsing, NBA/MLB context queries, prompt assembly, LLM call, persistence, response, lines 563-1301
- `dashboard/src/app/api/ask/history/route.ts`
  - 77 total lines
  - `GET(...)` and `DELETE(...)` chat history endpoints
- Client callers:
  - `dashboard/src/components/analysis/AskChat.tsx`
  - `dashboard/src/components/analysis/AnalysisModal.tsx`

Current `route.ts` owns:

- in-memory per-user rate limiting
- Supabase auth
- request JSON parsing and validation
- sport inference from stat prefix
- NBA prompt construction
- MLB prompt construction
- dozens of dashboard Supabase data fetches
- advanced stats mapping
- injury/teammate/opponent injury context
- depth chart derivation
- vs-opponent logs
- bookmaker line context
- conversation history trimming
- Anthropic client construction and message call
- chat persistence with timeout guard
- response serialization

The route is hard to test because a single `POST` path mixes pure string formatting, RLS-scoped data access, and network LLM side effects.

---

## Current ownership problems

### 1. Rate limiting is module-local and untyped

Current code:

- `RATE_LIMIT = 20`
- `WINDOW_MS = 24 * 60 * 60 * 1000`
- `rateLimitMap = new Map<string, RateBucket>()`
- `checkRateLimit(userId)`

Why this is wrong:

- In-memory map is Vercel-instance-local and behavior should be documented/characterized before any durable rate-limit migration.
- It is not reusable by history routes or future Ask variants.
- It cannot be tested independently without importing the full route.

Target owner:

- `dashboard/src/lib/ask/rateLimit.ts`

Initial rule:

- Preserve current in-memory behavior; do not move to DB/Redis in this migration.

---

### 2. Request validation and response schema are implicit

Current code parses:

- `question`
- `conversationHistory`
- `playerContext`
- `prediction`, `insights`, `bookmakerLines`, `isOverBet`, `edge`, `probability`

Why this is wrong:

- Client and route share implicit shape.
- Any schema drift can break `AskChat` silently.
- History route response shape is separately typed in `types/chat.ts` but main Ask body is route-local.

Target owners:

- `dashboard/src/lib/ask/contracts.ts`
- `dashboard/src/types/chat.ts` compatibility where applicable

Tests:

- invalid JSON returns 400.
- missing/too-long question returns 400.
- unauthenticated returns 401.
- rate-limited returns 429 with `remaining: 0`.
- success response still includes answer/conversation id fields currently consumed by `AskChat`.

---

### 3. NBA and MLB prompt builders are huge and query-coupled

Current pure-ish builders:

- `buildSystemPrompt(...)`: NBA-specific prompt and model context construction.
- `buildMlbSystemPrompt(...)`: MLB-specific prompt and model context construction.

They embed:

- stat labels
- combo stat handling
- date formatting
- model quantile sections
- game log sections
- rolling averages
- injury context
- opponent defense
- matchup lines
- bookmaker lines
- insights
- depth charts and injury timelines
- MLB pitcher/batter-specific sections
- park factors and opposing pitcher context

Target owners:

- `dashboard/src/lib/ask/prompts/nbaPrompt.ts`
- `dashboard/src/lib/ask/prompts/mlbPrompt.ts`
- `dashboard/src/lib/ask/prompts/sections.ts`
- `dashboard/src/lib/ask/statLabels.ts`

First extraction should move builders unchanged and re-export wrappers from route if needed.

---

### 4. Supabase context loading is monolithic

The `POST` handler performs many data fetches inline.

MLB path includes:

- game logs from pitching/batting tables
- rolling averages from pitching/batting average tables
- player info
- game schedule
- park factors
- player team lookup
- opposing pitcher averages/logs/info

NBA path includes:

- game logs
- advanced stats map
- player position/group
- team/opponent injury context
- rolling averages
- player injury
- vs-opponent logs
- team recent stats for depth chart
- teammate positions/injuries
- opponent defense by position
- bookmaker lines/context from client payload and/or DB-derived context

Target owners:

- `dashboard/src/lib/ask/context/nbaContextLoader.ts`
- `dashboard/src/lib/ask/context/mlbContextLoader.ts`
- `dashboard/src/lib/ask/context/injuryContextLoader.ts`
- `dashboard/src/lib/ask/context/depthChartLoader.ts`
- `dashboard/src/lib/ask/context/lineContext.ts`

Safety:

- Keep Supabase client passed in from route.
- Do not use Supabase admin client unless a separate auth/RLS design is approved.
- Preserve current query tables and filters first.

---

### 5. LLM invocation is route-local

Current route constructs:

- `new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })`
- `anthropic.messages.create(...)`
- messages from system prompt, trimmed history, and user question

Why this is wrong:

- Provider/model/config behavior cannot be tested independently.
- Error handling and missing key behavior are route-local.
- Future provider changes would touch the giant route.

Target owner:

- `dashboard/src/lib/ask/llmClient.ts`

Tests:

- messages are built with last 5 history items, preserving current behavior.
- provider response text extraction preserved.
- missing/failed provider call maps to current error behavior.

---

### 6. Chat persistence is mixed with response generation

Current route persists after LLM response via an async promise with a timeout guard.

History route owns:

- `GET /api/ask/history`
- `DELETE /api/ask/history`

Target owner:

- `dashboard/src/lib/ask/chatRepository.ts`

Responsibilities:

- find/create conversation
- append messages
- fetch history
- delete conversation
- timeout wrapper if kept

Compatibility:

- `history/route.ts` delegates to repository but keeps URL/response shape.

---

### 7. Sport routing is stat-prefix based and buried in POST

Current code:

- `const isMlb = prediction.stat.startsWith('batter_') || prediction.stat.startsWith('pitcher_')`
- pitcher vs batter branching throughout MLB path.

Target owner:

- `dashboard/src/lib/ask/sportRouter.ts`

Tests:

- NBA stats route to NBA context.
- `batter_*` and `pitcher_*` route to MLB context.
- unsupported stat behavior preserved.

---

## Target design by responsibility

### A. `lib/ask/contracts.ts`

Typed request/response/context contracts.

### B. `lib/ask/rateLimit.ts`

Current in-memory rate limiter, extracted and tested.

### C. `lib/ask/sportRouter.ts`

Sport/stat routing helpers.

### D. `lib/ask/prompts/*.ts`

Prompt sections and sport prompt builders.

### E. `lib/ask/context/*.ts`

Supabase context repositories/loaders.

### F. `lib/ask/llmClient.ts`

Anthropic wrapper and message construction.

### G. `lib/ask/chatRepository.ts`

Chat history/persistence repository.

### H. API compatibility routes

- `app/api/ask/route.ts`: authenticate, rate-limit, validate, orchestrate services, return response.
- `app/api/ask/history/route.ts`: delegate to repository.

---

## Refactor phases

### Phase 0: Characterization and inventory tests

Objective: Lock current route shape before extraction.

Files:

- Create: `dashboard/src/lib/ask/__tests__/askRoute.inventory.test.ts`
- Create fixture bodies under `dashboard/src/lib/ask/__fixtures__/`

Tests:

- `route.ts` exports `POST`.
- history route exports `GET` and `DELETE`.
- `AskChat` calls `/api/ask` and `/api/ask/history` with current paths.
- question length and auth/rate-limit response statuses characterized via mocked route dependencies if test harness exists.

Validation:

`cd dashboard && npm test -- askRoute.inventory`

If no test harness exists, first add the minimal test runner in a separate setup slice and do not mix with route extraction.

---

### Phase 1: Extract pure constants and sport/stat helpers

Objective: Move no-side-effect helpers first.

Files:

- Create: `dashboard/src/lib/ask/statLabels.ts`
- Create: `dashboard/src/lib/ask/sportRouter.ts`
- Create: `dashboard/src/lib/ask/formatting.ts`
- Create: `dashboard/src/lib/ask/__tests__/sportRouter.test.ts`
- Modify: `route.ts` to import/delegate

Tests:

- combo stat detection preserved.
- MLB stat-prefix detection preserved.
- date formatting preserved.
- stat labels unchanged.

---

### Phase 2: Extract prompt builders unchanged

Objective: Move `buildSystemPrompt` and `buildMlbSystemPrompt` behind tested modules.

Files:

- Create: `dashboard/src/lib/ask/prompts/nbaPrompt.ts`
- Create: `dashboard/src/lib/ask/prompts/mlbPrompt.ts`
- Create: `dashboard/src/lib/ask/prompts/sections.ts`
- Create: `dashboard/src/lib/ask/__tests__/promptBuilders.test.ts`
- Modify: `route.ts`

Tests:

- snapshot or substring parity for representative NBA prompt.
- snapshot or substring parity for representative MLB pitcher prompt.
- snapshot or substring parity for representative MLB batter prompt.
- quantile/model context sections preserved.

Safety:

- Do not edit prompt wording beyond imports unless snapshot tests are intentionally updated with approval.

---

### Phase 3: Extract contracts and request validation

Objective: Make input/output shapes explicit.

Files:

- Create: `dashboard/src/lib/ask/contracts.ts`
- Create: `dashboard/src/lib/ask/validation.ts`
- Create: `dashboard/src/lib/ask/__tests__/validation.test.ts`
- Modify: `route.ts`

Tests:

- missing question.
- over-500-char question.
- missing player context current behavior preserved.
- malformed conversation history current behavior preserved.

---

### Phase 4: Extract rate limiter

Objective: Move current in-memory limiter unchanged.

Files:

- Create: `dashboard/src/lib/ask/rateLimit.ts`
- Create: `dashboard/src/lib/ask/__tests__/rateLimit.test.ts`
- Modify: `route.ts`

Tests:

- first 20 requests allowed.
- 21st request denied.
- window expiration behavior preserved.
- remaining count semantics preserved.

Non-goal:

- Do not implement durable rate limiting in this slice.

---

### Phase 5: Extract LLM client/message builder

Objective: Isolate Anthropic side effect.

Files:

- Create: `dashboard/src/lib/ask/llmClient.ts`
- Create: `dashboard/src/lib/ask/__tests__/llmClient.test.ts`
- Modify: `route.ts`

Tests:

- history trimmed to last 5 messages.
- user question appended last.
- text blocks are joined as current route does.
- model/env defaults preserved.

---

### Phase 6: Extract chat repository and history route delegation

Objective: Centralize chat persistence.

Files:

- Create: `dashboard/src/lib/ask/chatRepository.ts`
- Create: `dashboard/src/lib/ask/__tests__/chatRepository.test.ts`
- Modify: `app/api/ask/route.ts`
- Modify: `app/api/ask/history/route.ts`

Tests:

- GET history response shape preserved.
- DELETE behavior preserved.
- POST persistence timeout behavior preserved or characterized.
- RLS/authenticated user filtering preserved.

---

### Phase 7: Extract MLB context loader

Objective: Move MLB Supabase query groups first because it is a contained branch.

Files:

- Create: `dashboard/src/lib/ask/context/mlbContextLoader.ts`
- Create: `dashboard/src/lib/ask/__tests__/mlbContextLoader.test.ts`
- Modify: `route.ts`

Tests:

- pitcher query table/columns preserved.
- batter query table/columns preserved.
- park factor and opposing pitcher context preserved.
- missing game info fallback preserved.

---

### Phase 8: Extract NBA context loader

Objective: Move NBA context query groups after prompt and validation seams exist.

Files:

- Create: `dashboard/src/lib/ask/context/nbaContextLoader.ts`
- Create: `injuryContextLoader.ts`, `depthChartLoader.ts` if needed.
- Create: `dashboard/src/lib/ask/__tests__/nbaContextLoader.test.ts`
- Modify: `route.ts`

Tests:

- game log/advanced stat map preserved.
- injury and opponent injury dedup preserved.
- depth chart calculations preserved.
- vs-opponent logs preserved.
- opponent defense query/group behavior preserved.

---

### Phase 9: Shrink route and add anti-regrowth guards

Recommended endpoint:

- `app/api/ask/route.ts` under 220 non-comment LOC.
- `history/route.ts` under 80 non-comment LOC and repository-backed.

Guards:

- no direct prompt templates in route.
- no direct Anthropic construction in route.
- no long Supabase query chains in route.
- route orchestrates services only.

---

## Files likely touched

Existing:

- `dashboard/src/app/api/ask/route.ts`
- `dashboard/src/app/api/ask/history/route.ts`
- `dashboard/src/components/analysis/AskChat.tsx` only if request/response types are imported later
- `dashboard/src/types/chat.ts`

New:

- `dashboard/src/lib/ask/contracts.ts`
- `dashboard/src/lib/ask/rateLimit.ts`
- `dashboard/src/lib/ask/sportRouter.ts`
- `dashboard/src/lib/ask/formatting.ts`
- `dashboard/src/lib/ask/statLabels.ts`
- `dashboard/src/lib/ask/prompts/nbaPrompt.ts`
- `dashboard/src/lib/ask/prompts/mlbPrompt.ts`
- `dashboard/src/lib/ask/prompts/sections.ts`
- `dashboard/src/lib/ask/context/nbaContextLoader.ts`
- `dashboard/src/lib/ask/context/mlbContextLoader.ts`
- `dashboard/src/lib/ask/context/injuryContextLoader.ts`
- `dashboard/src/lib/ask/context/depthChartLoader.ts`
- `dashboard/src/lib/ask/llmClient.ts`
- `dashboard/src/lib/ask/chatRepository.ts`
- `dashboard/src/lib/ask/__tests__/*.test.ts`

---

## Validation commands

Typecheck:

`cd dashboard && npm run typecheck`

Lint:

`cd dashboard && npm run lint`

Ask tests after adding harness:

`cd dashboard && npm test -- ask`

Route smoke with mocked dependencies if test harness supports it:

`cd dashboard && npm test -- askRoute`

Diff hygiene:

`git diff --check -- dashboard/src/app/api/ask dashboard/src/lib/ask dashboard/src/components/analysis/AskChat.tsx dashboard/src/types/chat.ts .hermes/plans/god-class-migrations/09-dashboard-ask-api-route-migration.md`

Manual QA after implementation, not during doc creation:

- Open dashboard prediction Analysis Modal.
- Ask one NBA question.
- Ask one MLB pitcher question.
- Ask one MLB batter question.
- Refresh and verify history loads.
- Delete chat history and verify empty state.

---

## Risk controls / non-goals

Non-goals:

- Do not change prompt wording without snapshot/parity approval.
- Do not change Anthropic model/provider behavior.
- Do not change rate-limit count/window.
- Do not replace in-memory rate limiting with DB/Redis in this migration.
- Do not use Supabase admin client for user-facing Ask context.
- Do not change chat DB schema.
- Do not change `AskChat` UX in the same PR.
- Do not merge this with AnalysisModal/dashboard page refactors.

Hard rules:

- Preserve API route URLs and methods.
- Preserve response shape consumed by `AskChat`.
- Preserve RLS/authenticated user behavior.
- Add prompt parity tests before modifying prompt builders.
- Any provider/model/prompt behavior change requires a separate approved slice.

---

## Expansion checkpoints learned from Kalshi

Trigger a new named sub-slice if you discover:

1. A prompt section has hidden product/marketing wording requirements.
2. A Supabase query relies on RLS side effects or authenticated user context.
3. A client component depends on incidental response fields.
4. A chat persistence timeout masks failures that need separate handling.
5. A rate-limit change would require durable storage.
6. NBA and MLB prompt builders diverge enough to need shared section primitives.
7. A context loader query is used by another dashboard component and should move to a shared repository.
8. A behavior-changing prompt/model fix appears; split it from extraction.
9. A route test harness is missing; create setup as its own slice.
10. A parity guard is needed before deleting route-local builders.

Progress log entries must distinguish: helper extracted, prompt parity verified, context loader introduced, route delegates, old inline block removed, behavior-changing issue deferred.

---

## First implementation PR recommendation

Start with pure helpers and inventory only:

1. Add inventory tests or static route-shape checks for `POST`, history `GET/DELETE`, and `AskChat` endpoint paths.
2. Extract `statLabels.ts`, `formatting.ts`, and `sportRouter.ts`.
3. Keep prompt builders, Supabase queries, LLM calls, and persistence in `route.ts`.
4. Run dashboard typecheck/lint and helper tests.

This creates a safe seam without changing prompt text, data access, or LLM behavior.

---

## Progress log

### 2026-05-19 initial migration documentation

Created from bounded code/brain deep dive.

Evidence inspected:

- Structure/LOC inventory for `dashboard/src/app/api/ask/route.ts` and `history/route.ts`.
- Targeted reads of Ask `POST(...)`, NBA/MLB prompt builders, and client callsites.
- Callsite scan for `/api/ask`, `AskChat`, and `AnalysisModal`.
- GBrain hard facts and critical invariants.

Current status:

- Documentation only.
- No dashboard code changed.
- No typecheck/lint/tests/API calls run.

---

## Done when

- Ask route is a thin authenticated orchestrator.
- Prompt builders, sport routing, validation, rate limit, LLM invocation, context loading, and chat persistence have separate tested owners.
- NBA/MLB prompt parity is verified before route-local builders are removed.
- `AskChat` continues to work with unchanged endpoint/response behavior.
- RLS/authenticated access remains preserved.
