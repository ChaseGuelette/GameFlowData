# Dashboard and Product Architecture Audit

> **Deprecation disposition (2026-07-18):** Kalshi sports and its active app/control surfaces are decommissioning targets. Findings F-01/F-02 and other Kalshi/arb-specific evidence are preserved here, but remediation priority is superseded by `12-kalshi-deprecation-and-project-pruning.md`: remove or disable privileged control surfaces in dependency order rather than invest in a Kalshi product hardening program. Non-Kalshi auth, billing, Ask, performance, testing, and accessibility findings remain active.

**Audit lane:** 07 — dashboard/product architecture
**Audit date:** 2026-07-18
**Mode:** Read-only source audit; this report is the only file written
**Scope:** `dashboard/src/app`, `dashboard/src/components`, `dashboard/src/lib`, `dashboard/src/types`, dashboard middleware/config/test files, relevant manifests, god-class lanes 09-10, and audit report 00
**Excluded:** secrets, live Supabase/Vercel/Stripe/API calls, DB inspection, deployment state, browser QA, implementation/config/register/plan edits

## Executive assessment

The highest-risk dashboard debt is not component size. It is missing authorization at server-side service-role mutation boundaries:

1. `/api/arb/verify` is outside middleware and performs a service-role update without any route-local authentication or admin authorization.
2. Kalshi queue/approval/resume/cancel APIs require only an authenticated user before using the service role, while admin enforcement exists only on the `/bot-tracker` page.
3. Subscription middleware excludes all API routes, so `/api/ask` is available to any authenticated account even when page subscriptions are enforced.

The paid-product path also has a reliability gap: Stripe webhook DB failures are logged but acknowledged with HTTP 200, which can permanently leave local entitlement state stale. The performance page eagerly starts four data domains, fetches unbounded histories, and renders query failures as zero/empty metrics for three tabs. The Ask route remains a 1,301-line, multi-round query/LLM/persistence orchestrator with process-local limiting and implicit runtime contracts.

Lane 09 and Lane 10 remain documentation-only, but both need refreshed starting assumptions. Lane 09 must preserve the now-existing chat persistence while prioritizing authorization, request contracts, durable usage enforcement, and bounded context loading. Lane 10 remains structurally current for the large route/page/component set, but blanket “history has no pagination” and “chat is not persisted” claims must not return.

## Method and coverage

### Sources reconciled

- `AGENTS.md`
- GameFlow structural audit and god-class migration guidance
- `.hermes/audits/tech-debt/00-existing-inventory-reconciliation.md`
- `.hermes/plans/god-class-migrations/09-dashboard-ask-api-route-migration.md`
- `.hermes/plans/god-class-migrations/10-dashboard-god-components-pages-migration.md`
- Current allowed dashboard source/config/manifests

### Mechanical inventory

The allowed source roots contain 146 TypeScript/TSX files. Current largest files are:

| Lines | Path |
|---:|---|
| 1,301 | `dashboard/src/app/api/ask/route.ts` |
| 1,043 | `dashboard/src/components/analysis/AnalysisModal.tsx` |
| 733 | `dashboard/src/app/(protected)/dashboard/page.tsx` |
| 688 | `dashboard/src/app/(protected)/performance/page.tsx` |
| 552 | `dashboard/src/components/dfs/DfsTable.tsx` |
| 546 | `dashboard/src/app/(protected)/dfs/page.tsx` |
| 482 | `dashboard/src/components/bot-tracker/TradeApprovalPanel.tsx` |
| 439 | `dashboard/src/app/(protected)/account/page.tsx` |

No dashboard test files were found. `dashboard/package.json:5-10,26-34` has no `test` or `typecheck` script and no test-runner/component-test dependencies.

### Severity vocabulary

- **Critical:** unauthenticated or under-authorized privileged mutation; direct live-trading/security exposure.
- **High:** paid-access bypass, durable state divergence, materially wrong user-facing data, or severe scale/reliability risk.
- **Medium:** recurring latency, ownership, UX failure, observability, or migration-safety debt.
- **Low:** bounded polish/accessibility debt without evidence of data or privilege impact.

---

## Findings

### F-01 — Critical: arb verification is an unauthenticated service-role mutation

**Evidence**

- `dashboard/src/middleware.ts:8-18` excludes every `api/` path from middleware.
- `dashboard/src/app/api/arb/verify/route.ts:4-8` creates a Supabase client directly with `SUPABASE_SERVICE_ROLE_KEY`.
- `dashboard/src/app/api/arb/verify/route.ts:9-25` accepts `id`, `action`, and `notes` and updates `verified_market_links` without calling `auth.getUser()` or `is_admin()`.
- `dashboard/src/lib/hooks/useArbScanner.ts:117-123` calls this endpoint and removes the row locally without checking `res.ok`.
- `dashboard/src/app/(protected)/arb-scanner/page.tsx:19-35` has only a client-rendered `useAdmin()` gate; that gate cannot protect the API.

**Concrete failure mode**

Any network client that can reach the deployed route can approve/reject an arbitrary verification row with service-role authority. The UI can also display a successful local removal after a failed server mutation because `decide` ignores the response status.

**Confidence:** High. The absence of route-local auth and middleware coverage is explicit in current code.

**Plan interaction:** Outside Lane 09’s Ask-only scope and only partially adjacent to Lane 10’s arb page. This should be a small security-boundary fix before broad component migration, not hidden inside a god-component refactor.

**Safe first evidence step:** Add a route characterization test with mocked Supabase clients proving anonymous and non-admin requests are rejected before `createAdminClient`/service-role mutation is reachable. Do not call the live route.

**Done when:** The route authenticates server-side, authorizes admin capability server-side, validates/coerces the body, records/checks mutation errors, returns non-2xx on failure, and tests prove anonymous/non-admin callers cannot reach the service-role update.

---

### F-02 — Critical: any authenticated user can read and mutate Kalshi live-trading control state

**Evidence**

- `dashboard/src/lib/supabase/middleware.ts:6,77-85` applies admin page gating only to `/bot-tracker`.
- `dashboard/src/middleware.ts:17` excludes API routes, so that page gate does not cover `/api/kalshi/*`.
- `dashboard/src/app/api/kalshi/queue/route.ts:5-14` checks only for a user, then uses an admin client to read a no-RLS trade queue.
- `dashboard/src/app/api/kalshi/approve/route.ts:10-16,30-40,63-93,112-194` checks only for a user, then can dismiss, retry, reject, approve, or approve-all service-role rows.
- `dashboard/src/app/api/kalshi/resume/route.ts:5-24` checks only for a user, then clears the live-trading halt with the admin client.
- `dashboard/src/app/api/kalshi/cancel-queue/route.ts:5-25` exposes the cancel queue to any authenticated user.
- `dashboard/src/app/api/kalshi/cancel-approve/route.ts:10-16,30-80` lets any authenticated user approve/reject cancellation rows.
- The approval updates set status/timestamps but no actor identity (`approve/route.ts:170-177`; `cancel-approve/route.ts:70-74`).

**Concrete failure mode**

A normal signed-in dashboard account can bypass the hidden admin page and directly approve queued live orders, retry failed orders, approve cancellations, or resume halted live trading. The resulting rows do not identify the acting user, limiting incident reconstruction.

**Confidence:** High. Every listed route has authentication but no admin authorization before service-role access.

**Plan interaction:** This is a server authorization boundary, not a Lane 10 component-splitting concern. Any future bot-tracker extraction must preserve a centralized server-side admin guard; client `useAdmin()` is presentation only.

**Safe first evidence step:** Build a static/route test matrix for all `/api/kalshi/*` handlers using mocked `getUser`, `rpc('is_admin')`, and admin clients. Assert service-role creation is unreachable for anonymous and non-admin callers.

**Done when:** All Kalshi privileged APIs share one tested server authorization guard, reject non-admin callers, validate bounded IDs/actions, attribute mutations to an actor/audit event where schema permits, and page visibility is no longer treated as authorization.

---

### F-03 — High: subscription enforcement stops at pages, not paid API capability

**Evidence**

- `dashboard/src/middleware.ts:17` explicitly excludes `api/`.
- `dashboard/src/lib/supabase/middleware.ts:63-75` enforces `is_subscribed` only in middleware when `SUBSCRIPTION_REQUIRED=true`.
- `dashboard/src/app/api/ask/route.ts:563-578` requires an authenticated user and applies a local rate limit, but never checks subscription entitlement.
- `dashboard/src/app/api/slate/route.tsx:52-53` is another protected-product API with its own auth path rather than the subscription middleware boundary.

**Concrete failure mode**

When subscription gating is enabled, an authenticated but unsubscribed account is redirected away from paid pages yet can call `/api/ask` directly and consume LLM/provider budget. Page gating therefore does not define product capability gating.

**Confidence:** High for `/api/ask`; medium for the intended paid status of every other API because product packaging was not verified live.

**Plan interaction:** Lane 09 should add an explicit entitlement owner to the thin route. Lane 10 must not assume protected page layout implies API authorization. Do not solve this by broadly putting webhooks/public callbacks behind middleware.

**Safe first evidence step:** Characterization tests with `SUBSCRIPTION_REQUIRED` on/off and mocked `is_subscribed`, covering anonymous, authenticated-unsubscribed, and subscribed Ask requests. Inventory each API as public/authenticated/subscribed/admin before changing matcher rules.

**Done when:** Paid APIs enforce entitlement server-side through a shared tested guard; public callbacks (Stripe webhook/auth callback) remain intentionally exempt; the API capability matrix is explicit and non-subscribers cannot consume paid provider calls.

---

### F-04 — High: Stripe webhook failures are acknowledged, allowing permanent entitlement drift

**Evidence**

- `dashboard/src/app/api/stripe/webhook/route.ts:69-90` logs a failed checkout upsert but continues.
- The same pattern exists for subscription update, deletion, and payment-failure writes at `webhook/route.ts:97-116,122-132,142-151`.
- `webhook/route.ts:159` returns `{ received: true }` with HTTP 200 after those logged DB errors.
- `dashboard/src/lib/supabase/middleware.ts:69-74` gates product access from local `is_subscribed` state.
- `dashboard/src/app/(protected)/account/page.tsx:296-305,336-339` displays “Your subscription is active” solely from `?checkout=success`, before proving webhook/local entitlement convergence.

**Concrete failure mode**

A transient Supabase write error causes Stripe to receive 200 and stop retrying while `user_subscriptions` remains missing or stale. A paying user can see a success banner and then be redirected to subscribe, or a cancellation/payment failure can remain unreflected locally.

**Confidence:** High. The response path after a returned Supabase error is explicit.

**Plan interaction:** Not owned by Lane 09/10. It is a bounded subscription-state reliability slice that should precede visual account/subscribe refactors.

**Safe first evidence step:** Unit-test each handled Stripe event with a verified mock event and a mocked Supabase error; assert a retryable non-2xx response and no success acknowledgement. No Stripe API call is needed.

**Done when:** Any required persistence failure returns non-2xx, retries are idempotent, webhook event identity/outcome is observable, unsupported events still acknowledge safely, and account success is reconciled against actual local subscription state rather than the query string alone.

---

### F-05 — High: checkout customer creation is non-idempotent and local subscription state is the delayed owner

**Evidence**

- `dashboard/src/app/api/stripe/checkout/route.ts:31-46` reads `stripe_customer_id`, creates a customer if absent, but does not persist that customer before returning.
- `checkout/route.ts:48-56` checks prior subscriptions only on that newly selected customer.
- `checkout/route.ts:60-74` creates a checkout session without an idempotency key.
- The customer ID is first persisted asynchronously by the webhook at `webhook/route.ts:69-87`.
- Displayed prices/features are duplicated in `dashboard/src/app/(public)/pricing/page.tsx:5-12,29-85` and `dashboard/src/app/(protected)/subscribe/page.tsx:6-13,55-118`, while actual price IDs come from env in `dashboard/src/lib/stripe.ts:15-18`.

**Concrete failure mode**

Two checkout requests before webhook completion can create two Stripe customers and two sessions for one user. Trial-abuse prevention is scoped to whichever customer was just created, and displayed dollar amounts can drift from the configured Stripe Price without any build-time failure.

**Confidence:** High for the race/duplication possibility and display-source duplication; actual occurrence was not queried.

**Plan interaction:** Separate product-domain ownership is needed (`plans`, `entitlement`, `billing customer/session`). Do not fold this into Lane 10 page decomposition without first locking checkout behavior.

**Safe first evidence step:** Mock two concurrent checkout handler calls with no local customer and assert customer/session creation count; add a pure test mapping plan keys to display metadata and configured price IDs.

**Done when:** Customer/session creation is idempotent per user/attempt, the customer mapping is durably established before reuse decisions, trial eligibility has one authoritative owner, and pricing copy/amount/plan IDs derive from one typed product catalog.

---

### F-06 — High: Ask usage enforcement is process-local, charged before validation, and bypassable across instances

**Evidence**

- `dashboard/src/app/api/ask/route.ts:8-32` stores a 20-per-day bucket in a module-level `Map`.
- `ask/route.ts:571-590` consumes quota before JSON parsing and question validation.
- `ask/route.ts:747-795,1238-1299` makes provider calls after that local check; provider failures still consume the local count.
- Existing Lane 09 correctly identifies instance-local behavior at `.hermes/plans/god-class-migrations/09-dashboard-ask-api-route-migration.md:74-96`, but its Phase 4 intentionally preserves it rather than making usage enforcement durable.

**Concrete failure mode**

Limits reset on cold starts and are independent across Vercel instances, so users can exceed intended spend controls. Conversely, malformed requests and provider outages consume a user’s quota before any answer is produced. The returned `remaining` value is not globally authoritative.

**Confidence:** High.

**Plan interaction:** Lane 09’s extraction remains useful, but “extract unchanged” is only a characterization phase. Durable usage/entitlement policy needs a separate behavior-changing, approved slice after tests.

**Safe first evidence step:** Pure characterization tests for invalid JSON, invalid question, provider failure, process restart, and two isolated limiter instances. Define whether quota means request attempts or successful answers before implementation.

**Done when:** A documented atomic usage policy is enforced across instances, validation occurs at the intended charge point, concurrent requests cannot overspend the allowance, remaining/reset semantics are accurate, and failure charging is intentional and tested.

---

### F-07 — High: performance eagerly fetches unbounded domains and turns several failures into believable zero metrics

**Evidence**

- `dashboard/src/app/(protected)/performance/page.tsx:73-86` starts props, DFS, My Bets, and track-record hooks regardless of active tab.
- `dashboard/src/lib/hooks/usePerformanceData.ts:19-29` fetches complete daily-log and resolved-bet histories with no date/range/limit.
- `usePerformanceData.ts:31-32` converts failed parallel responses to empty arrays without checking `logRes.error` or `betsRes.error`.
- DFS fetches two full histories at `usePerformanceData.ts:108-117`; My Bets fetches all resolved rows at `usePerformanceData.ts:133-160`.
- The page checks `isLoading` but not `error` for props/DFS/My Bets (`performance/page.tsx:397-430,434-475,531-573`), while only the track-record tab displays its error (`performance/page.tsx:618-628`).

**Concrete failure mode**

Opening one tab launches all four data domains. As histories grow, payload/CPU/render latency grows without bound. A permission, timeout, or query failure can render `$0`, `0.0%`, or empty charts as if they were valid performance, causing users to misread runtime failure as actual bankroll/model results.

**Confidence:** High from source behavior; production row counts/latency remain unverified.

**Plan interaction:** This is the strongest current Lane 10 performance-page slice. Extracting view models alone will not fix it; query ownership, active-tab enablement, bounded aggregate contracts, and explicit error states must be part of the refreshed plan. This is not a rediscovery of history-page pagination.

**Safe first evidence step:** Mock each query failure independently and record rendered state; instrument mocked row counts and assert inactive-tab query functions are not called. Then design aggregate/range contracts without querying production.

**Done when:** Only active/needed domains fetch, large histories are bounded or server-aggregated, every query error is distinguishable from valid empty/zero data, retry is available, and KPI definitions remain parity-tested.

---

### F-08 — Medium: Ask remains a latency-amplifying route with implicit client-controlled contracts

**Evidence**

- `dashboard/src/app/api/ask/route.ts` remains 1,301 lines; `POST` spans `ask/route.ts:563-1301`.
- `ask/route.ts:588-596` destructures nested `playerContext` without runtime schema validation and trusts client-provided prediction/edge/probability/lines.
- MLB performs an initial four-query round (`ask/route.ts:601-643`), a sequential team lookup (`ask/route.ts:650-668`), then another four-query round (`ask/route.ts:679-712`) before the provider call.
- NBA performs five base queries (`ask/route.ts:802-847`), subsequent advanced/team, injury enrichment, opponent, depth-chart, and timeline rounds (`ask/route.ts:857-1207`) before the provider call.
- Only persistence has a two-second race (`ask/route.ts:787-790,1287-1291`); context loading/provider invocation has no route-local timeout budget.
- Client/server request shape is implicit: `AskChat` sends the body at `dashboard/src/components/analysis/AskChat.tsx:102-120`, while `dashboard/src/types/chat.ts:1-16` types responses/history but not the Ask request.

**Concrete failure mode**

One slow query delays later rounds and the provider call; a malformed nested body can become a 500 instead of a 400; fabricated client context can produce authoritative-looking analysis inconsistent with stored predictions. Partial query errors are mostly treated as missing context, reducing answer quality without a user-visible degraded-state signal.

**Confidence:** High for structure/contracts; actual p95 latency was not observed.

**Plan interaction:** Directly confirms Lane 09’s route decomposition, but the refreshed sequence should put runtime contracts, server-owned canonical context, timeout/degradation policy, and query-round observability ahead of cosmetic extraction. Preserve current chat persistence and response shape.

**Safe first evidence step:** Add mocked request-schema tests and a dependency-latency harness that records query rounds/call counts for one NBA, MLB pitcher, and MLB batter request. No provider or DB call is needed.

**Done when:** Invalid bodies fail as typed 400s; security/product-critical context is server-derived or explicitly marked client display context; route latency has a bounded budget; query/provider failures are classified; and the route is a thin orchestrator over tested contracts/loaders.

---

### F-09 — Medium: AnalysisModal fetches and reduces raw props client-side, then presents failures as empty data

**Evidence**

- `dashboard/src/components/analysis/AnalysisModal.tsx:119-170` owns preferences, bankroll/Kelly controls, sport routing, and modal state.
- It performs sport-specific history queries inline at `AnalysisModal.tsx:172-224` and treats query errors as empty history.
- It queries props directly with no snapshot/date/range/limit at `AnalysisModal.tsx:226-254`, then sorts/deduplicates snapshots in the browser at `AnalysisModal.tsx:260-311`.
- Errors are only logged (`AnalysisModal.tsx:256-258`) and `linesLoading` is cleared (`AnalysisModal.tsx:313`), causing the render to use the same “No lines available” family of states (`AnalysisModal.tsx:749-751`).
- The component remains 1,043 lines and includes pure Kelly policy at `AnalysisModal.tsx:95-117` alongside DB access and rendering.

**Concrete failure mode**

A failed/slow raw-props query is indistinguishable from a legitimate market with no lines. Fetching all snapshots for a player/game/market and reducing them client-side transfers avoidable rows and couples a modal open to raw-table history size.

**Confidence:** High for code behavior; production query plan/row count was not inspected.

**Plan interaction:** Confirms Lane 10 Phases 1-3, but the data-hook extraction should introduce an explicit latest-lines contract and error/empty distinction rather than merely moving the same unbounded query.

**Safe first evidence step:** Mock current query results for multiple snapshots, no rows, and an error; capture parity for dedupe/staleness behavior and rendered states before extraction.

**Done when:** A focused hook/repository owns bounded latest-line retrieval, failure and legitimate empty states differ, Kelly/take-bet transforms are pure-tested, stale-line behavior is preserved, and modal rendering does not own raw query mechanics.

---

### F-10 — Medium: dashboard prediction ownership is duplicated and data failure collapses into fallback/empty UI

**Evidence**

- `dashboard/src/app/(protected)/dashboard/page.tsx:154-276` implements its own prediction query, mapping, injury filtering, game-time RPC fallback, error fallback, and loading state.
- A second owner exists at `dashboard/src/lib/hooks/usePredictions.ts:9-47`, with different filtering/mapping semantics.
- The page silently ignores injury/RPC failures (`dashboard/page.tsx:163-175,234-259`) and converts prediction errors to an empty list plus fallback games (`dashboard/page.tsx:268-275`).
- The page also performs direct book-availability querying at `dashboard/page.tsx:323-353` and owns filtering/sorting/view-model logic at `dashboard/page.tsx:413-469`.

**Concrete failure mode**

Two prediction consumers can diverge on exclusions, team mappings, stat filters, recommendation ordering, or game-time enrichment. A prediction permission/query failure can be shown as “no predictions yet” with fallback schedule data, masking an outage and preventing a meaningful retry/error report.

**Confidence:** High.

**Plan interaction:** Confirms Lane 10 dashboard-page ownership diagnosis. Refresh the plan to reconcile the existing but unused/different `usePredictions` owner rather than adding a third hook.

**Safe first evidence step:** Create fixture parity tests that run the page mapper and current hook mapper over the same NBA/MLB rows; mock a prediction error and verify the intended UI distinction from a valid empty slate.

**Done when:** One hook/repository owns the canonical query and mapping contract, page-only filters are pure view models, fallback games activate only for a valid empty result, errors are visible/retriable, and all consumers share typed prediction semantics.

---

### F-11 — Medium: route/component contracts and privileged actions lack consistent failure observability

**Evidence**

- `dashboard/src/lib/hooks/useArbScanner.ts:117-124` ignores verification response status and optimistically removes the link.
- `dashboard/src/app/(protected)/account/page.tsx:309-319` silently ends portal loading when the route fails or returns no URL; no error reaches the user.
- `dashboard/src/app/api/ask/history/route.ts:24-52,69-76` ignores Supabase errors and can return empty history or success after a failed delete.
- `dashboard/src/app/api/kalshi/approve/route.ts:188-194` returns counts/IDs but records no acting user or request correlation.
- API observability is primarily `console.error` (for example Stripe at `webhook/route.ts:43,52,89,115,131,150,155` and Ask at `ask/route.ts:782,794,1282,1295`) with no shared request/event context visible in scope.

**Concrete failure mode**

Users receive apparent success, empty history, or no feedback while mutations/persistence fail. Operators cannot reliably correlate a user action, API request, DB write, and external event during incident reconstruction.

**Confidence:** High for missing checks/feedback; platform log aggregation was not inspected.

**Plan interaction:** Lane 09’s chat repository extraction must preserve persistence but stop swallowing repository errors indiscriminately. Lane 10 hooks should standardize mutation result/error state. Privileged audit attribution belongs with F-02’s server guard.

**Safe first evidence step:** Build a route/hook error matrix with mocked non-2xx and Supabase failures; document expected user message, retryability, log fields, and mutation state before choosing an observability vendor.

**Done when:** Every mutation checks and surfaces outcome, expected empty differs from failed read, logs carry route/action/request/event/user identifiers without secrets, privileged changes are attributable, and retry behavior is explicit.

---

### F-12 — Medium: no dashboard test harness exists for two documentation-only migration lanes

**Evidence**

- Targeted dashboard test-file discovery returned zero files.
- `dashboard/package.json:5-10` exposes only dev/build/start/lint.
- `dashboard/package.json:26-34` has TypeScript/ESLint but no test runner, DOM environment, React testing utilities, or browser test dependency.
- Lane 09 Phase 0 (`09-dashboard-ask-api-route-migration.md:319-340`) and Lane 10 Phase 0 (`10-dashboard-god-components-pages-migration.md:273-296`) both already identify characterization/test setup as the prerequisite.
- Current migration targets remain very large (mechanical inventory above), so typecheck/lint alone cannot protect query, auth, KPI, prompt, or interaction parity.

**Concrete failure mode**

Security fixes and extractions can compile while changing authorization order, response shape, KPI math, query filters, chat persistence, modal actions, or empty/error behavior. Lane implementation has no executable RED/GREEN gate.

**Confidence:** High.

**Plan interaction:** Confirms both plans’ Phase 0. The harness should be one shared setup slice, not independently reinvented in each lane.

**Safe first evidence step:** Select the smallest Next 16/React 19-compatible runner in an isolated proposal and prove one pure unit test plus one route test with mocked dependencies. Do not combine setup with production extraction.

**Done when:** CI/local scripts run unit and route/component tests; auth matrices, API contracts, core view models, and error/empty states have characterization coverage; Lane 09/10 validation commands are real rather than aspirational.

---

### F-13 — Low/Medium: modal and tab accessibility semantics are incomplete, with narrow-screen overflow risk

**Evidence**

- The local performance `Modal` uses clickable backdrop/div structure but no `role="dialog"`, `aria-modal`, labelled relationship, focus trap, or close-button label at `dashboard/src/app/(protected)/performance/page.tsx:44-60`.
- The four performance tabs are plain buttons in a non-wrapping flex row without tab roles/current-state semantics at `performance/page.tsx:340-389`.
- History similarly renders four tab buttons in a fixed flex row at `dashboard/src/app/(protected)/history/page.tsx:184-231`.
- `AskChat`’s disclosure button lacks `aria-expanded`/`aria-controls` at `dashboard/src/components/analysis/AskChat.tsx:162-187`; the animated response state has no live-region/status semantics at `AskChat.tsx:233-243`.
- Positive counterexamples exist: navbar mobile toggles and DFS remove controls use `aria-label`, so accessible conventions are present but inconsistent.

**Concrete failure mode**

Keyboard/screen-reader users may not know dialog/disclosure/tab state, focus can escape behind a modal, and four-tab controls can compress or overflow on narrow screens. Loading answers may not be announced.

**Confidence:** Medium. Static semantics are clear; no keyboard/screen-reader/mobile browser run was performed.

**Plan interaction:** Add an accessibility characterization checklist to Lane 10 before splitting render trees. Do not mix a visual redesign into behavior-preserving extraction.

**Safe first evidence step:** Run static accessibility checks and manual keyboard/focus inspection locally against the existing UI in a future approved browser-QA slice; capture narrow viewport screenshots before edits.

**Done when:** Modals have dialog semantics, labels, focus entry/containment/return, and Escape behavior; tab/disclosure state is programmatically exposed; loading/errors use appropriate live semantics; and target controls work at agreed mobile widths without hidden actions.

---

## Lane 09 reconciliation — Ask API route

**Status:** Documentation-only; architecture diagnosis remains current, assumptions need refresh.

### Preserve as resolved/current behavior

- Chat persistence exists in both NBA and MLB branches: `dashboard/src/app/api/ask/route.ts:758-790,1254-1293`.
- Chat history read/delete exists: `dashboard/src/app/api/ask/history/route.ts:5-77`.
- `AskChat` loads persisted history and tracks `conversation_id`: `dashboard/src/components/analysis/AskChat.tsx:57-87,122-137`.
- Therefore, “AI chat is not persisted” is rejected historical debt and must not be reintroduced.

### Current Lane 09 scope

Keep the plan’s thin-route, contracts, prompt, context-loader, provider, and repository boundaries. Refresh priority/order around:

1. server-side entitlement guard;
2. runtime request/response contracts;
3. durable cross-instance usage policy (separate behavior-change slice after characterization);
4. bounded context query rounds and degradation/timeout policy;
5. persistence/history error semantics and observability;
6. preservation tests for current history and response shape.

The plan’s instruction to preserve in-memory limiting is safe only for initial extraction, not a final done condition.

## Lane 10 reconciliation — god components/pages

**Status:** Documentation-only; principal large-file/ownership diagnosis remains current.

Current sizes exactly match the original plan for the top targets: `AnalysisModal` 1,043, dashboard page 733, performance page 688, `DfsTable` 552, DFS page 546, and `TradeApprovalPanel` 482 lines.

### Refresh before implementation

- Do not rediscover blanket history pagination debt. Report 00 classifies that historical claim as resolved, and this audit does not create a duplicate finding.
- Do not rediscover chat persistence debt; current route/history/client evidence confirms it.
- Prioritize performance’s eager/unbounded/error-collapsing data ownership (F-07), AnalysisModal query/error ownership (F-09), dashboard duplicate prediction owners (F-10), and the shared test harness (F-12).
- Keep security authorization fixes (F-01/F-02) as bounded server slices; hiding/splitting client pages is not remediation.
- Pure view-model extraction remains the safest first component slice, but should follow executable characterization tests.

## Auth/RLS and capability boundary summary

| Surface | Current boundary | Assessment |
|---|---|---|
| Protected pages | Middleware `getUser`; optional `is_subscribed` | Reasonable page gate, but not API capability enforcement |
| `/bot-tracker` page | Middleware `is_admin` | Server page gate exists |
| `/arb-scanner` page | Client `useAdmin` only | Presentation gate, not a security boundary |
| Browser Supabase reads/writes | Anon client + authenticated cookie/RLS | Correct role pattern in scope; live RLS policies not inspected |
| `/api/ask` | Route `getUser`, process-local usage | Missing subscription capability enforcement |
| `/api/kalshi/*` | Route `getUser`, then service role | Critically under-authorized |
| `/api/arb/verify` | Service role only | Critically unauthenticated |
| Stripe webhook | Signature verification + service role | Correct trust shape, but failed writes are acknowledged |

This audit does not claim that any RLS policy is correct or incorrect; policies and live DB behavior were outside scope. The source-level invariant remains: browser/dashboard user data should operate as `authenticated` with RLS, and service-role use must sit behind explicit server authorization.

## Rejected suspicions / resolved items not reopened

1. **“AI chat is not persisted.” — Rejected/resolved.** Current POST branches persist messages, history GET/DELETE exists, and `AskChat` reloads it. Persistence reliability/error handling remains a narrower current concern.
2. **“History has no pagination.” — Not reopened.** Report 00 and the audit directive establish this as resolved history-page work. This report’s bounded-data finding is specifically about the separate performance surface.
3. **“No dashboard caching exists.” — Rejected.** React Query has shared defaults at `dashboard/src/components/providers/QueryProvider.tsx:6-25`; domain hooks set stale/refetch intervals; games/scoreboard routes use Next revalidation. Findings concern cache ownership and eager/unbounded queries, not total absence.
4. **“Admin UI visibility secures privileged APIs.” — Rejected as an architecture assumption.** Current API matcher exclusion and route-local code prove otherwise.
5. **“Service role is exposed to the browser.” — Not found.** Service-role references in allowed scope are server route/lib files. The issue is missing server authorization, not client-bundle exposure.
6. **“Stripe webhook is unsigned.” — Rejected.** `stripe-signature` and `constructEvent` verification are present at `dashboard/src/app/api/stripe/webhook/route.ts:33-54`.
7. **“All pages lack loading/empty/error states.” — Rejected.** Many components implement loading and legitimate empty states. The actionable gap is specific: several query errors collapse into those empty/zero states.
8. **“DFS is desktop-only.” — Rejected as a blanket claim.** `DfsTable` has mobile/desktop branches and overflow handling; no browser QA was performed to establish remaining defects.

## Coverage gaps and evidence limits

- No live RLS policies, table grants, RPC security mode, row counts, indexes, query plans, or data were inspected.
- No Supabase, Stripe, Vercel, Anthropic, NBA, MLB, Railway, or deployed dashboard request was made.
- No deployed environment variables, subscription flag, Stripe product prices, webhook delivery history, logs, traces, or runtime latency were inspected.
- No browser, mobile viewport, screen reader, keyboard, hydration, Core Web Vitals, or accessibility scanner run was performed.
- No build/lint/typecheck/test was run because this task was report-only and the dashboard has no test script; validation is limited to report hygiene.
- Root backend, database migrations/policies, generated artifacts, and dashboard directories outside the user-approved scope were not inspected.
- Public API intent for games/scoreboard was not adjudicated; they were not called.
- History pagination was accepted as resolved per reconciliation baseline and not re-audited as a new debt lane.
- Production impact probabilities are inferred from reachable source paths, not incident telemetry.

## Recommended dependency order (evidence before implementation)

1. **Authorization containment:** F-01 and F-02 route tests/guards; no UI refactor required.
2. **Paid capability boundary:** F-03 entitlement matrix and tests.
3. **Subscription reliability:** F-04 webhook retry semantics, then F-05 checkout idempotency/product catalog.
4. **Shared test harness:** F-12 as an isolated setup slice.
5. **Lane 09 refresh:** F-06/F-08 with persistence parity locked.
6. **Lane 10 data ownership:** F-07, F-09, F-10 before JSX-only splitting.
7. **Cross-cutting observability/accessibility:** F-11/F-13 folded into each characterized slice, not deferred to an unbounded cleanup.

No code, config, register, plan, DB, network, deployment, or live-trading action is authorized by this ordering.

## Validation performed

- Re-read the report after writing.
- Confirmed required fields are present for every finding: exact path/symbol/line evidence, concrete failure mode, confidence, plan interaction, safe first evidence step, and done condition.
- Confirmed resolved chat persistence and historical blanket history-pagination claims were not reintroduced as findings.
- Confirmed rejected suspicions and coverage gaps are recorded.
- Scoped diff hygiene: `git diff --check -- .hermes/audits/tech-debt/07-dashboard-product.md`.
