# GameFlow Comprehension + Tech Debt Program Implementation Plan

> **For Hermes:** Use this plan to bootstrap Chase's system-understanding workflow. Implement the scaffold directly; future code/debt investigations should use `gameflow-explore`, `gameflow-sql-runner`, and evidence-gated review before edits.

**Goal:** Make understanding GameFlowData, Hermes Agent, GBrain, and their operational boundaries a first-class deliverable of normal work while building an evidence-backed tech-debt/migration queue.

**Architecture:** Start with a lightweight explain-as-we-work Hermes skill plus a small human-readable `docs/understanding/` scaffold. Keep chat explanations ephemeral unless they become durable; promote durable architecture knowledge to repo docs or GameFlowBrain, and promote repeated procedures to Hermes skills. Tech debt enters the register only with evidence, risk, a safe first step, and validation.

**Tech Stack:** Hermes skills, GameFlowData markdown docs, GBrain retrieval/graph hygiene, git-reviewed docs, optional later Hermes cron for weekly review.

---

## Problem statement

Chase can operate the systems, but the project has become hard to explain end-to-end. The risk is not only missing documentation; it is that agent-driven work can keep adding behavior faster than Chase's mental model catches up. The desired state is: Chase can explain what each subsystem is, how data/control flow through it, why it exists, what can go wrong, and which debt/migrations are known versus speculative.

## Non-goals

- Do not create a large automatic documentation generator.
- Do not let an always-on agent rewrite docs without review.
- Do not turn every handoff into canonical architecture truth.
- Do not open tech-debt items from vibes alone; require evidence.
- Do not run broad repo scans or DB probes as part of this bootstrap.
- Do not change production code in this scaffold slice.

## Source-of-truth policy

- Repo docs under `docs/understanding/` are the human-facing learning layer tied to code review.
- GameFlowBrain/GBrain remains the retrieval and durable project-memory layer.
- Handoffs are chronological evidence, not the coherent textbook.
- Hermes skills define agent behavior during work.
- Tech-debt register entries are candidates until confirmed by code/log/test/GBrain/user evidence.

## Deliverables in the bootstrap slice

1. Create `docs/understanding/README.md` as the navigation page.
2. Create `docs/understanding/system-atlas.md` for the high-level map.
3. Create `docs/understanding/gameflow-data-flow.md` for data/control flow.
4. Create `docs/understanding/agent-workflow.md` for Hermes/GBrain/GameFlow operating workflow.
5. Create `docs/understanding/glossary.md` for recurring terms.
6. Create `docs/understanding/tech-debt-register.md` with candidate template and starter areas.
7. Create a user-local Hermes skill `gameflow-learning-mode` for explain-as-we-work behavior.
8. Validate docs exist, skill frontmatter is valid, and git diff is scoped.

## Operating loop after bootstrap

### During normal work

For any non-trivial task, Hermes should add a compact learning block:

```text
System context
- Subsystem:
- Larger role:
- Upstream:
- Downstream:
- Important invariants:
- Current confusion/debt signal:
- What Chase should remember:
```

This block should be short enough to not derail the work. If Chase asks for more, expand into a short explanation or update the relevant doc.

### When durable understanding emerges

Promote it to the right place:

- `docs/understanding/system-atlas.md`: stable cross-system map.
- `docs/understanding/gameflow-data-flow.md`: stable data/control flow.
- `docs/understanding/agent-workflow.md`: Hermes/GBrain workflow, retrieval, handoffs, skills.
- `docs/understanding/glossary.md`: recurring terms that confused Chase or agents.
- GameFlowBrain canonical pages: validated durable architecture facts that should be retrieved by future agents.
- Hermes skill: repeatable procedure or behavior change.

### When tech debt is found

Add it only if at least one evidence source exists:

- code path or exact file/function;
- failing or missing test;
- production log/incident/handoff;
- GBrain canonical page/lesson;
- repeated user confusion;
- explicit Chase observation.

Each debt item must include: area, evidence, why it matters, risk if ignored, safe first step, validation, status, and owner decision needed.

## Phase 1: Bootstrap comprehension scaffold

### Task 1: Create docs directory and navigation page

**Objective:** Establish the human-facing entry point.

**Files:**
- Create: `docs/understanding/README.md`

**Steps:**
1. Create the file with purpose, usage rules, and links to the other pages.
2. Keep it short; it should route Chase, not become the content dump.
3. Verification: file exists and links to all scaffold pages.

### Task 2: Create system atlas

**Objective:** Give Chase a one-page map of the major systems.

**Files:**
- Create: `docs/understanding/system-atlas.md`

**Steps:**
1. Add sections for GameFlowData, dashboard, Railway worker, Supabase, model artifacts, market/data providers, Hermes Agent, and GBrain/GameFlowBrain.
2. For each system, include what it is, why it exists, upstream/downstream, and current known risks.
3. Mark uncertain items as `needs-validation` rather than overclaiming.
4. Verification: page distinguishes application systems from agent/memory systems.

### Task 3: Create data-flow page

**Objective:** Explain how data and decisions move through the product.

**Files:**
- Create: `docs/understanding/gameflow-data-flow.md`

**Steps:**
1. Add a high-level flow: external data -> raw DB tables -> linking/processing -> feature stores -> model artifacts -> predictions/edges -> paper/live/dashboard.
2. Add NBA and MLB flow notes separately where they differ.
3. Include invariant callouts: empirical CDF, temporal integrity, Railway CDN-only NBA stats, no broad DB work without SQL-runner discipline.
4. Verification: page can answer “where does a recommendation come from?” at a high level.

### Task 4: Create agent workflow page

**Objective:** Explain how Hermes, GBrain, handoffs, skills, and implementation workers should interact.

**Files:**
- Create: `docs/understanding/agent-workflow.md`

**Steps:**
1. Explain start-session, investigate, plan, implement, verify, wrap-up.
2. Describe what belongs in memory vs skills vs GBrain vs repo docs.
3. Include source-routing caveats: GBrain source `gameflow`, direct MCP read fallback, canonical markdown, and SQL isolation.
4. Verification: page makes clear that GBrain retrieval is not the same as DB truth.

### Task 5: Create glossary

**Objective:** Start reducing vague hand-waving terms.

**Files:**
- Create: `docs/understanding/glossary.md`

**Steps:**
1. Add initial terms: GBrain, GameFlowBrain, handoff, skill, SQL runner, implementation worker, feature store, quote-clean, CLV, dense CLV, Railway worker, props-only, full lines job, empirical CDF, Q10 edge.
2. Keep definitions concise and operational.
3. Mark definitions that need review.
4. Verification: glossary terms link concept to where Chase sees it in real work.

### Task 6: Create tech-debt register

**Objective:** Establish a disciplined debt queue.

**Files:**
- Create: `docs/understanding/tech-debt-register.md`

**Steps:**
1. Add status taxonomy: suspect, confirmed, planned, in-progress, resolved, accepted.
2. Add evidence rules and candidate template.
3. Add starter candidate areas without pretending they are confirmed: GBrain direct-read/source-routing fragility, stale-pages metadata hygiene, god-class migration lanes, fragmented MLB stat-suite tooling, production scheduler/job complexity.
4. Verification: every starter item is clearly marked as candidate or known from existing evidence.

### Task 7: Create `gameflow-learning-mode` skill

**Objective:** Make explain-as-we-work behavior reusable.

**Files:**
- User-local Hermes skill: `gameflow-learning-mode`

**Steps:**
1. Create the skill with trigger, output contract, promotion rules, and debt evidence gates.
2. Include instructions not to over-explain trivial tasks.
3. Include instructions to ask before broad scans, DB work, or doc promotion that changes canonical truth.
4. Verification: `skill_view(name='gameflow-learning-mode')` loads successfully in future sessions; current session may need skill cache reload depending on Hermes behavior.

## Phase 2: First guided review session

Do this after bootstrap, not automatically in the same slice unless Chase asks.

1. Pick one subsystem, e.g. “Railway scheduler and NBA/MLB lines jobs.”
2. Read the relevant docs and code paths narrowly.
3. Produce a learning explanation in chat.
4. Update one doc page with durable understanding.
5. Add at most 1-3 tech-debt candidates with evidence.
6. Stop and review with Chase.

## Phase 3: Weekly/manual debt radar

After the workflow feels useful, add a manual or scheduled review.

Candidate command/prompt:

```text
Review recent GameFlow work for comprehension gaps and tech-debt candidates. Use GBrain latest handoffs, recent git diff/log, and existing docs/understanding. Do not edit docs automatically. Return Confirm / Shelf / Ignore recommendations with evidence and safe first steps.
```

Possible later automation:

- Hermes cron, weekly, no_agent=false.
- Enabled toolsets: GBrain/web not needed; file + terminal + skills if local repo review is required.
- Delivery: current chat only.
- Output only; no writes unless Chase confirms.

## Validation checklist

- [ ] `docs/understanding/` exists with the six scaffold files.
- [ ] `gameflow-learning-mode` skill exists and has valid frontmatter.
- [ ] No production code changed.
- [ ] No DB tools or broad repo scans were used for bootstrap.
- [ ] `git diff -- docs/understanding .hermes/plans/gameflow-comprehension-tech-debt-program.md` is scoped and readable.
- [ ] Existing unrelated modified docs remain separate.

## Suggested commit message

```text
docs: add GameFlow comprehension and tech-debt scaffold
```
