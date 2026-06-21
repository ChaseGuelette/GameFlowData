# GameFlow Agent Workflow

This page explains how Hermes Agent, GBrain, GameFlowBrain, skills, handoffs, SQL runner, and implementation workers should fit together during GameFlow work.

## Mental model

```text
Chase's intent
  -> Hermes active session
  -> skills define procedure
  -> GBrain retrieves project memory
  -> file tools / SQL runner / workers gather current evidence
  -> plan or implementation happens
  -> validation proves behavior
  -> handoff/docs/GBrain capture durable learning
```

## Roles

### Hermes Agent

What it does:
- Executes the active task: planning, explanation, file edits, tests, tool use, and coordination.

What it should not become:
- A hidden source of truth that only works because it has a giant prompt.
- A place to store task progress in memory.

Use Hermes for:
- explain-as-we-work learning blocks;
- short active-session decisions;
- code changes and validation;
- creating or updating skills when a reusable procedure emerges.

### Skills

What they are:
- Reusable procedures that change Hermes behavior for a class of work.

Use skills for:
- `gameflow-resume`: session startup and latest project context.
- `gameflow-explore`: bounded file discovery.
- `gameflow-sql-runner`: isolated DB truth.
- `gameflow-implementation-worker`: larger code changes.
- `gameflow-wrap-up`: session closeout and GBrain hygiene.
- `gameflow-learning-mode`: explaining systems and capturing debt candidates while working.

Do not use skills for:
- one-off task progress;
- stale issue lists;
- facts that should live in GameFlowBrain/docs.

### GBrain and GameFlowBrain

What they do:
- GameFlowBrain markdown is the canonical human-facing project memory.
- GBrain indexes it for retrieval, graph, timeline, and MCP access.

Use GBrain for:
- critical invariants;
- latest handoffs;
- execution plan/current roadmap;
- architecture decisions;
- lessons and hard facts;
- routing to canonical pages.

Do not use GBrain as:
- live DB truth;
- proof that production is currently healthy;
- a place for every raw chat summary.

Known caveat:
- Direct MCP `get_page`/`list_pages` can be source-routed incorrectly in some sessions. If known slugs fail but source-scoped query works, treat this as direct-read/source-routing fragility, not necessarily GBrain downtime.

### Handoffs

What they are:
- Chronological continuity notes.

Use handoffs for:
- what was done;
- decisions made;
- validation captured;
- blockers;
- recommended next steps;
- files/pages to read on resume.

Do not use handoffs as:
- the only authority for current architecture;
- a replacement for docs;
- a task database.

### SQL runner

What it does:
- Isolates database truth from the main context and prevents large result dumps or unsafe DB actions.

Use SQL runner for:
- live row counts;
- production DB state;
- schema/table reality;
- bounded preflight checks;
- independent verification before destructive DB-adjacent actions.

Do not use main-context Supabase MCP directly for GameFlow DB work.

### Implementation workers

What they do:
- Execute larger code changes from a precise spec while preserving main-context size and reducing repo-sprawl.

Use them for:
- multi-file implementation beyond small edits;
- approved plans with exact scope;
- refactors/migrations where diff review and tests matter.

Do not trust worker self-report alone:
- verify scoped git diff;
- read changed files where needed;
- run scoped validation.

## Standard work loop

### 1. Resume

Use `gameflow-resume`.

Output should include:
- latest handoff;
- brain health caveats;
- current repo status if code work is likely;
- recommended next work;
- relevant safety notes.

### 2. Explain context

If `gameflow-learning-mode` is loaded and the task is non-trivial, add a compact learning block:

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

### 3. Investigate

Choose the evidence route:

- Project truth: GBrain first.
- Code shape: `gameflow-explore` and targeted file reads/searches.
- DB truth: SQL runner.
- Runtime truth: Railway/Vercel/Supabase logs or live tool checks where available.

### 4. Plan

For multi-step or debt/migration work:
- write a plan under `.hermes/plans/` or a relevant docs directory;
- include non-goals, invariants, validation, and approval gates;
- keep long compute/DB actions behind explicit approval.

### 5. Implement

- Tiny docs/config edits: direct edits.
- Small code edits: direct edits with focused tests.
- Larger/multi-file work: implementation-worker lane from a spec.

### 6. Validate

Validation should be tied to the actual changed layer:
- markdown/diff hygiene for docs;
- pytest/ruff/compile for Python;
- npm/build/test for dashboard;
- SQL/log checks for production/runtime claims.

### 7. Capture learning

Decide where the learning belongs:

| Learning type | Destination |
|---|---|
| Short session explanation | Chat/handoff |
| Stable system explanation | `docs/understanding/` or GameFlowBrain |
| Critical invariant or durable project fact | GameFlowBrain hard facts/invariants, after review |
| Reusable procedure | Hermes skill |
| Evidence-backed migration/debt | `docs/understanding/tech-debt-register.md` or a `.hermes/plans/` plan |
| Temporary task progress | Handoff/session, not memory |

## Memory rule

Hermes persistent memory should stay compact. Save preferences, stable environment facts, and recurring corrections. Do not save PR numbers, task progress, temporary status, or “fixed X today.”

## Good closeout

A good wrap-up should answer:
- What changed?
- What was validated?
- What remains risky/unknown?
- What should Chase read next?
- Did we improve understanding docs, GBrain pages, or skills?
