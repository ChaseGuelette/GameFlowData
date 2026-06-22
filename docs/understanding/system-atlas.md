# System Atlas

This is the high-level map of the systems Chase uses when building and operating GameFlow. It is intentionally compact. Deeper subsystem docs should link out from here instead of turning this page into a dump.

## One-sentence architecture

GameFlowData is a sports analytics and trading platform: scheduled scrapers and processors collect market/stat/injury data into Supabase, model pipelines produce predictions and edges, Railway runs production automation, Vercel hosts the dashboard, and Hermes + GBrain provide the agent/workflow memory layer around the project.

## Core systems

### GameFlowData repo

What it is:
- The application/code repo for scrapers, processing, models, orchestration jobs, paper/live trading, dashboard, tests, migrations, and operational docs.

Larger role:
- This is where production behavior lives.
- It should be the source for code-reviewed docs that explain how the system works.

Important boundaries:
- Do not infer live DB truth from code alone.
- Do not infer current architecture from old handoffs alone.
- Broad repo-root scans are expensive; use targeted path routing.

### Supabase Postgres

What it is:
- The primary application database for raw market data, linked props, stats, predictions, recommendations, users, RLS-backed dashboard access, job executions, and trading records.

Larger role:
- The shared state layer between scrapers/processors, model jobs, dashboard, paper/live trading, and operational checks.

Important boundaries:
- Backend jobs use privileged DB access; dashboard code uses authenticated/RLS paths.
- Main-context agents should not query Supabase directly; use the GameFlow SQL-runner pattern.
- Large/index-sensitive tables need explicit safeguards and approval.

### Railway production worker

What it is:
- The always-on Python production runtime for scheduled GameFlow jobs.

Larger role:
- Runs APScheduler jobs from `src/orchestration/scheduler.py` and job entrypoints such as daily stats, lines, inference, edge refresh, MLB jobs, and Kalshi refreshes.

Important boundaries:
- NBA daily stats on Railway must be CDN-only; no `stats.nba.com` calls from Railway.
- `NBA_FULL_LINES_ENABLED=false` gates full NBA lines/injury jobs only, not every NBA-adjacent job.
- Production DB writes require the correct writable Supabase pooler path.

Read next:
- `railway-scheduler.md` for the current scheduler mental model, job groups, env gates, and audit notes.

### Vercel dashboard

What it is:
- The Next.js hosted dashboard/product surface.

Larger role:
- Lets users view picks, model outputs, bot tracker/history, account/subscription surfaces, and analysis UI.

Important boundaries:
- Dashboard access uses authenticated/RLS database paths.
- Product/UI pages can lag backend model changes if contracts are not documented.

### Model artifacts and training pipelines

What they are:
- Training code, validation code, and production artifacts for NBA/MLB/NCAAB model lanes.

Larger role:
- Convert historical stat/market context into predictive distributions and recommendation probabilities.

Important boundaries:
- Probabilities from Monte Carlo samples use empirical CDF `(samples > line).mean()`.
- Do not blindly “fix” Q10 miscalibration; it is a known edge in current GameFlow doctrine.
- Model changes require prior lessons/invariants retrieval and validation gates before promotion.

### Market/stat/injury data providers

What they are:
- External data sources such as NBA CDN, MLB Stats API, The Odds API, RapidAPI injuries, Kalshi, Polymarket, and local-only advanced-stat sources.

Larger role:
- Feed raw inputs into Supabase and the model pipeline.

Important boundaries:
- Data-provider behavior changes can look like model bugs.
- Some sources have runtime constraints: stats.nba.com is local-only, not Railway-safe.

### Hermes Agent

What it is:
- The AI agent runtime used for planning, code work, file exploration, task delegation, cron, skills, memory, and gateway workflows.

Larger role:
- The working interface around GameFlowData, GBrain, and code changes.

Important boundaries:
- Skills are reusable procedures.
- Memory is for compact stable facts/preferences, not task progress.
- Large implementation should use specs/workers/review gates rather than sprawling main-context edits.

### GBrain and GameFlowBrain

What they are:
- GameFlowBrain is the canonical human-facing markdown brain.
- GBrain is the indexed retrieval/graph/timeline layer over that markdown.

Larger role:
- First lookup layer for project truth, handoffs, decisions, invariants, lessons, and operating standards.

Important boundaries:
- GBrain retrieval is not live DB truth.
- Direct MCP page reads can have source-routing issues; source-scoped query fallback may be required.
- Markdown remains canonical for humans; GBrain owns retrieval, graph edges, embeddings, and timeline behavior.

## Current risk map

| Risk | Why it matters | Current mitigation |
|---|---|---|
| Agent work outpaces Chase's mental model | System becomes operable only through vague agent memory | Use `gameflow-learning-mode` and these docs |
| Handoffs become pseudo-truth | Chronological notes can be stale or partial | Promote durable facts to docs/GBrain only after review |
| Tech debt register becomes noise | Too many speculative “maybe” items reduce trust | Require evidence + safe first step |
| Model changes violate known lessons | Profitable quirks can be accidentally “fixed” | Retrieve hard facts/lessons before model recommendations |
| DB/runtime truth confused with code truth | Code may not match production state | Use SQL-runner/log checks for live state |

## Open questions to fill in over time

- Which production jobs are currently enabled per environment?
- Which dashboard pages are most coupled to backend schema/model contracts?
- Which model artifacts are actively loaded by production today?
- Which tech-debt lanes are structural risks versus cosmetic cleanup?
