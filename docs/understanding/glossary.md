# Glossary

Short operational definitions for terms Chase and agents use often. Keep entries compact and tied to where the term appears in real work.

## Agent and memory terms

### Hermes Agent

The AI agent runtime Chase uses for GameFlow work: chat, tool use, code edits, skills, memory, cron, delegation, gateway, and project workflows.

### Skill

A reusable procedure Hermes can load to change how it handles a task class. Skills are for repeatable workflows, not one-off task status.

### GBrain

The retrieval, graph, embedding, and timeline layer over GameFlowBrain markdown. It helps agents find project truth quickly, but it is not live DB truth.

### GameFlowBrain

The canonical human-facing markdown brain for GameFlow project knowledge. GBrain indexes it, but markdown remains the reviewed source for humans.

### Handoff

A chronological session continuity note: what happened, decisions, validation, blockers, and recommended next steps. Useful evidence, but not automatically current architecture truth.

### SQL runner

The isolated GameFlow workflow for database queries. Use it for live DB truth instead of putting direct Supabase work in the main agent context.

### Implementation worker

A delegated coding lane for larger/multi-file changes. It should receive a precise spec, then the main session verifies scoped diffs and tests.

### Explain-as-we-work

The `gameflow-learning-mode` behavior: while doing real work, Hermes gives Chase compact subsystem explanations and captures evidence-backed debt signals.

## GameFlow data/model terms

### Feature store

Code/tables that assemble model-ready inputs from historical stats, market lines, context, and derived features. Feature stores must preserve temporal integrity.

### Temporal integrity

The rule that a prediction/backtest only uses information that would have been known before the event or decision time.

### Empirical CDF

Probability computation from Monte Carlo samples using sample counts, e.g. `(samples > line).mean()`. GameFlow doctrine says not to replace this with Gaussian CDF.

### Q10 edge

The known GameFlow lesson that Q10 “miscalibration” can be the betting edge. Do not blindly correct it with global recalibration offsets.

### Quote-clean

Backtest/evaluation discipline where market lines are selected using realistic as-of timing rather than future/latest lines. Used to reduce line-timing leakage.

### CLV

Closing line value. A measure of whether the selected edge beats the later/closing market. In GameFlow, CLV/ranker/book gates help decide if a model slice is worth promoting.

### Dense CLV

A denser snapshot/linkage approach for CLV analysis, generally involving more complete intraday market snapshots and linkage to games/players/stat lines.

### Ranker gate

A validation layer that checks whether model edge ranking corresponds to better later outcomes/CLV, not just whether raw backtest ROI looked good.

### Prop line

The sportsbook line for a player stat. In feature/model work, prop-line timing and inclusion can create leakage if not handled as-of correctly.

## Production/runtime terms

### Railway worker

The production Python runtime that runs GameFlow scheduled jobs. It is where scheduler behavior, DB writes, scrapers, and model jobs execute in production.

### APScheduler

The scheduler framework used by the Railway worker to run recurring GameFlow jobs.

### Full lines job

A heavier lines run that can include game lines, player props, injuries, and linker work depending on flags. In NBA, these can be gated separately from props-only refresh.

### Props-only job

A lightweight lines refresh focused on player props and linker work. For NBA, this can still run even when `NBA_FULL_LINES_ENABLED=false`.

### CDN-only daily stats

The Railway-safe NBA daily stats mode that avoids stats.nba.com and uses CDN-accessible data sources.

### RLS

Supabase row-level security. Dashboard/client access uses authenticated/RLS paths; backend jobs use privileged server-side access.

### Pooler port 5432 vs 6543

GameFlow production DB writes need the writable Supabase session pooler path on port 5432. The transaction pooler path on 6543 can behave read-only for some write workloads.

## Documentation/process terms

### System atlas

The one-page-ish map of major GameFlow systems and their roles.

### Tech-debt register

The evidence-backed queue of debt/migration candidates. It should not contain speculative cleanup without evidence and a safe first step.

### Candidate debt

A suspected debt item with some evidence but not yet approved as work.

### Accepted debt

A known issue intentionally left in place because the risk/cost tradeoff is currently acceptable.
