# GameFlowData Agent Contract

GameFlowData is Chase's sports analytics, player-prop modeling, trading/alerts, and dashboard project. Keep always-loaded context small: use this file for non-negotiable safety/runtime rules only. Use GBrain and Hermes skills for detailed project knowledge.

## Canonical context

- Canonical project brain: `/home/chase/GameFlowBrain`
- GBrain source: `gameflow`
- First lookup for project truth: GBrain MCP, then selected markdown pages under `/home/chase/GameFlowBrain`
- Detailed invariants: `operations/critical-invariants`
- Current roadmap/build order: `execution-plan`
- Migration schema/resolver: `/home/chase/GameFlowBrain/schema.md` and `/home/chase/GameFlowBrain/RESOLVER.md`
- End-of-session hygiene: sync GBrain, run graph backfill, verify `orphan_pages = 0`

## Critical invariants

These rules must never be violated:

1. Never deploy global conformal recalibration offsets; this was repeatedly confirmed to hurt ROI.
2. Never put advanced stats scraping on Railway; `stats.nba.com` blocks datacenter IPs.
3. Railway `daily_stats_job` uses CDN-only mode (`--cdn-only`); no `stats.nba.com` calls from Railway.
4. Never run non-concurrent `CREATE INDEX` on `raw_player_props_combined`.
5. Probabilities use empirical CDF: `(samples > line).mean()`, never Gaussian CDF.
6. Python backend uses the `postgres` role; dashboard/client code uses `authenticated` with RLS.
7. The model's Q10 miscalibration is the edge; do not blindly "fix" it.
8. Main-context agents must not call Supabase MCP directly. Use the GameFlow SQL runner/delegated read-only pattern.
9. Explore agents are file-only and narrow. If exploration needs SQL, spawn/use SQL runner separately.
10. Before destructive DB-adjacent actions, verify any SQL-runner count with an independent count query.

## Hermes skills to load

Load the relevant skill before acting:

- `gameflow-resume`: session startup, latest handoff, current context.
- `gameflow-wrap-up`: handoff, GBrain sync, graph/orphan hygiene.
- `gameflow-explore`: bounded file discovery and path routing.
- `gameflow-sql-runner`: isolated DB-query workflow; never put SQL results in main context unnecessarily.
- `gameflow-implementation-worker`: large/multi-file implementation handoff and diff-review gates.

## Context protection

- Brain-first: start with GBrain/canonical markdown before reading source code.
- For source discovery, search targeted paths only. Avoid broad repo-root scans, especially across `node_modules/`, `venv/`, `.git/`, `.claude-flow/`, `backtest_results/`, and `tmpclaude-*`.
- Use direct file tools for 1-2 targeted searches. Use a bounded explorer for unknown scope or 3+ searches/reads.
- Keep SQL isolated in a delegated/read-only runner. Main context should receive concise summaries, not large result sets.

## Implementation lane

Use direct edits for tiny/config/markdown changes. For approved code plans that exceed roughly 20 changed lines, touch 2+ implementation files, or already have a precise spec, prefer the implementation-worker lane:

1. Write a precise spec with allowed edit scope, invariants, non-goals, and validation commands.
2. First try the `codex-spark-worker` Hermes profile. Fallbacks: OpenRouter Codex, OpenCode GLM, then direct edit.
3. Do not read large implementation files into main context when the spec already names the target files and behavior.
4. Review only scoped diffs: `git diff -- <target files>`. Never trust worker self-report without diff/test verification.
5. Run scoped validation. Fix small misses directly; revert or re-spec broad misses.

## Markdown/GBrain writing rules

- Use wikilinks only to pages that exist.
- Keep pages concise and actionable.
- File durable facts conservatively in `Operations/Hard-Facts.md`; validation-sensitive facts remain `needs-chase-validation` until Chase confirms.
- Do not revive stale Solokit `.session/` state. Use `operations/solokit-session-audit` first, and inspect the archive only for a concrete missing-decision question.

## Current project priorities

- Maintain NBA production profitability and daily automation.
- Ship/strengthen MLB models and paper/live trading workflows.
- Continue dashboard/Stripe monetization when that lane is active.
