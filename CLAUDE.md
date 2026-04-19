# GameFlowDataBrain - Agent Instructions

> Part of [[BRAIN-INDEX]]

## What Is This Brain?

GameFlowData is a sports analytics and machine learning platform that predicts player prop bet outcomes using XGBoost quantile regression, Gaussian copula Monte Carlo simulation, and Black-Litterman market blending. The NBA pipeline is live and profitable. MLB and NCAAB pipelines are under development. This brain organizes the entire project — models, pipeline, product, infrastructure, business, and operations — so any AI agent can contribute effectively.

## Owner
- **Role**: Founder & Solo Developer
- **Context**: ~2-3 months into building. NBA system is production-ready and running daily on Railway with automated scraping, inference, paper trading, and Discord alerts. Dashboard is live on Vercel. Currently expanding to MLB (batter pipeline in progress) and planning Stripe monetization.
- **Goals**: Ship MLB models, monetize via Stripe subscriptions, grow user base, maintain NBA model profitability

## Brain Structure
- [[Models]] - NBA/MLB/NCAAB model development, calibration, backtesting, feature engineering
- [[Pipeline]] - Scrapers, linkers, processing, orchestration, scheduling
- [[Product]] - Dashboard features, UX decisions, frontend architecture
- [[Infrastructure]] - Railway, Vercel, Supabase, Discord, monitoring
- [[Business]] - Monetization, Stripe, pricing, growth strategy
- [[Operations]] - Daily runbooks, invariants, incident response, maintenance
- [[Decisions]] - Key technical and business decisions with rationale
- [[Assets]] - Images, videos, PDFs, mockups, screenshots
- [[Handoffs]] - Session continuity notes
- [[Templates]] - Reusable note structures

## Conventions
- Use [[wikilinks]] for all cross-references between notes, but ONLY link to files that exist. Never create wikilinks to files that haven't been created yet.
- Keep files concise and actionable
- Tag files with relevant hashtags for discoverability
- Check [[Assets]] for related images, videos, PDFs when working on any task
- Update Handoffs/ at the end of every work session
- Reference the [[Execution-Plan]] as the source of truth for build order

## Critical Invariants
These rules must NEVER be violated:
1. **NEVER deploy global conformal recalibration offsets** — 4x confirmed to hurt ROI
2. **NEVER put advanced stats scraping on Railway** — stats.nba.com blocks datacenter IPs
3. **Railway daily_stats_job uses CDN only** — `--cdn-only` flag, no stats.nba.com calls
4. **NEVER run non-concurrent CREATE INDEX on `raw_player_props_combined`** (67M+ rows)
5. **Empirical CDF for probabilities** — always `(samples > line).mean()`, never Gaussian CDF
6. **Python backend uses `postgres` role** (bypasses RLS). Dashboard uses `authenticated` role.
7. **Model's Q10 "miscalibration" IS the edge** — correcting it removes profitability
8. **NEVER call Supabase MCP directly in main context** — delegate to sql-runner subagent (see Context Protection Rules)
9. **NEVER use Explore agents for SQL** — Explore is file-only (Read/Grep/Glob). Use sql-runner for DB queries.
10. **Keep Explore agents narrow** — max_turns: 10, focused prompts, 5-12 tool calls max
11. **After plan approval, hand off to GLM via OpenCode** — do NOT read target files or write code yourself. Pass the plan as the spec to `opencode run --attach` immediately.

## Assets
The [[Assets]] folder contains images, videos, PDFs, and other media. When working on any task, check Assets/ for related materials. You can analyze images, read PDFs, and process any file dropped there.

## Model Routing & Escalation

Default session model: **Sonnet**. Use cheaper models for mechanical work, escalate to Opus for hard reasoning.

### Escalation Rules — When to Spawn an Opus Subagent
When running as Sonnet, ALWAYS use Task tool with `model: "opus"` for:
- **"Why" questions** about model performance, ROI degradation, or calibration drift
- **Debugging that spans 3+ files** or involves subtle cross-system interactions
- **Architecture or design decisions** — choosing approaches, evaluating trade-offs
- **Calibration interpretation** (not data gathering — the interpretation step)
- **Novel problem-solving** where you're uncertain about the right approach
- **Planning multi-step implementations** that touch 5+ files

For everything else (file reading, SQL, small edits, clear-spec implementation, status checks), handle directly.

### Code Implementation via OpenCode + GLM (MANDATORY for plan-mode output)
A headless OpenCode server runs at `http://localhost:4096` (started from the project root).

**RULE: When a plan is approved, IMMEDIATELY hand off implementation to GLM via OpenCode.** Do NOT read the target files yourself. Do NOT write code yourself. The plan IS the spec — pass it directly to GLM. Your only job after planning is: (1) call OpenCode, (2) review the diff, (3) fix issues.

**This applies when ALL of these are true:**
- A plan or spec exists with clear file paths, function names, and behavior
- The change touches code (not config, not markdown, not brain files)
- The change is more than ~20 lines

**Workflow:**
1. Write the spec to a temp file, then pass it to OpenCode. **ALWAYS use `run_in_background: true`** on the Bash tool — GLM calls can exceed the 2-minute timeout.
   ```bash
   # For short specs (< 5 lines): inline as argument
   export OPENROUTER_API_KEY=$(grep OPENROUTER_API_KEY .env | cut -d'"' -f2) && opencode run --attach http://localhost:4096 -m openrouter/z-ai/glm-5.1 "short spec here"

   # For long specs (> 5 lines): write to file first, then reference it
   # Step A: Use the Write tool to create /tmp/glm_spec.md with the full plan
   # Step B: Run OpenCode with the file attached for context
   export OPENROUTER_API_KEY=$(grep OPENROUTER_API_KEY .env | cut -d'"' -f2) && opencode run --attach http://localhost:4096 -m openrouter/z-ai/glm-5.1 -f /tmp/glm_spec.md -f src/target_file.py "Implement the spec in the attached glm_spec.md file. The other attached files are existing code for context."
   ```
   **Use relative paths** in specs (not absolute) — the server runs from the project root.
   The `-f` flag attaches files for GLM to read as context (existing source files AND the spec file).
2. Check the background task output with `TaskOutput`. Then **review GLM's work**: run `git diff` and compare against the plan in your context. Check for:
   - Missing steps from the plan
   - Wrong imports or function signatures
   - Logic that doesn't match the spec
   - Project conventions violated (e.g., connection patterns, RLS, etc.)
3. Fix small issues directly (Edit tool). For larger problems, send corrections back to OpenCode with a targeted prompt.
4. Run tests/linting if applicable.

**Model tiers:** GLM 5.1 (best, $0.95/$3.15), GLM 4.7 ($0.39/$1.75 — simpler tasks), GLM 4.5 Air (FREE — trivial tasks)

**If the server is not running**, start it: `cd /c/Users/Chase/Projects/GameFlowData && export OPENROUTER_API_KEY=$(grep OPENROUTER_API_KEY .env | cut -d'"' -f2) && opencode serve --port 4096`

**Do NOT use OpenCode for:** small edits (< 20 lines), config changes, brain/markdown updates, or tasks requiring deep cross-system reasoning.

### Subagent Model Assignment
- **Haiku**: File search (explorer), SQL queries (sql-runner), status checks
- **Sonnet**: Code review, brain file I/O (resume/wrap-up), calibration data gathering
- **Opus**: Architecture decisions, complex debugging, calibration interpretation

### Context Protection Rules
- **NEVER call Supabase MCP tools directly in the main context.** Always delegate SQL to a `sql-runner` subagent (Task tool, `model: "haiku"`). SQL results can be thousands of tokens — they must stay in the subagent. The main context only receives the subagent's summary.
- **NEVER call Supabase MCP tools from Explore agents.** Explore agents are for file search only (Read, Grep, Glob). If exploration requires SQL, spawn a separate sql-runner subagent in parallel.
- **Keep Explore agent prompts narrow and bounded.** Bad: "explore the arb paper trader infrastructure". Good: "find the entry point for arb paper trading in src/arbitrage/ and list its public functions". Set `max_turns: 10` on Explore agents to prevent runaway exploration. A focused Explore should use 5-12 tool calls, not 25+.
- **Prefer Grep/Glob directly over Explore agents** for simple searches (finding a file, locating a function, checking imports). Only use Explore for multi-step investigation where you don't know what you're looking for.
- **Brain-first exploration.** When investigating a system, start from the BrainTree (`brain/` folder) for orientation before diving into source code. `brain/Pipeline/Component-Docs.md` indexes 40+ module docs via wikilinks. Read the relevant brain doc first to understand architecture, then go to source files for current implementation details. Pattern: Explore 1 reads brain docs for "what should exist", Explore 2 reads source for "what actually exists" — run in parallel.
- **After plan approval, hand off to GLM via OpenCode immediately.** Do NOT re-read files that were already explored during planning. Do NOT write code yourself. Pass the plan as the spec to `opencode run --attach`, attaching the target files with `-f`.

## Agent Personas
Available specialized agents in .claude/agents/:
- [[builder]] - Implements features, ships code, makes technical decisions
- [[strategist]] - Product thinking, monetization, growth, go-to-market
- [[analyst]] - Model performance, calibration, backtesting, data analysis
- [[ops]] - Infrastructure, monitoring, pipeline health, incident response
- [[explorer]] - Haiku-powered codebase search and file reading
- [[sql-runner]] - Haiku-powered database queries

## Commands
- /init-braintree - Initialize a new brain
- /resume-braintree - Resume from where you left off
- /wrap-up-braintree - End session with proper handoff
- /status-braintree - View progress dashboard
- /plan-braintree [step] - Plan a specific step
- /sprint-braintree - Plan the week's work
- /sync-braintree - Health check and sync
- /feature-braintree [name] - Plan a new feature
