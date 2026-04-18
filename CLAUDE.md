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

### Code Implementation via OpenCode + GLM
For significant code writing (new features, multi-file changes with a clear spec):
1. Write a clear, detailed spec describing what to implement (files, functions, behavior)
2. Call OpenCode via Bash:
   ```bash
   opencode run --model openrouter/z-ai/glm-5.1 -f <target-file> "<spec>" --format json
   ```
   Or for headless server mode (more reliable for file edits):
   ```bash
   opencode run --attach http://localhost:4096 --model openrouter/z-ai/glm-5.1 "<spec>"
   ```
3. Review the diff (`git diff`) and fix any issues — either directly or send back to OpenCode
4. GLM 5.1 is 5x cheaper than Sonnet on output tokens. Use GLM 4.7 ($0.39/$1.75) for simpler tasks.

Do NOT use this for small edits (< 20 lines) — edit directly instead.
Do NOT use this for tasks requiring deep understanding of the codebase — Sonnet/Opus should handle those.

### Subagent Model Assignment
- **Haiku**: File search (explorer), SQL queries (sql-runner), status checks
- **Sonnet**: Code review, brain file I/O (resume/wrap-up), calibration data gathering
- **Opus**: Architecture decisions, complex debugging, calibration interpretation

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
