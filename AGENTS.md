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
11. **After plan approval, HAND OFF to the implementation worker lane** — first try the `codex-spark-worker` Hermes profile for code/file writes. Do NOT read implementation files into your context when the spec already names the files and behavior. Write the spec, give the worker a narrow allowed edit scope, and review scoped diffs/tests afterward. Fall back to OpenRouter Codex, OpenCode GLM, or direct implementation only if the Spark worker lane fails or is inappropriate.
12. **NEVER trust a sql-runner count without verification before destructive actions** — Haiku agents can hallucinate plausible row counts. Before any UPDATE/DELETE based on a subagent's "N rows found", spawn a second sql-runner with `SELECT COUNT(*)` to verify the number is non-zero and matches. If the counts disagree, the subagent fabricated results.

## Assets
The [[Assets]] folder contains images, videos, PDFs, and other media. When working on any task, check Assets/ for related materials. You can analyze images, read PDFs, and process any file dropped there.

## Model Routing

The user manually selects the session model (`/model opus`, `/model sonnet`). No auto-escalation rules — the model in use handles everything directly.

For mechanical subtasks, delegate to cheaper subagents:
- **Haiku**: File search (explorer), SQL queries (sql-runner)
- **Sonnet**: Brain file I/O (resume/wrap-up braintree skills)

### Code Implementation Worker Lane (Codex Spark first — fallbacks: OpenRouter Codex, OpenCode GLM, direct edit)

**RULE: When a plan is approved and the result requires code/file writes, try the `codex-spark-worker` Hermes profile first.** Do NOT read implementation files into your own context first if the plan/spec already names the target files and behavior. Write a precise spec file, give the worker a narrow allowed edit scope, then review the scoped diff yourself.

`codex-spark-worker` is a Hermes profile configured for `openai-codex` + `gpt-5.3-codex-spark` with 400K context. It is the default implementation lane because it passed the 14-file markdown refactor benchmark that local Qwen failed.

**This applies when ANY of these are true:**
- The total estimated edits exceed ~20 lines of changes (even in a single file)
- The plan touches 2+ code files
- A plan or spec exists with clear file paths, function names, and behavior AND the changes touch code

**CRITICAL: Do NOT read implementation files into your own context after plan approval unless needed for review/fallback.** The plan/spec describes what to change; the worker should read and edit the files. Reading 1000+ line implementation files into your context defeats the worker-lane purpose and wastes tokens.

**Preferred workflow:**
1. Write the spec to a unique file, usually `.Codex/spark_spec_<YYYYMMDD_HHMMSS>.md`. Include exact target files/directories, acceptance criteria, and tests/lint commands if applicable.
2. Run Codex Spark in the constrained worker profile from the repo root:
   ```bash
   hermes -p codex-spark-worker chat --quiet --toolsets terminal,file,code_execution,todo -q "Read /mnt/c/Users/Chase/Projects/GameFlowData/.Codex/spark_spec_<timestamp>.md and implement it exactly. Edit only /mnt/c/Users/Chase/Projects/GameFlowData/<allowed-scope>. Do not ask questions. When complete, summarize what you changed."
   ```
3. **Review the worker's work (diff-only when possible).** Run `git diff -- <file1> <file2> ...` scoped to ONLY the files/directories listed in the spec. Do NOT use bare `git diff` — other terminals may have concurrent uncommitted changes that would pollute the review. Check for:
   - Missing steps from the plan
   - Wrong imports or function signatures
   - Logic that doesn't match the spec
   - Project invariants violated (e.g., connection patterns, RLS, Supabase constraints)
4. Run tests/linting/validators from the spec. Do not trust the worker's self-reported validation.
5. Fix small issues directly. If the worker badly misses the spec, revert the scoped changes and use a fallback.

**Fallback order:**
1. `codex-spark-worker` (`openai-codex` / `gpt-5.3-codex-spark`) — first choice for implementation writes.
2. OpenRouter `openai/gpt-5.3-codex` — use if Spark is unavailable or rate-limited and paid OpenRouter usage is acceptable.
3. OpenCode + GLM — use if the Codex lane fails or if you specifically need the OpenCode server workflow. A headless OpenCode server runs at `http://localhost:4096`.
4. Direct edit — use for small edits, config/markdown-only changes, or when all worker lanes fail.

**Codex Spark known issues:**
- If Hermes says Codex credentials are stale/rate-limited, try `hermes auth reset openai-codex` once, then rerun a smoke test:
  ```bash
  hermes -p codex-spark-worker chat --quiet --toolsets todo -q 'Reply exactly: spark-profile-ok'
  ```
- If auth is actually blocked, ask the user to re-authenticate. Do not silently pivot to a different paid route.

**OpenCode + GLM fallback notes:**
- Start server if needed: `cd /c/Users/Chase/Projects/GameFlowData && export OPENROUTER_API_KEY=$(grep OPENROUTER_API_KEY .env | cut -d'"' -f2) && opencode serve --port 4096`
- Prompt must come BEFORE `-f` flags: `"prompt" -f spec.md -f src/file.py`, never `-f spec.md "prompt"`.
- Use unique `.Codex/glm_spec_<YYYYMMDD_HHMMSS>.md` files.
- OpenCode exit code 1 can still mean files were written; verify scoped outputs before falling back.

**Do NOT use worker lanes for:** tiny edits (< 20 lines), config changes, brain/markdown updates that are faster to patch directly, tasks requiring deep cross-system reasoning, or SQL/database work.

### Context Protection Rules
- **NEVER call Supabase MCP tools directly in the main context.** Always delegate SQL to a `sql-runner` subagent (Task tool, `model: "haiku"`). SQL results can be thousands of tokens — they must stay in the subagent. The main context only receives the subagent's summary.
- **NEVER call Supabase MCP tools from Explore agents.** Explore agents are for file search only (Read, Grep, Glob). If exploration requires SQL, spawn a separate sql-runner subagent in parallel.
- **Keep Explore agent prompts narrow and bounded.** Bad: "explore the arb paper trader infrastructure". Good: "find the entry point for arb paper trading in src/arbitrage/ and list its public functions". Set `max_turns: 10` on Explore agents to prevent runaway exploration. A focused Explore should use 5-12 tool calls, not 25+.
- **Explore agent threshold rules:**
  - **1-2 searches**: Use Grep/Glob directly — no Explore agent needed
  - **3+ searches OR reading 3+ files**: Launch an Explore agent
  - **Unknown scope** (don't know which files are relevant): Launch an Explore agent
  - When in plan mode: ALWAYS use Explore agents for investigation
- **Keep Plan agent prompts authoritative and bounded.** Set `max_turns: 15` on all Plan agent Task calls. Include in the prompt: "The context provided above is authoritative — do NOT re-read files already described. Only look up files not covered in this prompt." A Plan agent should design a solution from the context it receives, not re-explore the codebase. If it needs more context, the Explore phase was insufficient — fix the Explore prompts, don't let Plan agents compensate with unbounded exploration.
- **Brain-first exploration.** When investigating a system, start from the BrainTree (`brain/` folder) for orientation before diving into source code. `brain/Pipeline/Component-Docs.md` indexes 40+ module docs via wikilinks. Read the relevant brain doc first to understand architecture, then go to source files for current implementation details. Pattern: Explore 1 reads brain docs for "what should exist", Explore 2 reads source for "what actually exists" — run in parallel.
- **After plan approval, hand off to the implementation worker lane immediately.** First try `codex-spark-worker` with a unique `.Codex/spark_spec_<timestamp>.md` and a narrow allowed edit scope. Do NOT read implementation files yourself unless needed for review/fallback. If Spark fails or is inappropriate, fall back to OpenRouter Codex, OpenCode GLM, or direct implementation in that order.

## Subagents
Specialized agents in .Codex/agents/:
- [[explorer]] - Haiku-powered codebase search and file reading (max 5-12 tool calls)
- [[sql-runner]] - Haiku-powered database queries (read-only, anti-hallucination rules)

## Commands
- /init-braintree - Initialize a new brain
- /resume-braintree - Resume from where you left off
- /wrap-up-braintree - End session with proper handoff
- /status-braintree - View progress dashboard
- /plan-braintree [step] - Plan a specific step
- /sprint-braintree - Plan the week's work
- /sync-braintree - Health check and sync
- /feature-braintree [name] - Plan a new feature
