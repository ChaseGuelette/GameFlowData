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
11. **After plan approval, HAND OFF to GLM via OpenCode** — do NOT read implementation files into your context. Write the spec, attach source files via `-f` flags, and let GLM do the reading and editing. Fall back to direct implementation ONLY if OpenCode fails (server down, escaping issues, timeout).
12. **NEVER trust a sql-runner count without verification before destructive actions** — Haiku agents can hallucinate plausible row counts. Before any UPDATE/DELETE based on a subagent's "N rows found", spawn a second sql-runner with `SELECT COUNT(*)` to verify the number is non-zero and matches. If the counts disagree, the subagent fabricated results.

## Assets
The [[Assets]] folder contains images, videos, PDFs, and other media. When working on any task, check Assets/ for related materials. You can analyze images, read PDFs, and process any file dropped there.

## Model Routing

The user manually selects the session model (`/model opus`, `/model sonnet`). No auto-escalation rules — the model in use handles everything directly.

For mechanical subtasks, delegate to cheaper subagents:
- **Haiku**: File search (explorer), SQL queries (sql-runner)
- **Sonnet**: Brain file I/O (resume/wrap-up braintree skills)

### Code Implementation via OpenCode + GLM (PREFERRED — fallback to direct edit)
A headless OpenCode server runs at `http://localhost:4096` (started from the project root).

**RULE: When a plan is approved, IMMEDIATELY hand off implementation to GLM via OpenCode.** Do NOT read implementation files into your context first — you already have the plan. Write the spec file, attach the target source files via `-f` flags (GLM reads them, not you), and hand off. If OpenCode fails (server down, escaping issues, timeout, file-not-found errors), fall back to implementing directly. Do not retry OpenCode more than once — if it fails, just do the work yourself.

**This applies when ANY of these are true:**
- The total estimated edits exceed ~20 lines of changes (even in a single file)
- The plan touches 2+ code files
- A plan or spec exists with clear file paths, function names, and behavior AND the changes touch code

**CRITICAL: Do NOT read implementation files into your own context after plan approval.** The plan describes what to change. The `-f` flag gives GLM the source files as context. Reading 1000+ line files into your context and then implementing directly defeats the purpose of GLM delegation and wastes tokens.

**Workflow:**
1. Write the spec to a file, then pass it to OpenCode. **ALWAYS use `run_in_background: true`** on the Bash tool — GLM calls can exceed the 2-minute timeout.
   ```bash
   # CRITICAL: Prompt MUST come BEFORE -f flags. OpenCode's -f is a greedy array
   # that swallows subsequent positional args as filenames. Wrong order = "File not found" error.

   # For short specs (< 5 lines): inline as the prompt argument
   export OPENROUTER_API_KEY=$(grep OPENROUTER_API_KEY .env | cut -d'"' -f2) && opencode run --attach http://localhost:4096 -m openrouter/z-ai/glm-5.1 "short spec here"

   # For long specs (> 5 lines): write to file first, then attach with -f
   # Step A: Generate a unique spec filename: .claude/glm_spec_<timestamp>.md
   #         Example: .claude/glm_spec_20260420_143052.md
   #         Use: SPEC_FILE=".claude/glm_spec_$(date +%Y%m%d_%H%M%S).md"
   # Step B: Use the Write tool to create the spec file with the full plan
   # Step C: Run OpenCode — PROMPT FIRST, then -f flags
   SPEC_FILE=".claude/glm_spec_$(date +%Y%m%d_%H%M%S).md"
   export OPENROUTER_API_KEY=$(grep OPENROUTER_API_KEY .env | cut -d'"' -f2) && opencode run --attach http://localhost:4096 -m openrouter/z-ai/glm-5.1 "Implement the spec in the attached file. The other attached files are existing code for context." -f "$SPEC_FILE" -f src/target_file.py
   ```
   **ARGUMENT ORDER**: `"prompt text" -f file1 -f file2` — NEVER `-f file1 "prompt"` (prompt gets eaten as a filename).
   **Use relative paths** in specs (not absolute) — the server runs from the project root.
   **Spec filenames must be unique** — use `.claude/glm_spec_<YYYYMMDD_HHMMSS>.md` (not a fixed name). Multiple sessions can run concurrently.
   The `-f` flag attaches files for GLM to read as context (existing source files AND the spec file).
2. **Review GLM's work (diff-only — do NOT read full files).** Check the background task output with `TaskOutput`, then run `git diff -- <file1> <file2> ...` scoped to ONLY the files listed in your spec. Do NOT use bare `git diff` — other terminals may have concurrent uncommitted changes that would pollute the review. You already have the plan in your context — compare the diff against it. Do NOT Read the modified files; the diff is sufficient. Check for:
   - Missing steps from the plan (GLM skipped something)
   - Wrong imports or function signatures
   - Logic that doesn't match the spec
   - Project conventions violated (e.g., connection patterns, RLS, etc.)
   If the diff matches the plan: done. If mismatches found: flag them to the user with specifics ("GLM missed step 3" or "wrong function signature on line X").
3. Fix small issues directly (Edit tool). For larger problems, send corrections back to OpenCode with a targeted prompt.
4. Run tests/linting if applicable.

**Model tiers:** `z-ai/glm-5.1` (default — best quality, $0.95/$3.15), `z-ai/glm-4.5-air:free` (fallback — FREE, simpler tasks)

**If the server is not running**, start it: `cd /c/Users/Chase/Projects/GameFlowData && export OPENROUTER_API_KEY=$(grep OPENROUTER_API_KEY .env | cut -d'"' -f2) && opencode serve --port 4096`

**Known failure modes (fall back to direct edit if any occur):**
- "File not found: <prompt text>" — Prompt is AFTER `-f` flags. Fix: put prompt BEFORE all `-f` flags (`"prompt" -f file`, not `-f file "prompt"`).
- `/tmp/` writes fail — Windows doesn't have `/tmp/`. Use `.claude/glm_spec_<timestamp>.md` instead.
- Bash tool returns "Error: Exit code 1" repeatedly — Bash may be non-functional in the session. Fall back to direct Edit.
- Concurrent OpenCode calls can collide — send one at a time.
- **Concurrent terminal diffs collide** — When multiple terminals delegate to GLM, bare `git diff` shows ALL terminals' changes. Always scope: `git diff -- file1.py file2.py` using only the files from your spec.
- **Exit code 1 ≠ GLM failed** — OpenCode transport can return exit code 1 even when GLM successfully wrote all files. Before falling back, check whether the target output file(s) exist and have content. If GLM wrote them, skip the fallback to avoid duplicate work. Only fall back if the files are missing or empty.

**Do NOT use OpenCode for:** small edits (< 20 lines), config changes, brain/markdown updates, or tasks requiring deep cross-system reasoning.

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
- **After plan approval, hand off to GLM via OpenCode immediately.** Do NOT read the implementation files yourself — write the spec to `.claude/glm_spec_<timestamp>.md` (unique per session), attach source files via `-f` flags, run with `"prompt" -f <spec_file> -f <source_files>` (prompt BEFORE -f), backgrounded. GLM reads the files, not you. If OpenCode fails, fall back to direct implementation — don't waste tool calls retrying.

## Subagents
Specialized agents in .claude/agents/:
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
