# Token-Saving Architecture Rework — Complete Record

**Sessions**: April 17-18, 2026 (spans 2 sessions, second resumed from context compaction)
**Goal**: Reduce Claude Code token consumption from ~1 hour of Max 5x usage to ~2.5-3 hours

---

## Problem Statement

On a $100/mo Anthropic plan (5x Max, ~225 Opus-equivalent messages per 5hr window), sessions were burning out in about an hour. Root causes identified:

1. **Claude-flow hooks** fired `npx @claude-flow/cli@latest hooks ...` on EVERY tool call (6 hook types: PreToolUse, PostToolUse, UserPromptSubmit, SessionStart, Stop, Notification)
2. **~80 unused slash commands** from claude-flow directories loaded into system prompt
3. **`claudeFlow` config block** (~100 lines) in settings.json consuming context
4. **No model routing** — everything ran on Opus, no subagent delegation
5. **No external model bridge** for cheap code generation

### What We Learned Was NOT a Problem
- **Skills only use 1.8K tokens** (0.9% of context) — not the major cost originally claimed
- **ARCHITECTURE.md wasn't actually loading** despite being in `context.files` — removing it was a no-op, not the "38K savings" originally estimated
- **The real token hogs**: MCP tools (11.6K), accumulated messages (grows to 50K+), and system tools (15.8K)

---

## Phase 1: Strip Claude-Flow Overhead (DONE)

### settings.json cleanup
**File**: `.claude/settings.json` — reduced from ~250 lines to ~17 lines

Removed:
- All 6 hook types (PreToolUse, PostToolUse, UserPromptSubmit, SessionStart, Stop, Notification)
- `statusLine` block
- `claudeFlow` config block (~100 lines of agent topology, swarm config, etc.)
- Claude-flow permissions
- `sk` and `swarm` command aliases
- `spec.md` and `ARCHITECTURE.md` from `context.files` (spec.md never existed, ARCHITECTURE.md wasn't loading)

Final settings.json:
```json
{
  "commands": { "lint": "pyright", "test": "pytest" },
  "auto_run_checks": { "lint": "pyright", "test": "pytest" },
  "context": { "files": ["CLAUDE.md"] },
  "permissions": { "allow": [], "deny": [] }
}
```

### Deleted command directories (all confirmed claude-flow boilerplate via git history)
- `.claude/commands/sparc/` — 30 files, SPARC methodology modes
- `.claude/commands/monitoring/` — 5 files, claude-flow swarm monitoring
- `.claude/commands/optimization/` — 5 files, claude-flow topology/cache optimization
- `.claude/commands/automation/` — 7 files, auto-agent/self-healing workflows
- `.claude/commands/hooks/` — 7 files, pre/post edit/task hooks
- `.claude/commands/github/` — 18 files, claude-flow github wrappers (NOT real GitHub tools — those come from MCP)
- `.claude/commands/analysis/` — 6 files, claude-flow token tracking

### Deleted individual command files
- `claude-flow-help.md`, `claude-flow-memory.md`, `swarm.md`

### Kept commands
- `check-calibration.md`, `check-kalshi.md` (refactored for subagent delegation)
- `start.md`, `end.md`, `status.md`, `validate.md`
- `learn.md`, `learn-show.md`, `learn-search.md`, `learn-curate.md`
- `init.md`, `adopt.md`
- `work-*.md` (6 files)
- Braintree commands (resume, wrap-up, etc.)

---

## Phase 2: Multi-Model Routing (DONE)

### Agent model assignments
**Existing agents updated:**
- `.claude/agents/builder.md` — added `model: sonnet`
- `.claude/agents/analyst.md` — added `model: sonnet`
- `.claude/agents/strategist.md` — added `model: sonnet`
- `.claude/agents/ops.md` — added `model: haiku`

**New agents created:**
- `.claude/agents/explorer.md` — Haiku-powered codebase search (Read, Grep, Glob only)
- `.claude/agents/sql-runner.md` — Haiku-powered database queries (mcp__supabase__execute_sql only, SELECT only)

### Escalation rules added to CLAUDE.md
Default session model is Sonnet. Opus is spawned as a subagent ONLY for:
- "Why" questions about model performance, ROI, calibration drift
- Debugging spanning 3+ files with subtle cross-system interactions
- Architecture or design decisions
- Calibration interpretation (not data gathering)
- Novel problem-solving where the approach is uncertain
- Planning multi-step implementations touching 5+ files

Key insight: **Sonnet can't reliably self-assess quality**, so escalation rules are mechanical pattern-matching checklists, not judgment calls.

### Haiku supervisor pattern — evaluated and rejected
Considered having Haiku as the primary conversation model that spawns Opus subagents for hard tasks. Rejected because:
- Haiku would interpret/simplify Opus output when relaying it back (quality loss)
- Sonnet as default gets 80% of the benefit without the relay problem
- The token savings from Haiku vs Sonnet default are marginal compared to the quality tradeoff

---

## Phase 3: OpenCode + GLM Integration (DONE)

### What it is
OpenCode is a CLI coding agent that can use any model via OpenRouter. We use it to delegate code writing to GLM models (by Zhipu AI), which are significantly cheaper than Claude:
- GLM 5.1: $0.95/$3.15 per M tokens (best quality, ~Sonnet-level for code)
- GLM 4.7: $0.39/$1.75 per M tokens (simpler tasks)
- GLM 4.5 Air: FREE on OpenRouter (trivial tasks)

Compare to: Sonnet $3/$15, Opus $5/$25

### Installation
- `npm install -g opencode-ai@latest` (NOT `opencode` — that package doesn't exist)
- OpenCode v1.4.10 installed
- OpenRouter API key stored in `.env` as `OPENROUTER_API_KEY`

### Headless server
- Start script: `scripts\start_opencode_server.bat`
- Runs on `http://localhost:4096`
- Script auto-loads API key from `.env`, kills stale processes on port 4096
- Server shows all GLM activity in its terminal (model used, files written, responses)
- **Handles concurrent requests** — tested and confirmed, multiple Claude terminals can share one server

### Workflow (codified in CLAUDE.md)
1. Claude (Sonnet) plans the implementation
2. Plan gets written to `/tmp/glm_spec.md` (for long specs) or passed inline (for short ones)
3. Claude calls: `opencode run --attach http://localhost:4096 -m openrouter/z-ai/glm-5.1 -f /tmp/glm_spec.md -f src/target.py "Implement the spec in the attached file"`
4. **MUST use `run_in_background: true`** on the Bash tool — GLM calls can exceed the 2-minute timeout
5. Claude checks `TaskOutput` for results
6. Claude reviews `git diff` against the plan still in its context
7. Claude fixes small issues directly or sends corrections back to OpenCode

### Critical lessons learned (from failed attempts)
- **Long specs break as shell arguments** — quotes, newlines, special chars cause escaping failures. Write to file first.
- **`-f` flag attaches files for GLM to read** — use it for both existing source files AND the spec file
- **Use relative paths** in specs — the server runs from the project root
- **Timeout kills conversations** — if `run_in_background` is not set and GLM takes >2min, the Bash tool times out and can disrupt the session
- **The npm package is `opencode-ai`** not `opencode` — the latter returns 404
- **PowerShell `curl` is an alias for `Invoke-WebRequest`** — the Linux-style install script (`curl -fsSL | bash`) fails in PowerShell

---

## Phase 4: Context Protection Rules (DONE)

### Added to CLAUDE.md Critical Invariants (8-11)
8. **NEVER call Supabase MCP directly in main context** — always delegate to sql-runner subagent (haiku). SQL results can be thousands of tokens that pollute the main context.
9. **NEVER use Explore agents for SQL** — Explore is file-only (Read/Grep/Glob). If exploration needs SQL, spawn a separate sql-runner in parallel.
10. **Keep Explore agents narrow** — `max_turns: 10`, focused prompts, 5-12 tool calls. Bad: "explore the arb paper trader infrastructure". Good: "find the entry point for arb paper trading in src/arbitrage/ and list its public functions".
11. **After plan approval, hand off to GLM** — do NOT read target files or write code yourself.

### Context Protection Rules section (detailed guidance)
- **Prefer Grep/Glob directly** over Explore agents for simple searches
- **Brain-first exploration**: start from `brain/` folder for architectural orientation before diving into source. `brain/Pipeline/Component-Docs.md` indexes 40+ module docs. Pattern: Explore 1 reads brain docs ("what should exist"), Explore 2 reads source ("what actually exists") — run in parallel.
- **After plan approval, hand off immediately** — do NOT re-read files already explored during planning

### Subagent limitations discovered
- Subagents CANNOT spawn their own subagents (one level deep only)
- If an Explore agent needs SQL context: main agent spawns sql-runner AND Explore in parallel, then synthesizes both summaries
- For serial dependencies (need SQL result before knowing which files to explore): sql-runner first, then Explore with the SQL summary baked into its prompt

---

## Phase 5: Braintree Refactoring (DONE)

### `/resume-braintree` refactored
- **REMOVED**: CLAUDE.md re-read (already auto-loaded via `context.files`)
- **CHANGED**: All context gathering delegated to a Sonnet subagent (was potentially main context)
- **CHANGED**: Reads 2 most recent handoffs (was 1) — user runs multiple terminals
- Main context only receives ~500 token structured summary

### `/wrap-up-braintree` refactored
- **Two-phase architecture**:
  1. Main context writes session summary bullet list (only it can see the conversation)
  2. Sonnet subagent takes that summary, reads brain files, creates handoff, updates indexes/execution plan
- User still reviews session summary before subagent runs (quality gate preserved)
- Main context gets ~100 token confirmation back

---

## Open Items (NOT YET ADDRESSED)

### 1. Brain doc maintenance
- Big design docs like `brain/Decisions/Kalshi-Integration-Design.md` (560 lines) drift from reality
- Example: Phase 5 says "future" but live trading is already implemented
- Wrap-up process is supposed to keep brain docs current but misses these big docs
- The `docs/` folder (80+ files from old context system) overlaps with brain docs — no decision on consolidation

### 2. GLM handoff is PREFERRED, not mandatory
- Multiple production failures observed:
  - "File not found" error: OpenCode treats the prompt text as a filename when `-f` flags are present
  - `/tmp/` path doesn't exist on Windows — spec file writes fail
  - Bash tool goes non-functional in some sessions (all calls return exit code 1)
  - Concurrent OpenCode calls can collide
- **Decision**: GLM handoff is attempt-first with fallback to direct Edit. Not enforced via hook.
- A PreToolUse hook was built and tested (`block-code-edits.sh`) but removed — too many OpenCode reliability issues to force it
- CLAUDE.md updated: "MANDATORY" → "PREFERRED — fallback to direct edit"
- Invariant 11 updated: "TRY GLM first, fall back if it fails"

### 3. ~30 user-level skills still loading
- agentdb-*, flow-nexus-*, v3-*, pair-programming, etc. from cached npm packages
- Only 1.8K tokens total so low priority, but they're noise
- These come from user-level installs, not project-level — harder to remove

### 4. docs/ folder vs brain overlap
- `docs/` has 80+ detailed module-level docs that were actively maintained by the old context system
- `brain/Pipeline/Component-Docs.md` already wikilinks to most of them
- No decision made on whether to merge into brain, keep both, or deprecate one

---

## Files Modified/Created This Session

### Modified
| File | Change |
|------|--------|
| `.claude/settings.json` | Stripped from ~250 to ~17 lines (hooks, claudeFlow, statusLine, context.files) |
| `CLAUDE.md` | Added Model Routing & Escalation, OpenCode+GLM workflow, Context Protection Rules, Critical Invariants 8-11 |
| `.claude/agents/builder.md` | Added `model: sonnet` |
| `.claude/agents/analyst.md` | Added `model: sonnet` |
| `.claude/agents/strategist.md` | Added `model: sonnet` |
| `.claude/agents/ops.md` | Added `model: haiku` |
| `.claude/commands/check-calibration.md` | Refactored to delegate SQL to haiku subagent |
| `.claude/commands/check-kalshi.md` | Refactored to delegate SQL to haiku subagent |
| `~/.claude/commands/resume-braintree.md` | Sonnet subagent delegation, 2 handoffs, no CLAUDE.md re-read |
| `~/.claude/commands/wrap-up-braintree.md` | Two-phase: main writes summary, Sonnet subagent does file I/O |
| `memory/MEMORY.md` | Trimmed, added token efficiency section, OpenCode operational patterns |

### Created
| File | Purpose |
|------|---------|
| `.claude/agents/explorer.md` | Haiku-powered codebase search subagent |
| `.claude/agents/sql-runner.md` | Haiku-powered SQL query subagent |
| `scripts/start_opencode_server.bat` | One-click OpenCode server startup for Windows |
| `scripts/setup_opencode.sh` | Linux/bash setup script (reference) |
| `memory/mlb_paper_trader.md` | Archived detailed MLB paper trader audit from MEMORY.md |
| `memory/stripe_integration.md` | Archived detailed Stripe integration from MEMORY.md |
| `docs/token-saving-architecture-rework.md` | This document |

### Deleted
| Path | Reason |
|------|--------|
| `.claude/commands/sparc/` (30 files) | Claude-flow boilerplate |
| `.claude/commands/monitoring/` (5 files) | Claude-flow boilerplate |
| `.claude/commands/optimization/` (5 files) | Claude-flow boilerplate |
| `.claude/commands/automation/` (7 files) | Claude-flow boilerplate |
| `.claude/commands/hooks/` (7 files) | Claude-flow boilerplate |
| `.claude/commands/github/` (18 files) | Claude-flow boilerplate (NOT real GitHub — those are MCP) |
| `.claude/commands/analysis/` (6 files) | Claude-flow boilerplate |
| `.claude/commands/claude-flow-help.md` | Claude-flow |
| `.claude/commands/claude-flow-memory.md` | Claude-flow |
| `.claude/commands/swarm.md` | Claude-flow |

---

## Key Design Decisions & Rationale

### Why no external MCP bridges (PAL-MCP-Server, Gemini MCP)?
- PAL-MCP-Server adds 33 tools to MCP context (more token bloat)
- External models can't use our Supabase/GitHub MCPs
- Solo developer shouldn't maintain proxy infrastructure
- OpenCode via CLI is simpler — it's just a Bash call

### Why Sonnet default, not Haiku?
- Haiku as supervisor would relay/interpret Opus output, losing quality
- Sonnet handles 85-90% of tasks at comparable quality to Opus
- The token savings from Haiku vs Sonnet don't justify the quality loss

### Why mechanical escalation rules?
- Sonnet genuinely can't self-assess when it's out of its depth
- "Escalate when you think you need it" doesn't work — it either always or never escalates
- Pattern-matching checklist (3+ file debugging, architecture decisions, etc.) is reliable

### Why GLM over other cheap models?
- GLM 5.1 benchmarks near Sonnet for code generation at 5x lower cost
- GLM 4.5 Air is completely FREE — good for trivial tasks
- OpenRouter provides simple API access to all GLM tiers
- OpenCode has native OpenRouter integration — no custom wrapper needed

### Why brain-first exploration?
- `brain/Pipeline/Component-Docs.md` indexes 40+ module docs via wikilinks
- `brain/Decisions/Kalshi-Integration-Design.md` has 560 lines of architecture, configs, fee formulas
- Reading brain docs first gives architectural orientation in ~2 reads vs ~25 blind file searches
- But brain docs can be stale — still need source code verification (hence the two-agent pattern)

### Why subagent delegation for SQL?
- A single SQL result from `raw_player_props_combined` can return thousands of tokens
- Those tokens stay in the main context forever (no garbage collection)
- Subagent runs the query, summarizes the result in ~100 tokens, main context only gets the summary
- Same pattern applies to large file reads during exploration

---

## Verification Checklist (for next session)

1. [ ] Start fresh Claude Code session — verify no hook errors on tool calls
2. [ ] Check that CLAUDE.md loads correctly (only file in context.files)
3. [ ] Run `.\scripts\start_opencode_server.bat` in a separate terminal
4. [ ] Start a task that requires planning + code implementation
5. [ ] Verify Sonnet delegates SQL to sql-runner subagent (not direct MCP call)
6. [ ] Verify plan completion triggers OpenCode handoff (not Sonnet writing code)
7. [ ] Verify OpenCode call uses `run_in_background: true`
8. [ ] Verify Sonnet reviews `git diff` after GLM finishes
9. [ ] Compare session duration to pre-rework baseline (~1 hour → target 2.5-3 hours)
10. [ ] Run `/check-calibration` to verify subagent delegation works for existing workflows
