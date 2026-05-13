# Hermes + GBrain Migration Checklist

> Part of [[Operations]]

**Status**: Draft migration plan
**Created**: May 11, 2026
**Purpose**: Practical list of old Claude Code / BrainTree / legacy Solokit artifacts to keep, drop, or rebuild as a Hermes + GBrain workflow for GameFlowData.

## Recommendation in One Sentence

Use **GBrain as the primary indexed retrieval + knowledge graph layer**, use **Hermes as the operating agent/runtime**, and keep the existing `brain/` markdown tree as the canonical human-readable project source until GBrain proves it can safely own the full project memory lifecycle.

This is not "GBrain vs BrainTree". The target is:

```text
Hermes = agent runtime, tools, MCP, profiles, delegation, cron, gateway
GBrain = indexed memory, graph retrieval, ingestion, maintenance, query layer
brain/ = canonical project docs during transition; eventually mostly managed through GBrain
AGENTS.md = small always-loaded safety/operating contract
```

## Migration Buckets

| Bucket | Meaning |
|---|---|
| Keep in `AGENTS.md` | Must be always visible before any tool use or code work. |
| Port to Hermes Skill | Reusable workflow the agent should explicitly load/run. |
| Port to GBrain | Knowledge, notes, entities, tasks, relationships, handoffs, reports, searchable history. |
| Replace with Hermes Native | Hermes already has better built-in support. |
| Archive/Delete | Stale Claude scaffolding, legacy Solokit artifacts, logs, caches, or one-off specs after salvage audit. |

---

## 1. Always-Loaded Project Instructions

### Source artifacts

- `CLAUDE.md`
- `AGENTS.md`
- `brain/Operations/Critical-Invariants.md`
- Critical parts of `.claude/agents/explorer.md`
- Critical parts of `.claude/agents/sql-runner.md`
- Critical parts of `.codex/hooks.json` / `.claude/settings.json`

### Action

**Keep, but shrink.** `AGENTS.md` should remain the always-loaded operating contract, but it should not become a giant brain dump. Move broad knowledge into GBrain and keep only safety-critical instructions in `AGENTS.md`.

### Keep in `AGENTS.md`

- Project identity: GameFlowData is sports analytics + player props + trading/alerts.
- Current major business goals: NBA maintain, MLB ship, monetization.
- Critical invariants:
  - Never deploy global conformal recalibration offsets.
  - Never put advanced stats scraping on Railway.
  - Railway daily stats job uses CDN-only path.
  - Never run non-concurrent `CREATE INDEX` on `raw_player_props_combined`.
  - Empirical CDF: `(samples > line).mean()`, never Gaussian CDF.
  - Python backend uses `postgres` role; dashboard uses `authenticated` role.
  - Q10 miscalibration is the edge; do not "fix" it blindly.
  - SQL must stay isolated from main context.
  - Explore agents are file-only.
  - Large code implementation goes through OpenCode/GLM when appropriate.
- Brain/GBrain-first lookup rule.
- Diff-only review rule for delegated implementation.

### Move out of `AGENTS.md`

- Long descriptions of every brain folder.
- Full OpenCode command tutorial.
- Full old BrainTree command list.
- Historical project state that changes weekly.
- Anything that can be retrieved from GBrain or a Hermes skill.

### Migration task

- [ ] Create a slim `AGENTS.md` v2 with only permanent invariants and runtime rules.
- [ ] Move expanded project background into GBrain pages.
- [ ] Keep `brain/Operations/Critical-Invariants.md` as the canonical detailed invariant doc and index it in GBrain.

---

## 2. BrainTree Markdown Brain (`brain/`)

### Source artifacts

- `brain/BRAIN-INDEX.md`
- `brain/Execution-Plan.md`
- `brain/Models/*`
- `brain/Pipeline/*`
- `brain/Product/*`
- `brain/Infrastructure/*`
- `brain/Business/*`
- `brain/Operations/*`
- `brain/Decisions/*`
- `brain/Handoffs/*`
- `brain/Templates/*`
- `brain/Assets/*`

### Action

**Port to GBrain as first-class indexed sources, not archive.** This is the highest-value migration target. The existing 117 markdown files are already structured well enough for GBrain import.

### How GBrain should change the workflow

Old BrainTree behavior:

```text
Agent manually reads BRAIN-INDEX -> latest handoff -> relevant folder docs -> source code
```

Target GBrain behavior:

```text
Agent queries GBrain -> gets ranked project pages + relationships + citations -> reads only selected canonical files if needed
```

### Keep during transition

- Keep `brain/` checked into repo.
- Continue writing important project docs as markdown.
- Use wikilinks where useful, but let GBrain handle richer graph/entity indexing.

### Eventually replace

- Manual "read this folder index first" flows can become GBrain `query`/`search` calls.
- Manual broken-link/orphan scans can become GBrain maintain/sync routines.
- Manual handoff discovery can become GBrain latest-report/latest-handoff query.

### Migration task

- [ ] Install GBrain via `git clone + bun install + bun link` path, not npm/global package.
- [ ] Add `brain/` as a GBrain source.
- [ ] Import all markdown files.
- [ ] Run a few validation queries:
  - "What are the critical GameFlow invariants?"
  - "What is the current MLB model status?"
  - "What did the latest handoff recommend?"
  - "How does the Railway daily stats job avoid stats.nba.com?"
- [ ] Keep `brain/` as source of truth until GBrain answers these correctly with citations.

---

## 3. Global BrainTree Commands

### Source artifacts

Global Claude commands found in `/mnt/c/Users/Chase/.claude/commands/`:

- `resume-braintree.md`
- `wrap-up-braintree.md`
- `status-braintree.md`
- `plan-braintree.md`
- `sprint-braintree.md`
- `sync-braintree.md`
- `feature-braintree.md`
- `init-braintree.md`

### Action

**Port only the two BrainTree commands the user actually used: resume and wrap-up. Cut the rest by default.**

The goal is not to recreate BrainTree OS inside Hermes. The goal is to preserve the two session-continuity rituals that worked, then let GBrain/Hermes-native workflows replace the rest if actual usage proves they are needed.

### Priority A: port now

The user reports that the only BrainTree commands they actually use are `resume-braintree` and `wrap-up-braintree`. Treat those two as the essential workflow to preserve.

#### `/resume-braintree` -> Hermes skill: `gameflow-resume`

Purpose to preserve:

- Detect project/brain.
- Load latest handoff.
- Load execution plan status.
- Recommend highest-priority next work.
- Present a concise session startup briefing.

How to improve with GBrain:

- Query GBrain for latest handoff and active work instead of manually globbing files.
- Retrieve related docs by semantic relevance, not only hardcoded folder paths.
- Ask GBrain for "current blockers" and "open questions" across all handoffs.

Hermes role:

- Skill defines the workflow and response format.
- Hermes uses file tools/GBrain MCP/terminal as needed.

#### `/wrap-up-braintree` -> Hermes skill: `gameflow-wrap-up`

Purpose to preserve:

- Audit session work.
- Update relevant project docs.
- Create next handoff.
- Update handoff index.
- Recommend next session files/tasks.

How to improve with GBrain:

- Write a handoff page and ingest/sync it into GBrain.
- Extract decisions, blockers, entities, and task updates into GBrain pages/relationships.
- Avoid duplicating stale summaries across too many markdown files.

Hermes role:

- Skill controls the final audit checklist.
- Hermes writes markdown and triggers GBrain sync/import.

### Priority B: cut by default; recreate only if needed

#### `/status-braintree` -> do not port initially

Do not port as a first-class command unless the user starts asking for project dashboards often. GBrain reports can cover this later.

#### `/plan-braintree` -> do not port initially

Use Hermes `writing-plans` plus GBrain context instead. If a repeated GameFlow planning pattern emerges, let Hermes create a new skill from actual usage.

#### `/sprint-braintree` -> do not port initially

If sprint planning becomes useful again, implement it through GBrain tasks/reports or Hermes kanban, not as a direct BrainTree clone.

#### `/sync-braintree` -> do not port initially

Replace with GBrain maintain/sync routines after GBrain is installed. Add a small Hermes wrapper only if the GBrain checks miss GameFlow-specific issues.

#### `/feature-braintree` -> do not port initially

Use ad hoc planning plus GBrain feature/spec pages. Skillify only after repeated use.

#### `/init-braintree` -> do not port

Replace with GBrain setup/migrate/import. The old `.braintree/brain.json` registry and viewer model is not the future target.

---

## 4. Legacy Project Claude Commands / Solokit Artifacts

### Source artifacts

Project commands found in `.claude/commands/`:

- `start.md`
- `end.md`
- `status.md`
- `validate.md`
- `init.md`
- `adopt.md`
- `work-new.md`
- `work-list.md`
- `work-show.md`
- `work-update.md`
- `work-delete.md`
- `work-next.md`
- `work-graph.md`
- `learn.md`
- `learn-show.md`
- `learn-search.md`
- `learn-curate.md`
- `start-development.md`
- `finish-feature.md`
- `check-calibration.md`
- `check-kalshi.md`

### Action

**Do not port.** The user does not use Solokit and wants anything Solokit-related ditched. Treat these as legacy scaffolding, not workflow requirements. The only item worth preserving from this group is the *intent* of `validate.md`, but not the implementation. The two domain-specific commands worth preserving separately are `check-calibration.md` and `check-kalshi.md`.

### Port / rewrite

#### `check-calibration.md` -> Hermes skill: `gameflow-check-calibration`

Keep. This is domain-specific and valuable.

Target behavior:

- Pull current calibration/performance context from GBrain.
- Use isolated SQL runner for DB checks.
- Report production drift, paper trading ROI, training baseline, and action recommendation.
- Respect invariant: do not suggest global conformal recalibration as a default fix.

#### `check-kalshi.md` -> Hermes skill: `gameflow-check-kalshi`

Keep. This is domain-specific and valuable.

Target behavior:

- Query recent bot/trader status through safe tools.
- Check logs/deployments if needed via Railway/Vercel tools.
- Use isolated SQL runner for DB state.
- Summarize live/paper state, unresolved fills, alerts, and risk.

#### `validate.md` -> do not port as-is; replace with a tiny GameFlow validation recipe

Checked source: `.claude/commands/validate.md` only wraps `sk validate` / `sk validate --fix` and lists generic tests/lint/format/coverage/git/acceptance checks. In this WSL environment, `sk` is not currently on PATH, so the command is not directly portable.

Recommendation:

- Do not create a large Hermes skill from this file.
- Capture the intent as a small `gameflow-validate` recipe later, after confirming the actual current repo commands.
- Prefer scoped checks based on changed files, not global expensive defaults.
- Include frontend checks only when dashboard files are touched.
- Include Python tests/lint only when backend/model/pipeline files are touched.
- Let Hermes build/refine this recipe over time as it observes which validation commands actually work.

### Replace with Hermes/GBrain native

- `start.md` / `end.md`: replaced by `gameflow-resume` and `gameflow-wrap-up`.
- `status.md`: replaced by `gameflow-status`.
- `work-*`: replace with GBrain tasks or Hermes kanban, not both.
- `learn-*`: replace with GBrain notes/skills/Hermes memory rules.
- `start-development.md` / `finish-feature.md`: fold useful checks into planning/wrap-up skills.

### Archive/delete after migration

- `init.md`
- `adopt.md`
- `start.md`, `end.md`, `status.md`, `start-development.md`, `finish-feature.md` unless a future audit finds a specific GameFlow-only check inside them.
- `work-*` commands after any still-relevant work items are migrated into GBrain tasks or Hermes kanban.
- `learn-*` commands after durable learnings are migrated into GBrain notes or Hermes skills.
- Old generic Solokit work item commands after `.session/` is audited/archived.

---

## 5. Subagents / Personas

### Source artifacts

- `.claude/agents/explorer.md`
- `.claude/agents/sql-runner.md`
- OpenCode/GLM workflow embedded in `CLAUDE.md` / `AGENTS.md`

### Action

**Keep the roles, but implement them in Hermes-native patterns.**

### Explorer

Old behavior:

- Haiku model.
- Read/Grep/Glob only.
- Must read `.claude/repo-map.md` first.
- Max 12 tool calls.
- No Bash.
- Summarize paths/line numbers/snippets.

Hermes target:

- Use `delegate_task` with `toolsets: ["file"]` for focused exploration.
- Prompt should say max 10-12 tool calls, no SQL, no code changes.
- Replace `.claude/repo-map.md` dependency with GBrain code/source query where possible.

Migration task:

- [ ] Create Hermes skill `gameflow-explore` or bake prompt template into `gameflow-resume`/planning skills.
- [ ] Add explicit rule: if exploration requires SQL, spawn sql-runner separately.

### SQL Runner

Old behavior:

- Haiku model.
- Supabase execute_sql only.
- Read `.claude/db-schema.md` first.
- SELECT-only.
- Quote exact counts.
- No hallucinated summaries.

Hermes target:

- Use a dedicated `sql-runner` delegate pattern or a Hermes profile with only Supabase read-only tools.
- Main context should still avoid direct Supabase MCP calls.
- Prefer hosted Supabase MCP in `read_only=true` mode where possible.
- Keep `.claude/db-schema.md` equivalent or generate a GBrain-indexed schema snapshot.

Migration task:

- [ ] Build a Hermes skill `gameflow-sql-runner` documenting exact prompt template.
- [ ] Consider disabling Supabase MCP tools in the main GameFlow profile if Hermes supports per-profile tool selection tightly enough.
- [ ] Keep manual anti-hallucination verification rule for destructive DB-adjacent work.

### Provider-Agnostic Implementation Worker (formerly GLM via OpenCode)

Old behavior:

- Claude writes spec.
- OpenCode runs GLM with spec + attached source files.
- GLM edits code.
- Main agent reviews scoped git diff only.

Recommendation: **keep the pattern, but demote it from mandatory default to a provider-agnostic implementation lane with clear triggers and review gates.**

Why keep it:

- It is a strong context-protection strategy for large edits.
- It keeps the main Hermes session focused on planning, invariants, and review instead of loading thousands of implementation tokens.
- It is provider-agnostic at the harness level: OpenCode can run GLM today, but the implementer model can change later.
- It creates a clean separation of roles: Hermes = architect/reviewer/orchestrator; OpenCode worker = code editor.
- It is useful when the repo is large, when multiple files are touched, or when source files are too big for the main session to read safely.

Why not make it universal:

- Small edits are faster and safer directly in Hermes.
- Delegation adds failure modes: wrong file attachment order, stale spec, OpenCode/server issues, unreviewed broad edits, overlapping concurrent diffs.
- If GBrain code indexing becomes good, Hermes may need less context protection for medium tasks because it can retrieve only the relevant code slices.
- The main value is not always token savings; it is controlled separation between planning and implementation.

Use the implementation-worker lane when any of these are true:

- Estimated code changes exceed roughly 50 lines.
- The task touches 2+ implementation files.
- The task needs broad refactoring or repeated mechanical edits.
- The relevant files are large enough that reading them into Hermes would pollute context.
- A written plan/spec already exists and can be handed to a worker.
- The implementation is mostly deterministic once the spec is clear.

Do not use the implementation-worker lane when any of these are true:

- Single small bug fix or config/doc edit.
- The task requires careful reasoning over current logs/errors in the main session.
- The user is still deciding requirements.
- The change is safety-critical and tiny enough for direct edit.
- The worker would need broad ambiguous exploration rather than implementation.

Hermes target:

- Keep the workflow as a Hermes-native skill: `gameflow-implementation-worker` or `gameflow-opencode-worker`.
- Make the implementer model configurable; GLM is the default, not the identity of the workflow.
- Move the full tutorial out of `AGENTS.md` and into the skill.
- Keep only a short trigger rule in `AGENTS.md`.
- Update paths from `.claude/glm_spec_*` to `.Codex/glm_spec_*` or a neutral `.agent/specs/` path.
- Prefer prompt-first argument ordering: `"prompt" -f spec -f source1 -f source2`.
- Always review scoped diffs only: `git diff -- file1 file2`, never bare `git diff` in a concurrent workspace.
- Treat non-zero OpenCode exit as suspicious but not definitive; verify whether files/diff changed before retrying.

Provider-agnostic framing:

- Skill name should probably be `gameflow-implementation-worker` or `gameflow-opencode-worker`, not `gameflow-glm-opencode`, if we want the abstraction to survive model churn.
- Current default lane: OpenCode + `openrouter/z-ai/glm-5.1`.
- Future lanes can be OpenCode + Claude/Sonnet, OpenCode + GPT, Codex CLI, or direct Hermes depending on cost/quality.
- The decision rule should select the *implementation lane*, not hardcode loyalty to GLM.

Quality gates:

- Before worker: spec must list exact files, behavior, non-goals, invariants, and tests/checks.
- During worker: one worker per repo/workdir unless using isolated worktrees.
- After worker: review scoped diff against the spec.
- After review: run scoped validation.
- If worker misses spec or makes broad unexpected changes, either patch directly if small or send one targeted correction. Do not keep retrying blindly.

Migration task:

- [ ] Create Hermes skill `gameflow-opencode-worker` or `gameflow-implementation-worker`.
- [ ] Keep only a 3-line trigger rule in `AGENTS.md`.
- [ ] Add a verification step: if OpenCode returns exit code 1, check target files/diff before assuming failure.
- [ ] Add a lane-selection checklist: direct Hermes vs delegate_task vs OpenCode worker.
- [ ] Add an effectiveness check after 3-5 uses: compare token usage, elapsed time, missed-spec rate, test pass rate, and review burden.

---

## 6. Hook-Based Safety

### Source artifacts

- `.claude/settings.json` PreToolUse hooks
- `.codex/hooks.json`
- `.claude/hooks/block-direct-supabase.sh`
- `.codex/hooks/block-direct-supabase.sh`

### Action

**Preserve the policy, improve the implementation if possible.**

The policy is important: direct Supabase calls in the main context can dump huge results and can lead to unsafe/destructive actions.

### Hermes reality

Hermes does not automatically inherit Claude Code hooks. Options:

1. **Tool selection approach**: remove Supabase MCP tools from the main GameFlow profile; expose them only to a SQL profile/subagent if possible.
2. **Skill/prompt approach**: keep hard instruction in `AGENTS.md` and use `delegate_task` for SQL.
3. **MCP read-only approach**: configure Supabase hosted MCP with `read_only=true` as a baseline safety rail.
4. **Custom Hermes tool/router approach**: later, build a wrapper tool that enforces SQL delegation.

### Migration task

- [ ] Keep `.codex/hooks.json` and `.claude/settings.json` for old tools until fully migrated.
- [ ] Configure Hermes GameFlow profile so Supabase access is read-only by default.
- [ ] Document SQL delegation in a Hermes skill.
- [ ] Later: implement or request a Hermes-native pre-tool policy hook if needed.

---

## 7. `.session/` / Solokit State

### Source artifacts

- `.session/tracking/work_items.json`
- `.session/tracking/learnings.json`
- `.session/specs/*.md`
- `.session/briefings/*.md`
- `.session/history/*.md`
- `.session/guides/*.md`
- `.session/config.json`
- `.session/config.schema.json`

### Action

**Audit for salvage value, then archive `.session/`. Do not treat this as a migration source of truth.**

The user reports Solokit was not used in practice and is likely wildly outdated by several months. This section is therefore about checking whether any historical artifact is still useful, not about migrating Solokit into the new system.

This should not remain an active third memory/task system.

### Default assumption

Assume stale unless proven otherwise. The current `brain/` handoffs, current code, current database/logs, and GBrain-indexed project docs should outrank anything in `.session/`.

### Audit for possible salvage

Check only for content that is still useful and not already represented in `brain/Handoffs/`, `brain/Decisions/`, or current source/docs:

- Specs that still describe an unbuilt or partially built capability.
- Durable lessons that are non-obvious and still true.
- Historical rationale for a decision that is missing from the brain.
- Validation/test ideas that still map to current code.

Candidate specs to spot-check, not blindly migrate:

- backtesting harness
- drift detection
- Railway scheduled task monitoring
- Discord webhook notifications
- dashboard implementation
- multi-sport dashboard
- Optuna tuning

### Do not migrate by default

- Work item statuses from `work_items.json` — likely stale.
- Old branch metadata.
- Old commit lists.
- Duplicate briefings.
- Solokit config/schema.
- Generic Solokit guides.
- Session history that is already superseded by `brain/Handoffs/`.

### If useful content is found

- Convert it into a small GBrain/project note with a clear "source: legacy Solokit audit" marker.
- Prefer summary + link/path to the old file over copying raw stale content.
- If it is procedural and still useful, convert it into a Hermes skill only after testing the workflow.
- If it is a task, create a fresh current task in GBrain/Hermes kanban rather than preserving stale Solokit status.

### Archive/delete

- Keep `.session/` read-only during the audit.
- After salvage audit, move it to `.archive/session-solokit/` or leave it clearly marked as legacy.
- Do not let future agents consult `.session/` unless explicitly doing historical archaeology.

### Migration task

- [ ] Run a lightweight audit of `.session/specs/*.md` titles and compare against current `brain/` + current code.
- [ ] Extract only still-relevant missing information into GBrain/project notes.
- [ ] Convert only durable non-obvious learnings into GBrain notes or Hermes skills.
- [ ] Archive `.session/` after the audit.

---

## 8. Claude Memory and Global Skill Setup

### Source artifacts

- `/mnt/c/Users/Chase/.claude/CLAUDE.md`
- `/mnt/c/Users/Chase/.claude/projects/.../memory/MEMORY.md`
- Claude session JSONL history under `/mnt/c/Users/Chase/.claude/projects/...`
- `/mnt/c/Users/Chase/.claude/skills/graphify/SKILL.md` — explicitly drop; do not migrate.

### Action

**Selective migration only.** Do not dump all Claude history into Hermes memory. Use GBrain for searchable historical context and Hermes memory only for stable preferences.

### Port

- Durable user preferences from Claude memory, if not already in Hermes memory.
- A small number of historically important Claude sessions if they explain decisions not captured in `brain/Handoffs/`.

### Drop entirely

- `graphify` global skill. Do not port to Hermes and do not recreate as a GBrain workflow unless a new, concrete need appears later.

### Do not port wholesale

- Debug logs.
- Shell snapshots.
- Paste cache.
- File-history backups.
- Todo JSON for old Claude sessions.
- Raw JSONL sessions unless needed for archival search.

### Migration task

- [ ] Inspect Claude project `memory/MEMORY.md` and compare against Hermes memory.
- [ ] Import only durable facts into Hermes memory or GBrain.
- [ ] Keep raw Claude JSONL as archive unless a specific missing decision/history is needed.

---

## 9. GBrain Features to Adopt

### Adopt early

- `gbrain import brain/` or equivalent source import.
- GBrain query/search for project context.
- GBrain maintain/sync health checks.
- Reports for status/handoff-style outputs.
- Task pages only for fresh current tasks created after legacy Solokit audit, not stale Solokit statuses.
- MCP server connected to Hermes.

### Adopt after validation

- Code indexing (`gbrain sources add <repo> --strategy code`) for codebase lookup.
- Cron/dream-cycle style maintenance.
- Daily task prep/briefing.
- Signal detector / automatic capture, but only after privacy/noise settings are clear.
- Minions/job queue if it proves better than Hermes cron/kanban for durable autonomous work.

### Use carefully

- Automatic entity enrichment: useful for Life OS, less obviously useful for GameFlow unless entities are companies, APIs, sportsbooks, players, teams, and model concepts.
- Personal/life memory mixed with GameFlow: keep separated via profile/source boundaries.
- Automatic writes to canonical project docs: require review until trust is established.

---

## 10. Hermes Features to Use Instead of Old Claude Setup

### Replace old systems with these

| Old setup | Hermes replacement |
|---|---|
| Claude slash commands | Hermes skills + normal prompts; optional custom slash later |
| Claude Task tool personas | `delegate_task` with toolset restrictions |
| Legacy Solokit work/status artifacts | Do not port; audit for salvage, then create fresh GBrain tasks or Hermes kanban items only if still current |
| Legacy Solokit session start/end | Replace with `gameflow-resume` / `gameflow-wrap-up` Hermes skills |
| Claude session search | Hermes `session_search` + GBrain imported handoffs/reports |
| Claude MCP config | Hermes MCP config/profile |
| Claude hooks | Hermes profile/tool selection + read-only MCP + skills/policy |
| Old global `CLAUDE.md` | Hermes profile/personality + skills |

### Keep separate profiles

Recommended profiles:

- `gameflow`: strict project invariants, repo cwd, Supabase/GitHub/Railway/Vercel/RapidAPI MCP, GBrain source for GameFlow.
- `personal`: Life OS, broader GBrain sources, calendar/email/notes if enabled, no GameFlow production invariants.

Do not let GameFlow's high-stakes betting/model invariants bleed into personal assistant behavior.

---

## 11. Proposed Migration Order

### Phase 0: Freeze old systems

- [ ] Stop adding new workflows to `.claude/commands/`.
- [ ] Stop treating `.session/` as active state.
- [ ] Keep old files read-only until audited/archived.
- [ ] Do not port Solokit or graphify.

### Phase 1: Install, initialize, import, and validate GBrain

This phase proves GBrain can index and retrieve GameFlow knowledge. It does **not** delete BrainTree files or rewrite project instructions.

- [x] Read the GBrain setup docs before acting:
  - `AGENTS.md`
  - `INSTALL_FOR_AGENTS.md`
  - `docs/architecture/brains-and-sources.md`
- [x] Verify prerequisites:
  - `git`
  - `node`
  - `bun` or install Bun if missing
  - embedding API key if vector search is desired
- [x] Install GBrain via the upstream-safe path:
  - `git clone https://github.com/garrytan/gbrain.git ~/gbrain`
  - `cd ~/gbrain`
  - `bun install`
  - `bun link`
  - `gbrain --version`
- [x] Initialize the GBrain database:
  - `gbrain init` (defaults to local PGLite; no separate PGLite install or manual DB setup for the first pass)
  - `gbrain doctor --json`
- [x] Decide topology before importing:
  - brain/database: local GameFlow brain first
  - source: GameFlow project docs from `GameFlowData/brain/`
  - Life OS stays separate for now
- [x] Import/index existing BrainTree markdown:
  - import `brain/` explicitly; `gbrain init` alone does not ingest BrainTree
  - keep `brain/` as canonical markdown during transition
- [x] Generate embeddings/indexes:
  - `gbrain embed --stale` if embedding provider is configured
  - if no embedding key, record that retrieval is keyword/basic until embeddings are enabled
  - completed with OpenAI embeddings: 288/288 chunks embedded across 119 pages
- [x] Backfill graph/timeline for imported markdown:
  - `gbrain extract links --source db --dry-run`
  - `gbrain extract links --source db`
  - `gbrain extract timeline --source db`
  - `gbrain stats`
- [x] Run validation queries with citations/source paths:
  - critical GameFlow invariants
  - latest handoff / recommended next work
  - current MLB model status
  - Railway/stats.nba.com constraint
  - Q10 miscalibration rule
- [x] Connect GBrain MCP to Hermes only after local CLI retrieval works:
  - start/test GBrain MCP
  - add/test GBrain MCP in Hermes
  - verify Hermes can query GBrain from the GameFlow profile

#### Phase 1 execution log — May 12, 2026

Status: **Phase 1 / 1.5 complete except MCP wiring**. GBrain local CLI retrieval is functional with sync, frontmatter, graph links, handoff timeline, and embeddings. Next gate is connecting/testing GBrain MCP in Hermes.

Completed:
- Installed Bun 1.3.13 via npm fallback because the standard Bun installer required missing `unzip` and sudo was unavailable.
- Cloned GBrain to `~/gbrain`.
- Ran `bun install` and `bun link`.
- Verified GBrain version: `gbrain 0.33.0`.
- Initialized local PGLite brain at `~/.gbrain/brain.pglite` using direct Bun execution:
  - `DATABASE_URL= GBRAIN_DATABASE_URL= bun ~/gbrain/src/cli.ts init --pglite --json`
- Initial import proved the old BrainTree markdown could load:
  - 119 markdown files
  - 288 chunks
  - 0 errors

Phase 1.5 cleanup completed:
- Created standalone local Git repo:
  - `/home/chase/GameFlowBrain`
  - commit `e57ec7f` — initial GameFlow brain import
  - commit `cf9c0d2` — GBrain frontmatter + graph backfill script
- Re-registered GBrain source `gameflow` to the standalone repo:
  - path: `/home/chase/GameFlowBrain`
  - federated: true
- Ran GBrain frontmatter generation against the standalone repo:
  - 119 files scanned
  - 119 frontmatter blocks generated/written
  - validation clean: 0 frontmatter errors
- Synced via real `gbrain sync --source gameflow`:
  - `last_sync_at`: 2026-05-12T17:08:13.628Z
  - sync freshness now passes
- Added a project-graph backfill script:
  - `/home/chase/GameFlowBrain/scripts/backfill_gameflow_graph.ts`
  - creates manual project-domain links and key timeline entries
- Graph/timeline state after initial backfill:
  - pages: 119
  - chunks: 288
  - links: 102
  - timeline entries: 10
  - embeddings: 0
- Verified graph traversal works, e.g. `operations/critical-invariants` links to calibration decisions, Railway setup, calibration guide, and Kalshi startup.
- Verified timeline retrieval works for `operations/hermes-gbrain-migration-checklist`.
- Added handoff timeline backfill script:
  - `/home/chase/GameFlowBrain/scripts/backfill_handoff_timeline.ts`
  - dry-run parses 57 `Handoffs/handoff-*.md` files, extracting date, summary/what-was-done fallback, and recommended next context
  - committed to GameFlowBrain as `9f5e377`
  - inserted 57 structured timeline entries on `handoffs/handoffs`
- Graph/timeline state after handoff timeline backfill:
  - pages: 119
  - chunks: 288
  - links: 102
  - timeline entries: 67
  - embeddings: 0

Important discovery:
- Bun auto-loads repo `.env`, and GameFlowData `.env` contains `DATABASE_URL`. If GBrain is run from the GameFlowData repo without clearing env vars, it routes to GameFlow Postgres/Supabase instead of local PGLite. Use:
  - `DATABASE_URL= GBRAIN_DATABASE_URL= bun ~/gbrain/src/cli.ts <command>`
- Running the standalone brain from `/home/chase/GameFlowBrain` avoids the GameFlowData `.env` collision, but the explicit env clearing is still safest for scripted commands.

Current doctor status:
- `gbrain doctor --json`: warnings, health score 90 after embeddings
- Clean/healthy:
  - connection: 119 pages
  - schema version: latest
  - frontmatter_integrity: clean
  - sync_freshness: all federated sources synced recently
  - embeddings: 100% coverage, 0 missing
- Remaining warnings:
  - PGLite pgvector/jsonb checks warn because they are limited/non-applicable in local PGLite

Embedding status:
- OpenAI API key added to GameFlowData `.env` as `OPENAI_API_KEY`.
- Ran embeddings from `/home/chase/GameFlowBrain` after sourcing GameFlowData `.env`:
  - `DATABASE_URL= GBRAIN_DATABASE_URL= bun ~/gbrain/src/cli.ts embed --stale`
- Embedding result:
  - 288 chunks embedded across 119 pages
  - `gbrain stats`: Embedded = 288
  - `gbrain doctor`: embeddings ok, 100% coverage, 0 missing
- Doctor status after embeddings:
  - status: warnings
  - health score: 90
  - remaining warnings are PGLite-local pgvector/jsonb checks only
- Query validation improved for semantic topics like MLB status and Q10 miscalibration, but broad queries still return ranked snippets rather than synthesized answers; MCP/Hermes should cite/select from these results.

Next checkpoint options:
1. Decide whether to make `/home/chase/GameFlowBrain` remote-backed on GitHub and/or replace `GameFlowData/brain` with a submodule or pointer file.
2. Add a GameFlow-specific GBrain/Hermes skill or convention for project-domain links (`models`, `pipelines`, `invariants`, `runbooks`, `decisions`, `handoffs`) instead of human-only entity assumptions.
3. Connect GBrain MCP to Hermes now that sync, frontmatter, graph, timeline, and embeddings are functional.
4. After MCP connection, validate Hermes can query GBrain from the GameFlow profile and cite source pages correctly.

MCP wiring completed — May 12, 2026:
- Added launch wrapper:
  - `/home/chase/.hermes/scripts/gbrain-mcp-gameflow.sh`
- Wrapper clears `DATABASE_URL` / `GBRAIN_DATABASE_URL`, changes to `/home/chase/GameFlowBrain`, and runs:
  - `bun /home/chase/gbrain/src/cli.ts serve`
- Added Hermes MCP server:
  - `gbrain` via `bash /home/chase/.hermes/scripts/gbrain-mcp-gameflow.sh`
  - 62/62 GBrain tools enabled
- Verification:
  - `hermes mcp list` shows `gbrain` enabled
  - fresh `hermes chat -Q -q ...` successfully used GBrain MCP and answered the global conformal recalibration invariant with citation `AGENTS.md#critical-invariants`
- Note:
  - `hermes mcp test gbrain` timed out in this terminal, but `mcp add` connected successfully and a fresh Hermes chat verified tool use end-to-end.

Next checkpoint options after MCP wiring:
1. Create the minimum Phase 2 Hermes skills: `gameflow-resume`, `gameflow-wrap-up`, `gameflow-sql-runner`, and `gameflow-implementation-worker`.
2. Decide whether to make `/home/chase/GameFlowBrain` remote-backed on GitHub and/or replace `GameFlowData/brain` with a submodule or pointer file.
3. Add a GameFlow-specific GBrain/Hermes convention for project-domain links (`models`, `pipelines`, `invariants`, `runbooks`, `decisions`, `handoffs`).
4. Begin Phase 3 stale Solokit `.session/` salvage audit only after the Phase 2 continuity skills exist.

### Phase 2: Port minimal core workflows to Hermes skills

- [ ] `gameflow-resume`
- [ ] `gameflow-wrap-up`
- [ ] `gameflow-sql-runner`
- [ ] `gameflow-implementation-worker` or `gameflow-opencode-worker`
- [ ] Optional/later from actual usage: `gameflow-status`
- [ ] Optional/later from actual usage: `gameflow-validate`
- [ ] Optional/later from actual usage: `gameflow-check-calibration`
- [ ] Optional/later from actual usage: `gameflow-check-kalshi`

### Phase 3: Audit and archive stale Solokit state

- [ ] Run a lightweight salvage audit of `.session/specs/*.md` titles against current `brain/` and current code.
- [ ] Extract only still-relevant missing information into GBrain/project notes.
- [ ] Convert only durable non-obvious learnings into GBrain notes or Hermes skills.
- [ ] Do not import Solokit work item statuses as current truth.
- [ ] Archive `.session/` after the audit.

### Phase 4: Simplify repo instructions

- [ ] Rewrite `AGENTS.md` to be smaller and sharper.
- [ ] Keep only critical invariants and short runtime rules in always-loaded instructions.
- [ ] Move detailed tutorials into Hermes skills.
- [ ] Preserve SQL isolation and implementation-worker lane selection.
- [ ] Update `brain/Operations/Claude-Commands.md` into a legacy/migration note or replace with this checklist.

### Phase 5: Let GBrain take more ownership

- [ ] Use GBrain as first lookup for project questions.
- [ ] Use GBrain reports for weekly/project status only if the user asks for that workflow.
- [ ] Use GBrain maintain for graph health.
- [ ] Consider code indexing once markdown import is stable.
- [ ] Only then consider reducing manual `brain/` maintenance.

---

## 12. What Not to Migrate

Do not migrate these as active systems:

- Most raw Claude JSONL sessions.
- Claude debug logs.
- Claude paste cache.
- Claude shell snapshots.
- Claude file-history backups.
- Generic legacy Solokit scaffolding (`init`, `adopt`, work-item commands, config/schema) after the salvage audit.
- Old `.flow_library` reference unless it is actually found and contains still-useful templates. Current inspection did not find `/mnt/c/Users/Chase/.claude/.flow_library`.
- Duplicate handoff/session summaries already represented in `brain/Handoffs/`.

---

## 13. Final Target Workflow

### Start session

```text
User: resume GameFlow
Hermes loads gameflow-resume skill
Hermes queries GBrain for latest handoff, active tasks, critical context
Hermes reads only 2-4 canonical files if needed
Hermes presents tight next-step options
```

### Investigate code

```text
Hermes queries GBrain/code index or spawns explorer delegate
Explorer is file-only and narrow
SQL, if needed, goes to sql-runner only
```

### Plan work

```text
Hermes uses GBrain context + writing-plans skill
Plan saved to brain/GBrain report/page
User approves
Large code changes go to the provider-agnostic implementation-worker lane
```

### Implement work

```text
Small edits: Hermes directly
Large/multi-file edits: implementation-worker lane from spec
Hermes reviews scoped git diff only
Hermes runs scoped tests/checks
```

### End session

```text
Hermes loads gameflow-wrap-up
Writes handoff markdown/report
Updates GBrain index
Extracts durable decisions/tasks/blockers
Keeps Hermes memory clean unless there is a stable user preference or reusable workflow
```

## Bottom Line

Migrate **workflow semantics**, not old tooling mechanics.

Keep:

- Critical invariants.
- SQL isolation.
- Explorer/sql-runner roles.
- Provider-agnostic implementation-worker context-protection workflow.
- Resume/wrap-up discipline.
- The valuable markdown brain content.

Replace:

- Manual BrainTree indexing with GBrain search/graph retrieval.
- Legacy Solokit work/status artifacts with a salvage audit, then fresh GBrain tasks or Hermes kanban only if still current.
- Claude commands with Hermes skills.
- Claude hooks with Hermes profile/tool isolation where possible.

Archive:

- `.session/` after salvage audit; extract only still-current specs/learnings.
- Raw Claude logs/history unless needed for a missing decision.
- Generic Claude scaffolding and legacy Solokit artifacts.
