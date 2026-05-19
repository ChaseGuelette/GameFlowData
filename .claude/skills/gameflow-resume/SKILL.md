---
name: gameflow-resume
description: Resume or start a GameFlowData session from Claude Code (Windows). Reads the latest handoff and execution plan from the canonical GameFlowBrain markdown via WSL and produces a compact startup briefing matching Hermes' gameflow-resume skill. Trigger when the user says "resume gameflow", "what were we doing", "pick up from the last handoff", "load current context", or types /gameflow-resume.
version: 1.0.0
author: Claude Code (mirror of Hermes gameflow-resume)
---

# GameFlow Resume (Claude Code, Windows)

## Purpose

Give a GameFlow session the same startup context Hermes' `gameflow-resume` skill produces, but using only the tools available in Claude Code on Windows. GBrain MCP is not available here — the canonical markdown brain at `\\wsl.localhost\Ubuntu\home\chase\GameFlowBrain` (Hermes' documented migration-fallback source) is read directly.

## When to use

User says any of:
- "resume GameFlow" / `/gameflow-resume`
- "what were we doing"
- "pick up from the last handoff"
- "load the current GameFlow context"
- "what should I work on next"

Do not use for broad historical archaeology, stale Solokit `.session/` inspection, or migration-checklist questions unless explicitly asked.

## Canonical source paths

All paths are WSL — access from Claude Code via the `\\wsl.localhost\Ubuntu\...` UNC prefix with `Read`, or via `wsl -d Ubuntu -- bash -lc "..."` for shell-only ops like `ls | sort`.

- Brain root: `\\wsl.localhost\Ubuntu\home\chase\GameFlowBrain`
- Handoffs: `\\wsl.localhost\Ubuntu\home\chase\GameFlowBrain\Handoffs\handoff-NNN.md`
- Execution plan: `\\wsl.localhost\Ubuntu\home\chase\GameFlowBrain\Execution-Plan.md`
- Hard facts: `\\wsl.localhost\Ubuntu\home\chase\GameFlowBrain\Operations\Hard-Facts.md`
- Critical invariants: `\\wsl.localhost\Ubuntu\home\chase\GameFlowBrain\Operations\Critical-Invariants.md`
- Brain index: `\\wsl.localhost\Ubuntu\home\chase\GameFlowBrain\BRAIN-INDEX.md`
- Active plans (this repo): `C:\Users\Chase\Projects\GameFlowData\.hermes\plans\*.md`

If `\\wsl.localhost\Ubuntu\...` paths return errors, fall back to:

```powershell
wsl -d Ubuntu -- bash -lc "cat /home/chase/GameFlowBrain/<path>"
```

## Latest handoff retrieval rule (mirrors Hermes)

Do **not** infer the latest handoff from semantic search or filename guessing. Use the numeric-suffix rule:

```powershell
wsl -d Ubuntu -- bash -lc "ls /home/chase/GameFlowBrain/Handoffs/ | sort -r | head -5"
```

Pick the first entry matching `handoff-\d+\.md`, then read it with the `Read` tool via the `\\wsl.localhost\Ubuntu\...` path. Always cross-check against the immediately preceding handoff when the user asks "what was supposed to happen" or "what DB writes are queued" — separate current-thread actions, deferred/rejected actions, and unrelated roadmap items.

## Procedure

1. **Locate latest handoff** with the `ls | sort -r | head` command above.
2. **Read in parallel** (single message, multiple tool calls):
   - Latest handoff (`Handoffs/handoff-NNN.md`)
   - `Execution-Plan.md`
   - Optionally, the preceding handoff if recency context matters
3. **Check for an active IDE plan**. If the system reminder shows the user has `.hermes/plans/*.md` open, read it — it's almost always the active lane.
4. **Verify on-disk artifacts the handoff references**. If the handoff says sweeps/configs/files exist, confirm:
   ```powershell
   wsl -d Ubuntu -- bash -lc "ls /mnt/c/Users/Chase/Projects/GameFlowData/backtest_results/ | grep <pattern>"
   ```
   Don't claim something is ready to run without checking.
5. **Produce the briefing** using the output template below.
6. **Stop after briefing**. Do not auto-continue into implementation or exploration unless the user explicitly says proceed.

## Retrieval budget

Bounded — this skill is not a brain audit:

- At most 2 handoffs read.
- One execution-plan read.
- One IDE-plan read if the user has one open.
- One on-disk verification per claimed artifact (only the ones in "Recommended Next Steps").
- Do not read `Operations/Hermes-GBrain-Migration-Checklist.md` unless the user asks about migration.
- Do not run `gbrain` CLI commands — they're not installed on this Windows machine, and Hermes' MCP route doesn't apply here.

## Output template

```text
Retrieval trace (when requested)
- Route: latest handoff (timeline) + execution-plan [+ IDE plan if open]
- Sources: <handoff slugs>, Execution-Plan.md [, .hermes/plans/<file>]
- GBrain MCP: unavailable on Windows Claude Code — read canonical markdown via \\wsl.localhost\Ubuntu\...

GameFlow resume
- Latest handoff: <handoff-NNN, date, 1-2 line summary>
- Current state: <active lane in 2-3 lines>
- Recommended next 1-3 actions: <from handoff §Recommended Next Steps, verified against on-disk state>
- Safety notes: <only invariants relevant to next work>
- Read next: <2-5 paths/slugs the user should open before acting>
- Caveats: <git push blocked, sweeps untested, model paused, etc.>
```

## Required safety context

Surface only the ones relevant to the proposed next work — do not dump all of them every time:

- Never deploy global conformal recalibration offsets.
- Never put advanced stats scraping on Railway.
- Railway `daily_stats_job` uses `--cdn-only`; no `stats.nba.com` calls from Railway.
- Never run non-concurrent `CREATE INDEX` on `raw_player_props_combined`.
- Probabilities use empirical CDF `(samples > line).mean()`, never Gaussian CDF.
- Q10 miscalibration is the edge — do not "fix" it.
- Main context must not call Supabase MCP directly; use the SQL-runner pattern (`.claude/agents/sql-runner.md`).
- Explore agents are file-only and narrow; spawn SQL runner separately for DB work.
- Before destructive DB-adjacent actions, verify SQL-runner counts with an independent count query.

## Command style for output

Per Chase's standing preference (PowerShell workflow):

- Default to copy-pasteable single-line PowerShell commands.
- No bash `\` line continuations unless explicitly labeled Bash/WSL.
- No `>>` continuation markers, no pasted shell prompts, no placeholder flags without values.
- When a WSL command is unavoidable, wrap it: `wsl -d Ubuntu -- bash -lc "<single line>"`.

## Common pitfalls (mirror of Hermes)

1. Treating old Solokit `.session/` state as current truth. It is legacy.
2. Reading the whole brain manually. Use the numeric-suffix handoff rule + execution plan first.
3. Letting hub/meta pages (`BRAIN-INDEX.md`, migration checklist) answer domain questions.
4. Trusting "we discussed this in session history" as equivalent to "it's in the brain". Verify against handoff/canonical pages.
5. Declaring a fix complete because the file changed — verify with substantive diff that ignores CRLF noise:
   ```powershell
   git diff --ignore-space-at-eol --ignore-cr-at-eol --name-only -- <paths>
   ```
6. For quote-clean / backtest-leakage resumes, do not infer "backtests are fixed" from CLV diagnostics alone. Verify the harness entrypoint (`src/backtesting/mlb/run_mlb_sweep.py`, `line_selection.py`, `mlb_backtest_harness.py`) and distinguish code-path trust from artifact/data-coverage trust. Old artifacts can remain contaminated.
7. Auto-continuing into implementation after the briefing. Stop and wait for the user.

## Verification checklist

- [ ] Latest handoff picked via numeric-suffix rule, not semantic guessing.
- [ ] Execution-plan read for current phase status.
- [ ] If the user had a `.hermes/plans/*.md` file open in IDE, it was read.
- [ ] Recommended-next artifacts (sweep dirs, configs, scripts) verified on disk.
- [ ] Only invariants relevant to next work were surfaced.
- [ ] No Supabase MCP calls were made from main context.
- [ ] Did not auto-continue into implementation.

## Differences from Hermes' gameflow-resume

| Capability | Hermes (WSL) | This skill (Windows Claude Code) |
|---|---|---|
| GBrain MCP (`list_pages`, `get_page`, `get_health`) | Available | **Not available** — read markdown directly |
| `gbrain` CLI | Available | Not installed; do not invoke |
| Authority map / page-class routing | Full | Simplified — markdown only |
| Latest-handoff rule | MCP `list_pages` sorted desc | `ls Handoffs/ | sort -r | head` |
| Canonical fallback path | `/home/chase/GameFlowBrain` | `\\wsl.localhost\Ubuntu\home\chase\GameFlowBrain` |
| Lessons / Hard-Facts retrieval pass | MCP-driven topic queries | Direct read of `Operations/Hard-Facts.md` + `Operations/Critical-Invariants.md` when modeling/architecture work is the next action |

If GBrain MCP later becomes available in Claude Code, prefer it over markdown for recency and add the authority-map routing back in.
