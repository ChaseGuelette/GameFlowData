# Remote GameFlow Kanban Board Setup and Validation Plan

**Date:** 2026-07-16

**Goal:** Configure a durable GameFlow-specific Hermes Kanban board on the `gameflow-agent` droplet, run it through the gateway-embedded dispatcher, and prove persistence, worker context, failure recovery, isolated coding, dependent review, and backup behavior end to end.

**Scope:** This is separate from the older remote Linux workstation migration. It assumes the droplet/repositories may already exist but independently verifies every prerequisite needed by Kanban.

## Change-control boundaries

- Read-only inventory is allowed immediately.
- Creating the GameFlow board, worker test profiles/cards, isolated worktrees/branches, and a board backup is approved by Chase in this session.
- Do not modify production GameFlow code, production databases, Railway, Supabase, or live trading state.
- Reboot only after service state and persistence configuration are captured.
- Test coding changes must be harmless and isolated; delete/archive test artifacts only after evidence is recorded.
- Never print secrets.

## Acceptance matrix

### A. SSH, reboot, and gateway service

- [x] Tailscale SSH attaches from the current machine.
- [x] Gateway service status and logs are inspectable.
- [x] Gateway is enabled to survive logout/reboot.
- [ ] Droplet reboots and returns reachable. **Not run: reboot approval was denied.**
- [ ] Gateway returns healthy after reboot. **Blocked on the reboot test above.**

### B. Embedded Kanban dispatcher

- [x] `kanban.dispatch_in_gateway` is enabled.
- [x] Dispatcher starts inside the gateway.
- [x] Dispatcher activity/failures are visible in logs and task run history.
- [x] Dispatcher can claim a ready task assigned to a real profile.

### C. Durable GameFlow board and repo binding

- [x] A clearly named GameFlow board exists on droplet-local persistent storage.
- [x] Board path is explicit and recorded.
- [ ] Board survives gateway restart and droplet reboot. **Gateway/process persistence passed; controlled reboot remains unrun.**
- [x] Default task workspace binds to `~/projects/GameFlowData` or an isolated worktree derived from it.
- [x] Main GameFlow checkout remains understandable and unpolluted.

### D. Worker context and access

- [x] Worker can read its task card and comment thread.
- [x] Worker receives Kanban worker guidance/tools.
- [x] Worker sees available Hermes profiles/agents relevant to its task.
- [x] Worker can read the GameFlow repo and `AGENTS.md`.
- [x] Worker can access GameFlow skills.
- [x] Worker can access GBrain MCP, or blocks explicitly if GBrain is unavailable.
- [x] Worker records a structured completion/block handoff.

### E. Detach/attach and loss recovery

- [x] A read-only audit worker continues while the operator detaches/reconnects.
- [x] Worker/run state remains visible after reconnect.
- [x] A deliberately killed worker produces a visible failed/lost run.
- [x] Dispatcher reclaims/retries it, or transitions it to an explicit blocked state after configured failures.
- [x] No task silently remains claimed forever.

### F. Isolated coding task

- [x] Coding card creates or uses a task-specific git worktree.
- [x] Worktree uses a named branch tied to the card.
- [x] Harmless test change is committed only on that branch.
- [x] Main checkout remains unchanged by the test task.
- [x] Worker handoff records branch, worktree, changed files, and validation.

### G. Dependent review

- [x] Reviewer card is created with implementation card as parent.
- [x] Reviewer remains gated until implementation terminal success.
- [x] Reviewer receives parent result/metadata/comments.
- [x] Reviewer records approval or a concrete block/finding.

### H. Board database backup

- [x] Board database path and journal mode are recorded.
- [x] A consistent backup is created without copying a live SQLite file unsafely.
- [x] Backup integrity check passes.
- [x] Restore is smoke-tested to a temporary location without replacing the live board.
- [x] Backup command/helper and retention location are documented.

## Execution order

1. Inventory remote Hermes binary/version, config, profiles, gateway service, Kanban CLI, repository, GBrain, and existing boards.
2. Upgrade/repair Hermes only if required for the documented Kanban feature; capture before/after version.
3. Create explicit GameFlow board and backup location.
4. Configure real worker profiles and gateway-embedded dispatcher against that board.
5. Start/restart gateway and validate dispatcher logs.
6. Run a minimal read-only worker card and verify card/repo/skills/GBrain access.
7. Validate detach/reconnect behavior.
8. Kill a controlled test worker and verify failure visibility plus reclaim/retry/block semantics.
9. Run harmless isolated worktree coding card.
10. Run dependent reviewer card and verify parent result propagation.
11. Create and integrity-check board backup; smoke-test restore to temporary storage.
12. Reboot droplet and rerun SSH/gateway/board/dispatcher checks.
13. Record PASS/FAIL evidence and unresolved blockers in this document.

## Evidence log

### Initial local evidence

- Tailscale SSH from the current machine succeeded on 2026-07-16.
- Remote hostname: `hermesAgentDroplet`.
- Remote user: `chase`.
- Non-interactive shell did not find `hermes` on `PATH`; installation/path inventory is required before interpreting this as an absent installation.

### Final evidence

- Hermes v0.18.2 is installed at `/home/chase/.local/bin/hermes`; the simple non-login SSH shell PATH was the only discovery issue.
- Board `gameflow` uses `/home/chase/.hermes/kanban/boards/gameflow/kanban.db`, SQLite WAL mode, and passed `integrity_check`.
- Default workspace is `/home/chase/projects/GameFlowData`; worker profiles are `gameflow-worker` and `gameflow-reviewer`.
- `hermes mcp test gbrain` passed with 67 tools; GBrain health returned `ok`, version `0.40.3.0`, engine `postgres`.
- Detach survival task `t_de27823d` sent pre/post-detach heartbeats and completed without file changes.
- Controlled-loss task `t_36182cf3` recorded the killed run, was reclaimed automatically, and completed on retry without file changes.
- Worktree task used branch `pilot/kanban-context-20260714` in the existing GameFlowData repository; the canonical `main` checkout remained clean.
- Dependent reviewer received parent branch/commit/files/tests metadata and approved the harmless pilot.
- Backup helper: `/home/chase/bin/backup-gameflow-kanban`; retention: newest 14 files under `/home/chase/.hermes/backups/kanban/gameflow/`.
- Daily persistent timer: `gameflow-kanban-backup.timer`; live-backup and temporary-restore integrity checks both returned `ok`.
- JobScraper still holds the shared Telegram token. The prepared profile cleanup and gateway restarts were denied by the approval gate and did not execute.

## Final result

**Operationally ready for the read-only GameFlow tech-debt evaluation.** All worker, dispatcher, context, GBrain, failure-recovery, worktree, reviewer, and backup gates passed. Remaining certification gaps are the controlled droplet reboot and separating Telegram from the email-only JobScraper profile; neither change was executed after approval was denied.
