# Remote Hermes Agent Setup

> Part of [[Operations]]

Purpose: set up an always-on Hermes Agent on a VPS/Droplet so GameFlowData can be operated from Discord/phone for small code changes, diagnostics, PR creation, and controlled deployment workflows.

## Target Outcome

A remote Hermes instance can:
- receive Discord commands
- clone and inspect the GameFlowData repo
- create branches/worktrees
- make small code edits
- run tests/lint/checks
- push branches and open PRs
- inspect GitHub/Railway/Vercel status where configured
- report concise summaries back to Discord

It should not initially:
- push directly to `main`
- deploy production without explicit approval
- run Supabase write queries
- modify model calibration logic without review
- touch secrets/API keys except during setup
- rewrite or regenerate model artifacts casually

## Recommended Architecture

Use two Hermes environments:

1. Local Hermes on WSL
   - primary deep-work environment
   - has access to the local working tree, data files, and full dev setup
   - best for large implementation, model/debugging work, and sensitive changes

2. Remote Hermes on VPS
   - always-on operator reachable from Discord
   - works from a clean repo clone on the server
   - creates PRs instead of directly mutating production
   - best for small code changes, log inspection, issue triage, doc fixes, and narrow patches

The remote agent should operate on Git branches/PRs. GitHub is the sync boundary between phone-driven remote work and local development.

## VPS Sizing

Minimum:
- Ubuntu VPS/Droplet
- 2 GB RAM
- 1 vCPU
- 20 GB disk

Preferred:
- 4 GB RAM
- 2 vCPU
- 40+ GB disk

The GameFlowData repo is currently fine for this pattern. It is not too large for a VPS clone. The main caveat is tracked model artifacts; if artifacts grow into multi-GB size, move them to external object storage, Git LFS, or release artifacts.

## Phase 0 — Security Model

Before enabling Discord command execution, decide the permission boundary.

Recommended initial permissions:
- GitHub: repo read/write for creating branches and PRs
- Discord: bot access only to a private channel/server
- Railway/Vercel: read logs first; deploy access only after testing
- Supabase: read-only MCP first, no direct write access
- Shell: normal user account, not root

Recommended hard gates:
- no direct `main` pushes
- no production deploys without explicit approval
- no destructive shell commands without approval
- no Supabase write SQL from the main agent context
- branch protection required on GitHub

## Phase 1 — Server Setup

On the VPS:

```bash
sudo apt update
sudo apt install -y git curl build-essential python3 python3-venv python3-pip nodejs npm tmux
```

Install Hermes:

```bash
curl -fsSL https://raw.githubusercontent.com/NousResearch/hermes-agent/main/scripts/install.sh | bash
hermes doctor
```

Create or select a dedicated profile:

```bash
hermes profile create gameflow-remote
hermes profile use gameflow-remote
```

Configure model/provider:

```bash
hermes setup model
# or
hermes model
```

Configure required env vars using Hermes' env path:

```bash
hermes config env-path
hermes config edit
```

Keep secrets in Hermes `.env`, not in the repo.

## Phase 2 — Clone GameFlowData

Create a stable repo location:

```bash
sudo mkdir -p /srv/repos
sudo chown -R $USER:$USER /srv/repos
cd /srv/repos
git clone <GAMEFLOWDATA_GITHUB_URL> GameFlowData
cd /srv/repos/GameFlowData
```

Install project dependencies according to the repo's current setup. Do not assume the VPS needs every local dependency on day one; start with enough to run lint/tests for small patches.

Configure Git identity:

```bash
git config --global user.name "Hermes Remote Agent"
git config --global user.email "hermes-agent@example.com"
```

Configure GitHub auth:

```bash
gh auth login
# or configure a least-privilege token for GitHub MCP / gh CLI
```

## Phase 3 — Connect Discord Gateway

Run Hermes gateway setup:

```bash
hermes gateway setup
hermes gateway run
```

After validating manually, install/start as a service:

```bash
hermes gateway install
hermes gateway start
hermes gateway status
```

Use a private Discord server/channel for the first iteration.

## Phase 4 — GameFlow Remote Profile Rules

The remote profile should load strict GameFlow context:

- repo working directory: `/srv/repos/GameFlowData`
- project instructions from `AGENTS.md`
- GameFlow invariants from the brain
- SQL isolation rules
- implementation-worker rules for large edits
- PR-first workflow

Suggested default behavior for Discord requests:

1. classify request risk
2. for code changes, create a branch
3. make minimal scoped edits
4. run relevant tests/lint
5. show summary and diff stats
6. push branch and open PR
7. wait for user approval before merge/deploy

## Phase 5 — MCP Connections

Initial MCP/tooling set:
- GitHub: yes
- Railway: read logs first, deploy later
- Vercel: read deployments/previews first, deploy later
- Supabase: read-only only at first

Avoid giving the remote agent broad production power immediately. Add capabilities only after testing them with harmless tasks.

## Phase 6 — First Validation Tasks

Run these in order from Discord:

1. Read-only smoke test
   - "Check the GameFlowData repo status and summarize the latest branch. Do not edit files."

2. Harmless doc PR
   - "Create a branch, add a one-line note to a sandbox markdown file, commit it, push it, and open a PR."

3. Small real doc fix
   - "Fix a typo in a markdown file, run any relevant checks, and open a PR."

4. Small code-only PR
   - "Make a tiny scoped code change, run the narrowest relevant test, and open a PR."

5. Log inspection
   - "Check recent Railway/Vercel logs and summarize errors. Do not deploy or edit files."

Do not enable production deploys until these pass cleanly.

## Operating Rules

Remote Hermes is allowed to do:
- small scoped PRs
- documentation updates
- low-risk bug fixes
- test/lint runs
- log inspection
- issue/PR triage
- branch cleanup with approval

Remote Hermes must ask before:
- deploying production
- merging PRs
- changing scheduled jobs
- changing model calibration/probability logic
- editing secrets or auth config
- running migrations
- running destructive commands

Remote Hermes must not:
- bypass branch protection
- run direct Supabase write SQL from main context
- commit API keys or secrets
- modify production artifacts without an explicit task
- ignore GameFlow critical invariants

## Phone Command Examples

Good Discord commands:

```text
In GameFlowData, inspect the latest CI failure and summarize likely cause. Do not edit files.
```

```text
Create a branch and fix the typo in the dashboard pricing copy. Run lint if available and open a PR.
```

```text
Check Railway logs for the latest MLB job failure. Summarize the error and suggest the smallest safe patch.
```

```text
Patch the failing import from the last Railway error, run the narrow related test, push a branch, and open a PR.
```

Avoid vague/high-risk commands:

```text
Fix production.
Deploy everything.
Clean up the database.
Change the model calibration.
```

## Rollout Recommendation

Start with a conservative remote agent:

1. read-only diagnostics
2. harmless doc PRs
3. small code PRs
4. log inspection
5. preview deploys
6. production deploy approval flow
7. broader automation only after the workflow earns trust

This keeps the always-on assistant useful from a phone while preserving the local WSL setup as the trusted deep-work environment.
