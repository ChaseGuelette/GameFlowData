# Phone-Accessible Agent Workstation Migration Plan

> **For Hermes:** This is a plan/runbook, not an implementation task. Do not execute setup changes without Chase approval.

**Goal:** Make Chase's agent stack reachable and usable from phone/laptop while preserving the current GameFlowData + GBrain + Hermes setup.

**Recommendation:** Build the manual Tailscale + SSH + tmux workstation first. Treat Fly Hermes as an optional later control plane, not the primary execution environment.

**Difficulty:** Moderate. Mostly setup/integration, not a hard migration. The main risks are WSL/network/service quirks, credential handling, and accidentally creating two competing Hermes/GBrain authorities.

---

## 0. Current observed state

Checked on 2026-05-18 from `/mnt/c/Users/Chase/Projects/GameFlowData`.

### Machine / OS

- Hostname: `DESKTOP-VPI6O1S`
- Current agent runtime: WSL2 Linux
- Current project path: `/mnt/c/Users/Chase/Projects/GameFlowData`
- Windows repo path: `C:\Users\Chase\Projects\GameFlowData`

### Hermes

- Hermes installed at: `/home/chase/.local/bin/hermes`
- Provider/model currently visible: OpenAI Codex / `gpt-5.5`
- OpenRouter key present
- Gemini key present
- OpenAI Codex OAuth logged in
- Gateway service currently stopped
- Messaging platforms not configured
- Cron jobs: 0

### GBrain / GameFlow

- GBrain GameFlow service active under WSL user systemd:
  - service: `gbrain-gameflow.service`
  - MCP/admin served locally on port `3131`
  - source: `gameflow`
- GameFlowData git remote:
  - `https://github.com/ChaseGuelette/GameFlowData.git`
- Latest GBrain resume state: active lane is MLB dense CLV / quote-clean tooling, but dense CLV snapshot linking is not complete.

### Missing / likely not configured yet

- `tailscale` command not found in WSL.
- Windows Tailscale service not detected from quick check.
- Windows OpenSSH client exists; OpenSSH server service was not detected from quick check.
- `tmux` is installed in WSL.

---

## 1. Target architecture

Use this as the foundation:

```text
Phone / laptop
  -> Tailscale private network
  -> SSH into workstation
  -> tmux persistent sessions
  -> Hermes CLI / Codex / local workers
  -> GameFlowData repo + GBrain + scripts
  -> git push/pull for durable state
```

Optional later:

```text
Fly Hermes
  -> Telegram/Discord lightweight control plane
  -> cron / reminders / monitoring
  -> can notify or dispatch to workstation later
```

Do **not** make Fly Hermes the primary GameFlow execution box. It is better as a coordinator.

---

## 2. Research notes

### Manual stack

Tailscale + SSH + tmux is a mature pattern:

- Tailscale gives private device-to-device networking without exposing SSH to the public internet.
- SSH gives direct shell access from laptop/phone.
- tmux keeps sessions alive when phone/laptop disconnects.
- git keeps code/plans/docs durable across machines and agent sessions.

Community search also shows this pattern around agentic coding hosts: people are building always-on dev hosts and Telegram/Discord bridges, but they generally still need a real execution machine behind the interface.

### Fly Hermes

Found `voyagerseven/hermes-fly`, described as a fast Fly.io deployment path for Hermes Agent.

Observed from repo/config:

- It mounts a persistent Fly volume at `/root/.hermes`.
- It can inject provider and gateway secrets into Hermes `.env`.
- It can start Hermes gateway when Telegram/Discord/Slack/Signal tokens exist.
- Default sample VM is small: `shared-cpu-1x`, `1024mb` RAM.
- Good fit: always-on lightweight web/gateway Hermes.
- Poor fit: heavy local dev, GPU/local LLM work, GameFlow DB/file workflows.

Conclusion: Fly Hermes is useful later, but does not replace the manual workstation for your real work.

---

## 3. Migration hardness estimate

### Overall: Moderate

Expected time:

- Minimum usable phone access: 1-2 hours
- Clean version with scripts and validation: 3-5 hours
- Fly Hermes control plane later: extra 1-3 hours

### Hard parts

1. WSL vs Windows boundary
   - Hermes/GBrain currently live in WSL.
   - Phone SSH can target either Windows or WSL, but WSL networking/service lifetime needs care.

2. Keeping GBrain canonical
   - Do not create a second unsynced GBrain on Fly.
   - GameFlow canonical truth remains local GBrain + `/home/chase/GameFlowBrain`.

3. Secrets and auth
   - Do not copy tokens casually between WSL, Windows, Fly, and phone clients.
   - Prefer controlled setup and one-time auth flows.

4. Service persistence
   - Need WSL systemd/linger or Windows startup behavior so GBrain/Hermes gateway are available after reboot.

5. Phone UX
   - SSH from phone works, but tiny screens are awkward. tmux layout and short commands matter.

---

## 4. Recommended phased build

## Phase 1 — Workstation access foundation

**Objective:** SSH from phone/laptop into the machine over Tailscale.

### 1.1 Install Tailscale on Windows host

Do this on Windows, not only inside WSL.

- Install Tailscale from official installer.
- Log in to the same tailnet on desktop and phone/laptop.
- Give this machine a stable recognizable name, e.g. `gameflow-desktop`.

Validation:

- From phone/laptop Tailscale app, confirm `gameflow-desktop` appears online.
- From another machine, ping or Tailscale ping the desktop.

### 1.2 Decide SSH target: Windows first, WSL second

Recommended initial path:

- Enable Windows OpenSSH Server.
- SSH into Windows over Tailscale.
- From there, enter WSL with `wsl`.

Why: Windows owns the host network and survives better as the entrypoint.

Later improvement:

- Add direct WSL SSH if needed.

Validation:

- From laptop/phone: SSH into Windows host over Tailscale.
- Run `wsl hostname` and confirm it reaches the WSL environment.

### 1.3 Install phone SSH client

Options:

- Termius: easiest polished phone UX.
- Blink Shell: strong iOS terminal.
- Native OpenSSH from laptop: enough for laptop use.

Create host profile:

- Host: Tailscale machine name or 100.x address
- User: Windows username initially
- Auth: SSH key preferred

Validation:

- Connect from phone on cellular, not just Wi-Fi.
- Enter WSL.
- Run `pwd`, `tmux ls`, and `hermes status --all`.

---

## Phase 2 — tmux + Hermes sessions

**Objective:** Start/stop/resume agent sessions safely from phone.

### 2.1 Create standard tmux sessions

Recommended session names:

- `gf-main` — primary GameFlow/Hermes session
- `gf-worker` — coding/local-worker session
- `gf-logs` — service/log watching

Core commands:

```bash
tmux new -s gf-main
```

Detach:

```text
Ctrl-b d
```

Reattach:

```bash
tmux attach -t gf-main
```

List:

```bash
tmux ls
```

### 2.2 Start Hermes inside tmux

From WSL:

```bash
cd /mnt/c/Users/Chase/Projects/GameFlowData
hermes
```

Validation:

- Start Hermes in tmux.
- Disconnect phone/laptop.
- Reconnect.
- `tmux attach -t gf-main` shows the same Hermes session still alive.

---

## Phase 3 — scripts for repeatability

**Objective:** Make phone usage short-command friendly.

Create scripts under repo or home scripts directory. Suggested repo-local scripts:

```text
scripts/agent/start_gameflow_tmux.sh
scripts/agent/check_gameflow_stack.sh
scripts/agent/open_gameflow.sh
scripts/agent/sync_gbrain.sh
```

### 3.1 `start_gameflow_tmux.sh`

Purpose:

- create/attach `gf-main`
- cd to GameFlowData
- start Hermes if not already running

### 3.2 `check_gameflow_stack.sh`

Purpose:

- show hostname, date, cwd
- show tmux sessions
- show Hermes status summary
- show GBrain service status
- show git status short

### 3.3 `open_gameflow.sh`

Purpose:

- cd into repo
- activate any needed environment if applicable
- print short next commands

Validation:

- From phone SSH, run one command and land in the right place.
- No copy-pasting long paths from phone.

---

## Phase 4 — Git / repo state discipline

**Objective:** Make agent work durable between phone/laptop/desktop sessions.

Rules:

- Agents do not rely on chat context as the only memory.
- Plans go in `.hermes/plans/` or GBrain as appropriate.
- Code changes are committed frequently.
- Handoffs go to GBrain for GameFlow canonical state.

Validation:

```bash
git status --short
git remote -v
```

Before mobile-driven work:

```bash
git pull --ff-only
```

After work:

```bash
git status --short
git diff --stat
```

---

## Phase 5 — GBrain preservation

**Objective:** Keep current GameFlow GBrain as the source of truth.

Rules:

- Do not create a separate GameFlow brain on Fly unless explicitly planned.
- Keep WSL GBrain service as canonical for now.
- Keep `GBRAIN_SOURCE=gameflow` convention.
- Keep GameFlow app DB env vars separate from GBrain commands.

Validation:

```bash
systemctl --user status gbrain-gameflow.service --no-pager
```

Optional health check via Hermes/GBrain tools during resume/wrap-up:

- page count stable
- embed coverage 100%
- missing embeddings 0
- orphan pages tracked as graph hygiene, not data loss

---

## Phase 6 — Optional Hermes gateway on workstation

**Objective:** Message Hermes directly from Telegram/Discord without Fly yet.

This is optional after SSH/tmux works.

Steps:

1. Run interactive gateway setup:

```bash
hermes gateway setup
```

2. Configure only one platform first, probably Telegram.
3. Install/start gateway service:

```bash
hermes gateway install
hermes gateway start
hermes gateway status
```

Validation:

- Send a message from phone.
- Confirm Hermes can respond.
- Confirm gateway survives SSH disconnect.

Risk:

- Gateway on WSL may depend on WSL staying alive after Windows sleep/reboot.
- If flaky, move gateway to Fly later.

---

## Phase 7 — Optional Fly Hermes control plane

**Objective:** Add always-on lightweight cloud Hermes only after local workstation is stable.

Use Fly Hermes for:

- Telegram/Discord always-on reachability
- cron/reminders
- summaries
- monitoring
- dispatching instructions

Do not use Fly Hermes for:

- primary GameFlow coding
- local model work
- GBrain canonical writes
- DB-heavy workflows

High-level steps:

1. Create/confirm Fly.io account.
2. Install `flyctl` locally.
3. Deploy Hermes Fly from a reviewed repo/template.
4. Attach persistent volume.
5. Set provider secrets with `fly secrets`.
6. Set gateway token secrets.
7. Configure strict allowed users/channels.
8. Validate with a harmless message.

Important design choice:

- Fly Hermes should have its own Hermes profile/state.
- It should not silently duplicate GameFlow GBrain.
- If it needs to touch GameFlow, prefer GitHub/API-level tasks or SSH dispatch back to the workstation.

---

## 5. Security checklist

- Use SSH keys, not passwords, for phone/laptop login.
- Keep SSH reachable only over Tailscale if possible.
- Do not open public port 22 on router.
- Use Tailscale ACLs if adding more devices/users.
- Do not paste API tokens into chat.
- Do not copy `.env` wholesale to Fly.
- Use Fly secrets for Fly-only secrets.
- Restrict Telegram/Discord allowed users.
- Keep GameFlow DB credentials local unless there is a specific approved need.

---

## 6. Validation checklist

Minimum success:

- [ ] Phone can connect to desktop over Tailscale on cellular.
- [ ] Phone can SSH into machine.
- [ ] Phone can enter WSL.
- [ ] `tmux` session survives disconnect/reconnect.
- [ ] Hermes starts inside tmux from GameFlowData repo.
- [ ] GBrain service still active after setup.
- [ ] Git status and pull/push work.
- [ ] No public SSH exposure required.

Clean success:

- [ ] One phone-friendly command attaches to GameFlow agent session.
- [ ] One phone-friendly command prints stack health.
- [ ] Gateway works locally or decision is made to defer to Fly.
- [ ] Setup steps documented in repo.
- [ ] No duplicate GBrain authority created.

Fly success, if added later:

- [ ] Fly Hermes responds via Telegram/Discord.
- [ ] Fly volume persists Hermes state across restart.
- [ ] Allowed users/channels enforced.
- [ ] Fly is treated as control plane, not GameFlow execution source.

---

## 7. Final recommendation

Build order:

1. Manual Tailscale + SSH + tmux workstation.
2. Phone-friendly scripts.
3. Git/GBrain workflow hardening.
4. Optional local Hermes gateway.
5. Optional Fly Hermes coordinator.

This is not a scary migration. It is a controlled remote-access and persistence upgrade around your existing setup. The key is to avoid turning it into a full platform migration. Keep GameFlowData and GBrain where they already work; make them reachable and durable first.
