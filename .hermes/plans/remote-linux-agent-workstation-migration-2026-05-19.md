# Remote Linux Agent Workstation Migration Plan

> **For Hermes:** This is an infrastructure migration/runbook, not permission to execute. Do not purchase servers, copy secrets, open network ports, or mutate remote databases without Chase approval.

**Goal:** Move the Hermes Agent + GBrain + GameFlowData execution environment onto an always-on remote Linux VPS so Chase can operate agents from phone/laptop, spawn independent coding agents, and keep GameFlow project memory current from anywhere.

**Recommendation:** Use a real Linux VPS as the execution workstation. Add Tailscale + SSH + tmux as the foundation. Use Telegram/Discord gateway after the server is stable. Do not use Fly/Railway/Render as the primary coding box; they are better as later control-plane/app-hosting layers.

**Target stack:** Ubuntu 24.04 LTS VPS, Tailscale, SSH, tmux, git/GitHub, Hermes Agent, GameFlowData repo, GameFlowBrain/GBrain Postgres service, helper scripts, optional Telegram/Discord gateway.

---

## 1. Decision: VPS provider

### Recommended provider

Use **Hetzner Cloud CX32** if cost matters most.

- Approx: ~€6.80/mo, verify at signup.
- Shape: 4 vCPU, 8 GB RAM, 80 GB SSD.
- Why: best price/performance for an always-on non-GPU Linux agent workstation.
- Region: pick closest US region if available, likely Ashburn VA or Hillsboro OR.
- Caveat: support/account review can be stricter than DigitalOcean; less handholding.

### Minimum viable

**Hetzner CX22**:

- Approx: ~€3.79/mo.
- Shape: 2 vCPU, 4 GB RAM, 40 GB SSD.
- Works for light Hermes + GBrain + repo tasks, but 4 GB RAM can feel tight with Postgres, embeddings, Node/Bun, Python envs, and multiple agents.

### Easier but more expensive

**DigitalOcean Basic Droplet**:

- 4 GB / 2 vCPU / 80 GB: about $24/mo.
- 8 GB / 4 vCPU / 160 GB: about $48/mo.
- Best docs/UI/support/dev-experience.

### US-first middle ground

**Vultr** or **Linode/Akamai**:

- 4 GB plan often around $20-24/mo.
- 8 GB plan often around $40-48/mo.
- Solid if Hetzner account/support friction is annoying.

### Not recommended as primary execution box

- **Fly.io:** good for lightweight Hermes gateway/control plane; not ideal for root SSH/tmux coding workstation.
- **Railway/Render:** good for app hosting; not a full Linux dev server.

### Final pick

Start with **Hetzner CX32, Ubuntu 24.04 LTS, 8 GB RAM**.

If setup is annoying or account approval fails, use **DigitalOcean 4 GB or 8 GB**.

---

## 2. Target architecture

```text
Phone / laptop
  -> Tailscale tailnet
  -> SSH into remote Linux VPS
  -> tmux persistent sessions
  -> Hermes CLI and independent worker Hermes sessions
  -> GameFlowData repo + GameFlowBrain/GBrain
  -> git/GitHub as durable code/doc state
  -> optional Telegram/Discord gateway
```

Canonical layers:

- **Execution layer:** remote Linux VPS.
- **Access layer:** Tailscale + SSH + Termius/Blink/OpenSSH.
- **Persistence layer:** tmux, systemd services, git commits, GBrain Postgres.
- **Project memory:** GameFlowBrain/GBrain + `.hermes/plans/` + repo docs.
- **Agent memory:** Hermes skills/memory/profiles, kept compact.
- **Mobile UX:** SSH first, gateway later.

---

## 3. Current local state to preserve

Checked 2026-05-19.

### GameFlowData

- Current local repo: `C:\Users\Chase\Projects\GameFlowData` / `/mnt/c/Users/Chase/Projects/GameFlowData`
- Git remote: `https://github.com/ChaseGuelette/GameFlowData.git`

### GameFlowBrain / GBrain

- Current local brain repo: `/home/chase/GameFlowBrain`
- Git remote: `https://github.com/ChaseGuelette/gbrain-private.git`
- Current GBrain health:
  - page_count: 206
  - embed_coverage: 100%
  - missing_embeddings: 0
  - stale_pages: 85
  - orphan_pages: 5
  - latest handoff: `handoffs/handoff-079`
- Note: stale/orphan warnings are currently known hygiene signals, not proof of missing embeddings.

### Critical migration rule

Do **not** create two competing GameFlow brains. During migration, one brain is canonical at a time. The remote GBrain becomes canonical only after validation passes.

---

## 4. Phase 0 — preflight before buying/building

### 0.1 Choose server plan

Decision:

- Preferred: Hetzner CX32, Ubuntu 24.04 LTS.
- Minimum: Hetzner CX22.
- Fallback: DigitalOcean 4 GB/8 GB Ubuntu 24.04 LTS.

### 0.2 Decide hostname

Suggested server hostname:

```text
gameflow-agent-01
```

Suggested Tailscale name:

```text
gameflow-agent
```

### 0.3 Decide what secrets will be copied

Copy intentionally only after server is secured:

- OpenRouter/OpenAI/Gemini provider keys or OAuth as needed.
- GitHub SSH key or GitHub CLI auth.
- GBrain token/config.
- GameFlow `.env` only if remote server needs those exact workflows.
- Telegram/Discord tokens only when gateway phase starts.

Do not copy raw `.env` files blindly.

---

## 5. Phase 1 — provision and secure VPS

### 1.1 Create VPS

Provider UI choices:

- OS: Ubuntu 24.04 LTS.
- Size: 4 vCPU / 8 GB RAM preferred.
- Region: closest US region.
- SSH key: add your laptop/desktop public key at provisioning if possible.
- Backups/snapshots: enable if cheap enough; otherwise manually snapshot after baseline setup.

### 1.2 First SSH from laptop/desktop

From local machine:

```bash
ssh root@<server_public_ip>
```

Expected: login succeeds with SSH key.

### 1.3 Create non-root user

On server:

```bash
adduser chase
usermod -aG sudo chase
```

Add SSH key for `chase`:

```bash
mkdir -p /home/chase/.ssh
cp /root/.ssh/authorized_keys /home/chase/.ssh/authorized_keys
chown -R chase:chase /home/chase/.ssh
chmod 700 /home/chase/.ssh
chmod 600 /home/chase/.ssh/authorized_keys
```

Validate from local:

```bash
ssh chase@<server_public_ip>
```

### 1.4 Basic server hardening

On server as `chase`:

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y ufw fail2ban unattended-upgrades curl git tmux htop jq ripgrep unzip ca-certificates gnupg lsb-release build-essential python3 python3-venv python3-pip nodejs npm
sudo ufw allow OpenSSH
sudo ufw enable
sudo systemctl enable --now fail2ban
```

After Tailscale is working, optionally restrict SSH to Tailscale only. Do not do that until phone/laptop Tailscale SSH path is verified.

---

## 6. Phase 2 — Tailscale + phone SSH foundation

### 2.1 Install Tailscale on server

On server:

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up --ssh --hostname=gameflow-agent
```

Login through the browser link.

### 2.2 Install Tailscale on phone/laptop

- Install Tailscale app on phone.
- Login to same tailnet.
- Confirm `gameflow-agent` appears online.

### 2.3 Add SSH config on laptop

Local `~/.ssh/config`:

```sshconfig
Host gameflow-agent
  HostName gameflow-agent
  User chase
  IdentityFile ~/.ssh/id_ed25519
  ServerAliveInterval 30
  ServerAliveCountMax 4
```

Validate:

```bash
ssh gameflow-agent
```

### 2.4 Configure Termius/Blink on phone

Host profile:

- Host: `gameflow-agent` or Tailscale 100.x IP.
- User: `chase`.
- Auth: SSH key.
- Port: 22.

Validation:

- Connect on cellular, not only Wi-Fi.
- Run `hostname`, `tmux ls`, `pwd`.

---

## 7. Phase 3 — tmux operating model

### 3.1 Create standard tmux sessions

Session names:

- `gf-main`: primary interactive Hermes session.
- `gf-worker-1`: independent coding worker.
- `gf-worker-2`: optional second worker.
- `gf-logs`: service/log watching.
- `gf-gbrain`: GBrain maintenance/debug shell if needed.

Commands:

```bash
tmux new -s gf-main
```

Detach: `Ctrl-b d`

Reattach:

```bash
tmux attach -t gf-main
```

List sessions:

```bash
tmux ls
```

### 3.2 Validate persistence

- Start tmux session.
- Run a harmless long command: `watch date`.
- Disconnect phone/laptop.
- Reconnect.
- Attach to same session.
- Confirm command still running.

---

## 8. Phase 4 — install Hermes Agent on server

### 4.1 Install Hermes

As `chase`:

```bash
curl -fsSL https://raw.githubusercontent.com/NousResearch/hermes-agent/main/scripts/install.sh | bash
```

Restart shell or source profile if installer asks.

Validate:

```bash
hermes --version
hermes doctor
hermes status --all
```

### 4.2 Configure model/provider

Use interactive setup first:

```bash
hermes setup
hermes model
```

Recommended provider roles:

- Main planning/research: OpenRouter/OpenAI/Anthropic/etc.
- Coding worker lane: Codex/OpenRouter/local model only after baseline is stable.
- Local model on this VPS: not recommended unless using a GPU server; use API models.

### 4.3 Configure tools

Run:

```bash
hermes tools
```

Enable at least:

- terminal
- file
- skills
- memory
- session_search
- delegation
- cronjob
- todo
- web/search if API keys/provider available

Start fresh session after tool changes.

---

## 9. Phase 5 — clone repos

### 5.1 Configure GitHub access

Preferred: SSH key or GitHub CLI.

Option A: SSH key

```bash
ssh-keygen -t ed25519 -C "gameflow-agent-01"
cat ~/.ssh/id_ed25519.pub
```

Add public key to GitHub.

Validate:

```bash
ssh -T git@github.com
```

Option B: GitHub CLI

```bash
sudo apt install -y gh
gh auth login
```

### 5.2 Clone GameFlowData

```bash
mkdir -p ~/projects
cd ~/projects
git clone git@github.com:ChaseGuelette/GameFlowData.git
cd ~/projects/GameFlowData
git status --short
git remote -v
```

If using HTTPS initially:

```bash
git clone https://github.com/ChaseGuelette/GameFlowData.git
```

### 5.3 Clone GameFlowBrain

```bash
cd ~
git clone git@github.com:ChaseGuelette/gbrain-private.git GameFlowBrain
cd ~/GameFlowBrain
git status --short
git remote -v
```

Important: resolve or intentionally carry any existing local uncommitted brain changes before declaring remote canonical. Current local `/home/chase/GameFlowBrain` has at least one modified file: `scripts/gbrain_health_check.sh`.

---

## 10. Phase 6 — install GameFlowData dependencies

Exact dependency commands may need adjustment after checking the repo, but baseline is:

```bash
cd ~/projects/GameFlowData
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

If dashboard work is needed:

```bash
cd ~/projects/GameFlowData/dashboard
npm install
```

Validation:

```bash
cd ~/projects/GameFlowData
source .venv/bin/activate
python -m py_compile src/orchestration/scheduler.py
python -m pytest tests/test_mlb_daily_player_props_scraper.py tests/test_link_mlb_clv_snapshots_safety.py -q
```

Do not run long training/backtest/scrape jobs automatically. Chase prefers to run training/backtest sweeps manually.

---

## 11. Phase 7 — bring up GBrain on remote

### 7.1 Install prerequisites

Likely needed:

```bash
sudo apt install -y postgresql postgresql-contrib
```

Also install Bun if GBrain requires it:

```bash
curl -fsSL https://bun.sh/install | bash
```

Restart shell or source Bun profile.

### 7.2 Install GBrain code/tooling

Use the existing GameFlow GBrain runbook/skill as source of truth. Expected shape:

- GBrain code installed/cloned.
- `~/GameFlowBrain` registered as source `gameflow`.
- Postgres DB similar to current local setup:
  - DB: `gbrain_gameflow`
  - role: `gbrain`
- MCP served on `http://localhost:3131/mcp`.
- token configured from server-local secret file.
- systemd user service: `gbrain-gameflow.service`.

### 7.3 Migrate/index brain

Target validation metrics should match local:

- page_count around 206.
- latest handoff `handoffs/handoff-079` or newer.
- embed coverage 100%.
- missing embeddings 0.
- no dead links.

### 7.4 Create systemd user service

Enable linger so services survive logout:

```bash
sudo loginctl enable-linger chase
```

Then enable GBrain user service once configured:

```bash
systemctl --user daemon-reload
systemctl --user enable --now gbrain-gameflow.service
systemctl --user status gbrain-gameflow.service --no-pager
```

### 7.5 Validate GBrain MCP locally

From server:

```bash
curl -s http://localhost:3131/health || true
```

From Hermes:

```bash
hermes mcp test gbrain
```

Inside fresh Hermes session, ask:

```text
Use GBrain MCP. Report page_count and latest handoff slug only.
```

Expected:

```text
page_count=206 latest_handoff=handoffs/handoff-079
```

---

## 12. Phase 8 — migrate Hermes state and configure MCP/tools/skills/profile

### 8.0 What to migrate from local Hermes

Do not copy the entire local Hermes home; it includes install artifacts, logs, caches, and possibly large session history. Copy selected state only.

Copy from the currently working local Hermes profile:

- `config.yaml`
- `.env`
- `auth.json` if present
- `skills/`
- `memories/`
- `profiles/` if present
- `mcp-tokens/` if present and still valid

Do not copy:

- `hermes-agent/` install directory
- `logs/`
- `audio_cache/`
- large `sessions/` unless explicitly wanted
- stale project fallback brains

Recommended local source for this migration is WSL `~/.hermes` because it currently has the GameFlow skills and working GBrain conventions. Windows native config can be consulted for provider/toolset fixes, but avoid path-specific Windows config values on Linux.

From local WSL, after remote SSH works, back up remote Hermes and sync selected state:

```bash
ssh chase@gameflow-agent 'ts=$(date +%Y%m%d_%H%M%S); [ -d ~/.hermes ] && cp -a ~/.hermes ~/.hermes.backup.$ts || true; mkdir -p ~/.hermes'
rsync -av --delete ~/.hermes/skills/ chase@gameflow-agent:~/.hermes/skills/
rsync -av --delete ~/.hermes/memories/ chase@gameflow-agent:~/.hermes/memories/
rsync -av ~/.hermes/config.yaml ~/.hermes/.env chase@gameflow-agent:~/.hermes/
[ -f ~/.hermes/auth.json ] && rsync -av ~/.hermes/auth.json chase@gameflow-agent:~/.hermes/ || true
[ -d ~/.hermes/profiles ] && rsync -av ~/.hermes/profiles/ chase@gameflow-agent:~/.hermes/profiles/ || true
[ -d ~/.hermes/mcp-tokens ] && rsync -av ~/.hermes/mcp-tokens/ chase@gameflow-agent:~/.hermes/mcp-tokens/ || true
ssh chase@gameflow-agent 'chmod 600 ~/.hermes/.env ~/.hermes/auth.json 2>/dev/null || true'
```

After copying, edit remote config for Linux paths and remote GBrain:

```bash
ssh chase@gameflow-agent
hermes config path
hermes config edit
```

Remove or fix Windows/WSL-only paths, then validate:

```bash
hermes config check
hermes status --all
hermes skills list | grep gameflow
```

Project `.env` files are separate from Hermes `.env`. Copy them only after deciding the remote server needs those workflows:

```bash
[ -f /mnt/c/Users/Chase/Projects/GameFlowData/.env ] && rsync -av /mnt/c/Users/Chase/Projects/GameFlowData/.env chase@gameflow-agent:~/projects/GameFlowData/.env || true
[ -f /mnt/c/Users/Chase/Projects/GameFlowData/.env.local ] && rsync -av /mnt/c/Users/Chase/Projects/GameFlowData/.env.local chase@gameflow-agent:~/projects/GameFlowData/.env.local || true
```

Do not print secrets. Do not paste `.env` contents into chat.

### 8.1 Add GBrain MCP to Hermes

Expected config shape in `~/.hermes/config.yaml`:

```yaml
mcp_servers:
  gbrain:
    enabled: true
    url: http://localhost:3131/mcp
    headers:
      Authorization: Bearer <server-local-gbrain-token>
    timeout: 30
    connect_timeout: 10
    tools:
      include:
        - get_page
        - list_pages
        - search
        - query
        - resolve_slugs
        - get_stats
        - get_health
        - run_doctor
        - get_links
        - get_backlinks
        - traverse_graph
```

Do not paste real token into docs/chat.

### 8.2 Copy/install key Hermes skills

Options:

- Copy selected skills from local Hermes home.
- Or install from skills repo if published.

Required GameFlow skills:

- `gameflow-resume`
- `gameflow-wrap-up`
- `gameflow-explore`
- `gameflow-sql-runner`
- `gameflow-implementation-worker`
- `gameflow-odds-data-pipelines`

Validate:

```bash
hermes skills list | grep gameflow
```

### 8.3 Configure delegation

Set remote Hermes delegation to a stable API model first. Local model workers can come later.

Expected shape:

```yaml
delegation:
  provider: openai-codex
  model: gpt-5.3-codex-spark
  inherit_mcp_toolsets: true
  max_concurrent_children: 3
```

Validate in Hermes with a small delegated file-only/code-free task.

### 8.4 Project context

Ensure `~/projects/GameFlowData/AGENTS.md` exists and contains GameFlow safety rules. If not, copy from current repo.

Start Hermes from repo root:

```bash
cd ~/projects/GameFlowData
hermes
```

Run:

```text
gameflow resume
```

Expected: uses GBrain MCP and latest handoff, not stale fallback files.

---

## 13. Phase 9 — helper scripts

Create scripts under one of:

- `~/bin/`
- `~/projects/GameFlowData/scripts/agent/`

Recommended `~/bin` commands for phone use:

### 9.1 `gf`

Attach/create main session:

```bash
#!/usr/bin/env bash
set -euo pipefail
SESSION=gf-main
REPO="$HOME/projects/GameFlowData"
if tmux has-session -t "$SESSION" 2>/dev/null; then
  exec tmux attach -t "$SESSION"
fi
exec tmux new -s "$SESSION" -c "$REPO" "hermes"
```

### 9.2 `gf-check`

Print stack health:

```bash
#!/usr/bin/env bash
set -euo pipefail
printf 'host: '; hostname
printf 'date: '; date
printf '\n== tmux ==\n'; tmux ls || true
printf '\n== git GameFlowData ==\n'; git -C "$HOME/projects/GameFlowData" status --short || true
printf '\n== git GameFlowBrain ==\n'; git -C "$HOME/GameFlowBrain" status --short || true
printf '\n== gbrain service ==\n'; systemctl --user --no-pager --lines=5 status gbrain-gameflow.service || true
printf '\n== hermes ==\n'; hermes status --all || true
```

### 9.3 `gf-worker`

Spawn independent worker:

```bash
#!/usr/bin/env bash
set -euo pipefail
N="${1:-1}"
SESSION="gf-worker-$N"
REPO="$HOME/projects/GameFlowData"
if tmux has-session -t "$SESSION" 2>/dev/null; then
  exec tmux attach -t "$SESSION"
fi
exec tmux new -s "$SESSION" -c "$REPO" "hermes"
```

Install scripts:

```bash
mkdir -p ~/bin
chmod +x ~/bin/gf ~/bin/gf-check ~/bin/gf-worker
printf '\nexport PATH="$HOME/bin:$PATH"\n' >> ~/.bashrc
```

Phone workflow:

```bash
ssh gameflow-agent
 gf-check
 gf
 gf-worker 1
```

---

## 14. Phase 10 — Telegram/Discord gateway

Do only after SSH/tmux/Hermes/GBrain are validated.

### 10.1 Configure one platform first

Prefer Telegram first for personal phone control.

```bash
hermes gateway setup
```

Set allowed users. Do not allow all users.

### 10.2 Install and start service

```bash
hermes gateway install
hermes gateway start
hermes gateway status
```

Validate:

- Send harmless Telegram/Discord message.
- Ask for `gf-check` style status.
- Confirm gateway survives SSH disconnect.
- Confirm gateway logs do not leak secrets.

### 10.3 Gateway role

Gateway is for:

- status checks
- small tasks
- notifications
- reminders/cron
- asking the agent to start or summarize worker sessions

For heavy coding, use SSH/tmux from phone/laptop.

---

## 15. Validation gates

### Gate A — server access

- [ ] Server reachable over public SSH initially.
- [ ] Non-root `chase` user works.
- [ ] Tailscale installed and server online.
- [ ] Phone can SSH over Tailscale on cellular.
- [ ] SSH keys work; password login not needed.

### Gate B — tmux/Hermes

- [ ] `tmux` session survives disconnect/reconnect.
- [ ] `hermes doctor` passes or issues are understood.
- [ ] Hermes starts from `~/projects/GameFlowData`.
- [ ] Basic file/terminal tools work.

### Gate C — repos

- [ ] GameFlowData cloned and git auth works.
- [ ] GameFlowBrain cloned and git auth works.
- [ ] No important local-only uncommitted brain changes were lost.
- [ ] Project dependency smoke tests pass.

### Gate D — GBrain

- [ ] Postgres/GBrain service active.
- [ ] Hermes MCP test for `gbrain` succeeds.
- [ ] GBrain health returns page_count around 206.
- [ ] Latest handoff is `handoffs/handoff-079` or newer.
- [ ] `gameflow resume` uses GBrain MCP, not stale fallback.

### Gate E — worker agents

- [ ] `gf` opens main Hermes session.
- [ ] `gf-worker 1` opens independent worker session.
- [ ] Multiple tmux Hermes sessions can coexist.
- [ ] Git status remains understandable after worker activity.

### Gate F — gateway

- [ ] Telegram/Discord responds only to authorized user.
- [ ] Gateway survives logout.
- [ ] Gateway can report status.
- [ ] Heavy coding remains SSH/tmux-first.

---

## 16. Risks and mitigations

### Risk: VPS underpowered

Mitigation: start with 8 GB RAM. Upgrade if Postgres + Hermes + Node/Python workers feel constrained.

### Risk: duplicate GBrain authority

Mitigation: remote GBrain becomes canonical only after latest handoff/health validation. Until then, local WSL GBrain remains canonical.

### Risk: secrets sprawl

Mitigation: no blind `.env` copy. Use scoped secrets. Use provider/gateway setup flows. Keep GameFlow DB secrets off gateway/Fly unless explicitly needed.

### Risk: mobile UX is clunky

Mitigation: short scripts (`gf`, `gf-check`, `gf-worker`), tmux named sessions, Termius snippets.

### Risk: agent workers conflict in git

Mitigation: one worker per tmux session, frequent git status checks, scoped branches for larger work, no broad `git add .`.

### Risk: remote server cannot run local model/GPU work

Mitigation: use API models for remote VPS. If local GPU worker is needed later, add home machine as a second Tailscale node, not the primary always-on executor.

---

## 17. Build order summary

1. Buy/provision VPS: Hetzner CX32 Ubuntu 24.04 LTS preferred.
2. Create `chase` sudo user and secure SSH.
3. Install Tailscale; verify phone SSH over cellular.
4. Install tmux/git/dev basics.
5. Install Hermes; run setup/model/tools.
6. Configure GitHub auth.
7. Clone GameFlowData and GameFlowBrain.
8. Install GameFlowData dependencies and run smoke checks.
9. Install Postgres/GBrain; import/index GameFlowBrain.
10. Configure Hermes GBrain MCP.
11. Validate `gameflow resume` returns latest handoff.
12. Add `gf`, `gf-check`, `gf-worker` scripts.
13. Validate independent Hermes worker sessions.
14. Add Telegram/Discord gateway.
15. Take VPS snapshot after clean baseline.

---

## 18. First implementation session checklist

When Chase says to proceed, do this in order:

1. Confirm provider/plan/region choice.
2. Chase creates VPS and gives SSH access details or runs initial invite flow.
3. Run server bootstrap commands.
4. Stop after Tailscale + phone SSH validation.
5. Continue Hermes/GameFlow/GBrain setup only after access foundation is proven.

Do not rush into copying secrets or migrating GBrain before Gate A and Gate B pass.

---

## 19. Current next steps from the live migration

Current live status as of this note:

- Droplet exists and is reachable.
- `chase` user exists.
- Tailscale works.
- Phone SSH via Termius works.
- tmux works; `gf-main` exists.
- Hermes is installed.
- Hermes config/env/memory/skills were copied.
- GameFlow skills are enabled.
- GameFlowData cloned at `~/projects/GameFlowData`.
- GameFlowBrain cloned at `~/GameFlowBrain`.
- GBrain app source/service is not installed yet.
- Telegram/Discord gateway is not configured yet.

Do the following on the Droplet as `chase`.

### 19.1 Reboot after package/security updates

```bash
sudo reboot
```

Wait 30-60 seconds, then reconnect from any authorized device:

```bash
ssh chase@gameflow-agent
```

### 19.2 Install GBrain prerequisites

```bash
sudo apt update
sudo apt install -y postgresql postgresql-contrib gh
curl -fsSL https://bun.sh/install | bash
source ~/.bashrc
bun --version
psql --version
```

### 19.3 Enable user services after logout

```bash
sudo loginctl enable-linger chase
```

### 19.4 Create local Postgres DB/user for GBrain

```bash
sudo systemctl enable --now postgresql
sudo -u postgres createuser gbrain || true
sudo -u postgres createdb -O gbrain gbrain_gameflow || true
```

### 19.5 Clone/install GBrain application source

This is separate from the markdown brain repo `~/GameFlowBrain`.

```bash
cd ~
git clone https://github.com/garrytan/gbrain.git gbrain
cd ~/gbrain
bun install
bun link
```

### 19.6 Apply GameFlow source-scoped query patch

```bash
cd ~/gbrain
git checkout -b fix/source-scoped-query-retrieval
git apply ~/GameFlowBrain/Operations/setup-handoffs/gbrain-source-scoped-query.patch
bun test test/source-scoped-query.test.ts
```

If `git apply` fails, stop and port the patch behavior manually: `gbrain query --source <source>` must filter keyword/vector retrieval by source.

### 19.7 Initialize GBrain

```bash
cd ~/gbrain
DATABASE_URL= GBRAIN_DATABASE_URL= gbrain init
DATABASE_URL= GBRAIN_DATABASE_URL= gbrain doctor --json
```

### 19.8 Register/sync/embed GameFlowBrain

```bash
DATABASE_URL= GBRAIN_DATABASE_URL= gbrain sources add gameflow --path ~/GameFlowBrain --name GameFlow --federated
DATABASE_URL= GBRAIN_DATABASE_URL= GBRAIN_SOURCE=gameflow gbrain sync --source gameflow --full --no-pull --yes
DATABASE_URL= GBRAIN_DATABASE_URL= GBRAIN_SOURCE=gameflow gbrain embed --stale
DATABASE_URL= GBRAIN_DATABASE_URL= GBRAIN_SOURCE=gameflow gbrain stats
```

Validation target:

- page count roughly matches local current brain, around 206+ pages.
- latest handoff should be `handoffs/handoff-079` or newer once MCP is wired.
- embed coverage should be 100% after embed completes.
- missing embeddings should be 0.

### 19.9 Then stop and verify

Paste only non-secret output/errors. Do not paste env files or tokens.

Next after this section:

1. Create GBrain HTTP wrapper.
2. Create `gbrain-gameflow.service`.
3. Configure Hermes MCP to `http://localhost:3131/mcp`.
4. Validate remote `gameflow resume`.
5. Create `gf`, `gf-check`, `gf-worker` scripts.
6. Configure Telegram/Discord gateway.

---

## 20. Access from any machine

Yes, this setup is designed to be usable from anywhere, not just the original Windows/WSL machine.

Access model:

```text
Any trusted device
  -> install Tailscale and log into Chase's tailnet
  -> add that device's SSH public key to the Droplet
  -> ssh chase@gameflow-agent
  -> attach tmux / use Hermes gateway
```

For a new laptop/desktop:

```bash
ssh-keygen -t ed25519 -C "device-name"
cat ~/.ssh/id_ed25519.pub
```

Append that public key to the Droplet's `~/.ssh/authorized_keys` using an already-authorized device:

```bash
echo 'PASTE_NEW_PUBLIC_KEY_HERE' | ssh chase@gameflow-agent 'mkdir -p ~/.ssh && cat >> ~/.ssh/authorized_keys && chmod 700 ~/.ssh && chmod 600 ~/.ssh/authorized_keys'
```

Then connect from the new device:

```bash
ssh chase@gameflow-agent
```

For phone:

- Install Tailscale.
- Install Termius/Blink.
- Generate/import an SSH key in the phone SSH app.
- Add the phone public key to Droplet `authorized_keys`.
- Connect to `gameflow-agent` as `chase`.

After Telegram/Discord gateway is configured:

```text
Phone Telegram/Discord -> Hermes gateway on Droplet -> agent works on server
```

SSH/tmux remains the backup/manual cockpit. Gateway is the convenience layer.