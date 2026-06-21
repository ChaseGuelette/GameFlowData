# GBrain Weekly Code-Source Refresh Job Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Add a bounded weekly maintenance job on `gameflow-agent` that refreshes GameFlow GBrain code-index sources and reports stale-source/embedding/orphan/doctor issues without turning routine handoff wrap-up into broad code-index maintenance.

**Architecture:** Keep the existing nightly handoff/docs sync as the deterministic daily path. Add a separate weekly systemd timer/service and script focused on code-source freshness for `gameflow-code-mlb-pilot`, `gameflow-code-modeling-core`, and `gameflow-code-mlb-backtests`. The job should be read/refresh/sync/report only; it must not auto-repair source routing, schema drift, or DB metadata without a separate Chase-approved plan.

**Tech Stack:** Remote Linux host `gameflow-agent`, user systemd timers, Bash, `gbrain` CLI via Bun, GameFlowBrain Postgres-backed GBrain, existing mirror directories under `/home/chase/`.

---

## Current Evidence

- Remote canonical brain: `gameflow-agent:/home/chase/GameFlowBrain`.
- Existing daily timer: `gbrain-nightly-sync-audit.timer` at roughly 03:17 UTC.
- Existing script: `/home/chase/bin/gbrain-nightly-sync-audit`.
- Existing daily script already attempts:
  - refresh code mirrors from `/home/chase/projects/GameFlowData`;
  - `gbrain sync --all --no-pull --yes --retry-failed`;
  - `gbrain embed --stale`;
  - graph/timeline/orphan/doctor checks.
- Latest doctor warning still reports code-index source freshness stale for:
  - `gameflow-code-mlb-pilot`
  - `gameflow-code-modeling-core`
  - `gameflow-code-mlb-backtests`
- Important implication: the current nightly `sync --all` path may not update source freshness when a code mirror has no git/file diff. The weekly job must explicitly verify freshness semantics rather than assuming “Already up to date” clears doctor warnings.

## Non-Goals

- Do not reindex the whole GameFlowData repo.
- Do not add new code-index sources.
- Do not auto-run `gbrain dream`, `gbrain autopilot`, broad source repair, or DB-adjacent metadata updates.
- Do not make routine handoff wrap-up depend on code-source freshness.
- Do not auto-pull or mutate GameFlowData production code checkout unless explicitly enabled and safe.

## Desired Behavior

Weekly job should:

1. Acquire its own lock so it cannot overlap with nightly sync or itself.
2. Refresh only known scoped mirror directories from the remote GameFlowData checkout.
3. For each code source, run a source-specific sync that updates freshness even if the mirror has no file changes.
4. Run `gbrain embed --stale`.
5. Run orphan and doctor checks.
6. Emit a compact summary log.
7. Exit nonzero only for actionable failures:
   - source sync failure;
   - missing embeddings;
   - orphan pages;
   - target code sources still reported stale after sync;
   - GBrain CLI/service/database unavailable.
8. Leave broader warnings (subagent model config, cycle freshness) as reported caveats unless they block source freshness.

## Proposed Files

Remote files on `gameflow-agent`:

- Create: `/home/chase/bin/gbrain-weekly-code-source-refresh`
- Create: `/home/chase/.config/systemd/user/gbrain-weekly-code-source-refresh.service`
- Create: `/home/chase/.config/systemd/user/gbrain-weekly-code-source-refresh.timer`

Optional local planning/doc file:

- Existing plan: `C:/Users/Chase/Projects/GameFlowData/.hermes/plans/gbrain-weekly-code-source-refresh.md`

## Proposed Schedule

- Weekly Sunday early AM UTC, offset away from daily job:
  - `OnCalendar=Sun *-*-* 04:17:00`
  - `RandomizedDelaySec=10m`
- Rationale: nightly sync runs around 03:17 UTC; this gives it roughly an hour of separation.

## Script Contract

The script should print sections and write logs to:

- `/home/chase/.gbrain/logs/weekly-code-source-refresh-<timestamp>.log`

It should load only the needed GBrain env:

- `$HOME/.gbrain/gameflow-db.env`
- optionally `$HOME/.hermes/.env` only for API keys if required by embedding

It should set:

```bash
export PATH="$HOME/.bun/bin:$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
export GBRAIN_SOURCE=gameflow
```

It should use a lock:

```bash
LOCK_FILE="$HOME/.gbrain/run/weekly-code-source-refresh.lock"
exec 9>"$LOCK_FILE"
flock -n 9 || { echo "Another weekly code-source refresh is running; exiting."; exit 0; }
```

It should avoid overlap with the nightly job if possible by also checking the nightly lock path:

```bash
if [[ -e "$HOME/.gbrain/run/nightly-sync-audit.lock" ]]; then
  # Try non-blocking lock on a separate fd; if held, exit 0 with a clear message.
fi
```

## Source List

Hard-code the current approved source IDs in one array:

```bash
CODE_SOURCES=(
  gameflow-code-mlb-pilot
  gameflow-code-modeling-core
  gameflow-code-mlb-backtests
)
```

Mirror directories expected:

```text
/home/chase/GameFlowCodePilot
/home/chase/GameFlowCodeModelingCore
/home/chase/GameFlowCodeMLBModelingBacktesting
```

The script should fail clearly if a source ID exists in GBrain config but the corresponding mirror directory is missing.

## Source Freshness Acceptance Gate

After sync, parse doctor JSON or query GBrain metadata to answer this exact question:

> Do the target code sources still appear in the `sync_freshness` failure list?

Acceptance:

- PASS if none of the target `CODE_SOURCES` are named in `sync_freshness` failures.
- WARN but do not fail for unrelated doctor warnings like subagent capability or cycle freshness.
- FAIL if target code sources remain stale.

Implementation note: if `gbrain sync --source <id> --no-pull --yes --retry-failed` prints `Already up to date` but doctor still says the source was last synced 29d ago, investigate and use the smallest available supported freshness refresh mechanism. Do not patch DB metadata directly without a separate plan. Candidate discovery commands:

```bash
gbrain sync --help
gbrain doctor --json
```

Look for official flags such as `--force`, `--rescan`, `--refresh`, or equivalent before considering any workaround.

## Notification Policy

Initial implementation can rely on systemd logs and script log files. Optional later enhancement: create a Hermes cron/no-agent watchdog that SSHes to `gameflow-agent`, checks the latest weekly log summary, and sends a message only on failure.

Do not spam daily success notifications.

## Implementation Tasks

### Task 1: Confirm CLI support for freshness refresh

**Objective:** Determine whether `gbrain sync` has an official force/rescan flag that updates source freshness even with no file changes.

**Files:**
- Read-only remote commands on `gameflow-agent`.

**Commands:**

```bash
ssh chase@gameflow-agent 'export PATH="$HOME/.bun/bin:$PATH"; gbrain sync --help | sed -n "1,220p"'
ssh chase@gameflow-agent 'export PATH="$HOME/.bun/bin:$PATH"; gbrain doctor --json > /tmp/gbrain-doctor-before.json; python3 - <<"PY"
import json
s=open("/tmp/gbrain-doctor-before.json").read(); i=s.find("{"); d=json.loads(s[i:])
for c in d.get("checks", []):
    if c.get("name") == "sync_freshness": print(c)
PY'
```

**Expected:** Identify whether a supported flag exists. If not, document that the first implementation only runs source-specific sync and reports if freshness remains stale.

### Task 2: Write the weekly script

**Objective:** Create `/home/chase/bin/gbrain-weekly-code-source-refresh` with locking, source-specific sync, embedding, orphan check, doctor parsing, and summary output.

**Files:**
- Create remote: `/home/chase/bin/gbrain-weekly-code-source-refresh`

**Required sections:**

```text
== start ==
== refresh code mirrors ==
== sync code sources ==
== embed stale ==
== final orphans ==
== doctor freshness gate ==
== summary ==
```

**Verification:**

```bash
ssh chase@gameflow-agent 'bash -n ~/bin/gbrain-weekly-code-source-refresh && chmod +x ~/bin/gbrain-weekly-code-source-refresh'
```

### Task 3: Add systemd service and timer

**Objective:** Install a weekly user systemd timer that runs the script on Sunday early AM UTC.

**Files:**
- Create remote: `/home/chase/.config/systemd/user/gbrain-weekly-code-source-refresh.service`
- Create remote: `/home/chase/.config/systemd/user/gbrain-weekly-code-source-refresh.timer`

**Service draft:**

```ini
[Unit]
Description=GameFlow GBrain weekly code-source refresh
Wants=gbrain-gameflow.service
After=gbrain-gameflow.service network-online.target

[Service]
Type=oneshot
ExecStart=%h/bin/gbrain-weekly-code-source-refresh
```

**Timer draft:**

```ini
[Unit]
Description=Run GameFlow GBrain weekly code-source refresh

[Timer]
OnCalendar=Sun *-*-* 04:17:00
Persistent=true
RandomizedDelaySec=10m
Unit=gbrain-weekly-code-source-refresh.service

[Install]
WantedBy=timers.target
```

**Verification:**

```bash
ssh chase@gameflow-agent 'systemctl --user daemon-reload && systemctl --user enable --now gbrain-weekly-code-source-refresh.timer && systemctl --user list-timers --all --no-pager | grep gbrain-weekly-code-source-refresh'
```

### Task 4: Run a manual dry/smoke execution

**Objective:** Run the script once manually and inspect whether target source freshness clears or remains a known warning.

**Command:**

```bash
ssh chase@gameflow-agent '~/bin/gbrain-weekly-code-source-refresh'
```

**Expected:**

- Exit 0 if target source freshness passes.
- If it exits nonzero because target freshness remains stale despite source-specific sync, stop and report. Do not patch DB metadata.

### Task 5: Document result in GameFlowBrain

**Objective:** Add a short operations note or update a handoff only if the job is actually installed.

**Files:**
- Prefer update existing operations page if one exists for GBrain maintenance.
- Otherwise create concise operations page under `Operations/` and link it from `Operations/Operations.md`.

**Verification:**

```bash
ssh chase@gameflow-agent 'cd /home/chase/GameFlowBrain && ~/bin/gbrain-nightly-sync-audit'
```

Acceptance gates:

- New/updated page exact readback via `gbrain get`.
- `gbrain orphans --json` reports zero orphans.

## Rollback

Disable timer:

```bash
ssh chase@gameflow-agent 'systemctl --user disable --now gbrain-weekly-code-source-refresh.timer'
```

Remove files only after confirming they are no longer referenced:

```bash
ssh chase@gameflow-agent 'rm -f ~/.config/systemd/user/gbrain-weekly-code-source-refresh.{service,timer} ~/bin/gbrain-weekly-code-source-refresh && systemctl --user daemon-reload'
```

## Acceptance Criteria

- Weekly timer exists and is enabled.
- Manual run completes or fails with a clear stale-source report.
- No broad GameFlowData repo indexing was introduced.
- Handoff wrap-up remains independent of code-source freshness warnings.
- Missing embeddings = 0.
- Orphans = 0.
- Target code sources no longer appear in `sync_freshness` failures, or the remaining stale-source issue is documented as a GBrain CLI/source-freshness semantics bug needing separate repair.
