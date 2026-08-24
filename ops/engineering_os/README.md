# GameFlow Engineering OS MVP 0

Read-only control-plane monitor for GameFlow runtime health. It stores only its own SQLite state, briefs, and health events. It does not import production `src/`, mutate Kanban, write GBrain pages, fetch Git remotes, touch product databases, deploy, or expose a public listener.

## Local Setup

```bash
cd /home/chase/projects/GameFlowData/ops/engineering_os
python3 -m venv /home/chase/.local/share/gameflow-engineering-os/venv
/home/chase/.local/share/gameflow-engineering-os/venv/bin/python -m pip install -e ".[test]"
install -d -m 700 /home/chase/.config/gameflow-engineering-os
install -d -m 700 /home/chase/.local/state/gameflow-engineering-os/logs
install -m 600 config/engineering_os.example.yaml /home/chase/.config/gameflow-engineering-os/engineering_os.yaml
```

Edit `/home/chase/.config/gameflow-engineering-os/engineering_os.yaml` for the approved Tailscale URL if desired. Leave disk and artifact growth thresholds as `null` until approved.

## Validation

```bash
pytest ops/engineering_os/tests -q
python -m compileall -q ops/engineering_os/src
/home/chase/.local/share/gameflow-engineering-os/venv/bin/gfos --help
/home/chase/.local/share/gameflow-engineering-os/venv/bin/gfos --config /home/chase/.config/gameflow-engineering-os/engineering_os.yaml check --json
/home/chase/.local/share/gameflow-engineering-os/venv/bin/gfos --config /home/chase/.config/gameflow-engineering-os/engineering_os.yaml brief --generate
/home/chase/.local/share/gameflow-engineering-os/venv/bin/gfos --config /home/chase/.config/gameflow-engineering-os/engineering_os.yaml brief --stdout
/home/chase/.local/share/gameflow-engineering-os/venv/bin/gfos --config /home/chase/.config/gameflow-engineering-os/engineering_os.yaml collect
/home/chase/.local/share/gameflow-engineering-os/venv/bin/gfos --config /home/chase/.config/gameflow-engineering-os/engineering_os.yaml events
curl --fail http://127.0.0.1:8765/healthz
```

## Systemd User Services

```bash
install -m 644 deploy/gameflow-engineering-os-*.service deploy/gameflow-engineering-os-*.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user start gameflow-engineering-os-web.service
systemctl --user start gameflow-engineering-os-collect.service
systemctl --user enable --now gameflow-engineering-os-collect.timer
systemctl --user status gameflow-engineering-os-web.service
systemctl --user status gameflow-engineering-os-collect.timer
```

Daily timer owner gate: do not enable `gameflow-engineering-os-brief.timer` until Chase approves the local generation time. After approval, edit `OnCalendar`, then run:

```bash
systemctl --user daemon-reload
systemctl --user start gameflow-engineering-os-brief.service
systemctl --user enable --now gameflow-engineering-os-brief.timer
systemctl --user status gameflow-engineering-os-brief.timer
```

## Tailscale Serve

Bind remains loopback: `127.0.0.1:8765`. Expose privately through Tailscale Serve, not Funnel:

```bash
tailscale serve --bg --https=443 http://127.0.0.1:8765
tailscale serve status
```

Rollback the route:

```bash
tailscale serve reset
```

If Tailscale Serve is not enabled for the tailnet yet, use the bounded fallback already validated on `gameflow-agent`: set `web.bind_host` to the host's Tailscale IPv4 address and `web.public_base_url` to `http://gameflow-agent:8765`, then restart only `gameflow-engineering-os-web.service`. This exposes the dashboard on the tailnet interface—not on a public interface—at `http://gameflow-agent:8765` (or the Tailscale IP). Prefer Serve HTTPS once the tailnet owner enables it. Never use Tailscale Funnel for MVP 0.

## Safe Failure Simulation

Use a temporary copy of the config that points one synthetic path, such as `paths.kanban_backups`, at an empty temporary directory owned by the monitor. Run `gfos --config /tmp/gfos-failure.yaml brief --generate`, then restore the normal config and rerun. Do not stop production services or mutate Kanban/GBrain/Git.

## Rollback

```bash
systemctl --user disable --now gameflow-engineering-os-brief.timer || true
systemctl --user disable --now gameflow-engineering-os-collect.timer || true
systemctl --user stop gameflow-engineering-os-web.service gameflow-engineering-os-collect.service gameflow-engineering-os-brief.service || true
journalctl --user -u gameflow-engineering-os-web.service --since today
```

Preserve `/home/chase/.local/state/gameflow-engineering-os` for diagnosis. Remove unit files only after evidence is captured. Hermes gateway, Kanban, GBrain, GameFlowData, and production services are independent.

## Certification Limits

Unit and local smoke tests do not certify seven daily briefs, phone access, host reboot recovery, or tailnet availability. Record those outcomes separately after the approved real-world certification window.
