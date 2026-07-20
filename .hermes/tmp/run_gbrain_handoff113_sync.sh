#!/usr/bin/env bash
set -Eeuo pipefail
export PATH="$HOME/.bun/bin:$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
export GBRAIN_SOURCE=gameflow
set -a
. "$HOME/.gbrain/gameflow-db.env"
set +a
export OPENAI_API_KEY="$(python3 - <<'PY'
from pathlib import Path
p = Path.home() / '.hermes' / '.env'
for raw in p.read_text(encoding='utf-8').splitlines():
    line = raw.strip()
    if not line or line.startswith('#') or not line.startswith('OPENAI_API_KEY='):
        continue
    value = line.split('=', 1)[1].strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'\"', "'"}:
        value = value[1:-1]
    print(value, end='')
    break
else:
    raise SystemExit('OPENAI_API_KEY not found')
PY
)"
: "${OPENAI_API_KEY:?OPENAI_API_KEY missing}"
: "${GBRAIN_DATABASE_URL:?GBRAIN_DATABASE_URL missing}"
cd "$HOME/GameFlowBrain"
gbrain sync --source gameflow --no-pull --yes --retry-failed
gbrain embed --stale
bun scripts/backfill_gameflow_graph.ts
gbrain orphans --json
gbrain doctor --json
gbrain get handoffs/handoff-113
gbrain backlinks handoffs/handoff-113
