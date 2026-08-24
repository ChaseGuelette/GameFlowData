# GameFlowData

GameFlowData is a focused sports-modeling and delivery system for NBA and MLB player props.

The repository owns five things:

1. Train and load NBA/MLB models.
2. Backtest and verify those models without temporal leakage.
3. Ingest league, injury, lineup, and sportsbook data.
4. Run daily prediction, paper-trading, resolution, and Discord-alert jobs.
5. Serve the customer dashboard.

Historical experiments, raw backtest output, local knowledge mirrors, and non-production model runs do not belong in this repository.

## Supported product

| Area | Supported scope |
|---|---|
| Sports | NBA and MLB |
| Models | NBA player props; MLB pitcher strikeouts and supported batter props |
| Market data | Sportsbook and DFS lines |
| Delivery | Website and Discord alerts |
| Trading | Sportsbook paper/user bet tracking |

Kalshi, Polymarket, cross-platform arbitrage, NCAAB, Bot Tracker, Arb Scanner, and Data Vault are retired from the active codebase.

## Repository map

```text
src/models/                 NBA training, inference, simulation, and production artifacts
src/models/mlb/             MLB training, lifecycle, feature contracts, and production artifacts
src/backtesting/            NBA and MLB replay, sweep, and verification code
src/scrapers/               Retained NBA/MLB/odds/injury/lineup data ingestion
src/processing/             Retained feature and linking pipelines
src/orchestration/          Daily jobs and scheduler
src/paper_trading/          Sportsbook, DFS, and user-bet tracking/resolution
src/discord_bot/            Shared Discord transport and retained alerts
src/db/                     Python database boundary
configs/mlb/                Configuration-driven MLB lifecycle runs
migrations/                 Active schema contracts; applied history is archived
scripts/                    Model audit, lifecycle, sync, and operations utilities
tests/                      Python regression suite
dashboard/                  Next.js customer application
ops/engineering_os/         Private read-only operations dashboard
docs/                       Small set of active runbooks
```

## Model artifacts

Only deployable production suites remain in Git:

```text
src/models/artifacts/production/
src/models/artifacts/production_playoffs/
src/models/mlb/artifacts/production/
```

Training runs, sweeps, ablations, backups, and rejected models are ignored and stored outside the repository. Never treat Git directory naming as promotion evidence; use the model/lifecycle manifests and verification outputs.

## Common verification

From `C:\Users\Chase\Projects\GameFlowData`:

```text
.\venv\Scripts\python.exe -m pytest
.\venv\Scripts\python.exe scripts\audit_mlb_model_artifacts.py --model-dir src\models\mlb\artifacts\production --json
```

From `dashboard\`:

```text
npm run lint
npm run build
```

Model training, sweeps, broad backfills, and other long jobs are launched manually by Chase after a dry-run/preflight.

## Dashboard

Retained customer surfaces:

- Props dashboard
- DFS
- Combined Performance and History
- Account and subscription management
- Public picks, pricing, and legal pages
- Ask AI, games, scoreboard, slate, and Stripe APIs

`/history` redirects to `/performance`.

## Critical invariants

- Never deploy global conformal recalibration offsets.
- Q10 behavior is edge-bearing; do not blindly recalibrate it.
- Probability from samples uses `(samples > line).mean()`, never a Gaussian approximation.
- Railway advanced-stats collection remains CDN-only; never call `stats.nba.com` from Railway.
- Never run a blocking `CREATE INDEX` on `raw_player_props_combined`.
- Preserve point-in-time feature and quote integrity.
- Main-context agents do not call Supabase directly; use the isolated SQL-runner workflow.

Canonical project knowledge and current decisions live in remote GBrain. `AGENTS.md` and `CLAUDE.md` define the repository safety contract.

## Archive

The pre-reduction source bundle, quarantined outputs, non-production artifacts, and SHA-256 manifest are stored outside the repository at:

```text
C:\Users\Chase\Archives\GameFlowData\2026-08-24-pre-prune\
```

This archive is rollback/evidence storage, not an active source tree.