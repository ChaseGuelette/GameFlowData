# Railway Deployment Guide

This guide explains how to deploy GameFlowData cron jobs to Railway.

## Overview

Railway runs 5 scheduled services:

| Service | Schedule (ET) | Schedule (UTC) | Purpose |
|---------|---------------|----------------|---------|
| `daily-stats` | 9:00 AM | 14:00 | Scrape NBA results, update stats |
| `lines-noon` | 12:00 PM | 17:00 | Scrape props/injuries |
| `lines-4pm` | 4:00 PM | 21:00 | Scrape props/injuries |
| `lines-6pm` | 6:00 PM | 23:00 | Scrape props/injuries |
| `inference` | 6:30 PM | 23:30 | Generate predictions |

## Quick Start

### 1. Install Railway CLI

```bash
# macOS/Linux
curl -fsSL https://railway.app/install.sh | sh

# Windows (PowerShell)
iwr https://railway.app/install.ps1 -useb | iex

# Or via npm
npm install -g @railway/cli
```

### 2. Login and Link

```bash
railway login
railway link
```

### 3. Set Environment Variables

In Railway dashboard or CLI:

```bash
railway variables set DATABASE_URL="postgresql://user:pass@host:port/db"
railway variables set ODDS_API_KEY="your-odds-api-key"
railway variables set RAPIDAPI_KEY="your-rapidapi-key"
```

### 4. Deploy

```bash
railway up
```

That's it! Railway will:
- Detect Python from `.python-version`
- Install dependencies from `requirements.txt`
- Create cron services from `railway.toml`

## Configuration Files

| File | Purpose |
|------|---------|
| `railway.toml` | Service definitions and cron schedules |
| `nixpacks.toml` | Build configuration (Python version, system deps) |
| `.python-version` | Python version specification |

## Timezone Notes

Railway cron schedules use **UTC**. The current config assumes **EST** (winter):
- ET + 5 hours = UTC

During **EDT** (daylight saving, Mar-Nov), jobs will run 1 hour earlier ET time.

To adjust for EDT, update `railway.toml`:
```toml
# EDT adjustments (subtract 1 hour from UTC)
cron = "0 13 * * *"  # 9 AM EDT (was 14:00 UTC for EST)
```

## Model Artifacts

Model artifacts use a **production folder** strategy:

```
src/models/artifacts/
├── run_*/              # ← gitignored (local training runs)
└── production/         # ← committed (deployed model)
```

### Promoting a New Model

After training, promote the best model to production:

```bash
# List available runs
python scripts/promote_model.py --list

# Promote latest run
python scripts/promote_model.py

# Promote specific run
python scripts/promote_model.py run_20260210_095220

# Commit and push
git add src/models/artifacts/production/
git commit -m "Promote run_20260210_095220 to production"
git push
```

Railway automatically redeploys when you push, picking up the new model.

## Monitoring

### View Logs

```bash
# All services
railway logs

# Specific service
railway logs --service inference
```

### Dashboard

Visit [railway.app/dashboard](https://railway.app/dashboard) to:
- View cron execution history
- Check service health
- Monitor resource usage
- Trigger manual runs

### Manual Trigger

To run a job manually:

```bash
# Via Railway CLI
railway run python src/orchestration/inference_job.py

# Or trigger from dashboard: Service → Deployments → Run
```

## Cost Estimate

Railway pricing (as of 2024):
- **Hobby Plan**: $5/month includes $5 usage credit
- **Usage**: ~$0.000231/min for compute

Your jobs:
- `daily-stats`: ~5-10 min/day = ~$0.07-0.14/month
- `lines` (3x): ~1.5 min/day total = ~$0.01/month
- `inference`: ~0.5 min/day = ~$0.004/month

**Total estimated: ~$0.10-0.20/month** (well under $5 credit)

## Troubleshooting

### Job Not Running

1. Check cron syntax in `railway.toml`
2. Verify service is deployed: `railway status`
3. Check logs: `railway logs --service <name>`

### Database Connection Errors

1. Verify `DATABASE_URL` is set: `railway variables`
2. Check Supabase allows Railway IPs (or use connection pooling)

### Import Errors

1. Ensure `PYTHONPATH` includes `/app/src`
2. Check `requirements.txt` has all dependencies

### "No module named pip" Error

Railway's Nixpacks builder may fail with `python3.11: No module named pip`:

```
/root/.nix-profile/bin/python3.11: No module named pip
```

**Fix:** Create `nixpacks.toml` with explicit pip installation:

```toml
[phases.setup]
nixPkgs = ["python311", "python311Packages.pip"]

[phases.install]
cmds = [
    "python3.11 -m ensurepip --upgrade",
    "python3.11 -m pip install --upgrade pip",
    "python3.11 -m pip install -r requirements.txt"
]

[start]
cmd = "python3.11 src/orchestration/scheduler.py"
```

This ensures pip is properly installed before attempting to install dependencies.

### Model Not Found

1. Verify `src/models/artifacts/run_*` directories are committed
2. Check `.gitignore` isn't excluding model files

## Integration with Vercel

If you have a dashboard on Vercel:

```
GitHub Repo
    │
    ├──► Railway (cron jobs)
    │       └── Writes to Supabase
    │
    └──► Vercel (dashboard)
            └── Reads from Supabase
```

Both deploy from the same repo. No additional configuration needed.

## Off-Season Management

Disable cron jobs during NBA off-season:

```bash
# In Railway dashboard: Service → Settings → Disable

# Or remove cron from railway.toml and redeploy
```

Re-enable before season starts (typically late October).
