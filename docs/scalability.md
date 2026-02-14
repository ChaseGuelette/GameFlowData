# Scalability Analysis

Current architecture capacity and scaling path for GameFlowData.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Railway   │────▶│  Supabase   │◀────│   Vercel    │
│  (Scheduler)│     │ (Postgres)  │     │ (Dashboard) │
└─────────────┘     └─────────────┘     └─────────────┘
       │                                       │
       ▼                                       ▼
┌─────────────┐                         ┌─────────────┐
│ External    │                         │    Users    │
│ APIs        │                         └─────────────┘
└─────────────┘
```

## Current Limits

| Component | Limit | Bottleneck |
|-----------|-------|------------|
| Supabase Free | 50 connections, 500 MB | ~50 concurrent users |
| Supabase Pro | 200 connections, 8 GB | ~200 concurrent users |
| Railway | Single worker | Sequential jobs only |
| Odds API | 500 req/mo free | Scraping frequency |
| NBA API | ~1 req/sec | Unofficial, rate limited |
| Vercel Free | 100 GB bandwidth | ~10K daily users |

## Capacity Tiers

| Tier | Concurrent | Monthly Users | Cost |
|------|------------|---------------|------|
| Current | 30-50 | 500-1K | ~$5/mo |
| Starter | 100-200 | 5K | ~$50/mo |
| Growth | 500-1K | 20K | ~$200/mo |

## What Breaks First

1. **Database connections** - Free tier limit (50) exhausted quickly
2. **Query performance** - No read replicas, complex queries slow
3. **API costs** - Commercial Odds API ~$200/mo
4. **Single scheduler** - No parallelization

## Scaling Path

### Quick Wins
- Enable Supabase connection pooling (PgBouncer) - 5x connections
- Supabase Pro tier ($25/mo) - 4x capacity
- Add Redis cache for predictions - 10x read speed
- API response caching headers - 3x throughput

### For 10K+ Users
- Read replicas for dashboard queries
- Queue system (Redis/SQS) for job orchestration
- CDN for static assets
- Rate limiting + authentication on API
- Horizontal scaling for Railway workers

## Cost Projection

| Scale | Supabase | Railway | Odds API | Total |
|-------|----------|---------|----------|-------|
| MVP | Free | $5 | Free | ~$5/mo |
| 1K users | $25 | $10 | $79 | ~$115/mo |
| 10K users | $75 | $25 | $199 | ~$300/mo |
| 50K users | $150 | $50 | $399 | ~$600/mo |
