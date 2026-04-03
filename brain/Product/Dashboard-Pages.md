# Dashboard Pages

> Part of [[Product]]

## Tech Stack
- Next.js 16, TypeScript, Tailwind CSS v4, Supabase Auth, Recharts
- URL: `game-flow-data.vercel.app`
- Route groups: `(public)`, `(auth)`, `(protected)`

## Multi-Sport Architecture (Session 1)
- **SportContext + SportConfig pattern**: React Context holds active sport, centralized config per sport
- Config file: `dashboard/src/lib/sport-config.ts` — table names, stat types, team data, CDN URLs, feature flags, column mappings
- Context: `dashboard/src/contexts/SportContext.tsx` — `useSport()` hook, localStorage persistence
- Sport toggle pill in Navbar (NBA/MLB), nav links conditionally shown per feature flags
- MLB feature flags (all `false` at launch): DFS, Stats Vault, AskChat, Injuries, Prediction Markets
- NBA: Prediction Markets enabled (`predictionMarkets: true`), MLB: disabled until Kalshi MLB data available
- MLB stat types: pitcher_strikeouts, batter_hits, batter_total_bases, batter_home_runs, batter_rbis, batter_runs_scored
- MLB tables: `mlb_daily_predictions`, `mlb_paper_bets`, `mlb_paper_trading_daily_log`

## Public Routes (`(public)/`)
| Route | Purpose |
|-------|---------|
| `/` | Landing page — free beta + Discord CTA |
| `/picks` | Public picks teaser — 3 real picks + blurred |
| `/pricing` | $0/mo beta access card |
| `/terms` | Terms of Service |
| `/privacy` | Privacy Policy |

## Auth Routes (`(auth)/`)
| Route | Purpose |
|-------|---------|
| `/login` | Email/password login |
| `/signup` | Sign up |

## Protected Routes (`(protected)/`)
| Route | Purpose |
|-------|---------|
| `/dashboard` | Main predictions — PropCards, FilterTabs, date selector, edge/BL/sportsbook/direction filters, live scoreboard. DFS-only predictions (PrizePicks, Fliff, etc.) filtered out at query level (Session 21). |
| `/dfs` | DFS Edge Finder — 3 modes, 6 stats, platform filters, slip type selector |
| `/history` | My Bets + Model History — status/direction filters, date range, per-stat win rates |
| `/performance` | My Bets + Props + DFS tabs — bankroll chart, stat breakdown, KPI cards |
| `/stats` | Data Vault — player/team/defense/play-type heatmap tables, percentile coloring |
| `/account` | Profile + bankroll settings + community card |
| `/subscribe` | Subscription page (currently redirects to /dashboard) |
| `/prediction-markets` | Kalshi prediction markets — edge overlay, sortable/filterable table, detail modal with fee breakdown, orderbook, countdown |

## Analysis Modal
Click any PropCard to open:
- L5 game chart
- Model context insights
- AI Q&A chat (Claude Haiku, 20 questions/day)
- Quantile distribution visualization
- Line shopping by state
- Kelly sizing recommendation
- "Take Bet" button with confidence stars (1-5)

## Cross-Device Sync
- `useUserBets` — optimistic UI + Supabase backend
- `useUserPreferences` — localStorage cache + Supabase DB

#dashboard #product #frontend
