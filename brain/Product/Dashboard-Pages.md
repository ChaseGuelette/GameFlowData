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
- MLB feature flags: DFS=false, **Stats Vault=true (Session 27)**, **AskChat=true (Session 35)**, Injuries=false, Prediction Markets=false
- NBA: Prediction Markets page removed (route deleted, `predictionMarkets: false`). Backend Kalshi infra remains.
- MLB stat types: pitcher_strikeouts, batter_hits, batter_rbis, batter_hrr
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
| `/dashboard` | Main predictions — PropCards, FilterTabs, date selector, model picks toggle, live scoreboard. **Session 35**: Collapsed 10+ filter controls to 4 (FilterTabs + All Bets/Model Picks toggle + Date selector + ⚙ Filters popover + Build Slate button). Removed Edge/BL tau selectors; hardcoded 0.03 edge threshold. `FilterPopover` contains State, Books, Game Status, Direction. DFS-only predictions filtered out at query level. |
| `/dfs` | DFS Edge Finder — 3 modes, 6 stats, platform filters, slip type selector |
| `/history` | My Bets + Model History — status/direction filters, date range, per-stat win rates. **Session 34**: Edit (pencil icon) + two-step confirm delete on ALL bet statuses (not just pending). EditBetModal pre-fills all fields; saves via UPDATE + calls `rebuild_user_daily_log`. **Session 39 (Phase 10)**: `All | Real | Paper` toggle added to My Bets header. Paper bets show blue PAPER badge in `BetCard`. |
| `/performance` | 4 tabs: **My Bets** + **Props** + **DFS** + **Record** — bankroll chart, stat breakdown, KPI cards. Record tab (formerly `/track-record`) has MonthlyGrid, ModelMetrics, CsvUpload, ManualBetForm, DailyBreakdown. Source toggle (My Bets / Paper / Combined), edge accuracy buckets, streaks. CSV import (drag-and-drop, preview, batched upsert). Calls `rebuild_user_daily_log` RPC after changes. |
| `/stats` | Data Vault — NBA: player/team/defense/play-type heatmap tables, percentile coloring. MLB (Session 27): `MLBStatsPage` component with Batters/Pitchers tabs, Box/Rates/Consistency categories, L3/L5/L10/L20/SZN window support. Requires migration 023 in Supabase. |
| `/account` | Profile + bankroll settings + community card |
| `/subscribe` | Subscription page (currently redirects to /dashboard) |
| ~~`/prediction-markets`~~ | **Removed** — Kalshi backend infra still exists but standalone UI page deleted. Edge data accessible via bot-tracker. |
| `/arb-scanner` | Admin-only Polymarket-Kalshi arb scanner — 4 summary cards (Total P&L, Win Rate, Active Bets, Detected 24h), sortable paper bets table (Kalshi+Poly price display, status badges), daily P&L log tab (green row tint on profitable days), date range filter. Gated by `useAdmin()` hook. Data from `arb_paper_bets` + `arb_paper_trading_daily_log` (authenticated_read RLS applied Session 33). **Session 33** (Phase 9.6). |

## Analysis Modal
Click any PropCard to open:
- L5 game chart
- **MLB batting table** (Session 35): shows all 6 stats (AB, H, TB, HR, RBI, R) with target stat column highlighted. Was previously only showing target stat.
- Model context insights
- AI Q&A chat (Claude Haiku, 20 questions/day) — **now live for MLB** (Session 35)
- **Quantile / Binary distribution**: Binary models (batter hits/HR — q10=q25=q50=0) now show `P(stat ≥ 1)` and `P(No stat)` probability cards instead of misleading quantile bars (Session 35).
- **Line shopping fix** (Session 35): Deduplication key changed from `bookmaker` → `bookmaker:line` so books with multiple lines (e.g. DraftKings 0.5 AND 2.5) both appear instead of collapsing.
- Line shopping by state
- Kelly sizing recommendation
- "Take Bet" button with confidence stars (1-5)
- **"Paper Trade" button** (Session 39 / Phase 10): alongside Take Bet, auto-stakes Kelly recommendation, no manual stake input, shows "Paper Set!" after click. Logs to `user_bets` with `is_paper_trade=true`. Resolved nightly by `resolve_user_paper_bets.py` (9:30 AM ET).

## Cross-Device Sync
- `useUserBets` — optimistic UI + Supabase backend
- `useUserPreferences` — localStorage cache + Supabase DB

#dashboard #product #frontend
