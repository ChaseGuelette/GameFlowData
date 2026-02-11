# Dashboard Documentation

## Overview

The GameFlow Dashboard is a Next.js web application for viewing daily NBA player prop predictions and analyzing betting opportunities. It connects to the same Supabase database as the Python backend, providing a user-friendly interface for the prediction data.

## Technology Stack

| Component | Technology |
|-----------|------------|
| Framework | Next.js 16 with App Router |
| Language | TypeScript |
| Styling | Tailwind CSS |
| Database | Supabase (PostgreSQL) |
| Auth | Supabase Auth |
| Charts | Recharts |
| Utilities | clsx, tailwind-merge |

## Directory Structure

```
dashboard/
├── src/
│   ├── app/                    # Next.js App Router pages
│   │   ├── page.tsx            # Main predictions dashboard
│   │   ├── login/page.tsx      # Authentication page
│   │   ├── history/page.tsx    # Bet history view
│   │   ├── performance/page.tsx # Performance metrics
│   │   ├── auth/callback/route.ts # Auth callback for email confirmation
│   │   └── layout.tsx          # Root layout with dark theme
│   ├── components/
│   │   ├── layout/             # Layout components
│   │   │   └── Navbar.tsx      # Navigation with bankroll display
│   │   ├── predictions/        # Prediction display components
│   │   │   ├── FilterTabs.tsx  # Stat type filtering
│   │   │   ├── PropCard.tsx    # Individual prediction card
│   │   │   └── PropGrid.tsx    # Grid layout for cards
│   │   ├── analysis/           # Analysis components
│   │   │   ├── AnalysisModal.tsx    # Detailed analysis modal
│   │   │   ├── Last5Chart.tsx       # Last 5 games chart
│   │   │   └── QuantileSummary.tsx  # Quantile distribution
│   │   ├── history/            # Bet history components
│   │   │   ├── BetCard.tsx     # Individual bet result card
│   │   │   ├── BetList.tsx     # Grid of bet cards
│   │   │   ├── HistoryFilters.tsx  # Status filter tabs
│   │   │   └── HistorySummary.tsx  # Summary stats bar
│   │   ├── performance/        # Performance metric components
│   │   │   ├── KPICard.tsx     # Single metric display
│   │   │   ├── BankrollChart.tsx   # Bankroll over time chart
│   │   │   └── StatBreakdown.tsx   # Per-stat performance table
│   │   └── shared/             # Shared components
│   │       ├── PlayerAvatar.tsx     # NBA headshots
│   │       └── Badge.tsx            # Stat and edge badges
│   ├── lib/
│   │   ├── supabase/           # Supabase client configuration
│   │   │   ├── client.ts       # Browser client
│   │   │   ├── server.ts       # Server client
│   │   │   └── middleware.ts   # Session handling
│   │   └── utils.ts            # Utility functions
│   ├── types/
│   │   └── predictions.ts      # TypeScript interfaces
│   └── middleware.ts           # Auth redirect middleware
├── public/                     # Static assets
├── .env.local                  # Environment variables (not committed)
├── next.config.ts              # Next.js configuration
├── tailwind.config.ts          # Tailwind configuration
├── tsconfig.json               # TypeScript configuration
└── package.json                # Dependencies
```

## Setup

### Prerequisites

- Node.js 18+
- npm or yarn
- Supabase project with existing tables

### Installation

```bash
cd dashboard
npm install
```

### Environment Variables

Create `.env.local` with:

```env
NEXT_PUBLIC_SUPABASE_URL=https://your-project.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=your-anon-key
```

### Development Server

```bash
npm run dev
```

Open http://localhost:3000

### Production Build

```bash
npm run build
npm run start
```

## Database Tables Used

### `daily_predictions`

Main predictions table with quantiles and edges.

**Columns used:**
- `prediction_date` — Date of prediction
- `player_id` — NBA player ID
- `game_id` — NBA game ID
- `stat` — Stat type (pts, reb, ast, threes)
- `q10`, `q25`, `q50`, `q75`, `q90` — Quantile predictions
- `prop_line` — Sportsbook line
- `over_prob`, `under_prob` — Model probabilities
- `over_edge`, `under_edge` — Edge vs market
- `implied_over`, `implied_under` — Market probabilities

### `players`

Player reference data for name enrichment.

**Columns used:**
- `player_id` — NBA player ID
- `full_name` — Display name

### `player_game_stats`

Historical game performance for Last 5 chart in AnalysisModal.

**Columns used:**
- `player_id` — NBA player ID
- `game_id` — NBA game ID
- `game_date` — Date of game
- `pts`, `reb`, `ast`, `fg3m` — Stat values for chart

**RLS Policy:** `Allow public read access on player_game_stats` (added Session 24)

### `raw_player_props_combined`

Sportsbook lines for line shopping in AnalysisModal.

**Columns used:**
- `player_id` — NBA player ID
- `game_id` — NBA game ID
- `market_key` — e.g., `player_points`, `player_rebounds`
- `bookmaker_key` — e.g., `draftkings`, `fanduel`
- `point` — Line value
- `over_price`, `under_price` — Odds for each side

**RLS Policy:** `Allow public read access on raw_player_props_combined` (added Session 24)

### `paper_trading_daily_log`

Paper trading results for bankroll display and performance tracking.

**Columns used:**
- `game_date` — Trading date
- `bankroll_after` — Bankroll after day's trading
- `total_bets` — Number of bets placed
- `bets_won` — Bets that won
- `bets_lost` — Bets that lost
- `bets_push` — Bets that pushed
- `total_staked` — Total amount staked
- `total_pnl` — Profit/loss for the day

### `paper_bets`

Individual bet records for history view.

**Columns used:**
- `id` — Bet identifier
- `game_date` — Date of the game
- `player_id` — NBA player ID
- `player_name` — Player display name
- `stat_type` — Stat type (pts, reb, ast, threes)
- `line` — Prop line value
- `bet_direction` — over or under
- `odds_at_bet` — Odds when bet was placed
- `stake` — Amount staked
- `edge` — Model edge at bet time
- `status` — pending, won, lost, push, cancelled
- `actual_value` — Actual stat value (after game)
- `pnl` — Profit/loss amount

## Components

### Navbar

Displays navigation and current bankroll.

```tsx
<Navbar bankroll={1250.00} />
```

### FilterTabs

Stat type filtering with All/PTS/REB/AST options (THREES removed in Session 22).

```tsx
<FilterTabs
  activeFilter="all"
  onFilterChange={(filter) => setFilter(filter)}
/>
```

### Matchup Filter

Game filter dropdown on the main page (added Session 24).

**Format:** `"LAL vs SAS"` — Teams sorted alphabetically

**Implementation:**
```typescript
const availableMatchups = [...new Set(predictions.map(p => {
  const teams = [p.team_abbrev || 'UNK', p.opponent_abbrev || 'UNK'].sort()
  return `${teams[0]} vs ${teams[1]}`
}))].sort()
```

**Filter logic:** Matches predictions where either team is in the selected matchup.

### PropCard

Individual prediction card with:
- Player avatar
- Stat badge
- Over/under probabilities
- Edge badge (high/medium/low)
- Prop line

```tsx
<PropCard
  prediction={prediction}
  onAnalyze={(pred) => openModal(pred)}
/>
```

### AnalysisModal

Detailed analysis popup with:
- Last 5 games bar chart (from `player_game_stats` table)
- Quantile distribution summary with visual bar
- Sportsbook line shopping with edge calculations
- Kelly bet sizing calculator with bankroll input
- Model probabilities, market implied probabilities, and edge breakdown

```tsx
<AnalysisModal
  prediction={selectedPrediction}
  onClose={() => setSelected(null)}
/>
```

**Features (as of Session 24):**

1. **Line Shopping** — Displays all available bookmaker lines for the prop:
   - Fetches from `raw_player_props_combined` table
   - Calculates actual edge using quantile-based probability estimation
   - For Under bets, higher lines = easier to hit (properly calculated)
   - Lines sorted by edge magnitude with "BEST" indicator
   - Bookmaker names formatted for cleaner display

2. **Kelly Bet Sizing** — Interactive bet sizing calculator:
   - Bankroll input persisted to localStorage
   - Preset Kelly fractions: Full (1.0), Half (0.5), Quarter (0.25), Eighth (0.125)
   - Toggle to switch to custom decimal input
   - Displays recommended bet size based on edge, odds, and Kelly fraction
   - Formula: `f = (p(b+1) - 1) / b` where p = model probability, b = decimal odds - 1

3. **Probability Estimation** — For line shopping edge calculations:
   - Uses 5-point quantile interpolation: (q10, 0.90), (q25, 0.75), (q50, 0.50), (q75, 0.25), (q90, 0.10)
   - Linear interpolation between adjacent points
   - Extrapolation above q90 for Under bets (higher lines = higher Under probability)
   - Capped between 0.90 and 0.99 for lines beyond q90

### PlayerAvatar

NBA player headshot with fallback.

```tsx
<PlayerAvatar
  playerId={201566}
  playerName="LeBron James"
  size="md"
/>
```

**Sizes:** `sm` (40px), `md` (64px), `lg` (96px)

**Headshot URL:** `https://cdn.nba.com/headshots/nba/latest/1040x760/{playerId}.png`

**Fallback:** Inline SVG silhouette (no external file needed)

## Authentication

Uses Supabase Auth with email/password.

### Middleware Protection

All routes except `/login` and static files are protected:

```typescript
// middleware.ts
export const config = {
  matcher: [
    '/((?!_next/static|_next/image|favicon.ico|.*\\.(?:svg|png|jpg|jpeg|gif|webp)$).*)',
  ],
}
```

Unauthenticated users are redirected to `/login`.

### Login Flow

1. User enters email/password
2. `supabase.auth.signInWithPassword()` called
3. On success, redirect to `/`
4. On failure, display error message

## Utilities

### `cn(...inputs)`

Merge Tailwind classes with conflict resolution.

```typescript
cn('px-4 py-2', 'px-6') // → 'py-2 px-6'
```

### `formatEdge(edge)`

Format edge as signed percentage.

```typescript
formatEdge(0.085) // → '+8.5%'
formatEdge(-0.03) // → '-3.0%'
```

### `getEdgeTier(edge)`

Classify edge magnitude.

```typescript
getEdgeTier(0.08) // → 'high' (≥7%)
getEdgeTier(0.05) // → 'medium' (≥5%)
getEdgeTier(0.03) // → 'low' (<5%)
```

### `getHeadshotUrl(playerId)`

Build NBA CDN headshot URL.

```typescript
getHeadshotUrl(201566)
// → 'https://cdn.nba.com/headshots/nba/latest/1040x760/201566.png'
```

## Styling

### Theme

Dark theme with slate color palette:

- Background: `bg-slate-900`
- Cards: `bg-slate-800`
- Borders: `border-slate-700`
- Text: `text-slate-50` (primary), `text-slate-400` (secondary)
- Accent: `blue-500`, `blue-600`

### Stat Colors

| Stat | Background | Text |
|------|------------|------|
| PTS | `bg-blue-500/10` | `text-blue-400` |
| REB | `bg-green-500/10` | `text-green-400` |
| AST | `bg-purple-500/10` | `text-purple-400` |
| THREES | `bg-orange-500/10` | `text-orange-400` |

### Edge Tier Colors

| Tier | Condition | Color |
|------|-----------|-------|
| High | ≥7% | `text-green-400` |
| Medium | ≥5% | `text-yellow-400` |
| Low | <5% | `text-slate-400` |

## Next.js Configuration

### Image Domains

NBA CDN allowed for player headshots:

```typescript
// next.config.ts
const nextConfig: NextConfig = {
  images: {
    remotePatterns: [
      {
        protocol: 'https',
        hostname: 'cdn.nba.com',
        pathname: '/headshots/**',
      },
    ],
  },
}
```

## Common Issues

### Middleware Deprecation Warning

```
⚠ The "middleware" file convention is deprecated.
Please use "proxy" instead.
```

This is a Next.js 16 warning. The middleware still works but should be migrated to the new "proxy" convention in the future.

### Dual Lockfile Warning

```
⚠ We detected multiple lockfiles...
```

Both root and dashboard directories have `package-lock.json`. Can be resolved by adding to `next.config.ts`:

```typescript
turbopack: {
  root: __dirname,
}
```

## Pages

### History Page (`/history`)

Displays bet history with filtering and summary statistics.

**Features:**
- Status filter tabs: All, Won, Lost, Push
- Summary bar with totals: bets, wins, losses, win rate, P&L
- Individual bet cards showing result vs line
- Last 30 days of data

**Components used:**
- `HistoryFilters` — Status filter tabs
- `HistorySummary` — Summary stats bar
- `BetList` → `BetCard` — Grid of bet results

### Performance Page (`/performance`)

Displays overall performance metrics and visualizations.

**Features:**
- KPI cards: Current Bankroll, Total P&L, Overall ROI, Win Rate
- Bankroll over time chart (Recharts AreaChart)
- Performance breakdown by stat type table

**Components used:**
- `KPICard` — Individual metric cards
- `BankrollChart` — Time series chart
- `StatBreakdown` — Per-stat table

### History Components

#### BetCard

Individual bet result display.

```tsx
<BetCard bet={bet} />
```

Shows:
- Player name and avatar
- Stat badge
- Over/Under direction with line
- Actual value
- Result badge (Won/Lost/Push)
- P&L amount

#### HistoryFilters

Status filter tabs.

```tsx
<HistoryFilters
  activeFilter="all"
  onFilterChange={(filter) => setFilter(filter)}
/>
```

Options: All, Won, Lost, Push

#### HistorySummary

Summary statistics bar.

```tsx
<HistorySummary bets={bets} />
```

Shows: Total bets, Wins, Losses, Win Rate, Total P&L

### Performance Components

#### KPICard

Single metric card with optional trend indicator.

```tsx
<KPICard
  label="Total P&L"
  value="+$1,234.56"
  trend="up"
  subValue="from 100 bets"
/>
```

**Props:**
- `label` — Metric name
- `value` — Display value
- `trend` — 'up' | 'down' | 'neutral' (optional, colors the value)
- `subValue` — Additional context (optional)

#### BankrollChart

Bankroll over time visualization.

```tsx
<BankrollChart data={dailyData} />
```

**Features:**
- Recharts AreaChart
- Green gradient when trending up, red when down
- Tooltip with date, bankroll, and daily P&L
- Responsive container

#### StatBreakdown

Per-stat performance table.

```tsx
<StatBreakdown stats={statData} />
```

**Columns:**
- Stat (with colored badge)
- Bets
- W-L
- Win %
- P&L
- ROI

## Future Enhancements

1. **Feature-based insights** — Display why the model likes a prop (e.g., "Team missing key rebounder")
2. **Lock of the Day** — Hero section highlighting top pick
3. **Date range selector** — Allow selecting custom date ranges for history/performance
4. **Vercel deployment** — Production hosting with environment variables
