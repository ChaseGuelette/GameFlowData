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
│   │   ├── (public)/           # Public routes (no auth required)
│   │   │   ├── page.tsx        # Landing page (free-beta + Discord CTA)
│   │   │   ├── picks/page.tsx  # Public picks teaser (SSR via RPC)
│   │   │   ├── pricing/page.tsx # $0/mo beta access card
│   │   │   ├── terms/page.tsx  # Terms of Service
│   │   │   └── privacy/page.tsx # Privacy Policy
│   │   ├── (auth)/             # Auth routes (redirect if logged in)
│   │   │   ├── login/page.tsx  # Login page
│   │   │   └── signup/page.tsx # Sign-up page
│   │   ├── (protected)/        # Auth-gated routes
│   │   │   ├── dashboard/page.tsx  # Main predictions dashboard
│   │   │   ├── dfs/page.tsx          # DFS Edge Finder (model/market/combined)
│   │   │   ├── history/page.tsx    # Bet history with filters
│   │   │   ├── performance/page.tsx # Performance metrics
│   │   │   ├── account/page.tsx    # Profile + community card
│   │   │   └── subscribe/page.tsx  # Redirects to /dashboard
│   │   ├── api/games/route.ts    # NBA CDN schedule proxy (fallback games)
│   │   ├── auth/callback/route.ts # Auth callback for email confirmation
│   │   └── layout.tsx          # Root layout with dark theme
│   ├── components/
│   │   ├── landing/            # Landing page components
│   │   │   ├── HeroSection.tsx # Hero with sign-up + Discord CTAs
│   │   │   └── FeatureGrid.tsx # Feature cards
│   │   ├── layout/             # Layout components
│   │   │   ├── Navbar.tsx      # Protected nav with bankroll display
│   │   │   ├── PublicNavbar.tsx # Public nav with Picks + Discord links
│   │   │   └── Footer.tsx      # Footer with Discord link
│   │   ├── predictions/        # Prediction display components
│   │   │   ├── BookFilterDropdown.tsx # Multi-select sportsbook checkbox dropdown
│   │   │   ├── FilterTabs.tsx  # Stat type filtering
│   │   │   ├── PlayOfTheDay.tsx# Featured top pick card
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
│   │   ├── dfs/                # DFS Edge Finder components
│   │   │   ├── DfsTable.tsx         # Sortable DFS comparison table
│   │   │   └── DfsFilters.tsx       # Platform, slip type, stat filters
│   │   ├── stats/              # Data Vault heatmap stat table components
│   │   │   ├── HeatmapTable.tsx     # Core table with sorting, percentile coloring
│   │   │   ├── StatTabs.tsx         # Players / Teams / Defense tab bar
│   │   │   ├── CategoryTabs.tsx     # Sub-category pill tabs
│   │   │   ├── WindowToggle.tsx     # L5 / L15 / SZN toggle
│   │   │   └── PositionFilter.tsx   # All / G / W / B position filter
│   │   ├── subscription/       # Subscription components (dormant)
│   │   │   └── PricingCard.tsx # Reusable pricing card for future Stripe
│   │   └── shared/             # Shared components
│   │       ├── PlayerAvatar.tsx     # NBA headshots
│   │       ├── Badge.tsx            # Stat and edge badges
│   │       └── BetSourceFilter.tsx  # Model Picks vs All Bets toggle
│   ├── lib/
│   │   ├── supabase/           # Supabase client configuration
│   │   │   ├── client.ts       # Browser client
│   │   │   ├── server.ts       # Server client
│   │   │   └── middleware.ts   # Session + auth handling (no paywall)
│   │   ├── constants.ts        # DISCORD_URL, TEAM_ABBREV shared map
│   │   ├── dfs-utils.ts        # Quantile interpolation, DFS EV, devigging, market edge (shared)
│   │   ├── insights.ts         # Template-based insight generator
│   │   ├── stats/columns.ts    # Column definitions for Data Vault tables
│   │   ├── subscription.ts     # Subscription utils (dormant)
│   │   └── utils.ts            # Utility functions
│   ├── types/
│   │   ├── predictions.ts      # TypeScript interfaces
│   │   ├── dfs.ts              # DFS line types, slip types, platform constants
│   │   ├── stats.ts            # Data Vault types (ColumnDef, StatRow, SortState, PlayTypeCategory, PlayTypeGrouping)
│   │   └── subscription.ts     # Subscription types (dormant)
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

### `dfs_paper_entries`

DFS multi-leg entry records for DFS performance tab.

**Columns used:**
- `entry_date`, `slip_type`, `status`, `legs_won`, `legs_lost`, `legs_push`, `legs_cancelled`, `payout_multiplier`, `pnl`, `avg_edge`, `stake`

### `dfs_paper_daily_log`

DFS daily aggregates for bankroll chart in DFS performance tab.

**Columns used:**
- `entry_date`, `entries_placed`, `entries_won`, `entries_lost`, `entries_partial`, `total_staked`, `total_pnl`, `cumulative_pnl`, `bankroll_after`

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

### BookFilterDropdown

Multi-select checkbox dropdown for sportsbook filtering (added Session 66). Replaces the old single-select `<select>` dropdown.

```tsx
<BookFilterDropdown
  excludedBooks={excludedBooks}       // Set<string> — empty = all included
  onChange={setExcludedBooks}          // (excluded: Set<string>) => void
  userState={userState}               // filters to state-legal books
/>
```

**Behavior:**
- Button shows "All Books" when all checked, "Books (N)" when some unchecked
- Floating panel with checkboxes for each state-legal sportsbook
- "Select All" / "Clear All" toggle at top
- Closes on outside click or Escape key
- Books filtered by `STATE_SPORTSBOOKS[userState]` from `sportsbook-availability.ts`

**Dashboard state:** `excludedBooks: Set<string>` (empty = no filtering). When non-empty, queries `raw_player_props_combined` with `.in('bookmaker', activeBooks)` to build availability set. State changes clean up stale exclusions.

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

### Date Selector

Date dropdown for viewing predictions from previous dates (added Session 26).

**Features:**
- Fetches available dates via `get_prediction_dates()` RPC function
- Defaults to today if predictions exist, otherwise most recent date
- Shows up to 30 days of historical data

**Implementation:**
```typescript
const [selectedDate, setSelectedDate] = useState<string>(getToday())
const [availableDates, setAvailableDates] = useState<string[]>([])

// Fetched via Supabase RPC
const { data } = await supabase.rpc('get_prediction_dates', { days_back: 30 })
```

### Edge Threshold Filter

Minimum edge filter dropdown (added Session 26).

**Options:**
| Value | Display | Description |
|-------|---------|-------------|
| 0 | Edge: All | Show all predictions with prop lines |
| 0.03 | Edge: ≥3% | Default threshold |
| 0.05 | Edge: ≥5% (Rec) | Recommended based on backtesting |
| 0.07 | Edge: ≥7% | Conservative |
| 0.10 | Edge: ≥10% | Selective |
| 0.15 | Edge: ≥15% | High edge only |
| 0.20 | Edge: ≥20% | Ultra selective |

**Filter logic:** Shows predictions where `max(over_edge, under_edge) >= threshold`

### Black-Litterman Blending Filter

BL probability blending dropdown (added Session 26).

**Options:**
| Value | Display | Description |
|-------|---------|-------------|
| none | BL: Off | Raw model probabilities |
| 0.03 | BL: τ=0.03 | Conservative - trust market more |
| 0.05 | BL: τ=0.05 | Moderate |
| 0.10 | BL: τ=0.10 (Rec) | Recommended - balanced |
| 0.15 | BL: τ=0.15 | Moderate aggressive |
| 0.25 | BL: τ=0.25 | Aggressive - trust model more |

**How BL Blending Works:**

1. **Confidence** = `min(|pred_mean - line| / pred_std / z_max, 1.0)`
   - Measures how far the line is from the model's prediction center
   - z_max = 1.0 (constant)

2. **Blending weight** = `min(tau * confidence, max_weight)`
   - tau controls model influence
   - max_weight = 0.50 (cap)

3. **Posterior probability** = blend in log-odds space:
   ```typescript
   const posteriorLogit = impliedLogit + w * (modelLogit - impliedLogit)
   const posteriorProb = 1 / (1 + Math.exp(-posteriorLogit))
   ```

4. **Blended edge** = `posteriorProb - impliedProb`

**Implementation:**
```typescript
const [edgeThreshold, setEdgeThreshold] = useState<number>(0.03)
const [blTau, setBlTau] = useState<number | null>(null)

// In filtering logic
if (blTau !== null && p.pred_mean && p.pred_std) {
  const confidence = calculateBLConfidence(p.pred_mean, p.pred_std, p.prop_line)
  const blendedOver = blendProbability(p.model_prob_over, p.implied_prob_over, blTau, confidence)
  effectiveOverEdge = blendedOver - p.implied_prob_over
}
```

### Live Betting Toggle (Session 52)

Pill-style toggle that controls visibility of predictions for games that have already started.

| Button | Active Style | Behavior |
|--------|-------------|----------|
| **Pre-Game** (default) | `bg-slate-700` | Hides predictions where `game_time ≤ now()` |
| **+ Live** | `bg-orange-600` | Shows all predictions including live/started games |

**State:** `const [showLive, setShowLive] = useState<boolean>(false)`

**Filter logic:** Applied only when viewing today's date (Session 65 fix). Previously applied to all dates, which hid ALL predictions for past dates since every game had ended:
```typescript
if (selectedDate === getToday()) {
  if (isGameDone(p.game_time)) return false
  if (!showLive && p.game_time) {
    if (new Date(p.game_time) <= new Date()) return false
  }
}
```

**UI Layout (Session 52):**
```
┌──────────┐ ┌──────────┐ ┌────────────────┐ ┌──────────────────┐ ┌──────────┐ ...
│ State  ▼ │ │  Book  ▼ │ │Pre-Game│+ Live │ │All Bets│Model    │ │ Date   ▼ │
└──────────┘ └──────────┘ └────────────────┘ └──────────────────┘ └──────────┘
 State        Sportsbook   Live Toggle        Model Picks         Date
```

### LIVE Tags & Game Times (Session 54)

All game-related components display game times and a live indicator when games have started:

**Components affected:** PropCard, PlayOfTheDay, TonightsGames

**Game time display:**
- Always visible — shows "TBD" when game_time is null/undefined via `formatGameTime()` in `utils.ts`
- Client-side backfill in `dashboard/page.tsx` propagates game_time from predictions that have it to same-game predictions that don't

**LIVE badge:**
- Pulsing red dot + "Live" text (uppercase, 10px bold)
- Appears when `isGameLive(game_time)` returns true (`game_time <= now()`)
- Styling: `bg-red-500/20 text-red-400 border border-red-500/30` with `animate-pulse` dot

**Utility functions (`utils.ts`):**
- `formatGameTime(gameTime)` — Returns time string (e.g., "7:30 PM") or "TBD"
- `isGameLive(gameTime)` — Returns true if game has started

### PropCard

Individual prediction card with:
- Player avatar
- Stat badge
- Over/under probabilities
- Edge badge (high/medium/low)
- Prop line
- Game time and LIVE tag

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

Routes are organized into three groups:
- **Public:** `/`, `/picks`, `/pricing`, `/terms`, `/privacy` — no auth required
- **Auth:** `/login`, `/signup` — redirects to `/dashboard` if already logged in
- **Protected:** Everything else — redirects to `/login` if not authenticated

No subscription/paywall check (free beta). All authenticated users have full access.

```typescript
const PUBLIC_ROUTES = ['/', '/picks', '/pricing', '/terms', '/privacy']
const AUTH_ROUTES = ['/login', '/signup']
```

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

### `calculateBLConfidence(predMean, predStd, line)` (Session 26)

Calculate Black-Litterman confidence from model distribution.

```typescript
calculateBLConfidence(25.5, 5.2, 24.0)
// → 0.288 (z = |25.5 - 24| / 5.2 = 0.288)
```

**Formula:** `min(|predMean - line| / predStd / Z_MAX, 1.0)` where Z_MAX = 1.0

### `blendProbability(modelProb, impliedProb, tau, confidence)` (Session 26)

Blend model and market probabilities in log-odds space.

```typescript
blendProbability(0.65, 0.52, 0.10, 0.50)
// → 0.556 (blended posterior probability)
```

**Formula:**
1. Convert to log-odds
2. `w = min(tau * confidence, 0.50)`
3. `posteriorLogit = impliedLogit + w * (modelLogit - impliedLogit)`
4. Convert back to probability

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
- **Bet Source Filter:** Toggle between "Model Picks" (edge ≥9%) and "All Bets" — defaults to Model Picks
- Status filter tabs: All, Won, Lost, Push
- Summary bar with totals: bets, wins, losses, win rate, P&L (reflects bet source filter)
- Individual bet cards showing result vs line
- Last 30 days of data

**Components used:**
- `BetSourceFilter` — Model Picks vs All Bets toggle
- `HistoryFilters` — Status filter tabs
- `HistorySummary` — Summary stats bar
- `BetList` → `BetCard` — Grid of bet results

### Performance Page (`/performance`)

Displays overall performance metrics and visualizations.

**Features:**
- **Bet Source Filter:** Toggle between "Model Picks" (edge ≥9%) and "All Bets" — defaults to Model Picks
- KPI cards: Current Bankroll, Total P&L, Overall ROI, Win Rate (all recalculated based on bet source)
- Bankroll over time chart (Recharts AreaChart) — simulates model-picks-only progression when filtered
- Performance breakdown by stat type table (filtered by bet source)

**Components used:**
- `BetSourceFilter` — Model Picks vs All Bets toggle
- `KPICard` — Individual metric cards
- `BankrollChart` — Time series chart
- `StatBreakdown` — Per-stat table

### BetSourceFilter Component (Session 31)

Toggle between viewing Model Picks only or all bets.

**File:** `dashboard/src/components/shared/BetSourceFilter.tsx`

**Usage:**
```tsx
<BetSourceFilter
  activeSource={betSource}   // 'model' | 'all'
  onSourceChange={setBetSource}
/>
```

**Options:**
| Value | Label | Description |
|-------|-------|-------------|
| `'model'` | Model Picks | Bets with edge ≥9% (matches production model config) |
| `'all'` | All Bets | All placed bets regardless of edge |

**Default:** `'model'` — Shows actual model performance

**Threshold:**
```typescript
export const MODEL_PICKS_EDGE_THRESHOLD = 0.09  // 9% edge
```

**Implementation in Performance Page:**
```typescript
// Filter bets by source
const filteredBets = useMemo(() => {
  if (betSource === 'model') {
    return allBets.filter(b => b.edge >= MODEL_PICKS_EDGE_THRESHOLD)
  }
  return allBets
}, [allBets, betSource])

// Recalculate KPIs from filtered bets
const { totalPnl, totalWins, totalLosses } = useMemo(() => {
  // ... computed from filteredBets
}, [filteredBets])

// Simulate model-picks-only bankroll progression
const chartData = useMemo(() => {
  if (betSource === 'all') return dailyData
  // Group filtered bets by date, calculate cumulative P&L
  // ...
}, [dailyData, filteredBets, betSource])
```

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

### Play of the Day (Session 28)

Featured hero card highlighting the model's highest-edge pick.

**Location:** Rendered above PropGrid on the main page

**Features:**
- Trophy badge header with amber/gold visual treatment
- Large player avatar (96x96), player name, team matchup, game time
- Stat badge + bet direction/line display
- Star rating visualization (1-5 based on edge magnitude)
- Edge badge and model probability display
- "Analyze Pick" button opens analysis modal
- Responsive layout (stacked mobile, horizontal desktop)

**Filter Integration:**
- Respects all active filters (date, edge threshold, BL blending, stat type, matchup)
- Uses `sortedPredictions[0]` — already filtered and sorted by max edge
- Hidden when no predictions available or during loading

**Usage:**
```tsx
<PlayOfTheDay
  prediction={sortedPredictions[0]}
  onAnalyze={(p) => setSelectedPrediction(p)}
/>
```

**Styling:**
```
┌────────────────────────────────────────────────────────────────────┐
│  🏆 PLAY OF THE DAY                                                 │
├────────────────────────────────────────────────────────────────────┤
│  [Avatar]  Player Name           [Stat]           Over/Under XX.X  │
│            Team vs Opponent      ★★★★★           +12.3% Edge       │
│            Game Time                              62.1% Model Prob │
│                                                  [Analyze Pick]    │
└────────────────────────────────────────────────────────────────────┘
```

**Colors:**
- Border: `border-2 border-amber-400/50`
- Background: `bg-gradient-to-r from-amber-950/30 to-slate-800`
- Trophy and stars: `text-amber-400`
- CTA button: `bg-amber-600 hover:bg-amber-500`

## Model Context Insights (Session 29)

The Analysis Modal displays template-based insights explaining **why** the model made its prediction. Insights are generated from feature values stored in `daily_predictions` table.

### Insight Generator

**File:** `dashboard/src/lib/insights.ts`

**Functions:**
- `generateInsights(prediction)` — Returns array of `Insight` objects
- `getInsightSummary(insights)` — Returns summary text like "2 favorable factors"

### Insight Categories

| Category | Feature Source | Examples |
|----------|----------------|----------|
| rest | `feat_rest_days`, `feat_is_back_to_back`, `feat_games_last_7d` | "Playing on a back-to-back", "Well-rested (3 days off)" |
| injury | `feat_team_out_count`, `feat_opp_out_count`, `feat_player_is_questionable` | "2 teammates out (45 combined MPG)", "Listed as Questionable" |
| trend | `feat_stat_l3_l15_ratio` | "Hot streak: L3 avg 15% above L15", "Cold stretch: L3 avg 12% below L15" |
| consistency | `feat_stat_std_l5` | "Very consistent (low variance)", "High variance game-to-game" |
| average | `feat_player_avg_stat_l5`, `prop_line` | "L5 avg (24.2) is 3.2 above line" |

### Context-Aware Sentiments

Insight sentiments depend on the bet direction (determined by comparing `over_edge` vs `under_edge`):

| Insight | Over Bet Sentiment | Under Bet Sentiment |
|---------|-------------------|---------------------|
| L5 avg above line | ✓ positive (green) | ⚠ negative (red) |
| L5 avg below line | ⚠ negative (red) | ✓ positive (green) |
| Hot streak (L3 > L15) | ✓ positive (green) | ⚠ negative (red) |
| Cold streak (L3 < L15) | ⚠ negative (red) | ✓ positive (green) |

### Feature Columns

Stored in `daily_predictions` table:

```
-- B2: Rest/Schedule
feat_rest_days INTEGER
feat_is_back_to_back BOOLEAN
feat_games_last_7d INTEGER

-- B1: Injury Context
feat_team_out_count INTEGER
feat_team_out_min_sum REAL
feat_opp_out_count INTEGER
feat_player_is_questionable BOOLEAN
feat_player_is_probable BOOLEAN

-- B3: Stat-specific Trends (populated per stat)
feat_player_avg_stat_l3 REAL
feat_player_avg_stat_l5 REAL
feat_player_avg_stat_l15 REAL
feat_stat_l3_l15_ratio REAL
feat_stat_std_l5 REAL

-- Opponent Context
feat_opp_abbrev VARCHAR(3)
```

### Usage in AnalysisModal

```tsx
import { generateInsights } from '@/lib/insights'

const insights = generateInsights(prediction)

{insights.length > 0 && (
  <div className="p-6 border-b border-slate-700">
    <h3 className="text-lg font-semibold text-slate-50 mb-3">Model Context</h3>
    <div className="space-y-2">
      {insights.map((insight, i) => (
        <div key={i} className={`flex items-center gap-2 text-sm ${
          insight.sentiment === 'positive' ? 'text-green-400' :
          insight.sentiment === 'negative' ? 'text-red-400' : 'text-slate-300'
        }`}>
          <span className="w-4 text-center">
            {insight.sentiment === 'positive' ? '✓' : insight.sentiment === 'negative' ? '⚠' : '•'}
          </span>
          <span>{insight.text}</span>
        </div>
      ))}
    </div>
  </div>
)}
```

## Vercel Deployment (Session 29)

Dashboard is deployed to Vercel at `game-flow-data.vercel.app`.

### Configuration

| Setting | Value |
|---------|-------|
| Root Directory | `dashboard` |
| Framework | Next.js (auto-detected) |
| Build Command | `npm run build` |
| Output Directory | `.next` |

### Environment Variables

Set in Vercel Dashboard → Project Settings → Environment Variables:

| Variable | Description |
|----------|-------------|
| `NEXT_PUBLIC_SUPABASE_URL` | Supabase project URL |
| `NEXT_PUBLIC_SUPABASE_ANON_KEY` | Supabase anonymous key |

### Vercel MCP Integration

Add Vercel MCP to Claude Code for deployment management:

```bash
claude mcp add --transport http vercel https://mcp.vercel.com
```

Then authenticate with `/mcp` in Claude Code.

### Deployment Process

1. Push code to GitHub
2. Vercel automatically builds and deploys
3. Preview deployments created for PRs
4. Production deployment on merge to main

## Public Picks Page (Session 35)

The `/picks` page is a public, shareable teaser designed to drive signups from social media.

**Route:** `dashboard/src/app/(public)/picks/page.tsx` (Server Component, SSR)

**Data Source:** Calls `get_public_picks(3)` RPC function which returns top 3 recommended picks for the current date, ordered by highest BL edge. Accessible by anon users (no auth required).

**Layout:**
- 3 real pick cards showing player name, stat, line, edge, and team matchup
- 6 blurred skeleton cards with overlay containing "Sign Up Free" and "Join Discord" CTAs
- Fallback message when no picks are available (e.g., before games are scheduled)

**Usage:** Share `/picks` link on Twitter/X, Discord, etc. to drive traffic.

## Shared Constants (Session 35)

**File:** `dashboard/src/lib/constants.ts`

- `DISCORD_URL` — Placeholder Discord invite link (update when server is live)
- `TEAM_ABBREV` — NBA team ID to abbreviation map, used by dashboard page and picks page

Imported by: HeroSection, PublicNavbar, Footer, landing page, pricing page, account page, picks page, dashboard page.

## Data Vault Page (`/stats`) — Session 39

Dense heatmap stat table for exploring player, team, and defense-vs-position rolling averages.

### Data Sources (Database Views)

Three Supabase views provide pre-joined, latest-row data:

| View | Rows | Source Tables |
|------|------|---------------|
| `player_stats_latest` | ~529 | `player_average_game_stats` + `player_average_advanced_stats` + `players` + `player_position_history` |
| `team_stats_latest` | 30 | `team_average_game_stats` + `teams` |
| `defense_by_position_latest` | 90 | `team_allowed_by_position` + `teams` (G/W/B positions only) |

All views use `DISTINCT ON` to get the latest row per entity, filtered to current season.

### Architecture

- All 3 views fetched in parallel on mount via `Promise.all`
- Data cached in React state — all filtering/sorting is client-side
- Column definitions use `{window}` placeholder (e.g., `avg_pts_{window}`) replaced at render time with `l5`/`l15`/`szn`

### Tabs & Categories

| Main Tab | Sub-categories | Filters |
|----------|---------------|---------|
| Players | Box Score, Shooting, Advanced, Consistency | Position, Team, Search, Min GP |
| Teams | Offense, Defense, Overall | — |
| Defense vs Position | Totals, Per 100 Poss | Position (G/W/B) |
| Play Types | Frequency, Efficiency | Offense/Defense toggle |

### HeatmapTable Component

**Percentile coloring** (5-step gradient):
| Percentile | Style |
|-----------|-------|
| < 0.25 | No highlight (slate-800 base) |
| 0.25–0.50 | `bg-blue-900/20` (subtle) |
| 0.50–0.75 | `bg-blue-800/40` (moderate) |
| 0.75–0.90 | `bg-blue-700/50 text-blue-100` (strong) |
| 0.90–1.00 | `bg-blue-600/60 text-white font-medium` (elite) |

For `invertHeatmap: true` columns (TOV, DRtg, PF), the scale is flipped.

**Value formatting:**
| Format | Example | Notes |
|--------|---------|-------|
| `int` | `5` | `Math.round(v)` |
| `dec1` | `24.3` | `v.toFixed(1)` |
| `dec2` | `1.85` | `v.toFixed(2)` |
| `pct1` | `45.6%` | `(v * 100).toFixed(1) + "%"` — DB stores decimals |
| `rawPct1` | `5.9%` | `v.toFixed(1) + "%"` — DB stores pre-multiplied values |
| `plusMinus1` | `+3.2` | Sign prefix + `v.toFixed(1)` |

**Table features:**
- Sticky name column (`sticky left-0 z-10`)
- Sticky header row (`sticky top-0 z-20`)
- Sortable column headers (click to toggle asc/desc)
- `max-h-[calc(100vh-280px)] overflow-auto` for scroll

### Components

| Component | File | Purpose |
|-----------|------|---------|
| `StatTabs` | `components/stats/StatTabs.tsx` | Players / Teams / Defense tab bar |
| `CategoryTabs` | `components/stats/CategoryTabs.tsx` | Generic sub-category pill tabs |
| `WindowToggle` | `components/stats/WindowToggle.tsx` | L5 / L15 / SZN toggle |
| `PositionFilter` | `components/stats/PositionFilter.tsx` | All / G / W / B position filter |
| `HeatmapTable` | `components/stats/HeatmapTable.tsx` | Core table with sorting + percentile coloring |
| `OffDefToggle` | `components/stats/OffDefToggle.tsx` | Offense / Defense toggle for play types |
| `HeatmapLegend` | `components/stats/HeatmapTable.tsx` | Percentile color legend (5-step gradient) |

### Column Definitions

Defined in `lib/stats/columns.ts`. Each `ColumnDef` has:
- `key` — Unique identifier
- `label` — Display header
- `dbColumn` — Template like `avg_pts_{window}` (resolved per window)
- `format` — Value display format
- `invertHeatmap` — Flip percentile scale (for negative stats)
- `windowless` — Don't swap window (for Consistency tab columns)

## State Selector (Session 47)

Dropdown filter that restricts AnalysisModal sportsbook lines to only show bookmakers legal in the user's US state.

### Data Model

**File:** `dashboard/src/lib/sportsbook-availability.ts`

- `US_STATES` — Array of `{ value, label }` for ~26 legal sports betting states plus "All States" (empty string)
- `STATE_SPORTSBOOKS` — Map of state abbreviation → allowed bookmaker key arrays
- `getAllowedBookmakers(stateCode)` — Returns allowed list or `null` for "All States"

**Key rules:**
- Offshore books (`pinnacle`, `novig`, `prophetx`, `bovada`) not in any state's list
- `williamhill_us` included alongside `caesars` (rebranded, old DB records exist)
- `fliff` excluded (sweepstakes, not regulated)
- `bet365` and `unibet` only in states where they actually operate (NJ, CO, OH, PA)

### Dashboard Integration

State synced cross-device via `useUserPreferences` hook (localStorage cache + Supabase `user_profiles` table). Both the dashboard page and AnalysisModal consume the same hook.

```typescript
// Both dashboard/page.tsx and AnalysisModal.tsx
const { prefs, updatePref } = useUserPreferences()
const userState = prefs.userState

// Update (dashboard page state selector)
onChange={(e) => updatePref('userState', e.target.value)}
```

### AnalysisModal Filtering

The `processedLines` useMemo filters by allowed bookmakers before calculating edges:

```typescript
const allowed = getAllowedBookmakers(userState)
return [...bookmakerLines]
  .filter((line) => !allowed || allowed.includes(line.bookmaker))
  .map((line) => { /* edge calc */ })
  .sort(...)
```

**Automatic effects (no extra code):**
- `selectedLine` derives from `processedLines` → now state-filtered
- Bet sizing uses selected line → uses legal books only
- BEST EDGE / EASIEST badges recompute on filtered set
- "No lines from MI-licensed books" shown when all bookmakers are filtered out

## Clickable Line Selection (Session 47)

Sportsbook line rows in the AnalysisModal are clickable. Selecting a line recalculates the bet sizing section using that line's odds and model probability.

### Implementation

```typescript
const [selectedLineIndex, setSelectedLineIndex] = useState<number>(0)

// Reset when processedLines changes (new player, state filter, etc.)
useEffect(() => {
  setSelectedLineIndex(0)
}, [processedLines])

const selectedLine = processedLines.length > 0 && processedLines[selectedLineIndex]?.lineEdge > 0
  ? processedLines[selectedLineIndex]
  : null
```

### Visual Feedback

- Selected line: green border + ring highlight (`ring-1 ring-green-500/30`)
- "SIZING" badge on selected row
- Non-selected rows: subtle hover border on hover

### Bet Sizing

The bet sizing section uses `selectedLine` instead of always using the best-edge line:

```typescript
const sizingOdds = selectedLine ? selectedLine.relevantOdds : fallbackOdds
const sizingModelProb = selectedLine ? selectedLine.modelProb : probability
```

## DFS Edge Finder Page (`/dfs`) — Sessions 48-49

Three-mode edge analysis comparing DFS platform lines (PrizePicks, Underdog, Pick6, Betr) against model probabilities and sportsbook consensus.

### Edge Modes

| Mode | Signal Source | Description |
|------|--------------|-------------|
| **Model Edge** | Quantile interpolation | Model's probability at DFS line vs break-even threshold |
| **Market Edge** | Devigged sportsbook consensus | Sharp market implied probability at DFS line vs break-even |
| **Combined** | Both agree | Only shows picks where model AND market both have +EV and agree on direction. Edge = `min(model, market)` |

### Data Flow

1. **Scraper** adds `us_dfs` region → DFS lines land in `raw_player_props_combined`
2. **`get_dfs_lines` RPC** returns latest DFS line per bookmaker/player/stat
3. **`get_sportsbook_lines` RPC** returns latest non-DFS bookmaker lines
4. **Page** fetches predictions + DFS lines + sportsbook lines in parallel (`Promise.all`)
5. **Model Edge:** Re-estimates model probability at DFS line via quantile interpolation
6. **Market Edge:** Indexes sportsbook lines by `{player_id}-{game_id}-{stat}`, finds books matching exact DFS line value, applies multiplicative devigging, computes consensus probability, identifies sharpest book (lowest vig)
7. **Combined:** Cross-references model and market data, includes only rows where both signals agree

### Key Insights

- DFS platforms often set different lines than sportsbooks (e.g., sharp has 24.5, PrizePicks has 25.5)
- Market Edge only computes probability when a sportsbook offers the **exact same line value** as the DFS platform (no approximation). Shows `"--"` otherwise.
- Combined mode is the highest bar — requires both model AND market consensus plus directional agreement

### Components

| Component | File | Purpose |
|-----------|------|---------|
| `DfsFilters` | `components/dfs/DfsFilters.tsx` | Edge mode toggle, platform tabs, slip type dropdown, stat filter, +EV toggle, live toggle |
| `DfsTable` | `components/dfs/DfsTable.tsx` | Mode-specific column layouts, sortable, color-coded edges |

### Filter State

| State | Type | Default | Description |
|-------|------|---------|-------------|
| `edgeMode` | `EdgeMode` | `'model'` | Which edge analysis to show |
| `platformFilter` | string | `'all'` | Filter by DFS platform |
| `slipType` | string | `'pp_6_flex'` | Break-even threshold for EV calc |
| `statFilter` | string | `'all'` | Filter by stat type |
| `evOnly` | boolean | `true` | Only show +EV picks |
| `showLive` | boolean | `false` | Include picks from started games |

### Slip Types

| Key | Label | Break-Even | Payout |
|-----|-------|-----------|--------|
| `pp_2_power` | PP 2-Pick | 57.7% | 3x |
| `ud_3_standard` | UD 3-Pick | 55.0% | 6x |
| `ud_5_standard` | UD 5-Pick | 54.9% | 20x |
| `pp_5_flex` | PP 5-Pick Flex | 54.25% | 10x |
| `pp_6_flex` | PP 6-Pick Flex | 54.21% | 25x |

### Table Columns (Per Mode)

**Model Edge:**
Player | Stat | Platform | Sharp | DFS | Diff | Pick | Model % | B/E | Edge

**Market Edge:**
Player | Stat | Platform | DFS Line | Pick | Market % | Books | Sharp Line | Line Diff | B/E | Edge

**Combined:**
Player | Stat | Platform | DFS Line | Pick | Model % | Market % | Books | B/E | Edge

### Shared Utilities

**File:** `dashboard/src/lib/dfs-utils.ts`

Model probability functions (Session 48):
- `estimateUnderProb(line, q10, q25, q50, q75, q90)` — Quantile interpolation for P(Under)
- `estimateOverProb(...)` — `1 - estimateUnderProb(...)`
- `calcDfsEv(modelProb, breakEven)` — `modelProb - breakEven`
- `calcAllSlipEvs(modelProb)` — EV for all slip types

Devigging and market utilities (Session 49):
- `americanToImpliedProb(odds)` — Convert American odds to raw implied probability
- `devig(overOdds, underOdds)` — Multiplicative devigging → `[devigged_over, devigged_under]`
- `computeVig(overOdds, underOdds)` — Compute booksum (vig indicator)
- `formatBookmaker(name)` — Display name formatting for sportsbooks

### Database

**RPC Function:** `get_dfs_lines(target_date date)` — SECURITY DEFINER function returning latest DFS lines. Uses `ROW_NUMBER()` partitioned by (player_id, game_id, bookmaker, market_key, outcome_label) ordered by snapshot_time DESC. Handles game ID format mismatch via LPAD.

**RPC Function:** `get_sportsbook_lines(target_date date)` — SECURITY DEFINER function returning latest non-DFS bookmaker lines for players with predictions on target date. Excludes DFS platforms (prizepicks, underdog, pick6, betr_us_dfs). Returns over_odds, under_odds, snapshot_time per player/game/bookmaker/market/line.

**Index:** `idx_props_bookmaker_dfs` — Partial index for DFS bookmaker queries on 26M+ row table.

**Index:** `idx_props_sportsbook_lookup` — Partial index for non-DFS sportsbook queries (excludes DFS platforms).

## NBA CDN Games API (`/api/games`) — Session 49

Server-side Next.js API route that fetches today's NBA game schedule from the NBA CDN.

### Purpose

Provides a reliable game list for the dashboard's fallback display when predictions haven't been generated yet (e.g., before the inference job runs). Replaces the previous `get_games_for_date` Supabase RPC which depended on the odds scraper having already run.

### Endpoint

`GET /api/games?date=YYYY-MM-DD`

### Data Source

Fetches `https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json` — the full NBA season schedule. Always available, no scraper dependency.

### Implementation

1. Parses `date` query parameter (YYYY-MM-DD format)
2. Fetches NBA CDN schedule JSON (1-hour `revalidate` cache)
3. Matches target date against CDN's MM/DD/YYYY format
4. Filters to regular season games (`gameId.startsWith('002')`)
5. Maps tri-codes (e.g., "LAL") to full team names (e.g., "Los Angeles Lakers")
6. Returns `[{ home_team, away_team, commence_time }]`

### File

`dashboard/src/app/api/games/route.ts`

## User Bet Tracking (Session 64)

Cross-device sync for user-placed bets and preferences, replacing per-device localStorage.

### Custom Hooks

**`dashboard/src/lib/hooks/useUserBets.ts`** — Bet tracking hook.
- Fetches user's bets for `selectedDate` from `user_bets` table on mount/date change
- Returns `{ takenBets: Set<string>, toggleBet: (prediction) => void, loading }`
- `toggleBet`: optimistic UI update → async Supabase upsert/delete → rollback on error
- On insert: auto-captures direction, odds, book, model_prob, edge from the Prediction object
- Key format: `${player_id}-${stat}` (same as PropCard/PropGrid)
- Uses `betRowsRef` (Map) to track row IDs for efficient deletes
- Unique constraint: `(user_id, game_date, player_id, stat_type)` — one bet per player/stat/date

**`dashboard/src/lib/hooks/useUserPreferences.ts`** — Preferences sync hook.
- Loads instantly from localStorage (SSR-safe), then syncs from `user_profiles` table
- On change: writes to localStorage (instant) and DB (500ms debounced)
- Covers: `userState`, `bankroll`, `kellyFraction`, `useCustomKelly`
- Auto-creates profile row on first use via upsert
- Returns `{ prefs, updatePref, loading }`

### Database Tables

**`user_profiles`** — Per-user preferences.
- `user_id` (uuid PK, FK auth.users), `user_state`, `bankroll` (default 1000), `kelly_fraction` (default 0.25), `use_custom_kelly`, `created_at`, `updated_at`
- RLS: users access only their own row

**`user_bets`** — User-placed bets from PropCard checkmark.
- `id` (bigserial PK), `user_id`, `prediction_id`, `game_date`, `player_id`, `player_name`, `stat_type`, `line`, `bet_direction`, `odds_at_bet`, `book_at_bet`, `model_prob`, `edge`, `stake`, `status` (default 'pending'), `actual_value`, `pnl`, `placed_at`, `resolved_at`
- Unique: `(user_id, game_date, player_id, stat_type)`
- Indexes: `(user_id, game_date DESC)`, partial on `status='pending'`
- RLS: users access only their own rows

### Auto-Resolution

**`src/paper_trading/user_bet_resolver.py`** — Backend resolution.
- `UserBetResolver.resolve_all_pending()` called from `daily_stats_job.py` after paper bet resolution
- Queries `user_bets WHERE status='pending' AND game_date < today`
- Gets actuals from `player_game_stats` via `team_game_stats` join
- Resolution logic mirrors `PaperTrader.resolve_bets()`: over/under comparison, DNP void, push handling
- Non-fatal: failures logged but don't fail the daily stats job

### Dashboard Changes

- **Dashboard page** — `useUserBets(selectedDate)` replaces localStorage `takenBets`. `useUserPreferences()` replaces localStorage `userState`.
- **AnalysisModal** — `useUserPreferences()` replaces 6 localStorage reads for bankroll/kelly/state.
- **History page** — Two tabs: "My Bets" (default, green) queries `user_bets`, "Model History" preserves existing paper_bets view.
- **Performance page** — Three tabs: "My Bets" (green), "Props", "DFS". My Bets shows KPIs, bankroll chart, and stat breakdown from `user_bets`.

## Future Enhancements

1. **Date range selector** — Allow selecting custom date ranges for history/performance
2. **Vercel Analytics** — Add `@vercel/analytics` for page view tracking
3. **Error monitoring** — Add Sentry for error tracking
4. **Health check endpoint** — `/api/health` for uptime monitoring
5. **Stake calculation in useUserBets** — Use Kelly from preferences to auto-calculate stake on bet placement
