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

### `paper_trading_daily_log`

Paper trading results for bankroll display.

**Columns used:**
- `game_date` — Trading date
- `bankroll` — Current bankroll value

## Components

### Navbar

Displays navigation and current bankroll.

```tsx
<Navbar bankroll={1250.00} />
```

### FilterTabs

Stat type filtering with All/PTS/REB/AST/THREES options.

```tsx
<FilterTabs
  activeFilter="all"
  onFilterChange={(filter) => setFilter(filter)}
/>
```

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
- Last 5 games bar chart
- Quantile distribution summary
- Full prediction metadata

```tsx
<AnalysisModal
  prediction={selectedPrediction}
  onClose={() => setSelected(null)}
/>
```

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

## Future Enhancements

1. **Feature-based insights** — Display why the model likes a prop (e.g., "Team missing key rebounder")
2. **Lock of the Day** — Hero section highlighting top pick
3. **Paper trading views** — Bet history, P&L charts, performance metrics
4. **Vercel deployment** — Production hosting with environment variables
