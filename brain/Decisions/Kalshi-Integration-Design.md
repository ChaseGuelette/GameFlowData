# Kalshi Prediction Market Integration — Design Doc

> Part of [[Decisions]]

**Created**: March 24, 2026
**Status**: Design complete, not yet implemented

## Overview

Kalshi is a CFTC-regulated prediction market exchange offering sports player props (NBA, MLB, NFL). Unlike sportsbooks, Kalshi uses binary YES/NO contracts priced in cents (0-99), settling at $1.00. Key advantages: no account limits/bans, full API trading, transparent order books. Key challenges: thin liquidity on props (~50 NBA players), $10K per-trade limit, fees eat 20-40% of a 5% edge.

GFD's models already output calibrated probabilities — a Kalshi YES price IS an implied probability, so the translation is trivial. This plan adds Kalshi as a parallel data source alongside sportsbooks, with its own scraper, edge calculator, paper trader, and UI integration.

---

## Research Summary

### Contract Structure
- Binary YES/NO contracts at specific thresholds (e.g., "Will LeBron score 30+ points?")
- One threshold per player/stat (no alternate lines like sportsbooks)
- Priced in cents: 65c YES = 65% implied probability
- Settle at $1.00 (win) or $0.00 (lose)
- Markets close ~1 hour before game time

### Fee Structure
- **Taker fee**: `ceil(0.07 * P * (1-P) * 100) / 100` per contract — max $0.02 at 50c
- **Maker fee**: `ceil(0.0175 * P * (1-P) * 100) / 100` per contract — ~75% discount vs taker
- A 5% edge becomes ~3-4% after taker fees, ~4.5% after maker fees
- **Maker orders (limit orders) are critical for viability**

### Liquidity
- ~50 NBA players covered on any given night
- Thin order books on props ($1-2.5K/night realistic deployment)
- $10,000 per-trade position limit
- Volume concentrates on star players

### Key Advantages Over Sportsbooks
- No account limits or bans (CFTC-regulated exchange)
- Full API trading with programmatic order placement
- Transparent order books (real price discovery)
- No devigging needed — YES price IS the implied probability

---

## Phase 1: Data Infrastructure

### Step 1.1: Kalshi API Client

**New file**: `src/scrapers/kalshi/__init__.py`
**New file**: `src/scrapers/kalshi/kalshi_client.py`

```python
class KalshiClient:
    """Low-level Kalshi API client with RSA-PSS SHA256 auth."""

    def __init__(self, api_key: str, private_key_path: str):
        # Auth: KALSHI-ACCESS-KEY, KALSHI-ACCESS-TIMESTAMP, KALSHI-ACCESS-SIGNATURE
        # Base URL: https://api.elections.kalshi.com/trade-api/v2

    def _sign_request(self, method, path, timestamp) -> str:
        # RSA-PSS SHA256 signature: sign(timestamp + method + path_no_query)

    # Read endpoints
    def list_markets(self, series_ticker=None, status="open", limit=200) -> dict
    def get_market(self, ticker: str) -> dict
    def get_orderbook(self, ticker: str, depth=5) -> dict

    # Write endpoints (Phase 5)
    def create_order(self, ticker, side, count, price=None) -> dict
    def get_positions(self) -> dict
    def get_balance(self) -> dict
```

**Dependency**: `cryptography` (RSA-PSS signing). Add to `requirements.txt`.

**Env vars**: `KALSHI_API_KEY`, `KALSHI_PRIVATE_KEY_PATH`

**Rate limits**: Basic tier = 20 reads/sec, 10 writes/sec. Token bucket or simple sleep guard.

---

### Step 1.2: Kalshi Utils (Fees + Probability)

**New file**: `src/scrapers/kalshi/kalshi_utils.py`

```python
def kalshi_price_to_prob(yes_price_cents: int) -> float:
    """YES price IS the implied probability. 65c = 65%."""
    return yes_price_cents / 100.0

def kalshi_mid_to_prob(yes_bid: int, yes_ask: int) -> float:
    """Midpoint removes spread-based vig. Analogous to devigging."""
    return (yes_bid + yes_ask) / 200.0

def kalshi_taker_fee(price_cents: int) -> float:
    """ceil(0.07 * P * (1-P) * 100) / 100. Max $0.02 at 50c."""

def kalshi_maker_fee(price_cents: int) -> float:
    """ceil(0.0175 * P * (1-P) * 100) / 100. ~75% discount vs taker."""

def fee_adjusted_edge(model_prob: float, kalshi_price_cents: int, is_maker=True) -> float:
    """Edge after expected fees: model_prob - implied - fee."""

KALSHI_STAT_MAP = {
    "PTS": "pts", "REB": "reb", "AST": "ast", "3PT": "3pm",
    "K": "pitcher_strikeouts", "H": "batter_hits", "HR": "batter_home_runs",
    "RBI": "batter_rbis", "TB": "batter_total_bases",
}
```

---

### Step 1.3: Kalshi Market Scraper

**New file**: `src/scrapers/kalshi/kalshi_market_scraper.py`

```python
class KalshiMarketScraper:
    """Discover sports prop markets, parse player/stat/line, link to player_id."""

    def __init__(self, client: KalshiClient, engine):
        self._player_cache = {}  # normalized_name -> player_id

    def discover_sports_markets(self, sport="nba") -> list[dict]:
        """Iterate KXNBA/KXMLB/KXNFL series, collect open markets."""

    def parse_market(self, market: dict) -> dict | None:
        """Extract player_name, stat_type, line from ticker + title.

        Ticker parsing: KXNBA-26MAR25-LEBRON-PTS-T29.5
        Title fallback: "Will LeBron James score 30+ points?"
        Regex patterns for title parsing.
        """

    def link_player(self, player_name: str, sport: str) -> int | None:
        """Reuse normalize_player() + SequenceMatcher from mlb_linker.py.
        Fuzzy match threshold: 0.85."""

    def scrape_and_store(self, sport="nba") -> int:
        """Full cycle: discover -> parse -> link -> store to kalshi_markets."""
```

**Player linking**: Reuse `normalize_player()` from `src/processing/mlb/mlb_linker.py`. Build lookup cache from `players` (NBA) or `mlb_players` (MLB) table at startup.

---

### Step 1.4: Database Schema

**Migration**: `kalshi_tables`

```sql
CREATE TABLE kalshi_markets (
    id BIGSERIAL PRIMARY KEY,
    -- Kalshi identifiers
    ticker TEXT NOT NULL,
    event_ticker TEXT,
    series_ticker TEXT,
    -- Parsed data
    sport TEXT NOT NULL,                     -- "nba", "mlb", "nfl"
    player_name TEXT NOT NULL,
    stat_type TEXT NOT NULL,                 -- Our stat key
    line NUMERIC(6,1) NOT NULL,
    market_title TEXT,
    -- Linked IDs (nullable)
    player_id INTEGER,
    game_id TEXT,
    -- Pricing (cents 0-99)
    yes_price INTEGER,
    no_price INTEGER,
    yes_bid INTEGER,
    yes_ask INTEGER,
    bid_ask_spread INTEGER,
    -- Liquidity
    volume INTEGER,
    open_interest INTEGER,
    -- Edge (populated by Phase 2)
    model_prob NUMERIC(6,4),
    kalshi_implied NUMERIC(6,4),
    raw_edge NUMERIC(6,4),
    maker_fee_adjusted_edge NUMERIC(6,4),
    taker_fee_adjusted_edge NUMERIC(6,4),
    sportsbook_consensus_line NUMERIC(6,1),
    line_vs_sportsbook NUMERIC(6,1),
    -- Timing
    close_time TIMESTAMPTZ,
    market_status TEXT DEFAULT 'open',
    snapshot_time TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(ticker, snapshot_time)
);

CREATE INDEX idx_kalshi_sport_date ON kalshi_markets(sport, snapshot_time);
CREATE INDEX idx_kalshi_player ON kalshi_markets(player_id) WHERE player_id IS NOT NULL;
CREATE INDEX idx_kalshi_ticker ON kalshi_markets(ticker);

CREATE TABLE kalshi_orderbook_snapshots (
    id BIGSERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    snapshot_time TIMESTAMPTZ DEFAULT NOW(),
    yes_bid INTEGER, yes_bid_size INTEGER,
    yes_ask INTEGER, yes_ask_size INTEGER,
    yes_depth JSONB,
    no_depth JSONB,
    mid_price NUMERIC(5,2),
    spread INTEGER,
    total_bid_depth INTEGER,
    total_ask_depth INTEGER,
    UNIQUE(ticker, snapshot_time)
);

CREATE INDEX idx_kalshi_ob_ticker ON kalshi_orderbook_snapshots(ticker, snapshot_time DESC);
```

RLS: Public read for `kalshi_markets` (same as `raw_player_props_combined`). Subscription-gated for `kalshi_paper_bets` (Phase 4).

---

### Step 1.5: Scheduler Integration

**New file**: `src/orchestration/kalshi_refresh_job.py`

```python
"""Kalshi Refresh Job — scrape markets + compute edges every 10 min."""

def main():
    # 1. Init KalshiClient
    # 2. KalshiMarketScraper.scrape_and_store(sport)
    # 3. Fetch orderbook snapshots for open markets
    # 4. Compute edges (Phase 2)
    # 5. Paper trading (Phase 4)
    # 6. Discord alerts (Phase 6)
```

**Modify**: `src/orchestration/scheduler.py`
- Add to `JOB_NAMES`: `"kalshi_refresh_job.py": "Kalshi Refresh"`
- Add cron trigger: every 10 min, 11 AM - 11 PM ET
- Add env validation for `KALSHI_API_KEY` (optional — job skips if not set)

**Note**: Unlike advanced stats scraping, Kalshi API works from any IP (proper API with keys). Can run on Railway with `KALSHI_PRIVATE_KEY_PATH` pointing to a file created from a base64-encoded env var. OR run locally via Task Scheduler like the advanced scraper.

---

## Phase 2: Edge Analysis

### Step 2.1: Kalshi Edge Calculator

**New file**: `src/models/kalshi_edge.py`

```python
class KalshiEdgeCalculator:
    """Compare model probabilities against Kalshi implied."""

    def compute_edges(self, target_date, sport="nba") -> pd.DataFrame:
        """
        1. Load latest kalshi_markets for target_date
        2. Load MC samples from daily_prediction_samples / mlb equivalent
        3. For each matched market:
           - model_prob = (samples > line).mean()  # empirical CDF
           - kalshi_implied = midpoint(yes_bid, yes_ask) / 100
           - raw_edge = model_prob - kalshi_implied
           - maker_fee_adjusted = raw_edge - maker_fee
           - taker_fee_adjusted = raw_edge - taker_fee
        4. Compare line to sportsbook consensus
        5. Update kalshi_markets with edge columns
        """

    def compare_to_sportsbook(self, kalshi_df, sport="nba") -> pd.DataFrame:
        """For each player/stat, find sportsbook consensus line.
        Compute: line_diff, is Kalshi softer or harder?"""
```

**Key difference from sportsbook edge**: No devigging needed. Kalshi mid-price IS the fair probability. Vig comes from the spread, which we measure but don't "remove."

### Step 2.2: Supabase RPC

```sql
CREATE OR REPLACE FUNCTION get_kalshi_edges(
    p_date DATE DEFAULT CURRENT_DATE,
    p_sport TEXT DEFAULT 'nba'
) RETURNS TABLE (
    ticker TEXT, player_name TEXT, player_id INTEGER,
    stat_type TEXT, line NUMERIC,
    yes_price INTEGER, yes_bid INTEGER, yes_ask INTEGER,
    bid_ask_spread INTEGER, volume INTEGER, open_interest INTEGER,
    close_time TIMESTAMPTZ,
    model_prob NUMERIC, kalshi_implied NUMERIC,
    raw_edge NUMERIC, maker_fee_adjusted_edge NUMERIC,
    sportsbook_consensus_line NUMERIC, line_vs_sportsbook NUMERIC
) LANGUAGE SQL STABLE SECURITY DEFINER
SET statement_timeout = '15s'
AS $$
    SELECT ... FROM kalshi_markets
    WHERE sport = p_sport
      AND snapshot_time::date = p_date
      AND player_id IS NOT NULL
    ORDER BY maker_fee_adjusted_edge DESC;
$$;
```

---

## Phase 3: Dashboard UI

### Step 3.1: Sportsbook Availability

**Modify**: `dashboard/src/lib/sportsbook-availability.ts`
- Add `{ value: 'kalshi', label: 'Kalshi (Exchange)' }` to `SPORTSBOOK_OPTIONS`
- Kalshi is CFTC-regulated, available in most states (not NY). Add to all `STATE_SPORTSBOOKS` entries except NY.

### Step 3.2: Types

**Modify**: `dashboard/src/types/predictions.ts`

```typescript
export interface KalshiMarket {
  ticker: string
  player_name: string
  player_id: number
  stat_type: string
  line: number
  yes_price: number       // cents
  yes_bid: number
  yes_ask: number
  bid_ask_spread: number
  volume: number
  open_interest: number
  close_time: string
  model_prob: number
  kalshi_implied: number
  raw_edge: number
  maker_fee_adjusted_edge: number
  taker_fee_adjusted_edge: number
  sportsbook_consensus_line?: number
  line_vs_sportsbook?: number
}
```

### Step 3.3: AnalysisModal — Kalshi Section

**Modify**: `dashboard/src/components/analysis/AnalysisModal.tsx`

Add a new section after "Sportsbook Lines" showing:
- YES/NO pricing with bid/ask (green/red cards)
- Model prob vs Kalshi implied vs fee-adjusted edge
- Liquidity: spread, volume, open interest
- Line comparison to sportsbook consensus
- Fee breakdown (maker/taker per contract)
- Market closure countdown

**New component**: `dashboard/src/components/analysis/KalshiCountdown.tsx`
- Shows "Closes in 3h 42m" with color coding: green > 2h, orange < 2h, red < 30m

### Step 3.4: Predictions Page — Kalshi Filter (Optional)

**Modify**: `dashboard/src/components/predictions/FilterTabs.tsx`
- Add "Kalshi" tab that filters predictions to only those with matching Kalshi markets, sorted by fee-adjusted edge

---

## Phase 4: Paper Trading

### Step 4.1: Kalshi Paper Trader

**New table** (migration):

```sql
CREATE TABLE kalshi_paper_bets (
    id BIGSERIAL PRIMARY KEY,
    game_date DATE NOT NULL,
    ticker TEXT NOT NULL,
    player_id INTEGER, player_name TEXT NOT NULL,
    stat_type TEXT NOT NULL, line NUMERIC(6,1) NOT NULL,
    side TEXT NOT NULL,                       -- "yes" or "no"
    price INTEGER NOT NULL,                   -- Limit price in cents
    contracts INTEGER NOT NULL,
    is_maker BOOLEAN DEFAULT TRUE,
    expected_fee NUMERIC(8,4),
    model_prob NUMERIC(6,4),
    kalshi_implied NUMERIC(6,4),
    edge NUMERIC(6,4),
    fee_adjusted_edge NUMERIC(6,4),
    status TEXT DEFAULT 'pending',            -- pending/filled/won/lost/cancelled
    fill_price INTEGER,
    actual_value NUMERIC(8,2),
    pnl NUMERIC(10,2),
    placed_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    UNIQUE(game_date, ticker, side)
);
```

**New file**: `src/paper_trading/kalshi_paper_trader.py`

```python
@dataclass
class KalshiPaperTrader:
    min_fee_adjusted_edge: float = 0.05       # 5% after fees
    max_contracts_per_market: int = 100
    max_daily_exposure: float = 1000.0
    min_volume: int = 50                      # Skip ghost markets
    max_spread: int = 10                      # Skip illiquid markets (>10c spread)

    def select_bets(self, target_date, sport="nba") -> list[dict]
    def size_position(self, edge, price_cents, bankroll) -> int  # Kelly
    def place_bets(self, bets) -> int
    def resolve_bets(self, target_date) -> dict
    def get_bankroll(self) -> float
```

### Step 4.2: Integration

Wire into `kalshi_refresh_job.py` — after edge calculation, call paper trader's resolve + select + place cycle (same pattern as `edge_refresh_job.py`).

---

## Phase 5: Live Trading (Future — Design Only)

**New file**: `src/trading/kalshi_live_trader.py`
- Gated behind `KALSHI_LIVE_TRADING_ENABLED=true`
- Limit orders only (maker fees)
- Self-imposed limits: $5K/market, $10K/day
- Auto-cancel 30 min before market close
- New table: `kalshi_live_orders`

**Not implementing now** — needs Phase 4 paper trading to prove profitability first.

---

## Phase 6: Discord Alerts

**Modify**: `src/discord_bot/alerts.py`

```python
def _build_kalshi_alert_embed(kalshi_edges_df, prediction_date, sport="nba") -> dict:
    """Violet-colored embed showing top 5 Kalshi edges.
    Fields: player/stat, YES price, spread, fee-adjusted edge,
            model vs implied, volume/OI, close time."""

async def send_kalshi_alert(edges_df, prediction_date, sport="nba") -> bool:
    """Send to DISCORD_CHANNEL_KALSHI or fallback to predictions channel."""

def send_kalshi_alert_sync(...) -> bool:
    """Sync wrapper for scheduled jobs."""
```

Integrated into `kalshi_refresh_job.py` — alert when any market has fee_adjusted_edge >= 5%.

---

## Dependency Graph

```
Phase 1 (Data Infrastructure)
  ├── 1.1 API Client
  ├── 1.2 Utils (fees/probs)
  ├── 1.3 Market Scraper (depends on 1.1, 1.2)
  ├── 1.4 Database Schema
  └── 1.5 Scheduler (depends on 1.3, 1.4)
       |
Phase 2 (Edge Analysis) ── depends on Phase 1
  ├── 2.1 Edge Calculator
  └── 2.2 Supabase RPC
       |
       ├──────────────────────┐
Phase 3 (Dashboard UI)   Phase 6 (Discord)  ── parallel after Phase 2
       |
Phase 4 (Paper Trading) ── depends on Phase 2
       |
Phase 5 (Live Trading)  ── future, depends on Phase 4 proving profit
```

---

## Files Summary

### New Files (10)
| File | Phase | Purpose |
|------|-------|---------|
| `src/scrapers/kalshi/__init__.py` | 1 | Package init |
| `src/scrapers/kalshi/kalshi_client.py` | 1 | API client with RSA-PSS auth |
| `src/scrapers/kalshi/kalshi_utils.py` | 1 | Fee calc, probability conversion, stat mapping |
| `src/scrapers/kalshi/kalshi_market_scraper.py` | 1 | Market discovery, parsing, player linking |
| `src/orchestration/kalshi_refresh_job.py` | 1 | Orchestration job (scrape + edges + paper + alerts) |
| `src/models/kalshi_edge.py` | 2 | Edge calculator (model prob vs Kalshi implied) |
| `dashboard/src/components/analysis/KalshiCountdown.tsx` | 3 | Market closure countdown component |
| `src/paper_trading/kalshi_paper_trader.py` | 4 | Paper trading with fill simulation |
| `src/trading/kalshi_live_trader.py` | 5 | Live trading (future) |

### Modified Files (5)
| File | Phase | Change |
|------|-------|--------|
| `src/orchestration/scheduler.py` | 1 | Add kalshi_refresh_job schedule |
| `dashboard/src/lib/sportsbook-availability.ts` | 3 | Add Kalshi to SPORTSBOOK_OPTIONS |
| `dashboard/src/types/predictions.ts` | 3 | Add KalshiMarket interface |
| `dashboard/src/components/analysis/AnalysisModal.tsx` | 3 | Add Kalshi section |
| `src/discord_bot/alerts.py` | 6 | Add Kalshi alert formatter |

### Database Migrations (2-3)
| Migration | Phase | Tables |
|-----------|-------|--------|
| `kalshi_tables` | 1 | `kalshi_markets`, `kalshi_orderbook_snapshots` |
| `kalshi_paper_bets` | 4 | `kalshi_paper_bets` |
| `kalshi_rls` | 1 | RLS policies for kalshi tables |

---

## Verification

1. **Phase 1**: Run `kalshi_refresh_job.py --sport nba --dry-run` — should discover markets, parse player/stat/line, link player_ids, print summary without writing to DB.
2. **Phase 2**: Run with edges — should show model_prob vs kalshi_implied for matched predictions. Compare edges to sportsbook edges for the same player/stat.
3. **Phase 3**: Open AnalysisModal for a player with a Kalshi market — should show YES/NO pricing, edge, liquidity, countdown.
4. **Phase 4**: Run paper trading for a full game day. Next day, resolve and check simulated P&L. Compare to sportsbook paper trading P&L.
5. **Phase 6**: Discord alert should fire with violet embed showing top Kalshi edges.

---

## Environment Variables

| Variable | Required | Phase | Notes |
|----------|----------|-------|-------|
| `KALSHI_API_KEY` | Phase 1 | API access key from kalshi.com/account |
| `KALSHI_PRIVATE_KEY_PATH` | Phase 1 | Path to RSA PEM private key (local file) |
| `KALSHI_LIVE_TRADING_ENABLED` | Phase 5 | "true" to enable (default: disabled) |
| `DISCORD_CHANNEL_KALSHI` | Phase 6 | Optional dedicated Discord channel |

## Key Design Notes

- **Kalshi data stays separate** from `raw_player_props_combined`. Different data model (YES/NO prices vs American odds, order book depth, market IDs).
- **Can run on Railway** — Kalshi API works from any IP (unlike stats.nba.com). Private key can be base64-encoded as env var.
- **One line per player/stat** on Kalshi (no alternate lines). Simpler than sportsbooks.
- **Markets close ~1h before game** — scraper must respect close_time.
- **Minimum liquidity filters** — skip markets with volume < 50 or spread > 10c.
- **Separate P&L tracking** — Kalshi paper trading has its own bankroll and performance metrics.
- **Probability conversion is trivial** — YES price in cents / 100 = implied probability. No devigging needed.
- **Maker orders are mandatory for profitability** — taker fees eat 20-40% of a 5% edge, maker fees only eat ~5-10%.
