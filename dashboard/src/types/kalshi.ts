// Kalshi Prediction Markets Types

export interface KalshiMarket {
  ticker: string
  player_name: string
  stat_type: string
  line: number
  player_id: number | null

  // Pricing (cents)
  yes_price: number
  no_price: number
  yes_bid: number
  yes_ask: number
  bid_ask_spread: number

  // Liquidity
  volume: number
  open_interest: number

  // Edge data (populated by edge calculator)
  model_prob: number | null
  kalshi_implied: number | null
  raw_edge: number | null
  maker_fee_adjusted_edge: number | null
  taker_fee_adjusted_edge: number | null

  // Sportsbook comparison
  sportsbook_consensus_line: number | null
  line_vs_sportsbook: number | null

  // Timing
  close_time: string | null
  market_status: string
  snapshot_time: string
}

export interface KalshiOrderbook {
  ticker: string
  snapshot_time: string
  yes_bid: number
  yes_ask: number
  yes_bid_size: number
  yes_ask_size: number
  depth: {
    yes: [number, number][]
    no: [number, number][]
  }
  mid_price: number
  spread: number
  total_bid_depth: number
  total_ask_depth: number
}

// Display labels for Kalshi stat types
export const KALSHI_STAT_LABELS: Record<string, string> = {
  pts: 'Points',
  reb: 'Rebounds',
  ast: 'Assists',
  stl: 'Steals',
  blk: 'Blocks',
  '3pm': '3-Pointers',
  pra: 'PRA',
  pr: 'Pts+Reb',
  pa: 'Pts+Ast',
  ra: 'Reb+Ast',
  pitcher_strikeouts: 'Strikeouts',
  batter_hits: 'Hits',
  batter_total_bases: 'Total Bases',
  batter_home_runs: 'Home Runs',
  batter_rbis: 'RBIs',
  batter_runs_scored: 'Runs',
}

// Fee calculation utilities (mirror Python kalshi_utils.py)
export function kalshiTakerFee(priceCents: number): number {
  const p = priceCents / 100
  return Math.ceil(0.07 * p * (1 - p) * 100) / 100
}

export function kalshiMakerFee(priceCents: number): number {
  const p = priceCents / 100
  return Math.ceil(0.0175 * p * (1 - p) * 100) / 100
}
