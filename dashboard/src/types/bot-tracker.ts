export interface KalshiLiveOrder {
  id: number
  game_date: string
  ticker: string
  sport: string
  player_id: number | null
  player_name: string | null
  stat_type: string
  line: number
  side: string
  order_type: string
  contracts: number
  kalshi_order_id: string | null
  fill_price: number | null
  fill_count: number | null
  total_cost: number | null
  fee_paid: number | null
  model_prob: number | null
  kalshi_implied: number | null
  edge: number | null
  fee_adjusted_edge: number | null
  status: string
  actual_value: number | null
  pnl: number | null
  placed_at: string | null
  filled_at: string | null
  resolved_at: string | null
  game_start_time: string | null
}

export interface KalshiPaperBet {
  id: number
  game_date: string
  ticker: string
  sport: string
  player_id: number | null
  player_name: string
  stat_type: string
  line: number
  side: string
  price: number
  contracts: number
  is_maker: boolean | null
  expected_fee: number | null
  model_prob: number | null
  kalshi_implied: number | null
  edge: number | null
  fee_adjusted_edge: number | null
  status: string | null
  fill_price: number | null
  actual_value: number | null
  pnl: number | null
  placed_at: string | null
  resolved_at: string | null
  created_at: string | null
  close_time: string | null
}

export interface KalshiDailyLog {
  game_date: string
  total_bets: number
  bets_won: number
  bets_lost: number
  bets_cancelled: number
  bets_pending: number
  total_cost: number
  total_pnl: number
  roi_pct: number
  cumulative_pnl: number
  balance_after: number
}

export interface KalshiLiveDailyLog extends KalshiDailyLog {
  created_at: string | null
  updated_at: string | null
}

export interface KalshiPaperDailyLog extends KalshiDailyLog {
  id: number
  sport: string
  bankroll_after: number
  created_at: string | null
  updated_at: string | null
}

export interface KalshiConfig {
  is_halted: boolean
  halt_reason: string | null
  halted_at: string | null
  streak_count: number
  starting_bankroll: number
  last_updated: string | null
}

export interface KalshiTradingStats {
  total_bets: number
  bets_won: number
  bets_lost: number
  bets_pending: number
  total_pnl: number
  total_cost: number
  total_fees?: number
}

export interface KalshiBotSummary {
  config: KalshiConfig
  live: KalshiTradingStats
  paper: KalshiTradingStats
}

export interface KalshiTradeQueueItem {
  id: number
  game_date: string
  ticker: string
  sport: string
  player_id: number | null
  player_name: string | null
  stat_type: string
  line: number
  side: string
  yes_price: number
  contracts: number
  expected_cost: number
  expected_fee: number | null
  model_prob: number | null
  kalshi_implied: number | null
  edge: number | null
  fee_adjusted_edge: number | null
  status: string
  proposed_at: string
  approved_at: string | null
  executed_at: string | null
  expires_at: string
}

export type DateRange = 'today' | '7d' | '30d' | 'all' | 'custom'
export type BotTab = 'live' | 'paper'

export const KALSHI_STAT_LABELS: Record<string, string> = {
  pitcher_strikeouts: 'Strikeouts',
  batter_hits: 'Hits',
  batter_home_runs: 'Home Runs',
  batter_total_bases: 'Total Bases',
  batter_rbis: 'RBIs',
  batter_runs: 'Runs',
  batter_stolen_bases: 'Stolen Bases',
  batter_walks: 'Walks',
  pts: 'Points',
  reb: 'Rebounds',
  ast: 'Assists',
  stl: 'Steals',
  blk: 'Blocks',
  fg3m: '3-Pointers',
  tov: 'Turnovers',
  pra: 'PTS+REB+AST',
  pr: 'PTS+REB',
  pa: 'PTS+AST',
  ra: 'REB+AST',
  sb: 'Steals+Blocks',
}
