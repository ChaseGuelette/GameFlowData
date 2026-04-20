export interface ArbPaperBet {
  id: string
  sport: string
  arb_type: 'pure' | 'soft'
  market_type: string
  team1: string | null
  team2: string | null
  description: string | null
  kalshi_ticker: string
  kalshi_side: 'yes' | 'no'
  kalshi_price: number
  kalshi_contracts: number
  poly_condition_id: string
  poly_side: 'yes' | 'no'
  poly_price: number
  poly_contracts: number
  combined_cost: number
  net_margin: number
  game_date: string | null
  arb_opportunity_id: string | null
  status: 'placed' | 'resolved_profit' | 'resolved_loss' | 'cancelled'
  placed_at: string
  resolved_at: string | null
  pnl: number | null
}

export interface ArbDailyLog {
  id: string
  log_date: string
  sport: string
  arbs_placed: number
  arbs_resolved: number
  arbs_profit: number
  arbs_loss: number
  arbs_cancelled: number
  total_pnl: number
  cumulative_pnl: number
}

export interface ArbSummary {
  total_placed: number
  active_bets: number
  total_resolved: number
  resolved_profit: number
  resolved_loss: number
  total_cancelled: number
  total_pnl: number
  win_rate: number
}

export type ArbDateRange = 'today' | '7d' | '30d' | 'all'
export type ArbTab = 'bets' | 'daily-log' | 'queue'

export type VerifiedMarketLink = {
  id: number
  kalshi_ticker: string
  poly_condition_id: string
  series: string | null
  kalshi_title: string | null
  poly_question: string | null
  match_confidence: number | null
  match_method: string | null
  status: 'pending' | 'approved' | 'rejected'
  notes: string | null
  created_at: string
  reviewed_at: string | null
}
