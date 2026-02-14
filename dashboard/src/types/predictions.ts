export type StatType = 'pts' | 'reb' | 'ast'

export interface Prediction {
  id: number
  prediction_date: string
  player_id: number
  game_id: string
  stat: StatType

  // Player info (from JOIN)
  player_name?: string
  team_abbrev?: string
  opponent_abbrev?: string

  // Quantile predictions
  q10: number
  q25: number
  q50: number
  q75: number
  q90: number

  // Distribution parameters (for BL blending)
  pred_mean: number
  pred_std: number

  // Edge calculations
  over_edge: number
  under_edge: number
  model_prob_over: number
  model_prob_under: number
  implied_prob_over: number
  implied_prob_under: number

  // Prop line info
  prop_line: number
  best_over_odds?: number
  best_under_odds?: number
  best_over_book?: string
  best_under_book?: string

  // Game time (UTC timestamp from NBA API)
  game_time?: string

  // Feature columns for insights (optional, may not exist yet)
  feat_rest_days?: number
  feat_is_back_to_back?: boolean
  feat_games_last_7d?: number
  feat_team_out_count?: number
  feat_team_out_min_sum?: number
  feat_opp_out_count?: number
  feat_player_is_questionable?: boolean
  feat_player_avg_stat_l5?: number
  feat_player_avg_stat_l15?: number
  feat_player_avg_stat_season?: number
  feat_player_avg_stat_l3?: number
  feat_stat_l3_l15_ratio?: number
  feat_stat_std_l5?: number
  feat_opp_allowed_stat_l15?: number
  feat_opp_team_abbrev?: string
  feat_prop_line?: number
  feat_player_season_avg_vs_line?: number
}

export interface PlayerGameStats {
  game_date: string
  pts: number
  reb: number
  ast: number
  fg3m: number
  min: number
}

export interface BookmakerLine {
  bookmaker: string
  line: number
  over_odds: number
  under_odds: number
}

export interface PaperTradingSummary {
  game_date: string
  total_bets: number
  wins: number
  losses: number
  pushes: number
  total_staked: number
  total_pnl: number
  cumulative_pnl: number
  bankroll: number
}

export type BetStatus = 'pending' | 'won' | 'lost' | 'push' | 'cancelled'

export interface PaperBet {
  id: number
  game_date: string
  player_id: number
  player_name: string
  stat_type: StatType
  line: number
  bet_direction: 'over' | 'under'
  odds_at_bet: number
  stake: number
  edge: number
  status: BetStatus
  actual_value: number | null
  pnl: number | null
}

export interface DailyPerformance {
  game_date: string
  total_bets: number
  bets_won: number
  bets_lost: number
  bets_push: number
  total_staked: number
  total_pnl: number
  cumulative_pnl: number
  bankroll_after: number
  roi_pct: number
}

export interface StatPerformance {
  stat: StatType
  total_bets: number
  wins: number
  losses: number
  win_rate: number
  total_pnl: number
  roi: number
}

// Helper to get stat display name
export const STAT_LABELS: Record<StatType, string> = {
  pts: 'Points',
  reb: 'Rebounds',
  ast: 'Assists',
}

// Stat colors for badges
export const STAT_COLORS: Record<StatType, string> = {
  pts: 'bg-blue-500',
  reb: 'bg-green-500',
  ast: 'bg-purple-500',
}
