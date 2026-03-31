// NBA stat types
export type NBAStatType = 'pts' | 'reb' | 'ast' | 'stl' | 'blk' | '3pm' | 'pra' | 'pr' | 'pa' | 'ra'

// MLB stat types
export type MLBStatType = 'pitcher_strikeouts' | 'batter_hits' | 'batter_total_bases' | 'batter_home_runs' | 'batter_rbis' | 'batter_runs_scored'

export type StatType = NBAStatType | MLBStatType

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

  // Black-Litterman blended values (server-computed for Model Picks)
  bl_over_prob?: number
  bl_under_prob?: number
  bl_over_edge?: number
  bl_under_edge?: number
  bl_confidence?: number
  is_recommended?: boolean

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

  // Position-matched injury features
  feat_team_out_same_pos_count?: number
  feat_team_out_same_pos_min_sum?: number
  feat_team_out_same_pos_usg_sum?: number
  feat_team_out_same_pos_starter_sum?: number

  // Sanity check flag — warning reason(s) if prediction looks suspect
  sanity_flag?: string
}

export interface GameStatusInfo {
  gameStatus: 1 | 2 | 3  // 1=Pre, 2=Live, 3=Final
  gameStatusText: string
  period: number
  gameClock: string
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

export interface BetContext {
  quantiles: { q10: number; q25: number; q50: number; q75: number; q90: number }
  l5_avg?: number | null
  l5_games?: Array<{ date: string; value: number }> | null
  season_avg?: number | null
  features: Record<string, number | boolean | string | null>
  insights: Array<{ category: string; text: string; sentiment: string }>
  probabilities: {
    model_prob: number
    implied_prob: number
    edge: number
  }
  kelly?: {
    fraction: number
    recommended_stake: number
    bankroll_pct: number
  } | null
  sportsbook_lines?: Array<{
    bookmaker: string
    line: number
    over_odds: number
    under_odds: number
  }> | null
  source: 'analysis_modal' | 'prop_card_toggle'
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
  bookmaker?: string  // Which sportsbook had the line
  team_abbrev?: string
  opponent_abbrev?: string
  bet_context?: BetContext | null
  user_confidence?: number | null
  placed_at?: string | null
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
  // NBA
  pts: 'Points',
  reb: 'Rebounds',
  ast: 'Assists',
  stl: 'Steals',
  blk: 'Blocks',
  '3pm': 'Threes',
  pra: 'Pts+Reb+Ast',
  pr: 'Pts+Reb',
  pa: 'Pts+Ast',
  ra: 'Reb+Ast',
  // MLB
  pitcher_strikeouts: 'Strikeouts',
  batter_hits: 'Hits',
  batter_total_bases: 'Total Bases',
  batter_home_runs: 'Home Runs',
  batter_rbis: 'RBIs',
  batter_runs_scored: 'Runs',
}

// Stat colors for badges
export const STAT_COLORS: Record<StatType, string> = {
  // NBA
  pts: 'bg-blue-500',
  reb: 'bg-green-500',
  ast: 'bg-purple-500',
  stl: 'bg-orange-500',
  blk: 'bg-red-500',
  '3pm': 'bg-cyan-500',
  pra: 'bg-amber-500',
  pr: 'bg-teal-500',
  pa: 'bg-indigo-500',
  ra: 'bg-emerald-500',
  // MLB
  pitcher_strikeouts: 'bg-red-600',
  batter_hits: 'bg-green-600',
  batter_total_bases: 'bg-blue-600',
  batter_home_runs: 'bg-yellow-500',
  batter_rbis: 'bg-orange-600',
  batter_runs_scored: 'bg-violet-500',
}
