import { type StatType } from './predictions'

export interface DfsLine {
  player_id: number
  game_id: string
  bookmaker: string
  market_key: string
  line: number
  over_odds: number | null
  under_odds: number | null
  snapshot_time: string
}

export interface DfsComparison {
  // From daily_predictions
  player_id: number
  player_name: string
  game_id: string
  stat: StatType
  team_abbrev: string
  opponent_abbrev: string
  game_time?: string

  // Sharp book reference
  sharp_line: number

  // Model predictions (quantiles for re-estimation at DFS lines)
  q10: number
  q25: number
  q50: number
  q75: number
  q90: number
  model_prob_over: number
  model_prob_under: number

  // Per-DFS-platform data
  dfs_lines: DfsPlatformLine[]
}

export interface DfsPlatformLine {
  bookmaker: string
  line: number
  line_diff: number
  model_prob_over: number
  model_prob_under: number
  best_direction: 'over' | 'under'
  best_prob: number
  ev_by_slip: Record<string, number>
}

// DFS platform break-even thresholds (probability per leg needed to break even)
export const DFS_SLIP_TYPES: Record<string, { label: string; breakEven: number; payout: string }> = {
  'ud_3_standard':  { label: 'UD 3-Pick',     breakEven: 0.550, payout: '6x'  },
  'ud_5_standard':  { label: 'UD 5-Pick',     breakEven: 0.549, payout: '20x' },
  'pp_5_flex':      { label: 'PP 5-Pick Flex', breakEven: 0.5425, payout: '10x' },
  'pp_6_flex':      { label: 'PP 6-Pick Flex', breakEven: 0.5421, payout: '25x' },
}

// Display names for DFS platforms
export const DFS_PLATFORM_NAMES: Record<string, string> = {
  'prizepicks': 'PrizePicks',
  'underdog': 'Underdog',
  'pick6': 'Pick6',
  'betr_us_dfs': 'Betr',
}

// Market key to stat type mapping
export const MARKET_TO_STAT: Record<string, StatType> = {
  'player_points': 'pts',
  'player_rebounds': 'reb',
  'player_assists': 'ast',
}

export const STAT_TO_MARKET: Record<StatType, string> = {
  pts: 'player_points',
  reb: 'player_rebounds',
  ast: 'player_assists',
}
