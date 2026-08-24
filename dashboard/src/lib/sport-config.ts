import type { StatType } from '@/types/predictions'

export type Sport = 'nba' | 'mlb'

export interface FilterTabDef {
  value: string
  label: string
}

export interface SportConfig {
  sport: Sport
  label: string
  predictionsTable: string
  paperBetsTable: string
  dailyLogTable: string
  predictionSamplesTable: string
  statTypes: StatType[]
  filterTabs: FilterTabDef[]
  comboStats: Set<StatType>
  teams: Record<number, string>
  teamNameToAbbrev: Record<string, string>
  getHeadshotUrl: (playerId: number) => string
  getTeamLogoUrl: (teamAbbrev: string) => string
  // Model Picks optimal params (from backtest sweep)
  modelPicksEdge: number        // Global edge threshold when Model Picks is toggled on (NBA) or minimum (MLB)
  modelPicksTau: number | null  // BL tau for Model Picks (NBA global); null = per-stat server-side for MLB
  // Per-stat BL configs from mlb_stat_config.py (empty for NBA)
  perStatConfig: Record<string, {
    edge: number        // Min edge % required to recommend
    tau: number         // BL tau — how aggressively to lean on the model vs market
    z_max: number       // Z-score at which model confidence saturates to 1.0
    max_weight: number  // Hard cap on blending weight (model can't dominate more than this)
  }>
  features: {
    dfs: boolean
    askChat: boolean
    injuries: boolean
    scoreboard: boolean
    predictionMarkets: boolean
  }
  // Column mappings for predictions table (MLB uses different column names)
  columns: {
    q10: string
    q25: string
    q50: string
    q75: string
    q90: string
    propLine: string
    bestOverOdds: string
    bestUnderOdds: string
    bestOverBook: string
    bestUnderBook: string
    modelProbOver: string
    modelProbUnder: string
    impliedProbOver: string
    impliedProbUnder: string
  }
}

// ---------------------------------------------------------------------------
// NBA Config
// ---------------------------------------------------------------------------

const NBA_TEAMS: Record<number, string> = {
  1610612737: 'ATL', 1610612738: 'BOS', 1610612751: 'BKN',
  1610612766: 'CHA', 1610612741: 'CHI', 1610612739: 'CLE',
  1610612742: 'DAL', 1610612743: 'DEN', 1610612765: 'DET',
  1610612744: 'GSW', 1610612745: 'HOU', 1610612754: 'IND',
  1610612746: 'LAC', 1610612747: 'LAL', 1610612763: 'MEM',
  1610612748: 'MIA', 1610612749: 'MIL', 1610612750: 'MIN',
  1610612740: 'NOP', 1610612752: 'NYK', 1610612760: 'OKC',
  1610612753: 'ORL', 1610612755: 'PHI', 1610612756: 'PHX',
  1610612757: 'POR', 1610612758: 'SAC', 1610612759: 'SAS',
  1610612761: 'TOR', 1610612762: 'UTA', 1610612764: 'WAS',
}

const NBA_TEAM_NAME_TO_ABBREV: Record<string, string> = {
  'Atlanta Hawks': 'ATL', 'Boston Celtics': 'BOS', 'Brooklyn Nets': 'BKN',
  'Charlotte Hornets': 'CHA', 'Chicago Bulls': 'CHI', 'Cleveland Cavaliers': 'CLE',
  'Dallas Mavericks': 'DAL', 'Denver Nuggets': 'DEN', 'Detroit Pistons': 'DET',
  'Golden State Warriors': 'GSW', 'Houston Rockets': 'HOU', 'Indiana Pacers': 'IND',
  'Los Angeles Clippers': 'LAC', 'Los Angeles Lakers': 'LAL', 'Memphis Grizzlies': 'MEM',
  'Miami Heat': 'MIA', 'Milwaukee Bucks': 'MIL', 'Minnesota Timberwolves': 'MIN',
  'New Orleans Pelicans': 'NOP', 'New York Knicks': 'NYK', 'Oklahoma City Thunder': 'OKC',
  'Orlando Magic': 'ORL', 'Philadelphia 76ers': 'PHI', 'Phoenix Suns': 'PHX',
  'Portland Trail Blazers': 'POR', 'Sacramento Kings': 'SAC', 'San Antonio Spurs': 'SAS',
  'Toronto Raptors': 'TOR', 'Utah Jazz': 'UTA', 'Washington Wizards': 'WAS',
}

export const NBA_CONFIG: SportConfig = {
  sport: 'nba',
  label: 'NBA',
  predictionsTable: 'daily_predictions',
  paperBetsTable: 'paper_bets',
  dailyLogTable: 'paper_trading_daily_log',
  predictionSamplesTable: 'daily_prediction_samples',
  statTypes: ['pts', 'reb', 'ast', 'stl', 'blk', '3pm', 'pra', 'pr', 'pa', 'ra'],
  filterTabs: [
    { value: 'all', label: 'All Props' },
    { value: 'pts', label: 'Points' },
    { value: 'reb', label: 'Rebounds' },
    { value: 'ast', label: 'Assists' },
    { value: 'combos', label: 'Combos' },
  ],
  comboStats: new Set<StatType>(['pra', 'pr', 'pa', 'ra']),
  teams: NBA_TEAMS,
  teamNameToAbbrev: NBA_TEAM_NAME_TO_ABBREV,
  modelPicksEdge: 0.09,
  modelPicksTau: 0.50,
  perStatConfig: {},
  getHeadshotUrl: (playerId: number) =>
    `https://cdn.nba.com/headshots/nba/latest/1040x760/${playerId}.png`,
  getTeamLogoUrl: (teamAbbrev: string) => {
    const abbrevToId: Record<string, number> = {}
    for (const [id, abbrev] of Object.entries(NBA_TEAMS)) {
      abbrevToId[abbrev] = Number(id)
    }
    const teamId = abbrevToId[teamAbbrev]
    return teamId
      ? `https://cdn.nba.com/logos/nba/${teamId}/global/L/logo.svg`
      : ''
  },
  features: {
    dfs: true,
    askChat: true,
    injuries: true,
    scoreboard: true,
    predictionMarkets: false,
  },
  columns: {
    q10: 'q10',
    q25: 'q25',
    q50: 'q50',
    q75: 'q75',
    q90: 'q90',
    propLine: 'prop_line',
    bestOverOdds: 'best_over_odds',
    bestUnderOdds: 'best_under_odds',
    bestOverBook: 'best_over_book',
    bestUnderBook: 'best_under_book',
    modelProbOver: 'model_prob_over',
    modelProbUnder: 'model_prob_under',
    impliedProbOver: 'implied_prob_over',
    impliedProbUnder: 'implied_prob_under',
  },
}

// ---------------------------------------------------------------------------
// MLB Config
// ---------------------------------------------------------------------------

const MLB_TEAMS: Record<number, string> = {
  133: 'ATH', 144: 'ATL', 109: 'AZ', 110: 'BAL', 111: 'BOS',
  112: 'CHC', 113: 'CIN', 114: 'CLE', 115: 'COL', 145: 'CWS',
  116: 'DET', 117: 'HOU', 118: 'KC', 108: 'LAA', 119: 'LAD',
  146: 'MIA', 158: 'MIL', 142: 'MIN', 121: 'NYM', 147: 'NYY',
  143: 'PHI', 134: 'PIT', 135: 'SD', 136: 'SEA', 137: 'SF',
  138: 'STL', 139: 'TB', 140: 'TEX', 141: 'TOR', 120: 'WSH',
}

const MLB_TEAM_NAME_TO_ABBREV: Record<string, string> = {
  'Arizona Diamondbacks': 'AZ', 'Atlanta Braves': 'ATL', 'Baltimore Orioles': 'BAL',
  'Boston Red Sox': 'BOS', 'Chicago Cubs': 'CHC', 'Chicago White Sox': 'CWS',
  'Cincinnati Reds': 'CIN', 'Cleveland Guardians': 'CLE', 'Colorado Rockies': 'COL',
  'Detroit Tigers': 'DET', 'Houston Astros': 'HOU', 'Kansas City Royals': 'KC',
  'Los Angeles Angels': 'LAA', 'Los Angeles Dodgers': 'LAD', 'Miami Marlins': 'MIA',
  'Milwaukee Brewers': 'MIL', 'Minnesota Twins': 'MIN', 'New York Mets': 'NYM',
  'New York Yankees': 'NYY', 'Athletics': 'ATH', 'Philadelphia Phillies': 'PHI',
  'Pittsburgh Pirates': 'PIT', 'San Diego Padres': 'SD', 'San Francisco Giants': 'SF',
  'Seattle Mariners': 'SEA', 'St. Louis Cardinals': 'STL', 'Tampa Bay Rays': 'TB',
  'Texas Rangers': 'TEX', 'Toronto Blue Jays': 'TOR', 'Washington Nationals': 'WSH',
}

export const MLB_CONFIG: SportConfig = {
  sport: 'mlb',
  label: 'MLB',
  predictionsTable: 'mlb_daily_predictions',
  paperBetsTable: 'mlb_paper_bets',
  dailyLogTable: 'mlb_paper_trading_daily_log',
  predictionSamplesTable: 'mlb_daily_prediction_samples',
  statTypes: [
    'pitcher_strikeouts', 'batter_hits',
  ],
  filterTabs: [
    { value: 'all', label: 'All Props' },
    { value: 'pitcher_strikeouts', label: 'Strikeouts' },
    { value: 'batter_hits', label: 'Hits' },
  ],
  comboStats: new Set<StatType>(),
  teams: MLB_TEAMS,
  teamNameToAbbrev: MLB_TEAM_NAME_TO_ABBREV,
  modelPicksEdge: 0.05,   // Min edge across all MLB stats (strikeouts threshold)
  modelPicksTau: null,    // BL applied server-side per stat; null disables global client-side blending
  // From src/models/mlb/mlb_stat_config.py — STAT_BL_CONFIGS + MLB_STATS
  perStatConfig: {
    pitcher_strikeouts: { edge: 0.12, tau: 0.75, z_max: 0.25, max_weight: 0.80 },
    batter_hits:        { edge: 0.10, tau: 0.90, z_max: 0.25, max_weight: 0.50 },
  },
  getHeadshotUrl: (playerId: number) =>
    `https://img.mlbstatic.com/mlb-photos/image/upload/d_people:generic:headshot:67:current.png/w_213,q_auto:best/v1/people/${playerId}/headshot/67/current`,
  getTeamLogoUrl: (teamAbbrev: string) =>
    `https://a.espncdn.com/combiner/i?img=/i/teamlogos/mlb/500/${teamAbbrev.toLowerCase()}.png&h=40&w=40`,
  features: {
    dfs: false,
    askChat: true,
    injuries: false,
    scoreboard: true,
    predictionMarkets: false,
  },
  columns: {
    q10: 'pred_q10',
    q25: 'pred_q25',
    q50: 'pred_q50',
    q75: 'pred_q75',
    q90: 'pred_q90',
    propLine: 'line',
    bestOverOdds: 'over_odds',
    bestUnderOdds: 'under_odds',
    bestOverBook: 'bookmaker',
    bestUnderBook: 'bookmaker',
    modelProbOver: 'over_prob',
    modelProbUnder: 'under_prob',
    impliedProbOver: 'implied_over',
    impliedProbUnder: 'implied_under',
  },
}

// ---------------------------------------------------------------------------
// Config lookup
// ---------------------------------------------------------------------------

export const SPORT_CONFIGS: Record<Sport, SportConfig> = {
  nba: NBA_CONFIG,
  mlb: MLB_CONFIG,
}

export function getSportConfig(sport: Sport): SportConfig {
  return SPORT_CONFIGS[sport]
}
