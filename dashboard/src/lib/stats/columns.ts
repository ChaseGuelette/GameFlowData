import type { ColumnDef } from '@/types/stats'

// ─── Player — Box Score ─────────────────────────────────────────────
export const playerBoxColumns: ColumnDef[] = [
  { key: 'gp', label: 'GP', tooltip: 'Games Played', dbColumn: 'games_{window}', format: 'int' },
  { key: 'mpg', label: 'MPG', tooltip: 'Minutes Per Game', dbColumn: 'avg_min_{window}', format: 'dec1' },
  { key: 'pts', label: 'PTS', tooltip: 'Points Per Game', dbColumn: 'avg_pts_{window}', format: 'dec1' },
  { key: 'reb', label: 'REB', tooltip: 'Rebounds Per Game', dbColumn: 'avg_reb_{window}', format: 'dec1' },
  { key: 'ast', label: 'AST', tooltip: 'Assists Per Game', dbColumn: 'avg_ast_{window}', format: 'dec1' },
  { key: 'stl', label: 'STL', tooltip: 'Steals Per Game', dbColumn: 'avg_stl_{window}', format: 'dec1' },
  { key: 'blk', label: 'BLK', tooltip: 'Blocks Per Game', dbColumn: 'avg_blk_{window}', format: 'dec1' },
  { key: 'tov', label: 'TOV', tooltip: 'Turnovers Per Game', dbColumn: 'avg_tov_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'pf', label: 'PF', tooltip: 'Personal Fouls Per Game', dbColumn: 'avg_pf_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'pm', label: '+/-', tooltip: 'Plus/Minus — point differential while on court', dbColumn: 'avg_plus_minus_{window}', format: 'plusMinus1' },
]

// ─── Player — Shooting ──────────────────────────────────────────────
export const playerShootingColumns: ColumnDef[] = [
  { key: 'fgm', label: 'FGM', tooltip: 'Field Goals Made Per Game', dbColumn: 'avg_fgm_{window}', format: 'dec1' },
  { key: 'fga', label: 'FGA', tooltip: 'Field Goals Attempted Per Game', dbColumn: 'avg_fga_{window}', format: 'dec1' },
  { key: 'fgpct', label: 'FG%', tooltip: 'Field Goal Percentage', dbColumn: 'avg_fg_pct_{window}', format: 'pct1' },
  { key: '3pm', label: '3PM', tooltip: 'Three-Pointers Made Per Game', dbColumn: 'avg_fg3m_{window}', format: 'dec1' },
  { key: '3pa', label: '3PA', tooltip: 'Three-Pointers Attempted Per Game', dbColumn: 'avg_fg3a_{window}', format: 'dec1' },
  { key: '3ppct', label: '3P%', tooltip: 'Three-Point Percentage', dbColumn: 'avg_fg3_pct_{window}', format: 'pct1' },
  { key: 'ftm', label: 'FTM', tooltip: 'Free Throws Made Per Game', dbColumn: 'avg_ftm_{window}', format: 'dec1' },
  { key: 'fta', label: 'FTA', tooltip: 'Free Throws Attempted Per Game', dbColumn: 'avg_fta_{window}', format: 'dec1' },
  { key: 'ftpct', label: 'FT%', tooltip: 'Free Throw Percentage', dbColumn: 'avg_ft_pct_{window}', format: 'pct1' },
  { key: 'efgpct', label: 'eFG%', tooltip: 'Effective Field Goal % — adjusts for 3-pointers', dbColumn: 'avg_efg_pct_{window}', format: 'pct1' },
  { key: 'tspct', label: 'TS%', tooltip: 'True Shooting % — accounts for FTs and 3-pointers', dbColumn: 'avg_ts_pct_{window}', format: 'pct1' },
]

// ─── Player — Advanced ──────────────────────────────────────────────
export const playerAdvancedColumns: ColumnDef[] = [
  { key: 'usg', label: 'USG%', tooltip: 'Usage Rate — % of team plays used while on court', dbColumn: 'avg_usg_pct_{window}', format: 'pct1' },
  { key: 'ortg', label: 'ORtg', tooltip: 'Offensive Rating — points produced per 100 possessions', dbColumn: 'avg_off_rtg_{window}', format: 'dec1' },
  { key: 'drtg', label: 'DRtg', tooltip: 'Defensive Rating — points allowed per 100 possessions', dbColumn: 'avg_def_rtg_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'netrtg', label: 'NetRtg', tooltip: 'Net Rating — ORtg minus DRtg', dbColumn: 'avg_net_rtg_{window}', format: 'plusMinus1' },
  { key: 'astpct', label: 'AST%', tooltip: 'Assist Percentage — % of teammate FGs assisted while on court', dbColumn: 'avg_ast_pct_{window}', format: 'pct1' },
  { key: 'asttov', label: 'AST/TO', tooltip: 'Assist-to-Turnover Ratio', dbColumn: 'avg_ast_tov_{window}', format: 'dec2' },
  { key: 'tovpct', label: 'TOV%', tooltip: 'Turnover Rate — turnovers per 100 plays', dbColumn: 'avg_tov_ratio_{window}', format: 'rawPct1', invertHeatmap: true },
  { key: 'rebpct', label: 'REB%', tooltip: 'Rebound Percentage — % of available rebounds grabbed', dbColumn: 'avg_reb_pct_{window}', format: 'pct1' },
  { key: 'orebpct', label: 'OREB%', tooltip: 'Offensive Rebound Percentage', dbColumn: 'avg_oreb_pct_{window}', format: 'pct1' },
  { key: 'drebpct', label: 'DREB%', tooltip: 'Defensive Rebound Percentage', dbColumn: 'avg_dreb_pct_{window}', format: 'pct1' },
  { key: 'pace', label: 'Pace', tooltip: 'Pace — possessions per 48 minutes', dbColumn: 'avg_pace_{window}', format: 'dec1' },
  { key: 'pie', label: 'PIE', tooltip: 'Player Impact Estimate — overall contribution metric', dbColumn: 'avg_pie_{window}', format: 'pct1' },
]

// ─── Player — Consistency (all windowless, L5 snapshot) ─────────────
export const playerConsistencyColumns: ColumnDef[] = [
  { key: 'std_pts', label: 'std PTS', tooltip: 'Standard deviation of points (L5) — lower is more consistent', dbColumn: 'std_pts_l5', format: 'dec1', invertHeatmap: true, windowless: true },
  { key: 'std_reb', label: 'std REB', tooltip: 'Standard deviation of rebounds (L5)', dbColumn: 'std_reb_l5', format: 'dec1', invertHeatmap: true, windowless: true },
  { key: 'std_ast', label: 'std AST', tooltip: 'Standard deviation of assists (L5)', dbColumn: 'std_ast_l5', format: 'dec1', invertHeatmap: true, windowless: true },
  { key: 'std_3pm', label: 'std 3PM', tooltip: 'Standard deviation of 3-pointers made (L5)', dbColumn: 'std_fg3m_l5', format: 'dec1', invertHeatmap: true, windowless: true },
  { key: 'std_min', label: 'std MIN', tooltip: 'Standard deviation of minutes (L5)', dbColumn: 'std_min_l5', format: 'dec1', invertHeatmap: true, windowless: true },
  { key: 'min_floor', label: 'MIN Floor', tooltip: 'Lowest minutes played in last 5 games', dbColumn: 'min_floor_l5', format: 'dec1', windowless: true },
  { key: 'starts', label: 'Starts(L5)', tooltip: 'Games started in last 5 (20+ min)', dbColumn: 'games_started_l5', format: 'int', windowless: true },
  { key: 'rest', label: 'Rest', tooltip: 'Days since last game', dbColumn: 'rest_days', format: 'int', windowless: true },
  { key: 'g7d', label: 'G/7d', tooltip: 'Games played in last 7 days', dbColumn: 'games_last_7d', format: 'int', windowless: true },
]

// ─── Team — Offense ─────────────────────────────────────────────────
export const teamOffenseColumns: ColumnDef[] = [
  { key: 'pts', label: 'PTS', tooltip: 'Points Per Game', dbColumn: 'avg_pts_{window}', format: 'dec1' },
  { key: 'fgm', label: 'FGM', tooltip: 'Field Goals Made Per Game', dbColumn: 'avg_fgm_{window}', format: 'dec1' },
  { key: 'fga', label: 'FGA', tooltip: 'Field Goals Attempted Per Game', dbColumn: 'avg_fga_{window}', format: 'dec1' },
  { key: 'fgpct', label: 'FG%', tooltip: 'Field Goal Percentage', dbColumn: 'avg_fg_pct_{window}', format: 'pct1' },
  { key: '3pm', label: '3PM', tooltip: 'Three-Pointers Made Per Game', dbColumn: 'avg_fg3m_{window}', format: 'dec1' },
  { key: '3pa', label: '3PA', tooltip: 'Three-Pointers Attempted Per Game', dbColumn: 'avg_fg3a_{window}', format: 'dec1' },
  { key: '3ppct', label: '3P%', tooltip: 'Three-Point Percentage', dbColumn: 'avg_fg3_pct_{window}', format: 'pct1' },
  { key: 'ftm', label: 'FTM', tooltip: 'Free Throws Made Per Game', dbColumn: 'avg_ftm_{window}', format: 'dec1' },
  { key: 'fta', label: 'FTA', tooltip: 'Free Throws Attempted Per Game', dbColumn: 'avg_fta_{window}', format: 'dec1' },
  { key: 'ftpct', label: 'FT%', tooltip: 'Free Throw Percentage', dbColumn: 'avg_ft_pct_{window}', format: 'pct1' },
  { key: 'ast', label: 'AST', tooltip: 'Assists Per Game', dbColumn: 'avg_ast_{window}', format: 'dec1' },
  { key: 'oreb', label: 'OREB', tooltip: 'Offensive Rebounds Per Game', dbColumn: 'avg_oreb_{window}', format: 'dec1' },
  { key: 'tov', label: 'TOV', tooltip: 'Turnovers Per Game', dbColumn: 'avg_tov_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'ortg', label: 'ORtg', tooltip: 'Offensive Rating — points per 100 possessions', dbColumn: 'avg_off_rtg_{window}', format: 'dec1' },
  { key: 'efgpct', label: 'eFG%', tooltip: 'Effective Field Goal % — adjusts for 3-pointers', dbColumn: 'avg_efg_pct_{window}', format: 'pct1' },
  { key: 'tspct', label: 'TS%', tooltip: 'True Shooting % — accounts for FTs and 3-pointers', dbColumn: 'avg_ts_pct_{window}', format: 'pct1' },
  { key: 'pace', label: 'Pace', tooltip: 'Pace — possessions per 48 minutes', dbColumn: 'avg_pace_{window}', format: 'dec1' },
]

// ─── Team — Defense ─────────────────────────────────────────────────
export const teamDefenseColumns: ColumnDef[] = [
  { key: 'drtg', label: 'DRtg', tooltip: 'Defensive Rating — points allowed per 100 possessions', dbColumn: 'avg_def_rtg_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'reb', label: 'REB', tooltip: 'Rebounds Per Game', dbColumn: 'avg_reb_{window}', format: 'dec1' },
  { key: 'dreb', label: 'DREB', tooltip: 'Defensive Rebounds Per Game', dbColumn: 'avg_dreb_{window}', format: 'dec1' },
  { key: 'stl', label: 'STL', tooltip: 'Steals Per Game', dbColumn: 'avg_stl_{window}', format: 'dec1' },
  { key: 'blk', label: 'BLK', tooltip: 'Blocks Per Game', dbColumn: 'avg_blk_{window}', format: 'dec1' },
  { key: 'tov_forced', label: 'TOV Forced', tooltip: 'Turnovers Forced Per Game', dbColumn: 'avg_tov_{window}', format: 'dec1' },
  { key: 'pm', label: '+/-', tooltip: 'Plus/Minus — point differential', dbColumn: 'avg_plus_minus_{window}', format: 'plusMinus1' },
]

// ─── Team — Overall ─────────────────────────────────────────────────
export const teamOverallColumns: ColumnDef[] = [
  { key: 'gp', label: 'GP', tooltip: 'Games Played', dbColumn: 'games_{window}', format: 'int' },
  { key: 'netrtg', label: 'NetRtg', tooltip: 'Net Rating — ORtg minus DRtg', dbColumn: 'avg_net_rtg_{window}', format: 'plusMinus1' },
  { key: 'pie', label: 'PIE', tooltip: 'Player Impact Estimate — overall contribution', dbColumn: 'avg_pie_{window}', format: 'pct1' },
  { key: 'pace', label: 'Pace', tooltip: 'Pace — possessions per 48 minutes', dbColumn: 'avg_pace_{window}', format: 'dec1' },
  { key: 'poss', label: 'Poss', tooltip: 'Possessions Per Game', dbColumn: 'avg_poss_{window}', format: 'dec1' },
  { key: 'rebpct', label: 'REB%', tooltip: 'Rebound Percentage', dbColumn: 'avg_reb_pct_{window}', format: 'pct1' },
  { key: 'ast_ratio', label: 'AST Ratio', tooltip: 'Assist Ratio — assists per 100 possessions', dbColumn: 'avg_ast_ratio_{window}', format: 'dec1' },
  { key: 'tov_ratio', label: 'TOV%', tooltip: 'Turnover Rate — turnovers per 100 possessions', dbColumn: 'avg_tov_ratio_{window}', format: 'rawPct1', invertHeatmap: true },
]

// ─── Defense vs Position — Totals ───────────────────────────────────
export const defenseTotalsColumns: ColumnDef[] = [
  { key: 'gp', label: 'GP', tooltip: 'Games Played', dbColumn: 'games_{window}', format: 'int' },
  { key: 'pts', label: 'PTS', tooltip: 'Points Allowed Per Game', dbColumn: 'pts_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'reb', label: 'REB', tooltip: 'Rebounds Allowed Per Game', dbColumn: 'reb_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'ast', label: 'AST', tooltip: 'Assists Allowed Per Game', dbColumn: 'ast_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'stl', label: 'STL', tooltip: 'Steals Allowed Per Game', dbColumn: 'stl_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'blk', label: 'BLK', tooltip: 'Blocks Allowed Per Game', dbColumn: 'blk_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: '3pt', label: '3PT', tooltip: 'Three-Pointers Allowed Per Game', dbColumn: 'threes_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'tov_forced', label: 'TOV Forced', tooltip: 'Turnovers Forced Per Game', dbColumn: 'tov_forced_{window}', format: 'dec1' },
  { key: 'fta', label: 'FTA', tooltip: 'Free Throw Attempts Allowed Per Game', dbColumn: 'fta_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'oreb', label: 'OREB', tooltip: 'Offensive Rebounds Allowed Per Game', dbColumn: 'oreb_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'pf', label: 'PF', tooltip: 'Personal Fouls Per Game', dbColumn: 'pf_allowed_{window}', format: 'dec1' },
  { key: 'ortg_allowed', label: 'ORtg Allowed', tooltip: 'Offensive Rating Allowed — opponent points per 100 poss', dbColumn: 'off_rtg_allowed_{window}', format: 'dec1', invertHeatmap: true },
]

// ─── Defense vs Position — Per 100 Possessions ──────────────────────
export const defensePer100Columns: ColumnDef[] = [
  { key: 'reb_p100', label: 'REB', tooltip: 'Rebounds Allowed Per 100 Possessions', dbColumn: 'reb_per100_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'ast_p100', label: 'AST', tooltip: 'Assists Allowed Per 100 Possessions', dbColumn: 'ast_per100_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'stl_p100', label: 'STL', tooltip: 'Steals Allowed Per 100 Possessions', dbColumn: 'stl_per100_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'blk_p100', label: 'BLK', tooltip: 'Blocks Allowed Per 100 Possessions', dbColumn: 'blk_per100_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: '3pt_p100', label: '3PT', tooltip: 'Three-Pointers Allowed Per 100 Possessions', dbColumn: 'threes_per100_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'tov_p100', label: 'TOV Forced', tooltip: 'Turnovers Forced Per 100 Possessions', dbColumn: 'tov_per100_forced_{window}', format: 'dec1' },
  { key: 'fta_p100', label: 'FTA', tooltip: 'Free Throw Attempts Allowed Per 100 Possessions', dbColumn: 'fta_per100_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'oreb_p100', label: 'OREB', tooltip: 'Offensive Rebounds Allowed Per 100 Possessions', dbColumn: 'oreb_per100_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'pf_p100', label: 'PF', tooltip: 'Personal Fouls Per 100 Possessions', dbColumn: 'pf_per100_allowed_{window}', format: 'dec1' },
  { key: 'ortg_p100', label: 'ORtg Allowed', tooltip: 'Offensive Rating Allowed Per 100 Possessions', dbColumn: 'off_rtg_allowed_{window}', format: 'dec1', invertHeatmap: true },
]

// ─── Play Types — Frequency (POSS_PCT per play type) ────────────────
export const playTypeFrequencyColumns: ColumnDef[] = [
  { key: 'isolation_poss_pct', label: 'ISO', tooltip: 'Isolation — % of possessions', dbColumn: 'isolation_poss_pct', format: 'rawPct1', windowless: true },
  { key: 'transition_poss_pct', label: 'TRANS', tooltip: 'Transition — % of possessions', dbColumn: 'transition_poss_pct', format: 'rawPct1', windowless: true },
  { key: 'prballhandler_poss_pct', label: 'PnR BH', tooltip: 'Pick & Roll Ball Handler — % of possessions', dbColumn: 'prballhandler_poss_pct', format: 'rawPct1', windowless: true },
  { key: 'prrollman_poss_pct', label: 'PnR RM', tooltip: 'Pick & Roll Roll Man — % of possessions', dbColumn: 'prrollman_poss_pct', format: 'rawPct1', windowless: true },
  { key: 'postup_poss_pct', label: 'POST', tooltip: 'Post Up — % of possessions', dbColumn: 'postup_poss_pct', format: 'rawPct1', windowless: true },
  { key: 'spotup_poss_pct', label: 'SPOT', tooltip: 'Spot Up — % of possessions', dbColumn: 'spotup_poss_pct', format: 'rawPct1', windowless: true },
  { key: 'handoff_poss_pct', label: 'HAND', tooltip: 'Handoff — % of possessions', dbColumn: 'handoff_poss_pct', format: 'rawPct1', windowless: true },
  { key: 'cut_poss_pct', label: 'CUT', tooltip: 'Cut — % of possessions', dbColumn: 'cut_poss_pct', format: 'rawPct1', windowless: true },
  { key: 'offscreen_poss_pct', label: 'OSCREEN', tooltip: 'Off Screen — % of possessions', dbColumn: 'offscreen_poss_pct', format: 'rawPct1', windowless: true },
  { key: 'offrebound_poss_pct', label: 'OREB', tooltip: 'Off Rebound (Putbacks) — % of possessions', dbColumn: 'offrebound_poss_pct', format: 'rawPct1', windowless: true },
  { key: 'misc_poss_pct', label: 'MISC', tooltip: 'Miscellaneous — % of possessions', dbColumn: 'misc_poss_pct', format: 'rawPct1', windowless: true },
]

// ─── Play Types — Efficiency (PPP per play type) ────────────────────
export const playTypeEfficiencyColumns: ColumnDef[] = [
  { key: 'isolation_ppp', label: 'ISO', tooltip: 'Isolation — Points Per Possession', dbColumn: 'isolation_ppp', format: 'dec2', windowless: true },
  { key: 'transition_ppp', label: 'TRANS', tooltip: 'Transition — Points Per Possession', dbColumn: 'transition_ppp', format: 'dec2', windowless: true },
  { key: 'prballhandler_ppp', label: 'PnR BH', tooltip: 'Pick & Roll Ball Handler — Points Per Possession', dbColumn: 'prballhandler_ppp', format: 'dec2', windowless: true },
  { key: 'prrollman_ppp', label: 'PnR RM', tooltip: 'Pick & Roll Roll Man — Points Per Possession', dbColumn: 'prrollman_ppp', format: 'dec2', windowless: true },
  { key: 'postup_ppp', label: 'POST', tooltip: 'Post Up — Points Per Possession', dbColumn: 'postup_ppp', format: 'dec2', windowless: true },
  { key: 'spotup_ppp', label: 'SPOT', tooltip: 'Spot Up — Points Per Possession', dbColumn: 'spotup_ppp', format: 'dec2', windowless: true },
  { key: 'handoff_ppp', label: 'HAND', tooltip: 'Handoff — Points Per Possession', dbColumn: 'handoff_ppp', format: 'dec2', windowless: true },
  { key: 'cut_ppp', label: 'CUT', tooltip: 'Cut — Points Per Possession', dbColumn: 'cut_ppp', format: 'dec2', windowless: true },
  { key: 'offscreen_ppp', label: 'OSCREEN', tooltip: 'Off Screen — Points Per Possession', dbColumn: 'offscreen_ppp', format: 'dec2', windowless: true },
  { key: 'offrebound_ppp', label: 'OREB', tooltip: 'Off Rebound (Putbacks) — Points Per Possession', dbColumn: 'offrebound_ppp', format: 'dec2', windowless: true },
  { key: 'misc_ppp', label: 'MISC', tooltip: 'Miscellaneous — Points Per Possession', dbColumn: 'misc_ppp', format: 'dec2', windowless: true },
]

// ─── Lookup helpers ─────────────────────────────────────────────────
export const playTypeColumnMap = {
  frequency: playTypeFrequencyColumns,
  efficiency: playTypeEfficiencyColumns,
} as const

export const playerColumnMap = {
  box: playerBoxColumns,
  shooting: playerShootingColumns,
  advanced: playerAdvancedColumns,
  consistency: playerConsistencyColumns,
} as const

export const teamColumnMap = {
  offense: teamOffenseColumns,
  defense: teamDefenseColumns,
  overall: teamOverallColumns,
} as const

export const defenseColumnMap = {
  totals: defenseTotalsColumns,
  per100: defensePer100Columns,
} as const
