import type { ColumnDef } from '@/types/stats'

// ─── Player — Box Score ─────────────────────────────────────────────
export const playerBoxColumns: ColumnDef[] = [
  { key: 'gp', label: 'GP', dbColumn: 'games_{window}', format: 'int' },
  { key: 'mpg', label: 'MPG', dbColumn: 'avg_min_{window}', format: 'dec1' },
  { key: 'pts', label: 'PTS', dbColumn: 'avg_pts_{window}', format: 'dec1' },
  { key: 'reb', label: 'REB', dbColumn: 'avg_reb_{window}', format: 'dec1' },
  { key: 'ast', label: 'AST', dbColumn: 'avg_ast_{window}', format: 'dec1' },
  { key: 'stl', label: 'STL', dbColumn: 'avg_stl_{window}', format: 'dec1' },
  { key: 'blk', label: 'BLK', dbColumn: 'avg_blk_{window}', format: 'dec1' },
  { key: 'tov', label: 'TOV', dbColumn: 'avg_tov_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'pf', label: 'PF', dbColumn: 'avg_pf_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'pm', label: '+/-', dbColumn: 'avg_plus_minus_{window}', format: 'plusMinus1' },
]

// ─── Player — Shooting ──────────────────────────────────────────────
export const playerShootingColumns: ColumnDef[] = [
  { key: 'fgm', label: 'FGM', dbColumn: 'avg_fgm_{window}', format: 'dec1' },
  { key: 'fga', label: 'FGA', dbColumn: 'avg_fga_{window}', format: 'dec1' },
  { key: 'fgpct', label: 'FG%', dbColumn: 'avg_fg_pct_{window}', format: 'pct1' },
  { key: '3pm', label: '3PM', dbColumn: 'avg_fg3m_{window}', format: 'dec1' },
  { key: '3pa', label: '3PA', dbColumn: 'avg_fg3a_{window}', format: 'dec1' },
  { key: '3ppct', label: '3P%', dbColumn: 'avg_fg3_pct_{window}', format: 'pct1' },
  { key: 'ftm', label: 'FTM', dbColumn: 'avg_ftm_{window}', format: 'dec1' },
  { key: 'fta', label: 'FTA', dbColumn: 'avg_fta_{window}', format: 'dec1' },
  { key: 'ftpct', label: 'FT%', dbColumn: 'avg_ft_pct_{window}', format: 'pct1' },
  { key: 'efgpct', label: 'eFG%', dbColumn: 'avg_efg_pct_{window}', format: 'pct1' },
  { key: 'tspct', label: 'TS%', dbColumn: 'avg_ts_pct_{window}', format: 'pct1' },
]

// ─── Player — Advanced ──────────────────────────────────────────────
export const playerAdvancedColumns: ColumnDef[] = [
  { key: 'usg', label: 'USG%', dbColumn: 'avg_usg_pct_{window}', format: 'pct1' },
  { key: 'ortg', label: 'ORtg', dbColumn: 'avg_off_rtg_{window}', format: 'dec1' },
  { key: 'drtg', label: 'DRtg', dbColumn: 'avg_def_rtg_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'netrtg', label: 'NetRtg', dbColumn: 'avg_net_rtg_{window}', format: 'plusMinus1' },
  { key: 'astpct', label: 'AST%', dbColumn: 'avg_ast_pct_{window}', format: 'pct1' },
  { key: 'asttov', label: 'AST/TO', dbColumn: 'avg_ast_tov_{window}', format: 'dec2' },
  { key: 'tovpct', label: 'TOV%', dbColumn: 'avg_tov_ratio_{window}', format: 'pct1', invertHeatmap: true },
  { key: 'rebpct', label: 'REB%', dbColumn: 'avg_reb_pct_{window}', format: 'pct1' },
  { key: 'orebpct', label: 'OREB%', dbColumn: 'avg_oreb_pct_{window}', format: 'pct1' },
  { key: 'drebpct', label: 'DREB%', dbColumn: 'avg_dreb_pct_{window}', format: 'pct1' },
  { key: 'pace', label: 'Pace', dbColumn: 'avg_pace_{window}', format: 'dec1' },
  { key: 'pie', label: 'PIE', dbColumn: 'avg_pie_{window}', format: 'pct1' },
]

// ─── Player — Consistency (all windowless, L5 snapshot) ─────────────
export const playerConsistencyColumns: ColumnDef[] = [
  { key: 'std_pts', label: 'std PTS', dbColumn: 'std_pts_l5', format: 'dec1', invertHeatmap: true, windowless: true },
  { key: 'std_reb', label: 'std REB', dbColumn: 'std_reb_l5', format: 'dec1', invertHeatmap: true, windowless: true },
  { key: 'std_ast', label: 'std AST', dbColumn: 'std_ast_l5', format: 'dec1', invertHeatmap: true, windowless: true },
  { key: 'std_3pm', label: 'std 3PM', dbColumn: 'std_fg3m_l5', format: 'dec1', invertHeatmap: true, windowless: true },
  { key: 'std_min', label: 'std MIN', dbColumn: 'std_min_l5', format: 'dec1', invertHeatmap: true, windowless: true },
  { key: 'min_floor', label: 'MIN Floor', dbColumn: 'min_floor_l5', format: 'dec1', windowless: true },
  { key: 'starts', label: 'Starts(L5)', dbColumn: 'games_started_l5', format: 'int', windowless: true },
  { key: 'rest', label: 'Rest', dbColumn: 'rest_days', format: 'int', windowless: true },
  { key: 'g7d', label: 'G/7d', dbColumn: 'games_last_7d', format: 'int', windowless: true },
]

// ─── Team — Offense ─────────────────────────────────────────────────
export const teamOffenseColumns: ColumnDef[] = [
  { key: 'pts', label: 'PTS', dbColumn: 'avg_pts_{window}', format: 'dec1' },
  { key: 'fgm', label: 'FGM', dbColumn: 'avg_fgm_{window}', format: 'dec1' },
  { key: 'fga', label: 'FGA', dbColumn: 'avg_fga_{window}', format: 'dec1' },
  { key: 'fgpct', label: 'FG%', dbColumn: 'avg_fg_pct_{window}', format: 'pct1' },
  { key: '3pm', label: '3PM', dbColumn: 'avg_fg3m_{window}', format: 'dec1' },
  { key: '3pa', label: '3PA', dbColumn: 'avg_fg3a_{window}', format: 'dec1' },
  { key: '3ppct', label: '3P%', dbColumn: 'avg_fg3_pct_{window}', format: 'pct1' },
  { key: 'ftm', label: 'FTM', dbColumn: 'avg_ftm_{window}', format: 'dec1' },
  { key: 'fta', label: 'FTA', dbColumn: 'avg_fta_{window}', format: 'dec1' },
  { key: 'ftpct', label: 'FT%', dbColumn: 'avg_ft_pct_{window}', format: 'pct1' },
  { key: 'ast', label: 'AST', dbColumn: 'avg_ast_{window}', format: 'dec1' },
  { key: 'oreb', label: 'OREB', dbColumn: 'avg_oreb_{window}', format: 'dec1' },
  { key: 'tov', label: 'TOV', dbColumn: 'avg_tov_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'ortg', label: 'ORtg', dbColumn: 'avg_off_rtg_{window}', format: 'dec1' },
  { key: 'efgpct', label: 'eFG%', dbColumn: 'avg_efg_pct_{window}', format: 'pct1' },
  { key: 'tspct', label: 'TS%', dbColumn: 'avg_ts_pct_{window}', format: 'pct1' },
  { key: 'pace', label: 'Pace', dbColumn: 'avg_pace_{window}', format: 'dec1' },
]

// ─── Team — Defense ─────────────────────────────────────────────────
export const teamDefenseColumns: ColumnDef[] = [
  { key: 'drtg', label: 'DRtg', dbColumn: 'avg_def_rtg_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'reb', label: 'REB', dbColumn: 'avg_reb_{window}', format: 'dec1' },
  { key: 'dreb', label: 'DREB', dbColumn: 'avg_dreb_{window}', format: 'dec1' },
  { key: 'stl', label: 'STL', dbColumn: 'avg_stl_{window}', format: 'dec1' },
  { key: 'blk', label: 'BLK', dbColumn: 'avg_blk_{window}', format: 'dec1' },
  { key: 'tov_forced', label: 'TOV Forced', dbColumn: 'avg_tov_{window}', format: 'dec1' },
  { key: 'pm', label: '+/-', dbColumn: 'avg_plus_minus_{window}', format: 'plusMinus1' },
]

// ─── Team — Overall ─────────────────────────────────────────────────
export const teamOverallColumns: ColumnDef[] = [
  { key: 'gp', label: 'GP', dbColumn: 'games_{window}', format: 'int' },
  { key: 'netrtg', label: 'NetRtg', dbColumn: 'avg_net_rtg_{window}', format: 'plusMinus1' },
  { key: 'pie', label: 'PIE', dbColumn: 'avg_pie_{window}', format: 'pct1' },
  { key: 'pace', label: 'Pace', dbColumn: 'avg_pace_{window}', format: 'dec1' },
  { key: 'poss', label: 'Poss', dbColumn: 'avg_poss_{window}', format: 'dec1' },
  { key: 'rebpct', label: 'REB%', dbColumn: 'avg_reb_pct_{window}', format: 'pct1' },
  { key: 'ast_ratio', label: 'AST Ratio', dbColumn: 'avg_ast_ratio_{window}', format: 'dec1' },
  { key: 'tov_ratio', label: 'TOV Ratio', dbColumn: 'avg_tov_ratio_{window}', format: 'dec1', invertHeatmap: true },
]

// ─── Defense vs Position — Totals ───────────────────────────────────
export const defenseTotalsColumns: ColumnDef[] = [
  { key: 'gp', label: 'GP', dbColumn: 'games_{window}', format: 'int' },
  { key: 'pts', label: 'PTS', dbColumn: 'pts_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'reb', label: 'REB', dbColumn: 'reb_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'ast', label: 'AST', dbColumn: 'ast_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'stl', label: 'STL', dbColumn: 'stl_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'blk', label: 'BLK', dbColumn: 'blk_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: '3pt', label: '3PT', dbColumn: 'threes_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'tov_forced', label: 'TOV Forced', dbColumn: 'tov_forced_{window}', format: 'dec1' },
  { key: 'fta', label: 'FTA', dbColumn: 'fta_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'oreb', label: 'OREB', dbColumn: 'oreb_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'pf', label: 'PF', dbColumn: 'pf_allowed_{window}', format: 'dec1' },
  { key: 'ortg_allowed', label: 'ORtg Allowed', dbColumn: 'off_rtg_allowed_{window}', format: 'dec1', invertHeatmap: true },
]

// ─── Defense vs Position — Per 100 Possessions ──────────────────────
export const defensePer100Columns: ColumnDef[] = [
  { key: 'reb_p100', label: 'REB', dbColumn: 'reb_per100_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'ast_p100', label: 'AST', dbColumn: 'ast_per100_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'stl_p100', label: 'STL', dbColumn: 'stl_per100_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'blk_p100', label: 'BLK', dbColumn: 'blk_per100_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: '3pt_p100', label: '3PT', dbColumn: 'threes_per100_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'tov_p100', label: 'TOV Forced', dbColumn: 'tov_per100_forced_{window}', format: 'dec1' },
  { key: 'fta_p100', label: 'FTA', dbColumn: 'fta_per100_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'oreb_p100', label: 'OREB', dbColumn: 'oreb_per100_allowed_{window}', format: 'dec1', invertHeatmap: true },
  { key: 'pf_p100', label: 'PF', dbColumn: 'pf_per100_allowed_{window}', format: 'dec1' },
  { key: 'ortg_p100', label: 'ORtg Allowed', dbColumn: 'off_rtg_allowed_{window}', format: 'dec1', invertHeatmap: true },
]

// ─── Lookup helpers ─────────────────────────────────────────────────
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
