export type WindowSuffix = 'l5' | 'l15' | 'szn'
export type StatsMainTab = 'players' | 'teams' | 'defense'
export type PlayerCategory = 'box' | 'shooting' | 'advanced' | 'consistency'
export type TeamCategory = 'offense' | 'defense' | 'overall'
export type DefenseCategory = 'totals' | 'per100'
export type PositionGroup = 'G' | 'W' | 'B'

export interface ColumnDef {
  key: string
  label: string
  tooltip?: string
  /** Template: "avg_pts_{window}" — {window} replaced at render */
  dbColumn: string
  format: 'int' | 'dec1' | 'dec2' | 'pct1' | 'plusMinus1'
  /** true for TOV, DRtg, PF (higher = worse) */
  invertHeatmap?: boolean
  /** true for columns that don't vary by window */
  windowless?: boolean
}

export type StatRow = Record<string, string | number | null> & {
  id: string | number
  name: string
  teamAbbrev?: string
  position?: string
}

export interface SortState {
  column: string
  direction: 'asc' | 'desc'
}
