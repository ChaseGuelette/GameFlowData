import type { StatType } from './predictions'

// ─── Slip Type Definitions (standard parlays only) ──────────────────────────
export const USER_DFS_SLIP_TYPES = {
  pp_2_power: { label: 'PP 2-Pick', legs: 2, payout: 3, breakEven: 0.577, platform: 'prizepicks' },
  ud_3_standard: { label: 'UD 3-Pick', legs: 3, payout: 6, breakEven: 0.550, platform: 'underdog' },
  ud_5_standard: { label: 'UD 5-Pick', legs: 5, payout: 20, breakEven: 0.549, platform: 'underdog' },
} as const

export type UserDfsSlipType = keyof typeof USER_DFS_SLIP_TYPES

// ─── Database Row Types ─────────────────────────────────────────────────────
export type EntryStatus = 'pending' | 'won' | 'lost' | 'push' | 'cancelled'

export interface UserDfsEntry {
  id: number
  user_id: string
  entry_date: string
  slip_type: UserDfsSlipType
  platform: string
  num_legs: number
  stake: number
  combined_prob: number
  kelly_fraction_used: number
  payout_multiplier: number
  edge_mode: string
  status: EntryStatus
  legs_won: number
  legs_lost: number
  legs_push: number
  legs_cancelled: number
  pnl: number
  placed_at: string
  resolved_at: string | null
}

export interface UserDfsLeg {
  id: number
  entry_id: number
  player_id: number
  player_name: string
  game_id: string
  stat_type: StatType
  line: number
  direction: 'over' | 'under'
  dfs_bookmaker: string
  model_prob: number | null
  market_prob: number | null
  edge: number
  status: EntryStatus
  actual_value: number | null
}

// ─── Slip Builder Draft Leg (in-memory before placement) ────────────────────
export interface SlipBuilderLeg {
  key: string // `${player_id}-${stat_type}-${bookmaker}-${line}`
  player_id: number
  player_name: string
  game_id: string
  stat_type: StatType
  line: number
  direction: 'over' | 'under'
  dfs_bookmaker: string
  model_prob: number | null
  market_prob: number | null
  edge: number
}

// ─── Entry with nested legs (for history display) ───────────────────────────
export interface UserDfsEntryWithLegs extends UserDfsEntry {
  legs: UserDfsLeg[]
}
