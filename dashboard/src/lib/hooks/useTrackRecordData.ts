'use client'

import { useQuery } from '@tanstack/react-query'
import { createClient } from '@/lib/supabase/client'
import { useSport } from '@/contexts/SportContext'
import { type DailyPerformance, type PaperBet, type StatPerformance } from '@/types/predictions'

export type TrackRecordSource = 'my_bets' | 'paper' | 'combined'

export interface MonthlyAggregate {
  month: string           // 'YYYY-MM'
  monthLabel: string      // 'March 2026'
  total_bets: number
  wins: number
  losses: number
  pushes: number
  total_pnl: number
  total_staked: number
  roi_pct: number
  cumulative_pnl: number
}

export interface TrackRecordKPIs {
  totalPnl: number
  totalStaked: number
  roi: number
  wins: number
  losses: number
  pushes: number
  profitableMonths: number
  totalMonths: number
}

export interface TrackRecordData {
  dailyLog: DailyPerformance[]
  bets: PaperBet[]
  monthlyAggregates: MonthlyAggregate[]
  statBreakdown: StatPerformance[]
  kpis: TrackRecordKPIs
}

// ── Month label formatter ────────────────────────────────────────────────────

const MONTH_NAMES = [
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December',
]

function formatMonthLabel(yyyyMm: string): string {
  const [year, month] = yyyyMm.split('-')
  return `${MONTH_NAMES[parseInt(month) - 1]} ${year}`
}

// ── Aggregation helpers ──────────────────────────────────────────────────────

function buildMonthlyAggregates(daily: DailyPerformance[]): MonthlyAggregate[] {
  const byMonth = new Map<string, MonthlyAggregate>()

  for (const row of daily) {
    const month = row.game_date.slice(0, 7) // 'YYYY-MM'
    const existing = byMonth.get(month)
    if (existing) {
      existing.total_bets   += row.total_bets
      existing.wins         += row.bets_won
      existing.losses       += row.bets_lost
      existing.pushes       += row.bets_push
      existing.total_pnl    += row.total_pnl
      existing.total_staked += row.total_staked
      // cumulative_pnl = last value for this month
      existing.cumulative_pnl = row.cumulative_pnl
    } else {
      byMonth.set(month, {
        month,
        monthLabel: formatMonthLabel(month),
        total_bets:    row.total_bets,
        wins:          row.bets_won,
        losses:        row.bets_lost,
        pushes:        row.bets_push,
        total_pnl:     row.total_pnl,
        total_staked:  row.total_staked,
        roi_pct:       0,
        cumulative_pnl: row.cumulative_pnl,
      })
    }
  }

  // Recalculate roi_pct per month
  for (const agg of byMonth.values()) {
    agg.roi_pct = agg.total_staked > 0
      ? (agg.total_pnl / agg.total_staked) * 100
      : 0
  }

  return [...byMonth.values()].sort((a, b) => b.month.localeCompare(a.month))
}

function buildKPIs(monthly: MonthlyAggregate[], daily: DailyPerformance[]): TrackRecordKPIs {
  const totalPnl     = monthly.reduce((s, m) => s + m.total_pnl, 0)
  const totalStaked  = monthly.reduce((s, m) => s + m.total_staked, 0)
  const wins         = monthly.reduce((s, m) => s + m.wins, 0)
  const losses       = monthly.reduce((s, m) => s + m.losses, 0)
  const pushes       = monthly.reduce((s, m) => s + m.pushes, 0)
  const roi          = totalStaked > 0 ? (totalPnl / totalStaked) * 100 : 0
  const profitableMonths = monthly.filter(m => m.total_pnl > 0).length

  void daily // may be used for streak info in the future

  return {
    totalPnl,
    totalStaked,
    roi,
    wins,
    losses,
    pushes,
    profitableMonths,
    totalMonths: monthly.length,
  }
}

function buildStatBreakdown(bets: PaperBet[]): StatPerformance[] {
  const byStatMap = new Map<string, { wins: number; losses: number; pnl: number; staked: number }>()

  for (const bet of bets) {
    if (!['won', 'lost', 'push'].includes(bet.status)) continue
    const entry = byStatMap.get(bet.stat_type) ?? { wins: 0, losses: 0, pnl: 0, staked: 0 }
    if (bet.status === 'won') entry.wins++
    if (bet.status === 'lost') entry.losses++
    entry.pnl    += bet.pnl ?? 0
    entry.staked += bet.stake
    byStatMap.set(bet.stat_type, entry)
  }

  return [...byStatMap.entries()].map(([stat, data]) => {
    const total = data.wins + data.losses
    return {
      stat: stat as PaperBet['stat_type'],
      total_bets: total,
      wins: data.wins,
      losses: data.losses,
      win_rate: total > 0 ? (data.wins / total) * 100 : 0,
      total_pnl: data.pnl,
      roi: data.staked > 0 ? (data.pnl / data.staked) * 100 : 0,
    }
  }).sort((a, b) => b.total_bets - a.total_bets)
}

// ── Merge two daily logs (by date) ───────────────────────────────────────────

function mergeDailyLogs(a: DailyPerformance[], b: DailyPerformance[]): DailyPerformance[] {
  const map = new Map<string, DailyPerformance>()

  for (const row of [...a, ...b]) {
    const existing = map.get(row.game_date)
    if (existing) {
      map.set(row.game_date, {
        game_date:      row.game_date,
        total_bets:     existing.total_bets + row.total_bets,
        bets_won:       existing.bets_won + row.bets_won,
        bets_lost:      existing.bets_lost + row.bets_lost,
        bets_push:      existing.bets_push + row.bets_push,
        total_staked:   existing.total_staked + row.total_staked,
        total_pnl:      existing.total_pnl + row.total_pnl,
        cumulative_pnl: 0, // recalculated below
        bankroll_after: 0, // recalculated below
        roi_pct:        0,
      })
    } else {
      map.set(row.game_date, { ...row })
    }
  }

  // Re-chain cumulative_pnl and bankroll_after
  const sorted = [...map.values()].sort((a, b) => a.game_date.localeCompare(b.game_date))
  let cumulative = 0
  const firstBankroll = sorted[0]?.bankroll_after ?? 1000
  // Estimate starting bankroll from first entry
  const startBankroll = firstBankroll - (sorted[0]?.total_pnl ?? 0)

  for (const row of sorted) {
    cumulative += row.total_pnl
    row.cumulative_pnl = cumulative
    row.bankroll_after = startBankroll + cumulative
    row.roi_pct = row.total_staked > 0 ? (row.total_pnl / row.total_staked) * 100 : 0
  }

  return sorted
}

// ── Fetch functions ──────────────────────────────────────────────────────────

async function fetchMyBetsData(statTypes: string[]) {
  const supabase = createClient()

  const [logRes, betsRes] = await Promise.all([
    supabase
      .from('user_bets_daily_log')
      .select('game_date, total_bets, bets_won, bets_lost, bets_push, total_staked, total_pnl, cumulative_pnl, bankroll_after, roi_pct')
      .order('game_date', { ascending: true }),
    supabase
      .from('user_bets')
      .select('id, game_date, player_id, player_name, stat_type, line, bet_direction, odds_at_bet, stake, edge, status, actual_value, pnl, book_at_bet, source')
      .in('stat_type', statTypes)
      .in('status', ['won', 'lost', 'push'])
      .order('game_date', { ascending: true }),
  ])

  const dailyLog = (logRes.data ?? []) as DailyPerformance[]
  const bets = (betsRes.data ?? []).map(row => ({
    id: row.id,
    game_date: row.game_date,
    player_id: row.player_id ?? 0,
    player_name: row.player_name,
    stat_type: row.stat_type,
    line: Number(row.line),
    bet_direction: row.bet_direction,
    odds_at_bet: row.odds_at_bet ? Number(row.odds_at_bet) : -110,
    stake: row.stake ? Number(row.stake) : 0,
    edge: row.edge ? Number(row.edge) : 0,
    status: row.status,
    actual_value: row.actual_value != null ? Number(row.actual_value) : null,
    pnl: row.pnl != null ? Number(row.pnl) : null,
    bookmaker: row.book_at_bet,
  })) as PaperBet[]

  if (logRes.error) throw logRes.error
  if (betsRes.error) throw betsRes.error

  return { dailyLog, bets }
}

async function fetchPaperData(config: {
  dailyLogTable: string
  paperBetsTable: string
}) {
  const supabase = createClient()

  const [logRes, betsRes] = await Promise.all([
    supabase
      .from(config.dailyLogTable)
      .select('game_date, total_bets, bets_won, bets_lost, bets_push, total_staked, total_pnl, cumulative_pnl, bankroll_after, roi_pct')
      .order('game_date', { ascending: true }),
    supabase
      .from(config.paperBetsTable)
      .select('id, game_date, player_id, player_name, stat_type, line, bet_direction, odds_at_bet, stake, edge, status, actual_value, pnl')
      .in('status', ['won', 'lost', 'push'])
      .order('game_date', { ascending: true }),
  ])

  const dailyLog = (logRes.data ?? []) as DailyPerformance[]
  const bets     = (betsRes.data ?? []) as PaperBet[]
  if (logRes.error) throw logRes.error
  if (betsRes.error) throw betsRes.error
  return { dailyLog, bets }
}

// ── Main hook ────────────────────────────────────────────────────────────────

async function fetchTrackRecord(
  source: TrackRecordSource,
  config: { dailyLogTable: string; paperBetsTable: string; statTypes: string[] }
): Promise<TrackRecordData> {
  let dailyLog: DailyPerformance[] = []
  let bets: PaperBet[] = []

  if (source === 'my_bets') {
    const result = await fetchMyBetsData(config.statTypes)
    dailyLog = result.dailyLog
    bets     = result.bets
  } else if (source === 'paper') {
    const result = await fetchPaperData(config)
    dailyLog = result.dailyLog
    bets     = result.bets
  } else {
    // combined
    const [myResult, paperResult] = await Promise.all([
      fetchMyBetsData(config.statTypes),
      fetchPaperData(config),
    ])
    dailyLog = mergeDailyLogs(myResult.dailyLog, paperResult.dailyLog)
    bets     = [...myResult.bets, ...paperResult.bets].sort(
      (a, b) => a.game_date.localeCompare(b.game_date)
    )
  }

  const monthlyAggregates = buildMonthlyAggregates(dailyLog)
  const kpis              = buildKPIs(monthlyAggregates, dailyLog)
  const statBreakdown     = buildStatBreakdown(bets)

  return { dailyLog, bets, monthlyAggregates, statBreakdown, kpis }
}

export function useTrackRecordData(source: TrackRecordSource, enabled = true) {
  const { sport, config } = useSport()

  return useQuery({
    queryKey: ['trackRecord', source, sport],
    queryFn: () => fetchTrackRecord(source, config),
    enabled,
    staleTime: 10 * 60 * 1000, // 10 minutes
  })
}
