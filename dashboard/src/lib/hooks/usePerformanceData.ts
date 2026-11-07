'use client'

import { useQuery } from '@tanstack/react-query'
import { createClient } from '@/lib/supabase/client'
import { useSport } from '@/contexts/SportContext'
import { type DailyPerformance, type PaperBet } from '@/types/predictions'

interface PaperBetWithRecommended extends PaperBet {
  is_recommended?: boolean
}

async function fetchPropsPerformance(config: {
  dailyLogTable: string
  paperBetsTable: string
  predictionsTable: string
}) {
  const supabase = createClient()

  const [logRes, betsRes] = await Promise.all([
    supabase
      .from(config.dailyLogTable)
      .select('game_date, total_bets, bets_won, bets_lost, bets_push, total_staked, total_pnl, cumulative_pnl, bankroll_after, roi_pct')
      .order('game_date', { ascending: true }),
    supabase
      .from(config.paperBetsTable)
      .select('id, prediction_id, game_date, player_id, player_name, stat_type, line, bet_direction, odds_at_bet, stake, edge, status, actual_value, pnl')
      .in('status', ['won', 'lost', 'push'])
      .order('game_date', { ascending: true }),
  ])

  const dailyData = (logRes.data ?? []) as DailyPerformance[]
  const betsData = betsRes.data ?? []

  if (logRes.error) throw logRes.error
  if (betsRes.error) throw betsRes.error

  // Enrich bets with is_recommended
  const predictionIds = [...new Set(betsData.map(b => b.prediction_id).filter(Boolean))]
  let enrichedBets: PaperBetWithRecommended[] = betsData as PaperBetWithRecommended[]

  if (predictionIds.length > 0) {
    const { data: predictionsData, error: predictionsError } = await supabase
      .from(config.predictionsTable)
      .select('id, is_recommended')
      .in('id', predictionIds)

    if (predictionsError) throw predictionsError

    if (predictionsData) {
      const recommendedMap = new Map<number, boolean>()
      for (const p of predictionsData) {
        recommendedMap.set(p.id, p.is_recommended ?? false)
      }
      enrichedBets = betsData.map(bet => ({
        ...bet,
        is_recommended: recommendedMap.get(bet.prediction_id) ?? false,
      })) as PaperBetWithRecommended[]
    }
  }

  const currentBankroll = dailyData.length > 0
    ? dailyData[dailyData.length - 1].bankroll_after
    : 0

  return { dailyData, allBets: enrichedBets, currentBankroll }
}

export function usePropsPerformance(enabled = true) {
  const { sport, config } = useSport()

  return useQuery({
    queryKey: ['performance', 'props', sport],
    queryFn: () => fetchPropsPerformance(config),
    enabled,
    staleTime: 10 * 60 * 1000, // 10 minutes
  })
}

async function fetchMyBetsPerformance(statTypes: string[], startDate: string, endDate: string) {
  const supabase = createClient()

  const { data, error } = await supabase
    .from('user_bets')
    .select('id, game_date, player_id, player_name, stat_type, line, bet_direction, odds_at_bet, stake, edge, status, actual_value, pnl, book_at_bet, is_paper_trade')
    .in('stat_type', statTypes)
    .in('status', ['won', 'lost', 'push'])
    .gte('game_date', startDate)
    .lte('game_date', endDate)
    .order('game_date', { ascending: true })

  if (error) throw error

  return (data ?? []).map(row => ({
    id: row.id,
    game_date: row.game_date,
    player_id: row.player_id,
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
    is_paper_trade: row.is_paper_trade ?? false,
  })) as PaperBet[]
}

export function useMyBetsPerformance(startDate: string, endDate: string, enabled = true) {
  const { sport, config } = useSport()
  const validRange = Boolean(startDate && endDate && startDate <= endDate)

  return useQuery({
    queryKey: ['performance', 'myBets', sport, startDate, endDate],
    queryFn: () => fetchMyBetsPerformance(config.statTypes, startDate, endDate),
    enabled: enabled && validRange,
    staleTime: 10 * 60 * 1000,
  })
}

export type { PaperBetWithRecommended }
