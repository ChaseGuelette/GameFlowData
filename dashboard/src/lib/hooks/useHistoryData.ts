'use client'

import { useQuery } from '@tanstack/react-query'
import { createClient } from '@/lib/supabase/client'
import { useSport } from '@/contexts/SportContext'
import { type PaperBet } from '@/types/predictions'
import type { UserDfsEntryWithLegs } from '@/types/dfs-entries'

interface PaperBetWithRecommended extends PaperBet {
  is_recommended?: boolean
  bookmaker?: string
}

const PAGE_SIZE = 1000

async function fetchModelHistory(
  config: { paperBetsTable: string; predictionsTable: string },
  startDate: string,
  endDate: string,
) {
  const supabase = createClient()

  const { data: betsData, error: betsError } = await supabase
    .from(config.paperBetsTable)
    .select('id, prediction_id, game_date, player_id, player_name, stat_type, line, bet_direction, odds_at_bet, stake, edge, status, actual_value, pnl')
    .gte('game_date', startDate)
    .lte('game_date', endDate)
    .order('game_date', { ascending: false })

  if (betsError) throw betsError
  const bets = betsData ?? []

  // Enrich with is_recommended and bookmaker
  const predictionIds = [...new Set(bets.map(b => b.prediction_id).filter(Boolean))]
  if (predictionIds.length === 0) return bets as PaperBetWithRecommended[]

  const { data: predictionsData, error: predictionsError } = await supabase
    .from(config.predictionsTable)
    .select('id, is_recommended, bookmaker')
    .in('id', predictionIds)

  if (predictionsError) throw predictionsError

  const recommendedMap = new Map<string, boolean>()
  const bookmakerMap = new Map<string, string>()
  if (predictionsData) {
    for (const p of predictionsData) {
      const key = String(p.id)
      recommendedMap.set(key, p.is_recommended ?? false)
      if (p.bookmaker) bookmakerMap.set(key, p.bookmaker)
    }
  }

  return bets.map(bet => {
    const key = String(bet.prediction_id)
    return {
      ...bet,
      is_recommended: recommendedMap.get(key) ?? false,
      bookmaker: bookmakerMap.get(key),
    }
  }) as PaperBetWithRecommended[]
}

export function useModelHistory(startDate: string, endDate: string, enabled = true) {
  const { sport, config } = useSport()

  return useQuery({
    queryKey: ['history', 'model', sport, startDate, endDate],
    queryFn: () => fetchModelHistory(config, startDate, endDate),
    enabled,
    staleTime: 10 * 60 * 1000,
  })
}

async function fetchMyBets(
  statTypes: string[],
  startDate: string,
  endDate: string,
) {
  const supabase = createClient()

  const { data, error } = await supabase
    .from('user_bets')
    .select('id, game_date, player_id, player_name, stat_type, line, bet_direction, odds_at_bet, stake, edge, status, actual_value, pnl, book_at_bet, team_abbrev, opponent_abbrev, bet_context, user_confidence, placed_at, is_paper_trade')
    .in('stat_type', statTypes)
    .gte('game_date', startDate)
    .lte('game_date', endDate)
    .order('game_date', { ascending: false })

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
    team_abbrev: row.team_abbrev ?? undefined,
    opponent_abbrev: row.opponent_abbrev ?? undefined,
    bet_context: row.bet_context ?? null,
    user_confidence: row.user_confidence ?? null,
    placed_at: row.placed_at ?? null,
    is_paper_trade: row.is_paper_trade ?? false,
  })) as PaperBet[]
}

export function useMyBets(startDate: string, endDate: string, enabled = true) {
  const { sport, config } = useSport()

  return useQuery({
    queryKey: ['history', 'myBets', sport, startDate, endDate],
    queryFn: () => fetchMyBets(config.statTypes, startDate, endDate),
    enabled,
    staleTime: 10 * 60 * 1000,
  })
}

async function fetchDfsEntries(startDate: string, endDate: string) {
  const supabase = createClient()

  const { data: entriesData, error: entriesError } = await supabase
    .from('user_dfs_entries')
    .select('*')
    .gte('entry_date', startDate)
    .lte('entry_date', endDate)
    .order('placed_at', { ascending: false })

  if (entriesError) throw entriesError
  if (!entriesData?.length) return [] as UserDfsEntryWithLegs[]

  const entryIds = entriesData.map(e => e.id)
  const { data: legsData, error: legsError } = await supabase
    .from('user_dfs_legs')
    .select('*')
    .in('entry_id', entryIds)

  if (legsError) throw legsError

  const legsByEntry = new Map<number, typeof legsData>()
  if (legsData) {
    for (const leg of legsData) {
      if (!legsByEntry.has(leg.entry_id)) legsByEntry.set(leg.entry_id, [])
      legsByEntry.get(leg.entry_id)!.push(leg)
    }
  }

  return entriesData.map(entry => ({
    ...entry,
    legs: legsByEntry.get(entry.id) ?? [],
  })) as UserDfsEntryWithLegs[]
}

export function useDfsEntries(startDate: string, endDate: string, enabled = true) {
  return useQuery({
    queryKey: ['history', 'dfs', startDate, endDate],
    queryFn: () => fetchDfsEntries(startDate, endDate),
    enabled,
    staleTime: 10 * 60 * 1000,
  })
}

async function fetchMLBModelHistory(startDate: string, endDate: string) {
  const supabase = createClient()

  const { data, error } = await supabase
    .from('mlb_paper_bets')
    .select('id, prediction_id, game_date, player_id, player_name, stat_type, line, bet_direction, odds_at_bet, stake, edge, status, actual_value, pnl')
    .gte('game_date', startDate)
    .lte('game_date', endDate)
    .order('game_date', { ascending: false })
    .limit(PAGE_SIZE)

  if (error) throw error
  return (data ?? []) as PaperBetWithRecommended[]
}

export function useMLBModelHistory(startDate: string, endDate: string, enabled = true) {
  return useQuery({
    queryKey: ['history', 'mlb_model', startDate, endDate],
    queryFn: () => fetchMLBModelHistory(startDate, endDate),
    enabled,
    staleTime: 10 * 60 * 1000,
  })
}

export { PAGE_SIZE }
export type { PaperBetWithRecommended }
