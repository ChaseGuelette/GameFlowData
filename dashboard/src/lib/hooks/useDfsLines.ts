'use client'

import { useQuery } from '@tanstack/react-query'
import { createClient } from '@/lib/supabase/client'
import { type DfsLine, type SportsbookLine } from '@/types/dfs'
import { type Prediction } from '@/types/predictions'
import { TEAM_ABBREV } from '@/lib/constants'

async function fetchDfsData(date: string) {
  const supabase = createClient()

  // Step 1: Fetch predictions and DFS lines in parallel
  const [predictionsRes, dfsRes] = await Promise.all([
    supabase
      .from('daily_predictions')
      .select('*')
      .eq('prediction_date', date)
      .not('line', 'is', null)
      .limit(3000),
    supabase
      .rpc('get_dfs_lines', { target_date: date }),
  ])

  let predictions: Prediction[] = []
  if (!predictionsRes.error && predictionsRes.data) {
    predictions = predictionsRes.data
      .filter(p => Number.isFinite(p.over_edge) || Number.isFinite(p.under_edge))
      .map(p => ({
        ...p,
        prop_line: p.line,
        model_prob_over: p.over_prob,
        model_prob_under: p.under_prob,
        implied_prob_over: p.implied_over,
        implied_prob_under: p.implied_under,
        q10: p.pred_q10,
        q25: p.pred_q25,
        q50: p.pred_q50,
        q75: p.pred_q75,
        q90: p.pred_q90,
        team_abbrev: TEAM_ABBREV[p.team_id] || 'UNK',
        opponent_abbrev: TEAM_ABBREV[p.opponent_id] || 'UNK',
      }))
  }

  let dfsLines: DfsLine[] = []
  if (!dfsRes.error && dfsRes.data) {
    dfsLines = dfsRes.data
  }

  // Step 2: Fetch sportsbook lines per-game in parallel
  const gameIds = [...new Set(dfsLines.map(dl => dl.game_id))]
  const sportsbookLines: SportsbookLine[] = []

  if (gameIds.length > 0) {
    const batchSize = 3
    const batches: string[][] = []
    for (let i = 0; i < gameIds.length; i += batchSize) {
      batches.push(gameIds.slice(i, i + batchSize))
    }
    const sbResults = await Promise.all(
      batches.map(batch =>
        supabase.rpc('get_sportsbook_lines_by_games', { p_game_ids: batch })
      )
    )
    for (const res of sbResults) {
      if (!res.error && res.data) {
        sportsbookLines.push(...res.data)
      }
    }
  }

  return { predictions, dfsLines, sportsbookLines }
}

export function useDfsLines(date: string, enabled = true) {
  return useQuery({
    queryKey: ['dfs', 'lines', date],
    queryFn: () => fetchDfsData(date),
    enabled,
    staleTime: 5 * 60 * 1000, // 5 minutes
  })
}
