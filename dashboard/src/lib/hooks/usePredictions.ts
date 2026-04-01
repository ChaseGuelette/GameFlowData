'use client'

import { useQuery } from '@tanstack/react-query'
import { createClient } from '@/lib/supabase/client'
import { useSport } from '@/contexts/SportContext'
import { type Prediction } from '@/types/predictions'
import { TEAM_ABBREV } from '@/lib/constants'

async function fetchPredictions(predictionsTable: string, date: string) {
  const supabase = createClient()

  const { data, error } = await supabase
    .from(predictionsTable)
    .select('*')
    .eq('prediction_date', date)
    .not('line', 'is', null)
    .limit(3000)

  if (error) throw error

  return (data ?? [])
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
    })) as Prediction[]
}

export function usePredictions(date: string) {
  const { sport, config } = useSport()

  return useQuery({
    queryKey: ['predictions', sport, date],
    queryFn: () => fetchPredictions(config.predictionsTable, date),
    staleTime: 5 * 60 * 1000, // 5 minutes
  })
}
