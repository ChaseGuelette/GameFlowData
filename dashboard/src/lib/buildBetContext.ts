import { type Prediction, type BetContext } from '@/types/predictions'
import { generateInsights } from '@/lib/insights'

interface BuildBetContextOptions {
  l5Avg?: number | null
  l5Games?: Array<{ date: string; value: number }> | null
  kelly?: {
    fraction: number
    recommended_stake: number
    bankroll_pct: number
  } | null
  sportsbookLines?: Array<{
    bookmaker: string
    line: number
    over_odds: number
    under_odds: number
  }> | null
  source: 'analysis_modal' | 'prop_card_toggle'
}

/**
 * Build a BetContext snapshot from a prediction and optional enrichment data.
 * Used by both the AnalysisModal (full context) and PropCard toggle (basic context).
 */
export function buildBetContext(prediction: Prediction, options: BuildBetContextOptions): BetContext {
  const overEdge = Number.isFinite(prediction.over_edge) ? prediction.over_edge : 0
  const underEdge = Number.isFinite(prediction.under_edge) ? prediction.under_edge : 0
  const isOver = overEdge > underEdge
  const modelProb = isOver ? prediction.model_prob_over : prediction.model_prob_under
  const impliedProb = isOver ? prediction.implied_prob_over : prediction.implied_prob_under
  const edge = isOver ? overEdge : underEdge

  // Extract all feat_* columns
  const features: Record<string, number | boolean | string | null> = {}
  const featureKeys = [
    'feat_rest_days', 'feat_is_back_to_back', 'feat_games_last_7d',
    'feat_team_out_count', 'feat_team_out_min_sum', 'feat_opp_out_count',
    'feat_player_is_questionable', 'feat_player_avg_stat_l5', 'feat_player_avg_stat_l15',
    'feat_player_avg_stat_season', 'feat_player_avg_stat_l3', 'feat_stat_l3_l15_ratio',
    'feat_stat_std_l5', 'feat_opp_allowed_stat_l15', 'feat_opp_team_abbrev',
    'feat_prop_line', 'feat_player_season_avg_vs_line',
  ] as const

  for (const key of featureKeys) {
    const val = prediction[key as keyof Prediction]
    if (val !== undefined) {
      features[key] = val as number | boolean | string | null
    }
  }

  const insights = generateInsights(prediction)

  return {
    quantiles: {
      q10: prediction.q10,
      q25: prediction.q25,
      q50: prediction.q50,
      q75: prediction.q75,
      q90: prediction.q90,
    },
    l5_avg: options.l5Avg ?? null,
    l5_games: options.l5Games ?? null,
    season_avg: prediction.feat_player_avg_stat_season ?? null,
    features,
    insights: insights.map(i => ({ category: i.category, text: i.text, sentiment: i.sentiment })),
    probabilities: {
      model_prob: modelProb,
      implied_prob: impliedProb,
      edge,
    },
    kelly: options.kelly ?? null,
    sportsbook_lines: options.sportsbookLines ?? null,
    source: options.source,
  }
}
