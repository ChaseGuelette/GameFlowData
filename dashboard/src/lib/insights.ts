import { type Prediction } from '@/types/predictions'

export interface Insight {
  category: 'rest' | 'injury' | 'trend' | 'consistency' | 'average'
  text: string
  sentiment: 'positive' | 'negative' | 'neutral'
}

/**
 * Generate contextual insights for a prediction based on feature values.
 * These insights explain WHY the model made the prediction it did.
 */
export function generateInsights(prediction: Prediction): Insight[] {
  const insights: Insight[] = []

  // B2: Rest/Schedule Insights
  if (prediction.feat_is_back_to_back) {
    insights.push({
      category: 'rest',
      text: 'Playing on a back-to-back',
      sentiment: 'negative'
    })
  } else if (prediction.feat_rest_days && prediction.feat_rest_days >= 3) {
    insights.push({
      category: 'rest',
      text: `Well-rested (${prediction.feat_rest_days} days off)`,
      sentiment: 'positive'
    })
  }

  if (prediction.feat_games_last_7d && prediction.feat_games_last_7d >= 4) {
    insights.push({
      category: 'rest',
      text: `Heavy schedule (${prediction.feat_games_last_7d} games in 7 days)`,
      sentiment: 'negative'
    })
  }

  // B1: Injury Context Insights
  if (prediction.feat_team_out_count && prediction.feat_team_out_count >= 2) {
    const mins = prediction.feat_team_out_min_sum
      ? prediction.feat_team_out_min_sum.toFixed(0)
      : '?'
    insights.push({
      category: 'injury',
      text: `${prediction.feat_team_out_count} teammates out (${mins} combined MPG)`,
      sentiment: 'positive' // More opportunity for this player
    })
  } else if (prediction.feat_team_out_count === 1) {
    insights.push({
      category: 'injury',
      text: '1 teammate out',
      sentiment: 'neutral'
    })
  }

  if (prediction.feat_opp_out_count && prediction.feat_opp_out_count >= 2) {
    insights.push({
      category: 'injury',
      text: `Opponent missing ${prediction.feat_opp_out_count} players`,
      sentiment: 'neutral' // Could go either way
    })
  }

  if (prediction.feat_player_is_questionable) {
    insights.push({
      category: 'injury',
      text: 'Listed as Questionable',
      sentiment: 'negative'
    })
  }

  // B3: Trend Insights
  if (prediction.feat_stat_l3_l15_ratio) {
    const ratio = prediction.feat_stat_l3_l15_ratio
    if (ratio > 1.15) {
      const pct = ((ratio - 1) * 100).toFixed(0)
      insights.push({
        category: 'trend',
        text: `Hot streak: L3 avg ${pct}% above L15`,
        sentiment: 'positive'
      })
    } else if (ratio < 0.85) {
      const pct = ((1 - ratio) * 100).toFixed(0)
      insights.push({
        category: 'trend',
        text: `Cold stretch: L3 avg ${pct}% below L15`,
        sentiment: 'negative'
      })
    }
  }

  // Consistency Insights
  if (prediction.feat_stat_std_l5 !== undefined && prediction.feat_stat_std_l5 !== null) {
    const std = prediction.feat_stat_std_l5
    if (std < 3) {
      insights.push({
        category: 'consistency',
        text: 'Very consistent (low variance)',
        sentiment: 'positive'
      })
    } else if (std > 8) {
      insights.push({
        category: 'consistency',
        text: 'High variance game-to-game',
        sentiment: 'neutral'
      })
    }
  }

  // Average vs Line Insights (if we have both L5 avg and the line)
  if (prediction.feat_player_avg_stat_l5 && prediction.prop_line) {
    const avg = prediction.feat_player_avg_stat_l5
    const line = prediction.prop_line
    const diff = avg - line

    if (diff > 3) {
      insights.push({
        category: 'average',
        text: `L5 avg (${avg.toFixed(1)}) is ${diff.toFixed(1)} above line`,
        sentiment: 'positive'
      })
    } else if (diff < -3) {
      insights.push({
        category: 'average',
        text: `L5 avg (${avg.toFixed(1)}) is ${Math.abs(diff).toFixed(1)} below line`,
        sentiment: 'negative'
      })
    }
  }

  return insights
}

/**
 * Get a summary text for the prediction based on insights.
 * Used for quick display without showing all individual insights.
 */
export function getInsightSummary(insights: Insight[]): string | null {
  if (insights.length === 0) return null

  const positive = insights.filter(i => i.sentiment === 'positive').length
  const negative = insights.filter(i => i.sentiment === 'negative').length

  if (positive > negative) {
    return `${positive} favorable factor${positive > 1 ? 's' : ''}`
  } else if (negative > positive) {
    return `${negative} concern${negative > 1 ? 's' : ''}`
  } else if (positive === negative && positive > 0) {
    return 'Mixed signals'
  }

  return null
}
