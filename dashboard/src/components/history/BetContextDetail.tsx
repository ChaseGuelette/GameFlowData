'use client'

import { type BetContext } from '@/types/predictions'
import { QuantileSummary } from '@/components/analysis/QuantileSummary'
import {
  BarChart,
  Bar,
  XAxis,
  ReferenceLine,
  ResponsiveContainer,
  Cell,
  Tooltip,
} from 'recharts'

interface BetContextDetailProps {
  context: BetContext
  userConfidence?: number | null
  line: number
}

export function BetContextDetail({ context, userConfidence, line }: BetContextDetailProps) {
  return (
    <div className="mt-3 pt-3 border-t border-slate-700 space-y-4">
      {/* Confidence Stars (display-only) */}
      {userConfidence && (
        <div className="flex items-center gap-2">
          <span className="text-xs text-slate-400">Confidence:</span>
          <div className="flex gap-0.5">
            {[1, 2, 3, 4, 5].map((star) => (
              <svg
                key={star}
                xmlns="http://www.w3.org/2000/svg"
                viewBox="0 0 20 20"
                fill="currentColor"
                className={`w-4 h-4 ${
                  star <= userConfidence ? 'text-yellow-400' : 'text-slate-600'
                }`}
              >
                <path fillRule="evenodd" d="M10.868 2.884c-.321-.772-1.415-.772-1.736 0l-1.83 4.401-4.753.381c-.833.067-1.171 1.107-.536 1.651l3.62 3.102-1.106 4.637c-.194.813.691 1.456 1.405 1.02L10 15.591l4.069 2.485c.713.436 1.598-.207 1.404-1.02l-1.106-4.637 3.62-3.102c.635-.544.297-1.584-.536-1.65l-4.752-.382-1.831-4.401Z" clipRule="evenodd" />
              </svg>
            ))}
          </div>
        </div>
      )}

      {/* Quantile Distribution */}
      <div>
        <div className="text-xs text-slate-400 mb-2">Model Distribution</div>
        <QuantileSummary
          q10={context.quantiles.q10}
          q25={context.quantiles.q25}
          q50={context.quantiles.q50}
          q75={context.quantiles.q75}
          q90={context.quantiles.q90}
          line={line}
        />
      </div>

      {/* L5 Games Chart + Season Average */}
      {context.l5_games && context.l5_games.length > 0 ? (
        <div>
          <div className="flex items-center justify-between mb-1">
            <span className="text-xs text-slate-400">Last 5 Games</span>
            <div className="flex gap-3 text-xs">
              {context.l5_avg != null && (
                <span className="text-slate-400">
                  L5 Avg: <span className="text-slate-200">{context.l5_avg.toFixed(1)}</span>
                </span>
              )}
              {context.season_avg != null && (
                <span className="text-slate-400">
                  Season: <span className="text-slate-200">{context.season_avg.toFixed(1)}</span>
                </span>
              )}
            </div>
          </div>
          <div className="h-24">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={context.l5_games.map((g, i) => ({
                  name: `G${i + 1}`,
                  value: g.value,
                  date: g.date,
                  overLine: g.value >= line,
                }))}
                margin={{ top: 4, right: 4, left: 0, bottom: 0 }}
              >
                <XAxis
                  dataKey="name"
                  axisLine={false}
                  tickLine={false}
                  tick={{ fill: '#94a3b8', fontSize: 10 }}
                />
                <Tooltip
                  content={({ active, payload }) => {
                    if (active && payload && payload.length) {
                      const item = payload[0].payload
                      return (
                        <div className="bg-slate-700 border border-slate-600 rounded px-2 py-1 text-xs">
                          <div className="text-slate-300">{item.date}</div>
                          <div className="text-slate-50 font-semibold">{item.value}</div>
                        </div>
                      )
                    }
                    return null
                  }}
                />
                <ReferenceLine
                  y={line}
                  stroke="#64748b"
                  strokeDasharray="3 3"
                  strokeWidth={1.5}
                />
                <Bar dataKey="value" radius={[3, 3, 0, 0]}>
                  {context.l5_games.map((g, i) => (
                    <Cell
                      key={`cell-${i}`}
                      fill={g.value >= line ? '#22c55e' : '#ef4444'}
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      ) : (context.l5_avg != null || context.season_avg != null) ? (
        <div className="flex gap-4 text-sm">
          {context.l5_avg != null && (
            <div className="text-slate-400">
              L5 Avg: <span className="text-slate-200">{context.l5_avg.toFixed(1)}</span>
            </div>
          )}
          {context.season_avg != null && (
            <div className="text-slate-400">
              Season Avg: <span className="text-slate-200">{context.season_avg.toFixed(1)}</span>
            </div>
          )}
        </div>
      ) : null}

      {/* Insights */}
      {context.insights.length > 0 && (
        <div>
          <div className="text-xs text-slate-400 mb-1.5">Model Context</div>
          <div className="space-y-1">
            {context.insights.map((insight, i) => (
              <div
                key={i}
                className={`flex items-center gap-2 text-xs ${
                  insight.sentiment === 'positive'
                    ? 'text-green-400'
                    : insight.sentiment === 'negative'
                      ? 'text-red-400'
                      : 'text-slate-300'
                }`}
              >
                <span className="w-3 text-center">
                  {insight.sentiment === 'positive' ? '\u2713' : insight.sentiment === 'negative' ? '\u26A0' : '\u2022'}
                </span>
                <span>{insight.text}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Probabilities */}
      <div className="flex gap-4 text-xs">
        <div className="text-slate-400">
          Model: <span className="text-slate-200">{(context.probabilities.model_prob * 100).toFixed(1)}%</span>
        </div>
        <div className="text-slate-400">
          Market: <span className="text-slate-200">{(context.probabilities.implied_prob * 100).toFixed(1)}%</span>
        </div>
        <div className="text-slate-400">
          Edge: <span className="text-green-400">{(context.probabilities.edge * 100).toFixed(1)}%</span>
        </div>
      </div>

      {/* Kelly Recommendation */}
      {context.kelly && (
        <div className="text-xs text-slate-400">
          Kelly: <span className="text-slate-200">${context.kelly.recommended_stake.toFixed(2)}</span>
          <span className="text-slate-500 ml-1">({(context.kelly.bankroll_pct * 100).toFixed(2)}% of bankroll)</span>
        </div>
      )}

      {/* Source Indicator */}
      <div className="text-xs text-slate-500">
        {context.source === 'analysis_modal' ? 'Placed via Analysis Modal' : 'Quick-toggled from card'}
      </div>
    </div>
  )
}
