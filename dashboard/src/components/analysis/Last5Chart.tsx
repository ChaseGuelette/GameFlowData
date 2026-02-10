'use client'

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  ReferenceLine,
  ResponsiveContainer,
  Cell,
  Tooltip,
} from 'recharts'
import { type PlayerGameStats } from '@/types/predictions'

interface Last5ChartProps {
  games: PlayerGameStats[]
  stat: keyof PlayerGameStats
  line: number
}

export function Last5Chart({ games, stat, line }: Last5ChartProps) {
  // Transform data for chart
  const data = games.map((game, index) => ({
    name: `G${index + 1}`,
    value: Number(game[stat]) || 0,
    date: game.game_date,
    overLine: Number(game[stat]) >= line,
  }))

  // Calculate y-axis domain
  const values = data.map((d) => d.value)
  const maxValue = Math.max(...values, line)
  const minValue = Math.min(...values, 0)
  const padding = (maxValue - minValue) * 0.1
  const yMax = Math.ceil(maxValue + padding)
  const yMin = Math.max(0, Math.floor(minValue - padding))

  return (
    <div className="h-48">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
          <XAxis
            dataKey="name"
            axisLine={false}
            tickLine={false}
            tick={{ fill: '#94a3b8', fontSize: 12 }}
          />
          <YAxis
            domain={[yMin, yMax]}
            axisLine={false}
            tickLine={false}
            tick={{ fill: '#94a3b8', fontSize: 12 }}
            width={30}
          />
          <Tooltip
            content={({ active, payload }) => {
              if (active && payload && payload.length) {
                const item = payload[0].payload
                return (
                  <div className="bg-slate-700 border border-slate-600 rounded px-3 py-2 text-sm">
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
            strokeWidth={2}
          />
          <Bar dataKey="value" radius={[4, 4, 0, 0]}>
            {data.map((entry, index) => (
              <Cell
                key={`cell-${index}`}
                fill={entry.overLine ? '#22c55e' : '#ef4444'}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      <div className="flex items-center justify-center space-x-4 mt-2 text-xs text-slate-400">
        <div className="flex items-center space-x-1">
          <div className="w-3 h-3 rounded bg-green-500" />
          <span>Over Line</span>
        </div>
        <div className="flex items-center space-x-1">
          <div className="w-3 h-3 rounded bg-red-500" />
          <span>Under Line</span>
        </div>
        <div className="flex items-center space-x-1">
          <div className="w-6 h-0.5 border-t-2 border-dashed border-slate-500" />
          <span>Line ({line})</span>
        </div>
      </div>
    </div>
  )
}
