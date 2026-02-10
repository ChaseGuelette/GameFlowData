'use client'

import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  ResponsiveContainer,
  Tooltip,
} from 'recharts'
import { type DailyPerformance } from '@/types/predictions'

interface BankrollChartProps {
  data: DailyPerformance[]
}

export function BankrollChart({ data }: BankrollChartProps) {
  if (data.length === 0) {
    return (
      <div className="h-64 flex items-center justify-center text-slate-500">
        No performance data available
      </div>
    )
  }

  // Calculate trend (up or down overall)
  const firstValue = data[0]?.bankroll_after || 0
  const lastValue = data[data.length - 1]?.bankroll_after || 0
  const isPositive = lastValue >= firstValue

  // Format data for chart
  const chartData = data.map((d) => ({
    date: d.game_date,
    bankroll: d.bankroll_after,
    pnl: d.total_pnl,
  }))

  // Calculate y-axis domain with padding
  const values = chartData.map(d => d.bankroll)
  const minValue = Math.min(...values)
  const maxValue = Math.max(...values)
  const padding = (maxValue - minValue) * 0.1 || maxValue * 0.1
  const yMin = Math.floor(minValue - padding)
  const yMax = Math.ceil(maxValue + padding)

  return (
    <div className="bg-slate-800 rounded-lg border border-slate-700 p-6">
      <h3 className="text-lg font-medium text-slate-50 mb-4">Bankroll Over Time</h3>
      <div className="h-64">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={chartData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
            <defs>
              <linearGradient id="bankrollGradient" x1="0" y1="0" x2="0" y2="1">
                <stop
                  offset="5%"
                  stopColor={isPositive ? '#22c55e' : '#ef4444'}
                  stopOpacity={0.3}
                />
                <stop
                  offset="95%"
                  stopColor={isPositive ? '#22c55e' : '#ef4444'}
                  stopOpacity={0}
                />
              </linearGradient>
            </defs>
            <XAxis
              dataKey="date"
              axisLine={false}
              tickLine={false}
              tick={{ fill: '#94a3b8', fontSize: 11 }}
              tickFormatter={(value) => {
                const date = new Date(value)
                return `${date.getMonth() + 1}/${date.getDate()}`
              }}
            />
            <YAxis
              domain={[yMin, yMax]}
              axisLine={false}
              tickLine={false}
              tick={{ fill: '#94a3b8', fontSize: 11 }}
              width={60}
              tickFormatter={(value) => `$${value.toLocaleString()}`}
            />
            <Tooltip
              content={({ active, payload }) => {
                if (active && payload && payload.length) {
                  const item = payload[0].payload
                  return (
                    <div className="bg-slate-700 border border-slate-600 rounded px-3 py-2 text-sm">
                      <div className="text-slate-300">{item.date}</div>
                      <div className="text-slate-50 font-semibold">
                        ${item.bankroll.toLocaleString()}
                      </div>
                      <div className={item.pnl >= 0 ? 'text-green-400' : 'text-red-400'}>
                        {item.pnl >= 0 ? '+' : ''}${item.pnl.toFixed(2)} today
                      </div>
                    </div>
                  )
                }
                return null
              }}
            />
            <Area
              type="monotone"
              dataKey="bankroll"
              stroke={isPositive ? '#22c55e' : '#ef4444'}
              strokeWidth={2}
              fill="url(#bankrollGradient)"
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
