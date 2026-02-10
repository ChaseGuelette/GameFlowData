'use client'

import { cn } from '@/lib/utils'

interface KPICardProps {
  label: string
  value: string
  subValue?: string
  trend?: 'up' | 'down' | 'neutral'
}

export function KPICard({ label, value, subValue, trend }: KPICardProps) {
  const trendColor = trend === 'up' ? 'text-green-400' : trend === 'down' ? 'text-red-400' : 'text-slate-50'

  return (
    <div className="bg-slate-800 rounded-lg border border-slate-700 p-6">
      <div className="text-sm text-slate-400 mb-2">{label}</div>
      <div className={cn('text-2xl font-bold', trendColor)}>{value}</div>
      {subValue && (
        <div className="text-xs text-slate-500 mt-1">{subValue}</div>
      )}
    </div>
  )
}
