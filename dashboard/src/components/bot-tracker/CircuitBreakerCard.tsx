'use client'

import type { KalshiConfig } from '@/types/bot-tracker'

interface CircuitBreakerCardProps {
  config: KalshiConfig
}

export function CircuitBreakerCard({ config }: CircuitBreakerCardProps) {
  const isHalted = config.is_halted

  return (
    <div
      className={`rounded-lg border p-4 ${
        isHalted
          ? 'border-red-500/50 bg-red-500/10'
          : 'border-green-500/50 bg-green-500/10'
      }`}
    >
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div
            className={`h-3 w-3 rounded-full ${
              isHalted ? 'bg-red-500 animate-pulse' : 'bg-green-500'
            }`}
          />
          <div>
            <h3 className="text-sm font-semibold text-slate-200">
              Circuit Breaker
            </h3>
            <p className={`text-xs ${isHalted ? 'text-red-400' : 'text-green-400'}`}>
              {isHalted ? 'HALTED' : 'ACTIVE'}
              {isHalted && config.halt_reason && ` — ${config.halt_reason}`}
            </p>
          </div>
        </div>
        <div className="text-right">
          <div className="text-xs text-slate-400">Loss Streak</div>
          <div className={`text-lg font-semibold ${config.streak_count >= 3 ? 'text-red-400' : 'text-slate-200'}`}>
            {config.streak_count}
          </div>
        </div>
      </div>
    </div>
  )
}
