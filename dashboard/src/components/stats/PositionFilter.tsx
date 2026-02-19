'use client'

import { useState } from 'react'
import { cn } from '@/lib/utils'
import type { PositionGroup } from '@/types/stats'

type PositionOption = 'all' | PositionGroup

const positions: { value: PositionOption; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'G', label: 'Guards' },
  { value: 'W', label: 'Wings' },
  { value: 'B', label: 'Bigs' },
]

interface PositionFilterProps {
  active: PositionOption
  onChange: (pos: PositionOption) => void
}

export function PositionFilter({ active, onChange }: PositionFilterProps) {
  const [showInfo, setShowInfo] = useState(false)

  return (
    <div className="flex items-center space-x-1">
      {positions.map((pos) => (
        <button
          key={pos.value}
          onClick={() => onChange(pos.value)}
          className={cn(
            'px-3 py-1.5 text-xs font-medium rounded-full transition-colors',
            active === pos.value
              ? 'bg-slate-600 text-white'
              : 'text-slate-400 hover:text-slate-200 bg-slate-800 hover:bg-slate-700'
          )}
        >
          {pos.label}
        </button>
      ))}
      <div className="relative">
        <button
          onClick={() => setShowInfo(!showInfo)}
          className="ml-1 w-5 h-5 rounded-full text-[10px] font-bold text-slate-400 hover:text-slate-200 bg-slate-800 hover:bg-slate-700 transition-colors flex items-center justify-center"
          aria-label="Position group info"
        >
          i
        </button>
        {showInfo && (
          <div className="absolute top-7 left-0 z-50 w-72 rounded-lg border border-slate-700 bg-slate-800 p-3 text-xs text-slate-300 shadow-xl">
            <p className="font-medium text-slate-200 mb-2">Position Groups</p>
            <ul className="space-y-1.5">
              <li><span className="font-medium text-white">Guards (G):</span> PG, SG — primary ball handlers and perimeter scorers</li>
              <li><span className="font-medium text-white">Wings (W):</span> SF, SG/SF — versatile two-way players on the perimeter</li>
              <li><span className="font-medium text-white">Bigs (B):</span> PF, C — interior players, rebounders, and rim protectors</li>
            </ul>
          </div>
        )}
      </div>
    </div>
  )
}

export type { PositionOption }
