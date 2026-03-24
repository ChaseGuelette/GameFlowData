'use client'

import { useState } from 'react'
import { cn } from '@/lib/utils'
import { DFS_SLIP_TYPES, DFS_PLATFORM_NAMES, type EdgeMode } from '@/types/dfs'
import { type StatType, STAT_LABELS } from '@/types/predictions'

type PlatformFilter = 'all' | string
type StatFilter = 'all' | StatType

interface DfsFiltersProps {
  edgeMode: EdgeMode
  onEdgeModeChange: (mode: EdgeMode) => void
  platformFilter: PlatformFilter
  onPlatformChange: (platform: PlatformFilter) => void
  slipType: string
  onSlipTypeChange: (slip: string) => void
  statFilter: StatFilter
  onStatChange: (stat: StatFilter) => void
  evOnly: boolean
  onEvOnlyChange: (evOnly: boolean) => void
  showLive: boolean
  onShowLiveChange: (showLive: boolean) => void
}

const platforms: { value: PlatformFilter; label: string }[] = [
  { value: 'all', label: 'All' },
  ...Object.entries(DFS_PLATFORM_NAMES).map(([key, label]) => ({ value: key, label })),
]

const stats: { value: StatFilter; label: string }[] = [
  { value: 'all', label: 'All' },
  ...Object.entries(STAT_LABELS).map(([k, v]) => ({ value: k as StatType, label: v })),
]

const edgeModes: { value: EdgeMode; label: string; activeColor: string }[] = [
  { value: 'model', label: 'Model Edge', activeColor: 'bg-blue-600 text-white' },
  { value: 'market', label: 'Market Edge', activeColor: 'bg-purple-600 text-white' },
  { value: 'combined', label: 'Combined', activeColor: 'bg-amber-600 text-white' },
]

export function DfsFilters({
  edgeMode,
  onEdgeModeChange,
  platformFilter,
  onPlatformChange,
  slipType,
  onSlipTypeChange,
  statFilter,
  onStatChange,
  evOnly,
  onEvOnlyChange,
  showLive,
  onShowLiveChange,
}: DfsFiltersProps) {
  const [filtersOpen, setFiltersOpen] = useState(false)

  return (
    <div className="space-y-3">
      {/* Always visible: edge mode toggle + mobile filter button */}
      <div className="flex items-center gap-3">
        <div className="flex items-center space-x-1 bg-slate-800 p-1 rounded-lg overflow-x-auto scrollbar-hide">
          {edgeModes.map((m) => (
            <button
              key={m.value}
              onClick={() => onEdgeModeChange(m.value)}
              className={cn(
                'px-3 py-1.5 text-sm font-medium rounded-md transition-colors whitespace-nowrap',
                edgeMode === m.value
                  ? m.activeColor
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700'
              )}
            >
              {m.label}
            </button>
          ))}
        </div>

        <button
          onClick={() => setFiltersOpen(!filtersOpen)}
          className={cn(
            'sm:hidden px-3 py-1.5 text-sm font-medium rounded-lg border transition-colors',
            filtersOpen
              ? 'bg-blue-600 border-blue-500 text-white'
              : 'bg-slate-800 border-slate-700 text-slate-300'
          )}
        >
          Filters {filtersOpen ? '−' : '+'}
        </button>
      </div>

      {/* Collapsible on mobile, always visible on sm+ */}
      <div className={cn(
        filtersOpen ? 'flex' : 'hidden',
        'sm:flex flex-col sm:flex-row sm:flex-wrap items-start sm:items-center gap-3'
      )}>
        {/* Platform filter tabs */}
        <div className="flex items-center space-x-1 bg-slate-800 p-1 rounded-lg overflow-x-auto scrollbar-hide max-w-full">
          {platforms.map((p) => (
            <button
              key={p.value}
              onClick={() => onPlatformChange(p.value)}
              className={cn(
                'px-3 py-1.5 text-sm font-medium rounded-md transition-colors whitespace-nowrap',
                platformFilter === p.value
                  ? 'bg-blue-600 text-white'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700'
              )}
            >
              {p.label}
            </button>
          ))}
        </div>

        {/* Slip type selector */}
        <select
          value={slipType}
          onChange={(e) => onSlipTypeChange(e.target.value)}
          className="bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-sm text-slate-200 focus:outline-none focus:border-blue-500"
        >
          {Object.entries(DFS_SLIP_TYPES).map(([key, slip]) => (
            <option key={key} value={key}>
              {slip.label} ({slip.payout})
            </option>
          ))}
        </select>

        {/* Stat filter tabs */}
        <div className="flex items-center space-x-1 bg-slate-800 p-1 rounded-lg overflow-x-auto scrollbar-hide max-w-full">
          {stats.map((s) => (
            <button
              key={s.value}
              onClick={() => onStatChange(s.value)}
              className={cn(
                'px-3 py-1.5 text-sm font-medium rounded-md transition-colors whitespace-nowrap',
                statFilter === s.value
                  ? 'bg-blue-600 text-white'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-700'
              )}
            >
              {s.label}
            </button>
          ))}
        </div>

        {/* +EV Only toggle */}
        <button
          onClick={() => onEvOnlyChange(!evOnly)}
          className={cn(
            'px-3 py-1.5 text-sm font-medium rounded-lg border transition-colors whitespace-nowrap',
            evOnly
              ? 'bg-green-600 border-green-500 text-white'
              : 'bg-slate-800 border-slate-700 text-slate-400 hover:text-slate-200'
          )}
        >
          +EV Only
        </button>

        {/* Live betting toggle */}
        <div className="flex items-center space-x-1 bg-slate-800 p-1 rounded-lg">
          <button
            onClick={() => onShowLiveChange(false)}
            className={cn(
              'px-3 py-1.5 text-sm font-medium rounded-md transition-colors whitespace-nowrap',
              !showLive
                ? 'bg-slate-700 text-slate-100'
                : 'text-slate-400 hover:text-slate-200'
            )}
          >
            Pre-Game
          </button>
          <button
            onClick={() => onShowLiveChange(true)}
            className={cn(
              'px-3 py-1.5 text-sm font-medium rounded-md transition-colors whitespace-nowrap',
              showLive
                ? 'bg-orange-600 text-white'
                : 'text-slate-400 hover:text-slate-200'
            )}
          >
            + Live
          </button>
        </div>
      </div>
    </div>
  )
}
