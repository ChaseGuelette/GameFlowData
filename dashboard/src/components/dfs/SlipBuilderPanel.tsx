'use client'

import { useState } from 'react'
import { SlipLegCard } from './SlipLegCard'
import {
  USER_DFS_SLIP_TYPES,
  type UserDfsSlipType,
  type SlipBuilderLeg,
} from '@/types/dfs-entries'
import type { EdgeMode } from '@/types/dfs'
import { calcParlayKelly } from '@/lib/dfs-kelly'
import { cn } from '@/lib/utils'

interface SlipBuilderPanelProps {
  legs: SlipBuilderLeg[]
  slipType: UserDfsSlipType
  onSlipTypeChange: (type: UserDfsSlipType) => void
  edgeMode: EdgeMode
  bankroll: number
  kellyFraction: number
  validation: {
    isValid: boolean
    requiredLegs: number
    currentLegs: number
    hasDuplicatePlayers: boolean
    message: string | null
  }
  placing: boolean
  error: string | null
  onRemoveLeg: (key: string) => void
  onClearAll: () => void
  onPlace: () => void
}

export function SlipBuilderPanel({
  legs,
  slipType,
  onSlipTypeChange,
  edgeMode,
  bankroll,
  kellyFraction,
  validation,
  placing,
  error,
  onRemoveLeg,
  onClearAll,
  onPlace,
}: SlipBuilderPanelProps) {
  const [confirming, setConfirming] = useState(false)
  // Mobile expansion
  const [mobileExpanded, setMobileExpanded] = useState(false)

  const config = USER_DFS_SLIP_TYPES[slipType]
  const kelly = calcParlayKelly(legs, slipType, bankroll, kellyFraction, edgeMode)

  const handlePlace = () => {
    if (confirming) {
      onPlace()
      setConfirming(false)
    } else {
      setConfirming(true)
    }
  }

  if (legs.length === 0) return null

  const panelContent = (
    <>
      {/* Slip type selector */}
      <div className="flex gap-1 mb-3">
        {(Object.entries(USER_DFS_SLIP_TYPES) as [UserDfsSlipType, typeof USER_DFS_SLIP_TYPES[UserDfsSlipType]][]).map(
          ([key, cfg]) => (
            <button
              key={key}
              onClick={() => { onSlipTypeChange(key); setConfirming(false) }}
              className={cn(
                'flex-1 px-2 py-1.5 text-xs font-medium rounded-md transition-colors',
                slipType === key
                  ? 'bg-blue-600 text-white'
                  : 'bg-slate-700 text-slate-400 hover:text-slate-200',
              )}
            >
              {cfg.label}
            </button>
          ),
        )}
      </div>

      {/* Legs list */}
      <div className="space-y-1.5 mb-3 max-h-64 overflow-y-auto">
        {legs.map(leg => (
          <SlipLegCard key={leg.key} leg={leg} onRemove={onRemoveLeg} />
        ))}
      </div>

      {/* Validation message */}
      {validation.message && (
        <div className={cn(
          'text-xs px-2 py-1.5 rounded mb-3',
          validation.hasDuplicatePlayers
            ? 'bg-red-500/20 text-red-400'
            : 'bg-yellow-500/20 text-yellow-400',
        )}>
          {validation.message}
        </div>
      )}

      {/* Stats */}
      {kelly && (
        <div className="space-y-1.5 mb-3 text-xs">
          <div className="flex justify-between text-slate-400">
            <span>Combined Prob</span>
            <span className="text-slate-200">{(kelly.combinedProb * 100).toFixed(1)}%</span>
          </div>
          <div className="flex justify-between text-slate-400">
            <span>Payout</span>
            <span className="text-slate-200">{kelly.payoutMultiplier}x</span>
          </div>
          <div className="flex justify-between text-slate-400">
            <span>EV</span>
            <span className={kelly.ev > 0 ? 'text-green-400' : 'text-red-400'}>
              {kelly.ev >= 0 ? '+' : ''}{(kelly.ev * 100).toFixed(1)}%
            </span>
          </div>
          <div className="flex justify-between text-slate-400">
            <span>Kelly Stake</span>
            <span className="text-green-400 font-medium">${kelly.stake.toFixed(2)}</span>
          </div>
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="text-xs bg-red-500/20 text-red-400 px-2 py-1.5 rounded mb-3">
          {error}
        </div>
      )}

      {/* Action buttons */}
      <div className="flex gap-2">
        <button
          onClick={handlePlace}
          disabled={!validation.isValid || placing || (kelly?.stake ?? 0) <= 0}
          className={cn(
            'flex-1 py-2 rounded-lg text-sm font-medium transition-colors',
            confirming
              ? 'bg-amber-600 hover:bg-amber-500 text-white'
              : 'bg-green-600 hover:bg-green-500 text-white',
            (!validation.isValid || placing || (kelly?.stake ?? 0) <= 0) && 'opacity-50 cursor-not-allowed',
          )}
        >
          {placing ? 'Placing...' : confirming
            ? `Confirm $${kelly?.stake.toFixed(2)} on ${config.label}?`
            : 'Place Entry'}
        </button>
        <button
          onClick={() => { onClearAll(); setConfirming(false) }}
          className="px-3 py-2 rounded-lg text-sm text-slate-400 hover:text-slate-200 hover:bg-slate-700 transition-colors"
        >
          Clear
        </button>
      </div>
    </>
  )

  return (
    <>
      {/* Desktop: fixed right panel */}
      <div className="hidden lg:block w-80 shrink-0">
        <div className="sticky top-24 bg-slate-800 rounded-lg border border-slate-700 p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-semibold text-slate-100">Slip Builder</h3>
            <span className="text-xs text-slate-400">
              {legs.length}/{validation.requiredLegs} legs
            </span>
          </div>
          {panelContent}
        </div>
      </div>

      {/* Mobile: bottom bar */}
      <div className="lg:hidden fixed bottom-0 left-0 right-0 z-50">
        {/* Collapsed bar */}
        {!mobileExpanded && (
          <button
            onClick={() => setMobileExpanded(true)}
            className="w-full bg-slate-800 border-t border-slate-700 px-4 py-3 flex items-center justify-between"
          >
            <div className="flex items-center gap-2">
              <span className="bg-blue-600 text-white text-xs font-bold rounded-full w-6 h-6 flex items-center justify-center">
                {legs.length}
              </span>
              <span className="text-sm font-medium text-slate-200">View Slip</span>
            </div>
            {kelly && kelly.stake > 0 && (
              <span className="text-sm text-green-400 font-medium">${kelly.stake.toFixed(2)}</span>
            )}
          </button>
        )}

        {/* Expanded panel */}
        {mobileExpanded && (
          <div className="bg-slate-800 border-t border-slate-700 p-4 max-h-[70vh] overflow-y-auto">
            <div className="flex items-center justify-between mb-3">
              <h3 className="text-sm font-semibold text-slate-100">Slip Builder</h3>
              <button
                onClick={() => setMobileExpanded(false)}
                className="text-slate-400 hover:text-slate-200 p-1"
              >
                <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
                </svg>
              </button>
            </div>
            {panelContent}
          </div>
        )}
      </div>
    </>
  )
}
