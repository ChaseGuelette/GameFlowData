'use client'

import type { VerifiedMarketLink } from '@/types/arb-scanner'

interface MatchQueueTableProps {
  links: VerifiedMarketLink[]
  loading: boolean
  decide: (id: number, action: 'approve' | 'reject') => void
}

function confidenceColor(confidence: number | null): string {
  if (confidence == null) return 'text-slate-400'
  if (confidence >= 0.80) return 'text-green-400'
  if (confidence >= 0.65) return 'text-yellow-400'
  return 'text-red-400'
}

export function MatchQueueTable({ links, loading, decide }: MatchQueueTableProps) {
  if (loading) {
    return (
      <div className="space-y-2">
        {[1, 2, 3].map((i) => (
          <div key={i} className="h-16 rounded-lg border border-slate-700 bg-slate-800 animate-pulse" />
        ))}
      </div>
    )
  }

  if (links.length === 0) {
    return (
      <div className="flex items-center justify-center h-40 text-slate-400 text-sm">
        No pending matches — queue is clear
      </div>
    )
  }

  return (
    <div className="overflow-x-auto">
      <div className="mb-3 text-xs text-slate-400">
        <span className="bg-slate-700 text-slate-300 px-2 py-0.5 rounded font-medium">
          {links.length} pending
        </span>
      </div>
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-700 text-left text-xs text-slate-400">
            <th className="pb-2 pr-4 font-medium">Series</th>
            <th className="pb-2 pr-4 font-medium">Kalshi Title</th>
            <th className="pb-2 pr-4 font-medium">Poly Question</th>
            <th className="pb-2 pr-4 font-medium text-right">Confidence</th>
            <th className="pb-2 text-right font-medium">Actions</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800">
          {links.map((link) => (
            <tr key={link.id} className="hover:bg-slate-800/50 transition-colors">
              <td className="py-2.5 pr-4 text-slate-300 font-mono text-xs whitespace-nowrap">
                {link.series ?? link.kalshi_ticker.split('-')[0]}
              </td>
              <td className="py-2.5 pr-4 text-slate-300 text-xs max-w-[220px] truncate" title={link.kalshi_title ?? ''}>
                {link.kalshi_title ?? '—'}
              </td>
              <td className="py-2.5 pr-4 text-slate-300 text-xs max-w-[260px] truncate" title={link.poly_question ?? ''}>
                {link.poly_question ?? '—'}
              </td>
              <td className={`py-2.5 pr-4 text-right font-mono text-xs ${confidenceColor(link.match_confidence)}`}>
                {link.match_confidence != null ? `${(link.match_confidence * 100).toFixed(0)}%` : '—'}
              </td>
              <td className="py-2.5 text-right whitespace-nowrap space-x-2">
                <button
                  onClick={() => decide(link.id, 'approve')}
                  className="px-2.5 py-1 text-xs font-medium rounded bg-green-600/20 text-green-400 hover:bg-green-600/40 transition-colors"
                >
                  Approve
                </button>
                <button
                  onClick={() => decide(link.id, 'reject')}
                  className="px-2.5 py-1 text-xs font-medium rounded border border-red-700/50 text-red-400 hover:bg-red-600/20 transition-colors"
                >
                  Reject
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
