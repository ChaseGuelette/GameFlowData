'use client'

import type { KalshiMarket } from '@/types/kalshi'
import { KALSHI_STAT_LABELS, kalshiMakerFee, kalshiTakerFee } from '@/types/kalshi'
import { KalshiCountdown } from './KalshiCountdown'

interface KalshiMarketDetailProps {
  market: KalshiMarket
  onClose: () => void
}

export function KalshiMarketDetail({ market, onClose }: KalshiMarketDetailProps) {
  const makerFeeYes = kalshiMakerFee(market.yes_price)
  const takerFeeYes = kalshiTakerFee(market.yes_price)
  const makerFeeNo = kalshiMakerFee(market.no_price)
  const takerFeeNo = kalshiTakerFee(market.no_price)

  const edgeColor = (edge: number | null) => {
    if (!edge) return 'text-slate-400'
    if (edge >= 0.05) return 'text-green-400'
    if (edge > 0) return 'text-green-300'
    if (edge > -0.03) return 'text-slate-400'
    return 'text-red-400'
  }

  return (
    <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4" onClick={onClose}>
      <div
        className="bg-slate-800 rounded-xl border border-slate-700 max-w-lg w-full max-h-[90vh] overflow-y-auto p-6"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-start justify-between mb-4">
          <div>
            <h3 className="text-lg font-semibold text-slate-100">
              {market.player_name}
            </h3>
            <p className="text-sm text-slate-400">
              {KALSHI_STAT_LABELS[market.stat_type] || market.stat_type} — Line {market.line}
            </p>
          </div>
          <button
            onClick={onClose}
            className="text-slate-400 hover:text-slate-200 p-1"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Pricing */}
        <div className="grid grid-cols-2 gap-3 mb-4">
          <div className="bg-slate-900 rounded-lg p-3">
            <div className="text-xs text-slate-500 mb-1">YES (Over)</div>
            <div className="text-xl font-bold text-green-400">{market.yes_price}c</div>
            <div className="text-xs text-slate-500">Bid: {market.yes_bid}c / Ask: {market.yes_ask}c</div>
          </div>
          <div className="bg-slate-900 rounded-lg p-3">
            <div className="text-xs text-slate-500 mb-1">NO (Under)</div>
            <div className="text-xl font-bold text-red-400">{market.no_price}c</div>
            <div className="text-xs text-slate-500 flex items-center gap-1">
              API Only
              <span className="bg-violet-600/20 text-violet-300 text-[10px] px-1 rounded">API</span>
            </div>
          </div>
        </div>

        {/* Model vs Kalshi */}
        {market.model_prob != null && market.kalshi_implied != null && (
          <div className="bg-slate-900 rounded-lg p-3 mb-4">
            <div className="text-xs text-slate-500 mb-2">Model vs Kalshi</div>
            <div className="flex items-center gap-4">
              <div>
                <div className="text-xs text-slate-500">Model Prob</div>
                <div className="text-lg font-semibold text-blue-400">
                  {(market.model_prob * 100).toFixed(1)}%
                </div>
              </div>
              <div className="text-slate-600">vs</div>
              <div>
                <div className="text-xs text-slate-500">Kalshi Implied</div>
                <div className="text-lg font-semibold text-slate-300">
                  {(market.kalshi_implied * 100).toFixed(1)}%
                </div>
              </div>
              <div className="ml-auto">
                <div className="text-xs text-slate-500">Raw Edge</div>
                <div className={`text-lg font-semibold ${edgeColor(market.raw_edge)}`}>
                  {market.raw_edge != null ? `${(market.raw_edge * 100).toFixed(1)}%` : '--'}
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Fee Breakdown */}
        <div className="bg-slate-900 rounded-lg p-3 mb-4">
          <div className="text-xs text-slate-500 mb-2">Fee Breakdown (per contract)</div>
          <div className="grid grid-cols-2 gap-3 text-sm">
            <div>
              <div className="text-slate-400">YES Contract</div>
              <div className="text-xs text-slate-500">
                Maker: ${makerFeeYes.toFixed(4)} | Taker: ${takerFeeYes.toFixed(4)}
              </div>
            </div>
            <div>
              <div className="text-slate-400">NO Contract</div>
              <div className="text-xs text-slate-500">
                Maker: ${makerFeeNo.toFixed(4)} | Taker: ${takerFeeNo.toFixed(4)}
              </div>
            </div>
          </div>
          <div className="mt-2 grid grid-cols-2 gap-3 text-sm">
            <div>
              <div className="text-xs text-slate-500">Maker Edge</div>
              <div className={`font-semibold ${edgeColor(market.maker_fee_adjusted_edge)}`}>
                {market.maker_fee_adjusted_edge != null
                  ? `${(market.maker_fee_adjusted_edge * 100).toFixed(2)}%`
                  : '--'}
              </div>
            </div>
            <div>
              <div className="text-xs text-slate-500">Taker Edge</div>
              <div className={`font-semibold ${edgeColor(market.taker_fee_adjusted_edge)}`}>
                {market.taker_fee_adjusted_edge != null
                  ? `${(market.taker_fee_adjusted_edge * 100).toFixed(2)}%`
                  : '--'}
              </div>
            </div>
          </div>
        </div>

        {/* Sportsbook Comparison */}
        {market.sportsbook_consensus_line != null && (
          <div className="bg-slate-900 rounded-lg p-3 mb-4">
            <div className="text-xs text-slate-500 mb-2">Sportsbook Comparison</div>
            <div className="flex items-center gap-4 text-sm">
              <div>
                <div className="text-xs text-slate-500">Kalshi Line</div>
                <div className="text-slate-200 font-medium">{market.line}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Sportsbook</div>
                <div className="text-slate-200 font-medium">{market.sportsbook_consensus_line}</div>
              </div>
              <div>
                <div className="text-xs text-slate-500">Difference</div>
                <div className={`font-medium ${
                  market.line_vs_sportsbook != null && market.line_vs_sportsbook > 0
                    ? 'text-amber-400' : 'text-green-400'
                }`}>
                  {market.line_vs_sportsbook != null
                    ? `${market.line_vs_sportsbook > 0 ? '+' : ''}${market.line_vs_sportsbook.toFixed(1)}`
                    : '--'}
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Liquidity & Timing */}
        <div className="grid grid-cols-3 gap-3 text-sm">
          <div className="bg-slate-900 rounded-lg p-3">
            <div className="text-xs text-slate-500">Volume</div>
            <div className="text-slate-200 font-medium">{market.volume.toLocaleString()}</div>
          </div>
          <div className="bg-slate-900 rounded-lg p-3">
            <div className="text-xs text-slate-500">Open Interest</div>
            <div className="text-slate-200 font-medium">{market.open_interest.toLocaleString()}</div>
          </div>
          <div className="bg-slate-900 rounded-lg p-3">
            <div className="text-xs text-slate-500">Closes In</div>
            <KalshiCountdown closeTime={market.close_time} />
          </div>
        </div>
      </div>
    </div>
  )
}
