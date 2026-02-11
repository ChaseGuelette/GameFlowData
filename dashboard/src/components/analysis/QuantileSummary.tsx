interface QuantileSummaryProps {
  q10: number
  q25: number
  q50: number
  q75: number
  q90: number
  line: number
}

// Safe toFixed that handles NaN
const safeFixed = (val: number, decimals: number = 1): string => {
  return Number.isFinite(val) ? val.toFixed(decimals) : '—'
}

export function QuantileSummary({ q10, q25, q50, q75, q90, line }: QuantileSummaryProps) {
  const quantiles = [
    { label: 'Q10', value: q10 },
    { label: 'Q25', value: q25 },
    { label: 'Q50', value: q50 },
    { label: 'Q75', value: q75 },
    { label: 'Q90', value: q90 },
  ]

  // Calculate position of line on the distribution (with safety checks)
  const minQ = Number.isFinite(q10) ? q10 : 0
  const maxQ = Number.isFinite(q90) ? q90 : 10
  const range = maxQ - minQ
  const linePosition = range > 0 ? ((line - minQ) / range) * 100 : 50
  const q50Position = range > 0 ? (((Number.isFinite(q50) ? q50 : 5) - minQ) / range) * 100 : 50

  return (
    <div className="space-y-4">
      {/* Quantile values */}
      <div className="flex justify-between text-center">
        {quantiles.map(({ label, value }) => (
          <div key={label} className="flex-1">
            <div className="text-slate-400 text-xs">{label}</div>
            <div className="text-slate-50 font-semibold">{safeFixed(value)}</div>
          </div>
        ))}
      </div>

      {/* Visual distribution bar */}
      <div className="relative h-8 bg-slate-700 rounded-lg overflow-hidden">
        {/* Gradient from Q10 to Q90 */}
        <div
          className="absolute inset-y-0 bg-gradient-to-r from-red-500/30 via-yellow-500/30 to-green-500/30"
          style={{ left: '0%', right: '0%' }}
        />

        {/* Line marker */}
        <div
          className="absolute top-0 bottom-0 w-0.5 bg-white z-10"
          style={{
            left: `${Math.max(0, Math.min(100, linePosition))}%`,
          }}
        >
          <div className="absolute -top-5 left-1/2 -translate-x-1/2 text-xs text-slate-300 whitespace-nowrap">
            Line: {line}
          </div>
        </div>

        {/* Q50 marker */}
        <div
          className="absolute top-0 bottom-0 w-1 bg-blue-500 z-5"
          style={{
            left: `${Math.max(0, Math.min(100, q50Position))}%`,
          }}
        />
      </div>

      <div className="flex justify-between text-xs text-slate-400">
        <span>Floor ({safeFixed(q10)})</span>
        <span className="text-blue-400">Median ({safeFixed(q50)})</span>
        <span>Ceiling ({safeFixed(q90)})</span>
      </div>
    </div>
  )
}
