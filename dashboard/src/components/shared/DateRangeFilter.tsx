'use client'

export type DatePreset = '7d' | '30d' | '90d' | 'all' | 'lifetime'

interface DateRangeFilterProps {
  startDate: string
  endDate: string
  onStartDateChange: (date: string) => void
  onEndDateChange: (date: string) => void
}

export function getDefaultStartDate(): string {
  const date = new Date()
  date.setDate(date.getDate() - 30)
  return date.toISOString().split('T')[0]
}

export function getDefaultEndDate(): string {
  return new Date().toISOString().split('T')[0]
}

export function formatDateRangeLabel(startDate: string, endDate: string): string {
  const start = new Date(`${startDate}T00:00:00`)
  const end = new Date(`${endDate}T00:00:00`)
  const format = (date: Date) => date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
  return `${format(start)} – ${format(end)}`
}

export function DateRangeFilter({
  startDate,
  endDate,
  onStartDateChange,
  onEndDateChange,
}: DateRangeFilterProps) {
  const applyPreset = (preset: DatePreset) => {
    const now = new Date()
    onEndDateChange(now.toISOString().split('T')[0])

    if (preset === 'lifetime') {
      onStartDateChange('2020-01-01')
      return
    }

    if (preset === 'all') {
      now.setMonth(now.getMonth() - 6)
    } else {
      now.setDate(now.getDate() - (preset === '7d' ? 7 : preset === '30d' ? 30 : 90))
    }
    onStartDateChange(now.toISOString().split('T')[0])
  }

  return (
    <div className="flex flex-wrap items-center gap-3">
      <div className="flex items-center gap-2">
        <input
          type="date"
          value={startDate}
          onChange={event => onStartDateChange(event.target.value)}
          className="px-2 py-1.5 bg-slate-800 border border-slate-700 rounded-md text-sm text-slate-200 focus:outline-none focus:border-blue-500"
        />
        <span className="text-slate-500 text-sm">to</span>
        <input
          type="date"
          value={endDate}
          onChange={event => onEndDateChange(event.target.value)}
          className="px-2 py-1.5 bg-slate-800 border border-slate-700 rounded-md text-sm text-slate-200 focus:outline-none focus:border-blue-500"
        />
      </div>
      <div className="flex items-center gap-1">
        {(['7d', '30d', '90d', 'all', 'lifetime'] as DatePreset[]).map(preset => (
          <button
            key={preset}
            onClick={() => applyPreset(preset)}
            className="px-2.5 py-1 rounded text-xs font-medium bg-slate-800 border border-slate-700 text-slate-400 hover:text-slate-200 hover:border-slate-500 transition-colors"
          >
            {preset === 'all' ? '6M' : preset === 'lifetime' ? 'ALL' : preset.toUpperCase()}
          </button>
        ))}
      </div>
    </div>
  )
}
