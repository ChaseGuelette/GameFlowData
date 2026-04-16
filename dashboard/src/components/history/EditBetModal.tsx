'use client'

import { useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import { useSport } from '@/contexts/SportContext'
import { type PaperBet, STAT_LABELS } from '@/types/predictions'

interface EditBetModalProps {
  bet: PaperBet
  onClose: () => void
  onSaved: () => void
}

export function EditBetModal({ bet, onClose, onSaved }: EditBetModalProps) {
  const { config } = useSport()

  const [form, setForm] = useState({
    game_date:     bet.game_date,
    player_name:   bet.player_name,
    stat_type:     bet.stat_type,
    line:          String(bet.line),
    bet_direction: bet.bet_direction as 'over' | 'under',
    odds_at_bet:   String(bet.odds_at_bet),
    stake:         String(bet.stake),
    status:        bet.status as 'won' | 'lost' | 'push' | 'pending',
    book_at_bet:   (bet.bookmaker ?? ''),
    pnl:           bet.pnl != null ? String(bet.pnl) : '',
    overridePnl:   bet.pnl != null,
  })

  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const set = (field: keyof typeof form, value: string | boolean) =>
    setForm(prev => ({ ...prev, [field]: value }))

  const calcPnl = () => {
    const odds  = parseFloat(form.odds_at_bet)
    const stake = parseFloat(form.stake)
    if (isNaN(odds) || isNaN(stake)) return null
    if (form.status === 'won') {
      return odds < 0
        ? parseFloat(((stake / Math.abs(odds)) * 100).toFixed(2))
        : parseFloat(((stake * odds) / 100).toFixed(2))
    }
    if (form.status === 'lost')  return -stake
    if (form.status === 'push')  return 0
    return null // pending
  }

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)

    const line  = parseFloat(form.line)
    const odds  = parseFloat(form.odds_at_bet)
    const stake = parseFloat(form.stake)

    if (!form.game_date)          { setError('Date is required'); return }
    if (!form.player_name.trim()) { setError('Player name is required'); return }
    if (!form.stat_type)          { setError('Stat type is required'); return }
    if (isNaN(line))              { setError('Line must be a number'); return }
    if (isNaN(odds))              { setError('Odds must be a number'); return }
    if (isNaN(stake) || stake < 0){ setError('Stake must be a non-negative number'); return }

    const pnl = form.overridePnl && form.pnl !== ''
      ? parseFloat(form.pnl)
      : calcPnl()

    setSaving(true)
    try {
      const supabase = createClient()
      const { data: { user } } = await supabase.auth.getUser()
      if (!user) { setError('Not authenticated'); return }

      const { error: updateErr } = await supabase
        .from('user_bets')
        .update({
          game_date:     form.game_date,
          player_name:   form.player_name.trim(),
          stat_type:     form.stat_type,
          line,
          bet_direction: form.bet_direction,
          odds_at_bet:   odds,
          stake,
          status:        form.status,
          pnl:           form.status === 'pending' ? null : pnl,
          book_at_bet:   form.book_at_bet.trim() || null,
        })
        .eq('id', bet.id)
        .eq('user_id', user.id)

      if (updateErr) throw updateErr

      // Rebuild daily log so track record reflects the change
      await supabase.rpc('rebuild_user_daily_log', { target_user_id: user.id })

      onSaved()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to save')
    } finally {
      setSaving(false)
    }
  }

  const statOptions = config.statTypes.map(st => ({
    value: st,
    label: STAT_LABELS[st] ?? st,
  }))

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-black/60" onClick={onClose} />
      <div className="relative bg-slate-800 border border-slate-700 rounded-xl w-full max-w-lg max-h-[90vh] overflow-y-auto shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-slate-700">
          <h2 className="text-base font-semibold text-slate-50">Edit Bet</h2>
          <button onClick={onClose} className="text-slate-400 hover:text-slate-200 transition-colors">
            <svg className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Form */}
        <form onSubmit={handleSave} className="p-5 space-y-4">
          {error && (
            <div className="bg-red-900/30 border border-red-700 rounded-lg px-4 py-3 text-sm text-red-300">
              {error}
            </div>
          )}

          <div className="grid grid-cols-2 gap-4">
            {/* Date */}
            <div>
              <label className="block text-xs text-slate-400 mb-1">Date</label>
              <input
                type="date"
                value={form.game_date}
                onChange={e => set('game_date', e.target.value)}
                className="w-full bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm text-slate-50 focus:outline-none focus:ring-1 focus:ring-blue-500"
                required
              />
            </div>

            {/* Book */}
            <div>
              <label className="block text-xs text-slate-400 mb-1">Sportsbook</label>
              <input
                type="text"
                value={form.book_at_bet}
                onChange={e => set('book_at_bet', e.target.value)}
                placeholder="DraftKings..."
                className="w-full bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm text-slate-50 placeholder:text-slate-600 focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
            </div>

            {/* Player */}
            <div className="col-span-2">
              <label className="block text-xs text-slate-400 mb-1">Player Name</label>
              <input
                type="text"
                value={form.player_name}
                onChange={e => set('player_name', e.target.value)}
                className="w-full bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm text-slate-50 focus:outline-none focus:ring-1 focus:ring-blue-500"
                required
              />
            </div>

            {/* Stat type */}
            <div>
              <label className="block text-xs text-slate-400 mb-1">Stat Type</label>
              <select
                value={form.stat_type}
                onChange={e => set('stat_type', e.target.value)}
                className="w-full bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm text-slate-50 focus:outline-none focus:ring-1 focus:ring-blue-500"
                required
              >
                {statOptions.map(o => (
                  <option key={o.value} value={o.value}>{o.label}</option>
                ))}
              </select>
            </div>

            {/* Line */}
            <div>
              <label className="block text-xs text-slate-400 mb-1">Line</label>
              <input
                type="number"
                step="0.5"
                value={form.line}
                onChange={e => set('line', e.target.value)}
                className="w-full bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm text-slate-50 focus:outline-none focus:ring-1 focus:ring-blue-500"
                required
              />
            </div>

            {/* Direction */}
            <div>
              <label className="block text-xs text-slate-400 mb-1">Direction</label>
              <div className="flex bg-slate-900 border border-slate-600 rounded-md overflow-hidden">
                {(['over', 'under'] as const).map(dir => (
                  <button
                    key={dir}
                    type="button"
                    onClick={() => set('bet_direction', dir)}
                    className={`flex-1 py-2 text-sm font-medium transition-colors ${
                      form.bet_direction === dir
                        ? dir === 'over' ? 'bg-blue-600 text-white' : 'bg-purple-600 text-white'
                        : 'text-slate-400 hover:text-slate-200'
                    }`}
                  >
                    {dir.toUpperCase()}
                  </button>
                ))}
              </div>
            </div>

            {/* Result */}
            <div>
              <label className="block text-xs text-slate-400 mb-1">Result</label>
              <select
                value={form.status}
                onChange={e => set('status', e.target.value)}
                className="w-full bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm text-slate-50 focus:outline-none focus:ring-1 focus:ring-blue-500"
              >
                <option value="won">Won</option>
                <option value="lost">Lost</option>
                <option value="push">Push</option>
                <option value="pending">Pending</option>
              </select>
            </div>

            {/* Odds */}
            <div>
              <label className="block text-xs text-slate-400 mb-1">Odds (American)</label>
              <input
                type="number"
                value={form.odds_at_bet}
                onChange={e => set('odds_at_bet', e.target.value)}
                className="w-full bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm text-slate-50 focus:outline-none focus:ring-1 focus:ring-blue-500"
                required
              />
            </div>

            {/* Stake */}
            <div>
              <label className="block text-xs text-slate-400 mb-1">Stake ($)</label>
              <input
                type="number"
                step="0.01"
                min="0"
                value={form.stake}
                onChange={e => set('stake', e.target.value)}
                className="w-full bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm text-slate-50 focus:outline-none focus:ring-1 focus:ring-blue-500"
                required
              />
            </div>

            {/* P&L — auto-calculated or manual override */}
            {form.status !== 'pending' && (
              <div className="col-span-2">
                <div className="flex items-center justify-between mb-1">
                  <label className="text-xs text-slate-400">P&L ($)</label>
                  <label className="flex items-center gap-1.5 cursor-pointer text-xs text-slate-500">
                    <input
                      type="checkbox"
                      checked={form.overridePnl}
                      onChange={e => set('overridePnl', e.target.checked)}
                      className="rounded"
                    />
                    Override (auto: {calcPnl() != null ? `$${calcPnl()!.toFixed(2)}` : '—'})
                  </label>
                </div>
                <input
                  type="number"
                  step="0.01"
                  value={form.overridePnl ? form.pnl : String(calcPnl() ?? '')}
                  onChange={e => set('pnl', e.target.value)}
                  disabled={!form.overridePnl}
                  className="w-full bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm text-slate-50 disabled:text-slate-500 disabled:border-slate-700 focus:outline-none focus:ring-1 focus:ring-blue-500"
                />
              </div>
            )}
          </div>

          <div className="flex gap-3 pt-1">
            <button
              type="submit"
              disabled={saving}
              className="flex-1 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white font-medium py-2 px-4 rounded-lg text-sm transition-colors"
            >
              {saving ? 'Saving...' : 'Save Changes'}
            </button>
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 rounded-lg text-sm text-slate-400 hover:text-slate-200 hover:bg-slate-700 transition-colors"
            >
              Cancel
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
