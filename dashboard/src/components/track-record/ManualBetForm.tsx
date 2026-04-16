'use client'

import { useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import { useSport } from '@/contexts/SportContext'
import { STAT_LABELS } from '@/types/predictions'

interface ManualBetFormProps {
  onSuccess?: () => void
  onCancel?: () => void
}

const INITIAL_FORM = {
  game_date: '',
  player_name: '',
  stat_type: '',
  line: '',
  bet_direction: 'over' as 'over' | 'under',
  odds_at_bet: '-110',
  stake: '',
  status: 'won' as 'won' | 'lost' | 'push' | 'pending',
  book_at_bet: '',
}

export function ManualBetForm({ onSuccess, onCancel }: ManualBetFormProps) {
  const { config } = useSport()
  const [form, setForm] = useState(INITIAL_FORM)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const statOptions = config.statTypes.map(st => ({
    value: st,
    label: STAT_LABELS[st] ?? st,
  }))

  const handleChange = (field: keyof typeof INITIAL_FORM, value: string) => {
    setForm(prev => ({ ...prev, [field]: value }))
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)

    const line = parseFloat(form.line)
    const odds = parseFloat(form.odds_at_bet)
    const stake = parseFloat(form.stake)

    if (!form.game_date) { setError('Date is required'); return }
    if (!form.player_name.trim()) { setError('Player name is required'); return }
    if (!form.stat_type) { setError('Stat type is required'); return }
    if (isNaN(line)) { setError('Line must be a number'); return }
    if (isNaN(odds)) { setError('Odds must be a number'); return }
    if (isNaN(stake) || stake <= 0) { setError('Stake must be a positive number'); return }

    // Calculate PnL
    let pnl = 0
    if (form.status === 'won') {
      pnl = odds < 0
        ? (stake / Math.abs(odds)) * 100
        : (stake * odds) / 100
    } else if (form.status === 'lost') {
      pnl = -stake
    } else if (form.status === 'push') {
      pnl = 0
    }

    setSubmitting(true)
    try {
      const supabase = createClient()
      const { data: { user } } = await supabase.auth.getUser()
      if (!user) { setError('Not authenticated'); return }

      const { error: insertErr } = await supabase
        .from('user_bets')
        .upsert({
          user_id:        user.id,
          game_date:      form.game_date,
          player_name:    form.player_name.trim(),
          stat_type:      form.stat_type,
          line,
          bet_direction:  form.bet_direction,
          odds_at_bet:    odds,
          stake,
          status:         form.status,
          pnl:            form.status === 'pending' ? null : pnl,
          book_at_bet:    form.book_at_bet.trim() || null,
          source:         'manual',
          prediction_id:  null,
          player_id:      null,
        }, {
          onConflict: 'user_id,game_date,player_name,stat_type,bet_direction',
        })

      if (insertErr) throw insertErr

      // Rebuild daily log
      await supabase.rpc('rebuild_user_daily_log', { target_user_id: user.id })

      setForm(INITIAL_FORM)
      onSuccess?.()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to save bet')
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      {error && (
        <div className="bg-red-900/30 border border-red-700 rounded-lg px-4 py-3 text-sm text-red-300">
          {error}
        </div>
      )}

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        {/* Date */}
        <div>
          <label className="block text-xs text-slate-400 mb-1">Date</label>
          <input
            type="date"
            value={form.game_date}
            onChange={e => handleChange('game_date', e.target.value)}
            className="w-full bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm text-slate-50 focus:outline-none focus:ring-1 focus:ring-blue-500"
            required
          />
        </div>

        {/* Book */}
        <div>
          <label className="block text-xs text-slate-400 mb-1">Sportsbook (optional)</label>
          <input
            type="text"
            value={form.book_at_bet}
            onChange={e => handleChange('book_at_bet', e.target.value)}
            placeholder="DraftKings, FanDuel..."
            className="w-full bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm text-slate-50 placeholder:text-slate-600 focus:outline-none focus:ring-1 focus:ring-blue-500"
          />
        </div>

        {/* Player */}
        <div className="sm:col-span-2">
          <label className="block text-xs text-slate-400 mb-1">Player Name</label>
          <input
            type="text"
            value={form.player_name}
            onChange={e => handleChange('player_name', e.target.value)}
            placeholder="LeBron James"
            className="w-full bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm text-slate-50 placeholder:text-slate-600 focus:outline-none focus:ring-1 focus:ring-blue-500"
            required
          />
        </div>

        {/* Stat type */}
        <div>
          <label className="block text-xs text-slate-400 mb-1">Stat Type</label>
          <select
            value={form.stat_type}
            onChange={e => handleChange('stat_type', e.target.value)}
            className="w-full bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm text-slate-50 focus:outline-none focus:ring-1 focus:ring-blue-500"
            required
          >
            <option value="">Select stat...</option>
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
            onChange={e => handleChange('line', e.target.value)}
            placeholder="22.5"
            className="w-full bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm text-slate-50 placeholder:text-slate-600 focus:outline-none focus:ring-1 focus:ring-blue-500"
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
                onClick={() => handleChange('bet_direction', dir)}
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

        {/* Odds */}
        <div>
          <label className="block text-xs text-slate-400 mb-1">Odds (American)</label>
          <input
            type="number"
            value={form.odds_at_bet}
            onChange={e => handleChange('odds_at_bet', e.target.value)}
            placeholder="-110"
            className="w-full bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm text-slate-50 placeholder:text-slate-600 focus:outline-none focus:ring-1 focus:ring-blue-500"
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
            onChange={e => handleChange('stake', e.target.value)}
            placeholder="50.00"
            className="w-full bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm text-slate-50 placeholder:text-slate-600 focus:outline-none focus:ring-1 focus:ring-blue-500"
            required
          />
        </div>

        {/* Result */}
        <div>
          <label className="block text-xs text-slate-400 mb-1">Result</label>
          <select
            value={form.status}
            onChange={e => handleChange('status', e.target.value)}
            className="w-full bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm text-slate-50 focus:outline-none focus:ring-1 focus:ring-blue-500"
          >
            <option value="won">Won</option>
            <option value="lost">Lost</option>
            <option value="push">Push</option>
            <option value="pending">Pending</option>
          </select>
        </div>
      </div>

      <div className="flex gap-3 pt-2">
        <button
          type="submit"
          disabled={submitting}
          className="flex-1 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white font-medium py-2 px-4 rounded-lg text-sm transition-colors"
        >
          {submitting ? 'Saving...' : 'Save Bet'}
        </button>
        {onCancel && (
          <button
            type="button"
            onClick={onCancel}
            className="px-4 py-2 rounded-lg text-sm text-slate-400 hover:text-slate-200 hover:bg-slate-700 transition-colors"
          >
            Cancel
          </button>
        )}
      </div>
    </form>
  )
}
