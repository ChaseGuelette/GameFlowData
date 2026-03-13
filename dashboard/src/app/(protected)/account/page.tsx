'use client'

import { useEffect, useState } from 'react'
import { createClient } from '@/lib/supabase/client'
import { DISCORD_URL } from '@/lib/constants'
import { useUserPreferences } from '@/lib/hooks/useUserPreferences'

export default function AccountPage() {
  const [email, setEmail] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const { prefs, updatePref } = useUserPreferences()

  useEffect(() => {
    async function fetchAccountData() {
      const supabase = createClient()
      const { data: { user } } = await supabase.auth.getUser()
      if (user) {
        setEmail(user.email ?? null)
      }
      setLoading(false)
    }

    fetchAccountData()
  }, [])

  if (loading) {
    return (
      <main className="flex-1 flex items-center justify-center">
        <div className="text-slate-400">Loading account...</div>
      </main>
    )
  }

  return (
    <main className="flex-1 max-w-2xl mx-auto w-full px-4 sm:px-6 lg:px-8 py-8">
      <h1 className="text-2xl font-bold text-slate-50 mb-8">Account</h1>

      {/* Profile */}
      <div className="bg-slate-800 border border-slate-700 rounded-lg p-6 mb-6">
        <h2 className="text-lg font-semibold text-slate-50 mb-4">Profile</h2>
        <div className="space-y-3">
          <div className="flex justify-between">
            <span className="text-slate-400">Email</span>
            <span className="text-slate-200">{email}</span>
          </div>
          <div className="flex justify-between items-center">
            <span className="text-slate-400">Plan</span>
            <span className="px-2 py-1 rounded text-xs font-medium bg-blue-500/20 text-blue-400">
              Free Beta
            </span>
          </div>
        </div>
      </div>

      {/* Bankroll Settings */}
      <div className="bg-slate-800 border border-slate-700 rounded-lg p-6 mb-6">
        <h2 className="text-lg font-semibold text-slate-50 mb-4">Bankroll Settings</h2>
        <div className="space-y-4">
          <div>
            <label className="block text-sm text-slate-400 mb-1">Initial Bankroll</label>
            <p className="text-xs text-slate-500 mb-2">What you started with — used for ROI and growth calculations</p>
            <div className="relative">
              <span className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400">$</span>
              <input
                type="number"
                min={0}
                step={100}
                value={prefs.initialBankroll}
                onChange={e => {
                  const val = parseFloat(e.target.value)
                  if (!isNaN(val) && val >= 0) updatePref('initialBankroll', val)
                }}
                className="w-full pl-7 pr-3 py-2 bg-slate-900 border border-slate-600 rounded-md text-slate-100 focus:outline-none focus:border-blue-500"
              />
            </div>
          </div>
          <div>
            <label className="block text-sm text-slate-400 mb-1">Current Bankroll</label>
            <p className="text-xs text-slate-500 mb-2">Your actual bankroll right now — used for bet sizing</p>
            <div className="relative">
              <span className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400">$</span>
              <input
                type="number"
                min={0}
                step={100}
                value={prefs.bankroll}
                onChange={e => {
                  const val = parseFloat(e.target.value)
                  if (!isNaN(val) && val >= 0) updatePref('bankroll', val)
                }}
                className="w-full pl-7 pr-3 py-2 bg-slate-900 border border-slate-600 rounded-md text-slate-100 focus:outline-none focus:border-blue-500"
              />
            </div>
          </div>
        </div>
      </div>

      {/* Community */}
      <div className="bg-slate-800 border border-slate-700 rounded-lg p-6">
        <h2 className="text-lg font-semibold text-slate-50 mb-4">Community</h2>
        <p className="text-slate-400 text-sm mb-4">
          Join our Discord for daily picks, discussion, and updates on new features.
        </p>
        <a
          href={DISCORD_URL}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-block px-6 py-2 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-md transition-colors"
        >
          Join Discord
        </a>
      </div>
    </main>
  )
}
