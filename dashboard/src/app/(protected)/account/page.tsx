'use client'

import { useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'
import Link from 'next/link'
import { createClient } from '@/lib/supabase/client'
import { DISCORD_URL } from '@/lib/constants'
import { useUserPreferences } from '@/lib/hooks/useUserPreferences'
import type { UserSubscription, SubscriptionStatus } from '@/types/subscription'

function StatusBadge({ status, sub }: { status: SubscriptionStatus | null; sub: UserSubscription | null }) {
  if (!status || status === 'inactive') {
    return <span className="px-2 py-1 rounded text-xs font-medium bg-slate-600/30 text-slate-400">No Subscription</span>
  }
  if (status === 'trialing') {
    const trialEnd = sub?.trial_end ? new Date(sub.trial_end).toLocaleDateString() : null
    return (
      <span className="px-2 py-1 rounded text-xs font-medium bg-green-500/20 text-green-400">
        Free Trial{trialEnd ? ` — ends ${trialEnd}` : ''}
      </span>
    )
  }
  if (status === 'active') {
    if (sub?.cancel_at_period_end) {
      const until = sub.current_period_end ? new Date(sub.current_period_end).toLocaleDateString() : null
      return (
        <span className="px-2 py-1 rounded text-xs font-medium bg-yellow-500/20 text-yellow-400">
          Canceling{until ? ` — access until ${until}` : ''}
        </span>
      )
    }
    const renewal = sub?.current_period_end ? new Date(sub.current_period_end).toLocaleDateString() : null
    return (
      <span className="px-2 py-1 rounded text-xs font-medium bg-green-500/20 text-green-400">
        Active{renewal ? ` — renews ${renewal}` : ''}
      </span>
    )
  }
  if (status === 'past_due') {
    return <span className="px-2 py-1 rounded text-xs font-medium bg-red-500/20 text-red-400">Past Due</span>
  }
  if (status === 'canceled') {
    return <span className="px-2 py-1 rounded text-xs font-medium bg-slate-600/30 text-slate-400">Canceled</span>
  }
  return null
}

function planName(planId: string | null): string {
  if (!planId) return 'None'
  if (planId === process.env.NEXT_PUBLIC_STRIPE_PRICE_MONTHLY) return 'Pro Monthly'
  if (planId === process.env.NEXT_PUBLIC_STRIPE_PRICE_ANNUAL) return 'Pro Annual'
  // Fallback: infer from plan_id string
  if (planId.toLowerCase().includes('annual') || planId.toLowerCase().includes('yearly')) return 'Pro Annual'
  if (planId.toLowerCase().includes('month')) return 'Pro Monthly'
  return 'Pro'
}

export default function AccountPage() {
  const router = useRouter()
  const [email, setEmail] = useState<string | null>(null)
  const [userId, setUserId] = useState<string | null>(null)
  const [sub, setSub] = useState<UserSubscription | null>(null)
  const [loading, setLoading] = useState(true)
  const [portalLoading, setPortalLoading] = useState(false)
  const { prefs, updatePref } = useUserPreferences()

  useEffect(() => {
    async function fetchAccountData() {
      const supabase = createClient()
      const { data: { user } } = await supabase.auth.getUser()
      if (user) {
        setEmail(user.email ?? null)
        setUserId(user.id)

        const { data } = await supabase
          .from('user_subscriptions')
          .select('*')
          .eq('user_id', user.id)
          .single()

        if (data) setSub(data as UserSubscription)
      }
      setLoading(false)
    }

    fetchAccountData()
  }, [])

  // Show success message if redirected from checkout
  const [checkoutSuccess, setCheckoutSuccess] = useState(false)
  useEffect(() => {
    if (typeof window !== 'undefined') {
      const params = new URLSearchParams(window.location.search)
      if (params.get('checkout') === 'success') {
        setCheckoutSuccess(true)
        // Clean URL
        window.history.replaceState({}, '', '/account')
      }
    }
  }, [])

  async function handleManageBilling() {
    setPortalLoading(true)
    try {
      const res = await fetch('/api/stripe/portal', { method: 'POST' })
      const data = await res.json()
      if (res.ok && data.url) {
        router.push(data.url)
      }
    } finally {
      setPortalLoading(false)
    }
  }

  if (loading) {
    return (
      <main className="flex-1 flex items-center justify-center">
        <div className="text-slate-400">Loading account...</div>
      </main>
    )
  }

  const hasStripeCustomer = !!sub?.stripe_customer_id

  return (
    <main className="flex-1 max-w-2xl mx-auto w-full px-4 sm:px-6 lg:px-8 py-8">
      <h1 className="text-2xl font-bold text-slate-50 mb-8">Account</h1>

      {checkoutSuccess && (
        <div className="mb-6 p-4 bg-green-500/10 border border-green-500/30 rounded-lg text-green-400 text-sm">
          Your subscription is active. Welcome to GameFlow Pro!
        </div>
      )}

      {/* Profile */}
      <div className="bg-slate-800 border border-slate-700 rounded-lg p-6 mb-6">
        <h2 className="text-lg font-semibold text-slate-50 mb-4">Profile</h2>
        <div className="space-y-3">
          <div className="flex justify-between">
            <span className="text-slate-400">Email</span>
            <span className="text-slate-200">{email}</span>
          </div>
          <div className="flex justify-between items-center">
            <span className="text-slate-400">Status</span>
            <StatusBadge status={sub?.status ?? null} sub={sub} />
          </div>
          {sub && sub.status !== 'inactive' && (
            <div className="flex justify-between items-center">
              <span className="text-slate-400">Plan</span>
              <span className="text-slate-200 text-sm">{planName(sub.plan_id)}</span>
            </div>
          )}
        </div>
      </div>

      {/* Billing */}
      <div className="bg-slate-800 border border-slate-700 rounded-lg p-6 mb-6">
        <h2 className="text-lg font-semibold text-slate-50 mb-4">Billing</h2>
        {hasStripeCustomer ? (
          <div className="space-y-3">
            <p className="text-slate-400 text-sm">
              Manage your payment method, cancel, or switch plans via the billing portal.
            </p>
            <button
              onClick={handleManageBilling}
              disabled={portalLoading}
              className="px-6 py-2 bg-slate-700 hover:bg-slate-600 disabled:opacity-50 disabled:cursor-not-allowed text-slate-200 font-medium rounded-md transition-colors text-sm"
            >
              {portalLoading ? 'Loading...' : 'Manage Billing'}
            </button>
          </div>
        ) : (
          <div className="space-y-3">
            <p className="text-slate-400 text-sm">
              You don&apos;t have an active subscription yet. Get full access to predictions, model picks, and more.
            </p>
            <Link
              href="/subscribe"
              className="inline-block px-6 py-2 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-md transition-colors text-sm"
            >
              Subscribe Now
            </Link>
          </div>
        )}
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
