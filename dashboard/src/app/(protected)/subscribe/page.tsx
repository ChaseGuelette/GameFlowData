'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'

const features = [
  'Full prediction access (pts, reb, ast)',
  'Curated Model Picks daily',
  'Black-Litterman edge analysis',
  'Quantile distributions & confidence bands',
  'Paper trading performance tracking',
  'Bet history & stat breakdowns',
]

export default function SubscribePage() {
  const router = useRouter()
  const [loading, setLoading] = useState<'pro_monthly' | 'pro_annual' | null>(null)
  const [error, setError] = useState<string | null>(null)

  async function handleCheckout(plan: 'pro_monthly' | 'pro_annual') {
    setLoading(plan)
    setError(null)
    try {
      const res = await fetch('/api/stripe/checkout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ plan }),
      })
      const data = await res.json()
      if (!res.ok) {
        setError(data.error ?? 'Something went wrong. Please try again.')
        return
      }
      router.push(data.url)
    } catch {
      setError('Something went wrong. Please try again.')
    } finally {
      setLoading(null)
    }
  }

  return (
    <main className="flex-1 max-w-4xl mx-auto w-full px-4 sm:px-6 lg:px-8 py-12">
      <div className="text-center mb-10">
        <h1 className="text-3xl font-bold text-slate-50">Choose Your Plan</h1>
        <p className="mt-3 text-slate-400">7-day free trial included — no credit card charge until trial ends</p>
      </div>

      {error && (
        <div className="mb-6 p-4 bg-red-500/10 border border-red-500/30 rounded-lg text-red-400 text-sm text-center">
          {error}
        </div>
      )}

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-6">
        {/* Monthly */}
        <div className="bg-slate-800 border border-slate-700 rounded-xl p-8 flex flex-col">
          <div className="mb-6">
            <p className="text-sm font-medium text-blue-400 uppercase tracking-wide">Monthly</p>
            <div className="mt-2 flex items-baseline gap-1">
              <span className="text-5xl font-bold text-slate-50">$19</span>
              <span className="text-2xl text-slate-400">.99</span>
              <span className="text-slate-400 ml-1">/mo</span>
            </div>
            <p className="text-sm text-slate-500 mt-1">Billed monthly</p>
          </div>

          <ul className="space-y-3 mb-8 flex-1">
            {features.map((f) => (
              <li key={f} className="flex items-start gap-3">
                <span className="text-green-400 mt-0.5 shrink-0">&#10003;</span>
                <span className="text-sm text-slate-300">{f}</span>
              </li>
            ))}
          </ul>

          <button
            onClick={() => handleCheckout('pro_monthly')}
            disabled={loading !== null}
            className="w-full py-3 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold rounded-lg transition-colors"
          >
            {loading === 'pro_monthly' ? 'Redirecting...' : 'Start Free Trial'}
          </button>
        </div>

        {/* Annual */}
        <div className="bg-slate-800 border border-blue-500/50 rounded-xl p-8 flex flex-col relative">
          <div className="absolute -top-3 left-1/2 -translate-x-1/2">
            <span className="px-3 py-1 bg-blue-600 text-white text-xs font-bold rounded-full uppercase tracking-wide">
              Save 17%
            </span>
          </div>

          <div className="mb-6">
            <p className="text-sm font-medium text-blue-400 uppercase tracking-wide">Annual</p>
            <div className="mt-2 flex items-baseline gap-1">
              <span className="text-5xl font-bold text-slate-50">$199</span>
              <span className="text-slate-400 ml-1">/yr</span>
            </div>
            <p className="text-sm text-slate-500 mt-1">$16.58/mo — billed annually</p>
          </div>

          <ul className="space-y-3 mb-8 flex-1">
            {features.map((f) => (
              <li key={f} className="flex items-start gap-3">
                <span className="text-green-400 mt-0.5 shrink-0">&#10003;</span>
                <span className="text-sm text-slate-300">{f}</span>
              </li>
            ))}
          </ul>

          <button
            onClick={() => handleCheckout('pro_annual')}
            disabled={loading !== null}
            className="w-full py-3 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold rounded-lg transition-colors"
          >
            {loading === 'pro_annual' ? 'Redirecting...' : 'Start Free Trial'}
          </button>
        </div>
      </div>

      <p className="text-center text-xs text-slate-500 mt-6">
        7-day free trial. Cancel anytime. Secure checkout via Stripe.
      </p>
    </main>
  )
}
