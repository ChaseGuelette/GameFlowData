import Link from 'next/link'
import { createClient } from '@/lib/supabase/server'
import { DISCORD_URL } from '@/lib/constants'

const features = [
  'Full prediction access (pts, reb, ast)',
  'Curated Model Picks daily',
  'Black-Litterman edge analysis',
  'Quantile distributions & confidence bands',
  'Paper trading performance tracking',
  'Bet history & stat breakdowns',
]

export default async function PricingPage() {
  const supabase = await createClient()
  const { data: { user } } = await supabase.auth.getUser()
  const ctaHref = user ? '/subscribe' : '/signup'

  return (
    <div className="py-16 sm:py-24">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-12">
          <h1 className="text-4xl font-bold text-slate-50">Pricing</h1>
          <p className="mt-4 text-lg text-slate-400">
            Start with a 7-day free trial — no credit card charge until trial ends.
          </p>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-6 max-w-3xl mx-auto">
          {/* Monthly */}
          <div className="bg-slate-800 border border-slate-700 rounded-xl p-8 flex flex-col">
            <div className="text-center mb-6">
              <p className="text-sm font-medium text-blue-400 uppercase tracking-wide">Monthly</p>
              <div className="mt-2 flex items-baseline justify-center gap-1">
                <span className="text-5xl font-bold text-slate-50">$19</span>
                <span className="text-2xl text-slate-400">.99</span>
                <span className="text-slate-400 ml-1">/mo</span>
              </div>
              <p className="text-sm text-slate-500 mt-1">Billed monthly</p>
            </div>
            <ul className="space-y-3 mb-8 flex-1">
              {features.map((f) => (
                <li key={f} className="flex items-center gap-2 text-sm text-slate-300">
                  <span className="text-green-400 shrink-0">&#10003;</span>
                  {f}
                </li>
              ))}
            </ul>
            <Link
              href={ctaHref}
              className="block text-center px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg transition-colors"
            >
              Start Free Trial
            </Link>
          </div>

          {/* Annual */}
          <div className="bg-slate-800 border border-blue-500/50 rounded-xl p-8 flex flex-col relative">
            <div className="absolute -top-3 left-1/2 -translate-x-1/2">
              <span className="px-3 py-1 bg-blue-600 text-white text-xs font-bold rounded-full uppercase tracking-wide">
                Save 17%
              </span>
            </div>
            <div className="text-center mb-6">
              <p className="text-sm font-medium text-blue-400 uppercase tracking-wide">Annual</p>
              <div className="mt-2 flex items-baseline justify-center gap-1">
                <span className="text-5xl font-bold text-slate-50">$199</span>
                <span className="text-slate-400 ml-1">/yr</span>
              </div>
              <p className="text-sm text-slate-500 mt-1">$16.58/mo — billed annually</p>
            </div>
            <ul className="space-y-3 mb-8 flex-1">
              {features.map((f) => (
                <li key={f} className="flex items-center gap-2 text-sm text-slate-300">
                  <span className="text-green-400 shrink-0">&#10003;</span>
                  {f}
                </li>
              ))}
            </ul>
            <Link
              href={ctaHref}
              className="block text-center px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg transition-colors"
            >
              Start Free Trial
            </Link>
          </div>
        </div>

        <div className="flex flex-col items-center gap-3 mt-8">
          <a
            href={DISCORD_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="text-sm text-slate-400 hover:text-slate-300 transition-colors"
          >
            Questions? Join our Discord →
          </a>
          <p className="text-center text-xs text-slate-500">
            7-day free trial. Cancel anytime. Secure checkout via Stripe.
          </p>
        </div>
      </div>
    </div>
  )
}
