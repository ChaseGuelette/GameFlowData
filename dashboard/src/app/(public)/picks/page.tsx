import Link from 'next/link'
import { createClient } from '@/lib/supabase/server'
import { DISCORD_URL, TEAM_ABBREV } from '@/lib/constants'

type PublicPick = {
  player_name: string
  stat: string
  line: number
  over_edge: number
  under_edge: number
  bl_over_edge: number
  bl_under_edge: number
  team_id: number
  opponent_id: number
  game_time: string
  prediction_date: string
}

function PickCard({ pick }: { pick: PublicPick }) {
  const bestEdge = Math.max(pick.bl_over_edge ?? 0, pick.bl_under_edge ?? 0)
  const direction = (pick.bl_over_edge ?? 0) >= (pick.bl_under_edge ?? 0) ? 'OVER' : 'UNDER'
  const teamAbbrev = TEAM_ABBREV[pick.team_id] || 'UNK'
  const oppAbbrev = TEAM_ABBREV[pick.opponent_id] || 'UNK'

  return (
    <div className="bg-slate-800 border border-slate-700 rounded-xl p-5">
      <div className="flex items-center justify-between mb-3">
        <span className="text-xs text-slate-500">{teamAbbrev} vs {oppAbbrev}</span>
        <span className={`text-xs font-semibold px-2 py-0.5 rounded ${
          direction === 'OVER'
            ? 'bg-green-500/20 text-green-400'
            : 'bg-red-500/20 text-red-400'
        }`}>
          {direction}
        </span>
      </div>
      <p className="text-lg font-bold text-slate-50">{pick.player_name}</p>
      <p className="text-sm text-slate-400 capitalize">{pick.stat.replace(/_/g, ' ')}</p>
      <div className="mt-3 flex items-baseline gap-2">
        <span className="text-2xl font-bold text-slate-100">{pick.line}</span>
        <span className="text-sm text-green-400 font-medium">+{(bestEdge * 100).toFixed(1)}% edge</span>
      </div>
    </div>
  )
}

function BlurredCard() {
  return (
    <div className="bg-slate-800 border border-slate-700 rounded-xl p-5 select-none" aria-hidden>
      <div className="flex items-center justify-between mb-3">
        <span className="blur-sm text-xs text-slate-500">BOS vs NYK</span>
        <span className="blur-sm text-xs font-semibold px-2 py-0.5 rounded bg-green-500/20 text-green-400">
          OVER
        </span>
      </div>
      <p className="blur-sm text-lg font-bold text-slate-50">Player Name</p>
      <p className="blur-sm text-sm text-slate-400">Points</p>
      <div className="mt-3 flex items-baseline gap-2">
        <span className="blur-sm text-2xl font-bold text-slate-100">24.5</span>
        <span className="blur-sm text-sm text-green-400 font-medium">+12.3% edge</span>
      </div>
    </div>
  )
}

export default async function PicksPage() {
  const supabase = await createClient()
  const { data: picks } = await supabase.rpc('get_public_picks', { pick_limit: 3 })

  const publicPicks = (picks ?? []) as PublicPick[]

  return (
    <div className="py-16 sm:py-24">
      <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-12">
          <h1 className="text-4xl font-bold text-slate-50">Today&apos;s Top Picks</h1>
          <p className="mt-4 text-lg text-slate-400">
            AI model picks updated daily. Sign up free for full access.
          </p>
        </div>

        {/* Real picks */}
        {publicPicks.length > 0 ? (
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 mb-4">
            {publicPicks.map((pick, i) => (
              <PickCard key={i} pick={pick} />
            ))}
          </div>
        ) : (
          <p className="text-center text-slate-500 mb-8">
            No picks available yet today. Check back closer to game time.
          </p>
        )}

        {/* Blurred teaser cards */}
        <div className="relative">
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {Array.from({ length: 6 }).map((_, i) => (
              <BlurredCard key={i} />
            ))}
          </div>
          <div className="absolute inset-0 flex flex-col items-center justify-center bg-slate-900/60 rounded-xl">
            <p className="text-xl font-bold text-slate-50 mb-2">Want more picks?</p>
            <p className="text-slate-400 mb-6 text-center max-w-sm">
              Sign up for free access to all predictions, Model Picks, and performance tracking.
            </p>
            <div className="flex flex-col sm:flex-row gap-3">
              <Link
                href="/signup"
                className="px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg transition-colors"
              >
                Sign Up Free
              </Link>
              <a
                href={DISCORD_URL}
                target="_blank"
                rel="noopener noreferrer"
                className="px-6 py-3 border border-slate-600 hover:border-slate-500 text-slate-300 hover:text-white font-semibold rounded-lg transition-colors"
              >
                Join Discord
              </a>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
