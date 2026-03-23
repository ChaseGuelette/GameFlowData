// Stat colors match STAT_COLORS from dashboard/src/lib/constants.ts
const STAT_PILL_COLORS: Record<string, string> = {
  Points: 'bg-blue-500/20 text-blue-400',
  Rebounds: 'bg-green-500/20 text-green-400',
  Assists: 'bg-purple-500/20 text-purple-400',
  Steals: 'bg-orange-500/20 text-orange-400',
  Blocks: 'bg-red-500/20 text-red-400',
  Threes: 'bg-cyan-500/20 text-cyan-400',
}

const sports = [
  {
    name: 'NBA',
    league: 'National Basketball Association',
    icon: '🏀',
    status: 'live' as const,
    markets: ['Points', 'Rebounds', 'Assists', 'Steals', 'Blocks', 'Threes'],
  },
  {
    name: 'MLB',
    league: 'Major League Baseball',
    icon: '⚾',
    status: 'coming_soon' as const,
    markets: ['Strikeouts', 'Hits', 'Home Runs', 'RBIs'],
  },
  {
    name: 'NCAAB',
    league: 'NCAA Basketball',
    icon: '🏀',
    status: 'coming_soon' as const,
    markets: ['Spreads', 'Totals', 'Moneylines'],
  },
]

export function SupportedMarkets() {
  return (
    <section className="py-16 sm:py-24">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <h2 className="text-3xl font-bold text-slate-50 text-center mb-12">
          Supported Markets
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {sports.map((sport) => (
            <div
              key={sport.name}
              className={`bg-slate-800 border rounded-xl p-6 hover:border-slate-600 transition-colors ${
                sport.status === 'live'
                  ? 'border-blue-500/30'
                  : 'border-slate-700'
              }`}
            >
              <div className="flex items-center gap-3 mb-3">
                <span className="text-3xl">{sport.icon}</span>
                <div>
                  <h3 className="text-lg font-semibold text-slate-50">{sport.name}</h3>
                  <p className="text-xs text-slate-500">{sport.league}</p>
                </div>
              </div>
              <div className="mb-4">
                {sport.status === 'live' ? (
                  <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-500/20 text-green-400">
                    <span className="relative flex h-2 w-2">
                      <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75" />
                      <span className="relative inline-flex rounded-full h-2 w-2 bg-green-500" />
                    </span>
                    Live
                  </span>
                ) : (
                  <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-slate-700 text-slate-400">
                    Coming Soon
                  </span>
                )}
              </div>
              <div className="border-t border-slate-700 pt-4">
                <div className={`flex flex-wrap gap-2 ${sport.status === 'coming_soon' ? 'opacity-60' : ''}`}>
                  {sport.markets.map((market) => (
                    <span
                      key={market}
                      className={`px-2.5 py-1 rounded-md text-xs font-medium ${
                        STAT_PILL_COLORS[market] || 'bg-slate-700 text-slate-300'
                      }`}
                    >
                      {market}
                    </span>
                  ))}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
