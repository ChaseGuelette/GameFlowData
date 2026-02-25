import { useRef, useCallback, useEffect } from 'react'
import Image from 'next/image'

// Reverse map: abbreviation → NBA team ID (for logo URLs)
const ABBREV_TO_ID: Record<string, number> = {
  ATL: 1610612737, BOS: 1610612738, BKN: 1610612751,
  CHA: 1610612766, CHI: 1610612741, CLE: 1610612739,
  DAL: 1610612742, DEN: 1610612743, DET: 1610612765,
  GSW: 1610612744, HOU: 1610612745, IND: 1610612754,
  LAC: 1610612746, LAL: 1610612747, MEM: 1610612763,
  MIA: 1610612748, MIL: 1610612749, MIN: 1610612750,
  NOP: 1610612740, NYK: 1610612752, OKC: 1610612760,
  ORL: 1610612753, PHI: 1610612755, PHX: 1610612756,
  POR: 1610612757, SAC: 1610612758, SAS: 1610612759,
  TOR: 1610612761, UTA: 1610612762, WAS: 1610612764,
}

function teamLogoUrl(abbrev: string): string {
  const id = ABBREV_TO_ID[abbrev]
  if (!id) return ''
  return `https://cdn.nba.com/logos/nba/${id}/global/L/logo.svg`
}

export interface GameInfo {
  matchupKey: string
  teams: [string, string]
  gameTime: string | null
  predictionCount: number
}

interface TonightsGamesProps {
  games: GameInfo[]
  activeMatchup: string
  onSelectMatchup: (matchup: string) => void
  isToday: boolean
}

function formatGameTime(gameTime: string | null): string | null {
  if (!gameTime) return null
  const d = new Date(gameTime)
  if (isNaN(d.getTime())) return null
  return d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' })
}

export function TonightsGames({ games, activeMatchup, onSelectMatchup, isToday }: TonightsGamesProps) {
  const scrollRef = useRef<HTMLDivElement>(null)
  const isDragging = useRef(false)
  const startX = useRef(0)
  const scrollLeftPos = useRef(0)

  const onMouseDown = useCallback((e: React.MouseEvent) => {
    const el = scrollRef.current
    if (!el) return
    isDragging.current = true
    startX.current = e.pageX - el.offsetLeft
    scrollLeftPos.current = el.scrollLeft
    el.style.cursor = 'grabbing'
  }, [])

  const onMouseMove = useCallback((e: React.MouseEvent) => {
    if (!isDragging.current) return
    const el = scrollRef.current
    if (!el) return
    e.preventDefault()
    const x = e.pageX - el.offsetLeft
    el.scrollLeft = scrollLeftPos.current - (x - startX.current)
  }, [])

  const onMouseUp = useCallback(() => {
    isDragging.current = false
    if (scrollRef.current) scrollRef.current.style.cursor = 'grab'
  }, [])

  // Attach wheel listener with { passive: false } so preventDefault actually works
  useEffect(() => {
    const el = scrollRef.current
    if (!el) return

    const handleWheel = (e: WheelEvent) => {
      if (el.scrollWidth <= el.clientWidth) return
      e.preventDefault()
      el.scrollLeft += e.deltaY
    }

    el.addEventListener('wheel', handleWheel, { passive: false })
    return () => el.removeEventListener('wheel', handleWheel)
  }, [games.length])

  if (games.length === 0) return null

  const activeCls = 'bg-blue-600/90 border-blue-500 text-white shadow-lg shadow-blue-500/20'
  const inactiveCls = 'bg-slate-800/80 border-slate-700 text-slate-300 hover:border-slate-500 hover:bg-slate-750'

  return (
    <section className="mb-6">
      <h2 className="text-sm font-medium text-slate-400 mb-3">
        {isToday ? "Tonight's Games" : 'Games'}
      </h2>
      <div
        ref={scrollRef}
        onMouseDown={onMouseDown}
        onMouseMove={onMouseMove}
        onMouseUp={onMouseUp}
        onMouseLeave={onMouseUp}
        className="flex gap-3 overflow-x-auto pb-2 cursor-grab select-none"
        style={{ scrollbarWidth: 'none', msOverflowStyle: 'none' }}
      >
        {/* All Games pill */}
        <button
          onClick={() => onSelectMatchup('all')}
          className={`shrink-0 px-5 py-3 rounded-xl border text-sm font-semibold transition-all ${
            activeMatchup === 'all' ? activeCls : inactiveCls
          }`}
        >
          All Games
        </button>

        {games.map((g) => {
          const time = formatGameTime(g.gameTime)
          const isActive = activeMatchup === g.matchupKey
          const logo1 = teamLogoUrl(g.teams[0])
          const logo2 = teamLogoUrl(g.teams[1])

          return (
            <button
              key={g.matchupKey}
              onClick={() => onSelectMatchup(g.matchupKey)}
              className={`shrink-0 px-4 py-3 rounded-xl border transition-all flex items-center gap-3 ${
                isActive ? activeCls : inactiveCls
              }`}
            >
              {/* Team logos + vs */}
              <div className="flex items-center gap-1.5">
                {logo1 && (
                  <Image src={logo1} alt={g.teams[0]} width={32} height={32} className="object-contain" unoptimized />
                )}
                <span className="text-xs text-slate-500 font-medium">vs</span>
                {logo2 && (
                  <Image src={logo2} alt={g.teams[1]} width={32} height={32} className="object-contain" unoptimized />
                )}
              </div>

              {/* Matchup text + time */}
              <div className="flex flex-col items-start">
                <span className="text-sm font-semibold whitespace-nowrap">
                  {g.teams[0]} vs {g.teams[1]}
                </span>
                {time && (
                  <span className={`text-xs ${isActive ? 'text-blue-200' : 'text-slate-500'}`}>
                    {time}
                  </span>
                )}
              </div>

              {/* Prediction count badge */}
              <span className={`text-xs rounded-full px-2 py-0.5 font-medium ${
                isActive
                  ? 'bg-blue-500/50 text-blue-100'
                  : 'bg-slate-700 text-slate-400'
              }`}>
                {g.predictionCount}
              </span>
            </button>
          )
        })}
      </div>
    </section>
  )
}
