import { ImageResponse } from 'next/og'
import { NextRequest } from 'next/server'
import { readFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import { createClient } from '@/lib/supabase/server'

interface SlatePickData {
  player_id: number
  player_name: string
  stat: string
  line: number
  direction: 'Over' | 'Under'
  edge: number
  team_abbrev: string
  opponent_abbrev: string
  game_time?: string
}

interface SlateRequest {
  picks: SlatePickData[]
  date: string
}

// Edge tier colors (from theme.py)
function getEdgeColor(edge: number): string {
  const abs = Math.abs(edge)
  if (abs >= 0.07) return '#22C55E' // green
  if (abs >= 0.05) return '#EAB308' // yellow
  return '#64748B' // slate
}

// Star count (matches PropCard formula)
function getStarCount(edge: number): number {
  const safe = Math.abs(edge) || 0
  return Math.min(5, Math.max(1, Math.ceil(safe * 50)))
}

// Stat badge colors
const STAT_BADGE_COLORS: Record<string, string> = {
  pts: '#3B82F6',
  reb: '#14B8A6',
  ast: '#A855F7',
}

// Stat labels
const STAT_LABEL: Record<string, string> = {
  pts: 'PTS',
  reb: 'REB',
  ast: 'AST',
}

export async function POST(request: NextRequest) {
  // Auth check — only logged-in users can generate slates
  const supabase = await createClient()
  const { data: { user } } = await supabase.auth.getUser()
  if (!user) {
    return new Response('Unauthorized', { status: 401 })
  }

  let body: SlateRequest
  try {
    body = await request.json()
  } catch {
    return new Response('Invalid JSON', { status: 400 })
  }
  const { picks, date } = body

  if (!picks || picks.length === 0 || !date) {
    return new Response('No picks provided', { status: 400 })
  }

  // Limit to 5 picks
  const slatePicks = picks.slice(0, 5)

  // Load Montserrat Bold font — fs for local dev, fetch fallback for Vercel
  let fontData: ArrayBuffer
  try {
    const fontPath = fileURLToPath(new URL('./Montserrat-Bold.ttf', import.meta.url))
    const buffer = await readFile(fontPath)
    fontData = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
  } catch {
    fontData = await fetch(
      new URL('./Montserrat-Bold.ttf', import.meta.url)
    ).then((res) => res.arrayBuffer())
  }

  // Format date for display
  const dateObj = new Date(date + 'T00:00:00')
  const displayDate = dateObj.toLocaleDateString('en-US', {
    weekday: 'short',
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  })

  const imageHeight = 1080 // fixed height — picks space evenly

  const fonts = [{ name: 'Montserrat', data: fontData, style: 'normal' as const, weight: 700 as const }]

  return new ImageResponse(
    (
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          width: '100%',
          height: '100%',
          backgroundColor: '#020617',
          fontFamily: 'Montserrat',
          color: '#F8FAFC',
        }}
      >
        {/* Header */}
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            padding: '30px 40px 20px',
          }}
        >
          <div
            style={{
              display: 'flex',
              fontSize: 42,
              fontWeight: 700,
              letterSpacing: '2px',
              color: '#F8FAFC',
            }}
          >
            {"TODAY'S TOP PICKS"}
          </div>
          <div
            style={{
              display: 'flex',
              fontSize: 22,
              color: '#94A3B8',
              marginTop: '8px',
            }}
          >
            {displayDate}
          </div>
          {/* Divider line */}
          <div
            style={{
              display: 'flex',
              width: '920px',
              height: '2px',
              backgroundColor: '#334155',
              marginTop: '20px',
            }}
          />
        </div>

        {/* Pick rows */}
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            padding: '0 40px',
            justifyContent: 'center',
            gap: '12px',
            flex: 1,
          }}
        >
          {slatePicks.map((pick, i) => {
            const stars = getStarCount(pick.edge)
            const edgeColor = getEdgeColor(pick.edge)
            const statColor = STAT_BADGE_COLORS[pick.stat] || '#3B82F6'
            const statLabel = STAT_LABEL[pick.stat] || pick.stat.toUpperCase()
            const bgColor = i % 2 === 0 ? '#1E293B' : '#253347'

            return (
              <div
                key={i}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  backgroundColor: bgColor,
                  borderRadius: '16px',
                  padding: '20px 24px',
                  borderLeft: `4px solid ${edgeColor}`,
                }}
              >
                {/* Player headshot */}
                <div
                  style={{
                    display: 'flex',
                    width: '90px',
                    height: '90px',
                    borderRadius: '50%',
                    overflow: 'hidden',
                    backgroundColor: '#334155',
                    flexShrink: 0,
                  }}
                >
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <img
                    src={`https://cdn.nba.com/headshots/nba/latest/1040x760/${pick.player_id}.png`}
                    alt=""
                    width={90}
                    height={90}
                    style={{
                      objectFit: 'cover',
                      borderRadius: '50%',
                    }}
                  />
                </div>

                {/* Player info */}
                <div
                  style={{
                    display: 'flex',
                    flexDirection: 'column',
                    marginLeft: '20px',
                    flex: 1,
                  }}
                >
                  <div
                    style={{
                      display: 'flex',
                      fontSize: 26,
                      fontWeight: 700,
                      color: '#F8FAFC',
                    }}
                  >
                    {pick.player_name}
                  </div>
                  <div
                    style={{
                      display: 'flex',
                      fontSize: 18,
                      color: '#94A3B8',
                      marginTop: '4px',
                    }}
                  >
                    {pick.team_abbrev} vs {pick.opponent_abbrev}
                  </div>
                </div>

                {/* Stat badge */}
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    backgroundColor: statColor,
                    borderRadius: '8px',
                    padding: '6px 14px',
                    fontSize: 16,
                    fontWeight: 700,
                    color: '#FFFFFF',
                    marginRight: '20px',
                  }}
                >
                  {statLabel}
                </div>

                {/* Direction + Line */}
                <div
                  style={{
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: 'center',
                    marginRight: '24px',
                  }}
                >
                  <div
                    style={{
                      display: 'flex',
                      fontSize: 28,
                      fontWeight: 700,
                      color: '#F8FAFC',
                    }}
                  >
                    {pick.direction} {pick.line}
                  </div>
                  <div
                    style={{
                      display: 'flex',
                      fontSize: 16,
                      color: edgeColor,
                      fontWeight: 700,
                      marginTop: '2px',
                    }}
                  >
                    {pick.edge >= 0 ? '+' : ''}
                    {(pick.edge * 100).toFixed(1)}% edge
                  </div>
                </div>

                {/* Star rating */}
                <div
                  style={{
                    display: 'flex',
                    gap: '4px',
                    flexShrink: 0,
                  }}
                >
                  {Array.from({ length: 5 }).map((_, si) => (
                    <svg key={si} width="22" height="22" viewBox="0 0 24 24">
                      <path
                        d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"
                        fill={si < stars ? '#FACC15' : '#334155'}
                      />
                    </svg>
                  ))}
                </div>
              </div>
            )
          })}
        </div>

        {/* Footer */}
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            padding: '20px 40px 30px',
          }}
        >
          <div
            style={{
              display: 'flex',
              fontSize: 20,
              color: '#64748B',
              letterSpacing: '1px',
            }}
          >
            gameflowdata.com
          </div>
        </div>
      </div>
    ),
    {
      width: 1080,
      height: imageHeight > 1080 ? imageHeight : 1080,
      fonts,
    }
  )
}
