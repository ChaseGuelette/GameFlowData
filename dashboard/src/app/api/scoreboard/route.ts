import { NextRequest, NextResponse } from 'next/server'

// ---------------------------------------------------------------------------
// NBA Scoreboard (CDN)
// ---------------------------------------------------------------------------
const NBA_SCOREBOARD_URL =
  'https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json'

const NBA_CDN_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  Referer: 'https://www.nba.com/',
  Origin: 'https://www.nba.com',
}

interface CDNScoreboardGame {
  gameId: string
  gameStatus: number
  gameStatusText: string
  period: number
  gameClock: string
}

async function fetchNbaScoreboard() {
  const resp = await fetch(NBA_SCOREBOARD_URL, {
    headers: NBA_CDN_HEADERS,
    next: { revalidate: 30 },
  })

  if (!resp.ok) return {}

  const data = await resp.json()
  const games: CDNScoreboardGame[] = data?.scoreboard?.games ?? []

  const statusMap: Record<
    string,
    { gameStatus: number; gameStatusText: string; period: number; gameClock: string }
  > = {}

  for (const g of games) {
    statusMap[g.gameId] = {
      gameStatus: g.gameStatus,
      gameStatusText: g.gameStatusText,
      period: g.period,
      gameClock: g.gameClock,
    }
  }

  return statusMap
}

// ---------------------------------------------------------------------------
// MLB Scoreboard (MLB Stats API)
// ---------------------------------------------------------------------------

interface MLBScheduleGame {
  gamePk: number
  status: {
    statusCode: string
    detailedState: string
  }
  linescore?: {
    currentInning?: number
    currentInningOrdinal?: string
    inningHalf?: string
    outs?: number
  }
}

function mlbGameStatusNum(statusCode: string): number {
  // 1 = Pre, 2 = Live, 3 = Final
  if (['S', 'P', 'PW'].includes(statusCode)) return 1
  if (['I', 'MA', 'MF', 'MI'].includes(statusCode)) return 2
  return 3 // F, O, FR, DR, etc.
}

function mlbGameStatusText(game: MLBScheduleGame): string {
  const code = game.status.statusCode
  if (code === 'F' || code === 'O') return 'Final'
  if (['S', 'P', 'PW'].includes(code)) return game.status.detailedState || 'Scheduled'

  // Live game — show inning info
  const ls = game.linescore
  if (ls?.inningHalf && ls?.currentInning) {
    const half = ls.inningHalf === 'Top' ? 'Top' : 'Bot'
    return `${half} ${ls.currentInning}`
  }
  return game.status.detailedState || 'In Progress'
}

async function fetchMlbScoreboard() {
  const today = new Date()
  const dateStr = today.toISOString().slice(0, 10) // YYYY-MM-DD
  const url = `https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=${dateStr}&hydrate=linescore`

  const resp = await fetch(url, { next: { revalidate: 30 } })
  if (!resp.ok) return {}

  const data = await resp.json()
  const games: MLBScheduleGame[] = data?.dates?.[0]?.games ?? []

  const statusMap: Record<
    string,
    { gameStatus: number; gameStatusText: string; period: number; gameClock: string }
  > = {}

  for (const g of games) {
    statusMap[g.gamePk.toString()] = {
      gameStatus: mlbGameStatusNum(g.status.statusCode),
      gameStatusText: mlbGameStatusText(g),
      period: g.linescore?.currentInning ?? 0,
      gameClock: g.linescore?.outs != null ? `${g.linescore.outs} out${g.linescore.outs !== 1 ? 's' : ''}` : '',
    }
  }

  return statusMap
}

// ---------------------------------------------------------------------------
// Route handler
// ---------------------------------------------------------------------------

export async function GET(request: NextRequest) {
  try {
    const sport = request.nextUrl.searchParams.get('sport') || 'nba'

    const statusMap = sport === 'mlb'
      ? await fetchMlbScoreboard()
      : await fetchNbaScoreboard()

    return NextResponse.json(statusMap)
  } catch (err) {
    console.error('Scoreboard fetch error:', err)
    return NextResponse.json({})
  }
}
