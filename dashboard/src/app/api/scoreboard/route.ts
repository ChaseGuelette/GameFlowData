import { NextResponse } from 'next/server'

const SCOREBOARD_URL =
  'https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json'

const CDN_HEADERS = {
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

export async function GET() {
  try {
    const resp = await fetch(SCOREBOARD_URL, {
      headers: CDN_HEADERS,
      next: { revalidate: 30 },
    })

    if (!resp.ok) {
      return NextResponse.json({})
    }

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

    return NextResponse.json(statusMap)
  } catch (err) {
    console.error('NBA CDN scoreboard fetch error:', err)
    return NextResponse.json({})
  }
}
