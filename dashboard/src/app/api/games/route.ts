import { NextRequest, NextResponse } from 'next/server'

const SCHEDULE_URL = 'https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json'
const CDN_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  Referer: 'https://www.nba.com/',
  Origin: 'https://www.nba.com',
}

// NBA tri-code to full team name (matches TEAM_NAME_TO_ABBREV in constants.ts)
const TRICODE_TO_NAME: Record<string, string> = {
  ATL: 'Atlanta Hawks', BOS: 'Boston Celtics', BKN: 'Brooklyn Nets',
  CHA: 'Charlotte Hornets', CHI: 'Chicago Bulls', CLE: 'Cleveland Cavaliers',
  DAL: 'Dallas Mavericks', DEN: 'Denver Nuggets', DET: 'Detroit Pistons',
  GSW: 'Golden State Warriors', HOU: 'Houston Rockets', IND: 'Indiana Pacers',
  LAC: 'LA Clippers', LAL: 'Los Angeles Lakers', MEM: 'Memphis Grizzlies',
  MIA: 'Miami Heat', MIL: 'Milwaukee Bucks', MIN: 'Minnesota Timberwolves',
  NOP: 'New Orleans Pelicans', NYK: 'New York Knicks', OKC: 'Oklahoma City Thunder',
  ORL: 'Orlando Magic', PHI: 'Philadelphia 76ers', PHX: 'Phoenix Suns',
  POR: 'Portland Trail Blazers', SAC: 'Sacramento Kings', SAS: 'San Antonio Spurs',
  TOR: 'Toronto Raptors', UTA: 'Utah Jazz', WAS: 'Washington Wizards',
}

interface CDNGame {
  gameId: string
  gameStatus: number
  gameDateTimeUTC: string
  homeTeam: { teamTricode: string }
  awayTeam: { teamTricode: string }
}

interface CDNGameDate {
  gameDate: string // "MM/DD/YYYY 00:00:00"
  games: CDNGame[]
}

export async function GET(req: NextRequest) {
  const targetDate = req.nextUrl.searchParams.get('date')
  if (!targetDate) {
    return NextResponse.json({ error: 'date parameter required' }, { status: 400 })
  }

  try {
    const resp = await fetch(SCHEDULE_URL, { headers: CDN_HEADERS, next: { revalidate: 3600 } })
    if (!resp.ok) {
      return NextResponse.json({ error: 'Failed to fetch NBA schedule' }, { status: 502 })
    }

    const data = await resp.json()
    const gameDates: CDNGameDate[] = data?.leagueSchedule?.gameDates ?? []

    // Parse targetDate (YYYY-MM-DD) to match CDN format (MM/DD/YYYY)
    const [year, month, day] = targetDate.split('-')
    const cdnDatePrefix = `${month}/${day}/${year}`

    const matchingDate = gameDates.find(d => d.gameDate.startsWith(cdnDatePrefix))
    if (!matchingDate) {
      return NextResponse.json([])
    }

    // Filter to regular season games (game_id starts with "002") that aren't cancelled
    const games = matchingDate.games
      .filter(g => g.gameId.startsWith('002'))
      .map(g => ({
        home_team: TRICODE_TO_NAME[g.homeTeam.teamTricode] ?? g.homeTeam.teamTricode,
        away_team: TRICODE_TO_NAME[g.awayTeam.teamTricode] ?? g.awayTeam.teamTricode,
        commence_time: g.gameDateTimeUTC || null,
      }))

    return NextResponse.json(games)
  } catch (err) {
    console.error('NBA CDN schedule fetch error:', err)
    return NextResponse.json({ error: 'Internal error' }, { status: 500 })
  }
}
