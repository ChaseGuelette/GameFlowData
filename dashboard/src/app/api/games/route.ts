import { NextRequest, NextResponse } from 'next/server'

// ---------------------------------------------------------------------------
// NBA Schedule (CDN)
// ---------------------------------------------------------------------------

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

async function fetchNbaGames(targetDate: string) {
  const resp = await fetch(SCHEDULE_URL, { headers: CDN_HEADERS, next: { revalidate: 3600 } })
  if (!resp.ok) return null

  const data = await resp.json()
  const gameDates: CDNGameDate[] = data?.leagueSchedule?.gameDates ?? []

  // Parse targetDate (YYYY-MM-DD) to match CDN format (MM/DD/YYYY)
  const [year, month, day] = targetDate.split('-')
  const cdnDatePrefix = `${month}/${day}/${year}`

  const matchingDate = gameDates.find(d => d.gameDate.startsWith(cdnDatePrefix))
  if (!matchingDate) return []

  // Filter to regular season games (game_id starts with "002") that aren't cancelled
  return matchingDate.games
    .filter(g => g.gameId.startsWith('002'))
    .map(g => ({
      home_team: TRICODE_TO_NAME[g.homeTeam.teamTricode] ?? g.homeTeam.teamTricode,
      away_team: TRICODE_TO_NAME[g.awayTeam.teamTricode] ?? g.awayTeam.teamTricode,
      commence_time: g.gameDateTimeUTC || null,
    }))
}

// ---------------------------------------------------------------------------
// MLB Schedule (MLB Stats API)
// ---------------------------------------------------------------------------

interface MLBScheduleGame {
  gamePk: number
  gameDate: string // ISO datetime
  status: { statusCode: string }
  teams: {
    home: { team: { id: number; name: string } }
    away: { team: { id: number; name: string } }
  }
}

async function fetchMlbGames(targetDate: string) {
  const url = `https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=${targetDate}&gameType=R`
  const resp = await fetch(url, { next: { revalidate: 3600 } })
  if (!resp.ok) return null

  const data = await resp.json()
  const games: MLBScheduleGame[] = data?.dates?.[0]?.games ?? []

  return games
    .filter(g => !['D', 'DR', 'CR'].includes(g.status.statusCode)) // exclude cancelled/postponed
    .map(g => ({
      home_team: g.teams.home.team.name,
      away_team: g.teams.away.team.name,
      commence_time: g.gameDate || null,
    }))
}

// ---------------------------------------------------------------------------
// Route handler
// ---------------------------------------------------------------------------

export async function GET(req: NextRequest) {
  const targetDate = req.nextUrl.searchParams.get('date')
  if (!targetDate) {
    return NextResponse.json({ error: 'date parameter required' }, { status: 400 })
  }

  const sport = req.nextUrl.searchParams.get('sport') || 'nba'

  try {
    const games = sport === 'mlb'
      ? await fetchMlbGames(targetDate)
      : await fetchNbaGames(targetDate)

    if (games === null) {
      return NextResponse.json({ error: `Failed to fetch ${sport.toUpperCase()} schedule` }, { status: 502 })
    }

    return NextResponse.json(games)
  } catch (err) {
    console.error(`${sport.toUpperCase()} schedule fetch error:`, err)
    return NextResponse.json({ error: 'Internal error' }, { status: 500 })
  }
}
