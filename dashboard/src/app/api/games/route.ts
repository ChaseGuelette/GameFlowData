import { NextRequest, NextResponse } from 'next/server'
import { TEAM_ABBREV } from '@/lib/constants'
import { createClient } from '@/lib/supabase/server'

// ---------------------------------------------------------------------------
// NBA Schedule (CDN)
// ---------------------------------------------------------------------------

const SCHEDULE_URLS = [
  'https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json',
  'https://cdn.nba.com/static/json/staticData/scheduleLeagueV2_1.json',
]
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
  postponedStatus?: string
  homeTeam: { teamTricode: string }
  awayTeam: { teamTricode: string }
}

interface CDNGameDate {
  gameDate: string // "MM/DD/YYYY 00:00:00"
  games: CDNGame[]
}

async function fetchNbaGames(targetDate: string) {
  let gameDates: CDNGameDate[] | null = null
  for (const url of SCHEDULE_URLS) {
    try {
      const resp = await fetch(url, {
        headers: CDN_HEADERS, next: { revalidate: 1800 }, signal: AbortSignal.timeout(15000),
      })
      if (!resp.ok) {
        console.warn('NBA schedule endpoint unavailable', url, resp.status)
        continue
      }
      const data = await resp.json()
      const dates = data?.leagueSchedule?.gameDates
      if (!Array.isArray(dates) || dates.length === 0 || !dates.every(d =>
        typeof d?.gameDate === 'string' && Array.isArray(d.games)
      )) {
        console.warn('NBA schedule endpoint returned an invalid payload', url)
        continue
      }
      gameDates = dates
      break
    } catch {
      console.warn('NBA schedule endpoint request failed', url)
    }
  }
  if (gameDates === null) return fetchPersistedNbaGames(targetDate)

  // Parse targetDate (YYYY-MM-DD) to match CDN format (MM/DD/YYYY)
  const [year, month, day] = targetDate.split('-')
  const cdnDatePrefix = `${month}/${day}/${year}`

  const matchingDate = gameDates.find(d => d.gameDate.startsWith(cdnDatePrefix))
  if (!matchingDate) return []

  // Competitive games only; exclude the Cup final, cancelled and postponed games.
  return matchingDate.games
    .filter(g => typeof g.gameId === 'string' && /^(002|004|005)\d{7}$/.test(g.gameId)
      && g.gameStatus !== 4 && (!g.postponedStatus || g.postponedStatus === 'N'))
    .map(g => ({
      home_team: TRICODE_TO_NAME[g.homeTeam.teamTricode] ?? g.homeTeam.teamTricode,
      away_team: TRICODE_TO_NAME[g.awayTeam.teamTricode] ?? g.awayTeam.teamTricode,
      commence_time: g.gameDateTimeUTC || null,
    }))
}

async function fetchPersistedNbaGames(targetDate: string) {
  // Use the signed-in user's authenticated/RLS client, never a service-role key.
  // Fetch all date rows before checking freshness: filtering stale rows in SQL
  // could turn a partially stale slate into an apparently complete one.
  const supabase = await createClient()
  const { data, error } = await supabase.from('nba_game_schedule')
    .select('game_id,home_team_id,away_team_id,commence_time_utc,game_status,postponed_status,source,observed_at,source_updated_at')
    .eq('game_date', targetDate).order('game_id').limit(31)
  if (error || !data?.length || data.length > 30) return null
  const now = Date.now()
  const fresh = (value: string | null) => {
    const age = now - Date.parse(value || '')
    return Number.isFinite(age) && age >= -15 * 60_000 && age <= 90 * 60_000
  }
  if (data.some(g => g.source !== 'nba_cdn' || !/^(002|004|005)\d{7}$/.test(g.game_id)
    || !fresh(g.observed_at) || !fresh(g.source_updated_at))) return null
  const active = data.filter(g => g.game_status !== 4
    && (!g.postponed_status || g.postponed_status === 'N'))
  if (active.some(g => !TEAM_ABBREV[g.home_team_id] || !TEAM_ABBREV[g.away_team_id]
    || !Number.isFinite(Date.parse(g.commence_time_utc || '')))) return null
  return active.map(g => ({
    home_team: TRICODE_TO_NAME[TEAM_ABBREV[g.home_team_id]],
    away_team: TRICODE_TO_NAME[TEAM_ABBREV[g.away_team_id]],
    commence_time: g.commence_time_utc,
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
  if (!targetDate || !/^\d{4}-\d{2}-\d{2}$/.test(targetDate)) {
    return NextResponse.json({ error: 'date parameter must be YYYY-MM-DD' }, { status: 400 })
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
