/**
 * CSV/TSV parser for user bet history exports.
 * Supports the user's Excel format and common aliases from other sportsbook exports.
 */

export interface ParsedBet {
  game_date: string        // YYYY-MM-DD
  book_at_bet: string | null
  player_name: string
  stat_type: string
  line: number
  bet_direction: 'over' | 'under'
  odds_at_bet: number
  stake: number
  status: 'won' | 'lost' | 'push' | 'pending'
  pnl: number
  source: 'csv_import'
}

export interface ParseResult {
  parsed: ParsedBet[]
  errors: { row: number; message: string }[]
  warnings: string[]
}

// ── Column name aliases ──────────────────────────────────────────────────────

const DATE_COLS        = ['date', 'game date', 'game_date', 'bet date']
const BOOK_COLS        = ['sports book', 'sportsbook', 'book', 'bookmaker', 'sports_book']
const PLAYER_COLS      = ['player', 'player name', 'player_name', 'name']
const STAT_COLS        = ['prop type', 'prop_type', 'stat', 'stat type', 'stat_type', 'market', 'category']
const LINE_COLS        = ['line', 'prop line', 'prop_line', 'number', 'value']
const DIRECTION_COLS   = ['over/under', 'over_under', 'direction', 'bet direction', 'bet_direction', 'side', 'type']
const ODDS_COLS        = ['odds', 'odds at bet', 'odds_at_bet', 'price', 'american odds']
const STAKE_COLS       = ['stake', 'wager', 'bet amount', 'bet_amount', 'amount']
const RESULT_COLS      = ['win/loss', 'win_loss', 'result', 'outcome', 'status']
const PNL_COLS         = ['profit', 'p&l', 'pnl', 'net', 'profit/loss']
// Columns to ignore (running totals we recalculate)
const SKIP_COLS        = ['payout', 'total staked', 'total_staked', 'total profit', 'total_profit', 'roi on stake', 'roi on bankroll', 'roi']

// ── Stat type normalization ──────────────────────────────────────────────────

const STAT_MAP: Record<string, string> = {
  // NBA
  'assists':            'ast',
  'assist':             'ast',
  'ast':                'ast',
  'points':             'pts',
  'point':              'pts',
  'pts':                'pts',
  'rebounds':           'reb',
  'rebound':            'reb',
  'reb':                'reb',
  'steals':             'stl',
  'steal':              'stl',
  'stl':                'stl',
  'blocks':             'blk',
  'block':              'blk',
  'blk':                'blk',
  '3-pointers':         '3pm',
  '3-pointer':          '3pm',
  'threes':             '3pm',
  'three pointers':     '3pm',
  'three-pointers':     '3pm',
  '3pm':                '3pm',
  '3 pointers':         '3pm',
  'pts+reb+ast':        'pra',
  'pts+reb':            'pr',
  'pts+ast':            'pa',
  'reb+ast':            'ra',
  'pra':                'pra',
  'pr':                 'pr',
  'pa':                 'pa',
  'ra':                 'ra',
  // MLB
  'strikeouts':         'pitcher_strikeouts',
  'strikeout':          'pitcher_strikeouts',
  'ks':                 'pitcher_strikeouts',
  'pitcher strikeouts': 'pitcher_strikeouts',
  'pitcher_strikeouts': 'pitcher_strikeouts',
  'hits':               'batter_hits',
  'hit':                'batter_hits',
  'batter hits':        'batter_hits',
  'batter_hits':        'batter_hits',
  'rbis':               'batter_rbis',
  'rbi':                'batter_rbis',
  'runs batted in':     'batter_rbis',
  'batter rbis':        'batter_rbis',
  'batter_rbis':        'batter_rbis',
  'total bases':        'batter_total_bases',
  'batter_total_bases': 'batter_total_bases',
  'home runs':          'batter_home_runs',
  'home run':           'batter_home_runs',
  'hr':                 'batter_home_runs',
  'batter_home_runs':   'batter_home_runs',
  'runs':               'batter_runs_scored',
  'runs scored':        'batter_runs_scored',
  'batter_runs_scored': 'batter_runs_scored',
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function findCol(headers: string[], candidates: string[]): number {
  const lower = headers.map(h => h.toLowerCase().trim())
  for (const candidate of candidates) {
    const idx = lower.indexOf(candidate)
    if (idx !== -1) return idx
  }
  return -1
}

/** Parse M/D/YYYY, MM/DD/YYYY, YYYY-MM-DD → YYYY-MM-DD */
function parseDate(raw: string): string | null {
  raw = raw.trim()
  // Already ISO
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw
  // M/D/YYYY or MM/DD/YYYY
  const parts = raw.split('/')
  if (parts.length === 3) {
    const [m, d, y] = parts
    const mm = m.padStart(2, '0')
    const dd = d.padStart(2, '0')
    const yyyy = y.length === 2 ? `20${y}` : y
    return `${yyyy}-${mm}-${dd}`
  }
  // M-D-YYYY
  const dashes = raw.split('-')
  if (dashes.length === 3 && dashes[0].length <= 2) {
    const [m, d, y] = dashes
    return `${y}-${m.padStart(2, '0')}-${d.padStart(2, '0')}`
  }
  return null
}

function normalizeDirection(raw: string): 'over' | 'under' | null {
  const v = raw.toLowerCase().trim()
  if (v === 'over' || v === 'o') return 'over'
  if (v === 'under' || v === 'u') return 'under'
  return null
}

function normalizeStatus(raw: string): ParsedBet['status'] | null {
  const v = raw.toLowerCase().trim()
  if (v === 'win' || v === 'w' || v === 'won') return 'won'
  if (v === 'loss' || v === 'l' || v === 'lost') return 'lost'
  if (v === 'push' || v === 'p' || v === 'tie') return 'push'
  if (v === 'pending' || v === '') return 'pending'
  return null
}

/**
 * Parse odds. If the value is positive and assumeNegative is true, negate it
 * (typical sportsbook juice like 110 → -110). If it already has a sign, use as-is.
 */
function parseOdds(raw: string, assumeNegative: boolean): number {
  const clean = raw.trim().replace(/[+]/, '')
  const n = parseFloat(clean)
  if (isNaN(n)) return -110
  // If unsigned positive (e.g., "110") and caller wants to assume negative
  if (n > 0 && assumeNegative && !raw.trim().startsWith('+')) return -n
  return n
}

function parseNumber(raw: string): number | null {
  const n = parseFloat(raw.trim().replace(/[$,]/g, ''))
  return isNaN(n) ? null : n
}

/** Split CSV respecting quoted fields */
function splitCsvLine(line: string): string[] {
  const result: string[] = []
  let inQuotes = false
  let current = ''
  for (let i = 0; i < line.length; i++) {
    const ch = line[i]
    if (ch === '"') {
      inQuotes = !inQuotes
    } else if ((ch === ',' || ch === '\t') && !inQuotes) {
      result.push(current.trim())
      current = ''
    } else {
      current += ch
    }
  }
  result.push(current.trim())
  return result
}

// ── Main parser ──────────────────────────────────────────────────────────────

export interface ParseOptions {
  /** If true, unsigned positive odds are treated as negative (e.g. 110 → -110) */
  assumeNegativeOdds?: boolean
}

export function parseBetsCsv(
  csvText: string,
  options: ParseOptions = {}
): ParseResult {
  const { assumeNegativeOdds = true } = options
  const parsed: ParsedBet[] = []
  const errors: { row: number; message: string }[] = []
  const warnings: string[] = []

  // Normalize line endings and split
  const lines = csvText.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n')

  // Find header row (first non-empty line)
  let headerIdx = -1
  for (let i = 0; i < lines.length; i++) {
    if (lines[i].trim().length > 0) {
      headerIdx = i
      break
    }
  }
  if (headerIdx === -1) {
    return { parsed, errors: [{ row: 0, message: 'File is empty' }], warnings }
  }

  const headers = splitCsvLine(lines[headerIdx])

  // Map column indices
  const colDate      = findCol(headers, DATE_COLS)
  const colBook      = findCol(headers, BOOK_COLS)
  const colPlayer    = findCol(headers, PLAYER_COLS)
  const colStat      = findCol(headers, STAT_COLS)
  const colLine      = findCol(headers, LINE_COLS)
  const colDirection = findCol(headers, DIRECTION_COLS)
  const colOdds      = findCol(headers, ODDS_COLS)
  const colStake     = findCol(headers, STAKE_COLS)
  const colResult    = findCol(headers, RESULT_COLS)
  const colPnl       = findCol(headers, PNL_COLS)

  // Validate required columns
  const missing: string[] = []
  if (colDate === -1)   missing.push('Date')
  if (colPlayer === -1) missing.push('Player')
  if (colStat === -1)   missing.push('Prop Type / Stat')
  if (colLine === -1)   missing.push('Line')
  if (colDirection === -1) missing.push('Over/Under')
  if (colResult === -1) missing.push('Win/Loss / Result')

  if (missing.length > 0) {
    return {
      parsed,
      errors: [{ row: headerIdx + 1, message: `Missing required columns: ${missing.join(', ')}` }],
      warnings,
    }
  }

  // Process data rows
  for (let i = headerIdx + 1; i < lines.length; i++) {
    const line = lines[i].trim()
    if (!line) continue

    const rowNum = i + 1
    const cells = splitCsvLine(line)

    // Skip summary/total rows (first cell is empty or contains "Total")
    const firstCell = cells[0]?.toLowerCase().trim() ?? ''
    if (!firstCell || firstCell.startsWith('total') || firstCell.startsWith('roi')) continue

    try {
      // Date
      const rawDate = cells[colDate]?.trim() ?? ''
      const game_date = parseDate(rawDate)
      if (!game_date) {
        errors.push({ row: rowNum, message: `Cannot parse date: "${rawDate}"` })
        continue
      }

      // Player
      const player_name = cells[colPlayer]?.trim() ?? ''
      if (!player_name) {
        errors.push({ row: rowNum, message: 'Player name is empty' })
        continue
      }

      // Stat type
      const rawStat = cells[colStat]?.trim() ?? ''
      const stat_type = STAT_MAP[rawStat.toLowerCase()] ?? rawStat
      if (!rawStat) {
        errors.push({ row: rowNum, message: 'Stat type is empty' })
        continue
      }
      if (!STAT_MAP[rawStat.toLowerCase()]) {
        warnings.push(`Row ${rowNum}: Unknown stat type "${rawStat}", storing as-is`)
      }

      // Line
      const rawLine = cells[colLine]?.trim() ?? ''
      const line_val = parseNumber(rawLine)
      if (line_val === null) {
        errors.push({ row: rowNum, message: `Cannot parse line: "${rawLine}"` })
        continue
      }

      // Direction
      const rawDir = cells[colDirection]?.trim() ?? ''
      const bet_direction = normalizeDirection(rawDir)
      if (!bet_direction) {
        errors.push({ row: rowNum, message: `Cannot parse direction: "${rawDir}"` })
        continue
      }

      // Result
      const rawResult = cells[colResult]?.trim() ?? ''
      const status = normalizeStatus(rawResult)
      if (status === null) {
        errors.push({ row: rowNum, message: `Cannot parse result: "${rawResult}"` })
        continue
      }

      // Odds (optional — default -110)
      const odds_at_bet = colOdds !== -1
        ? parseOdds(cells[colOdds]?.trim() ?? '', assumeNegativeOdds)
        : -110

      // Stake (optional — default 0)
      const stakeRaw = colStake !== -1 ? cells[colStake]?.trim() ?? '' : ''
      const stake = parseNumber(stakeRaw) ?? 0

      // PnL (optional — recalculate from odds+stake if missing)
      let pnl = 0
      if (colPnl !== -1 && cells[colPnl]) {
        const rawPnl = parseNumber(cells[colPnl])
        if (rawPnl !== null) {
          // For losses, ensure PnL is negative
          if (status === 'lost') {
            pnl = rawPnl > 0 ? -rawPnl : rawPnl
            if (pnl === 0 && stake > 0) pnl = -stake
          } else {
            pnl = rawPnl
          }
        }
      }
      // If PnL still 0 for won/lost, recalculate from odds + stake
      if (pnl === 0 && stake > 0 && status !== 'push' && status !== 'pending') {
        if (status === 'won') {
          pnl = odds_at_bet < 0
            ? (stake / Math.abs(odds_at_bet)) * 100
            : (stake * odds_at_bet) / 100
        } else if (status === 'lost') {
          pnl = -stake
        }
      }

      // Book (optional)
      const book_at_bet = colBook !== -1 ? (cells[colBook]?.trim() || null) : null

      parsed.push({
        game_date,
        book_at_bet,
        player_name,
        stat_type,
        line: line_val,
        bet_direction,
        odds_at_bet,
        stake,
        status,
        pnl,
        source: 'csv_import',
      })
    } catch (err) {
      errors.push({ row: rowNum, message: `Unexpected error: ${String(err)}` })
    }
  }

  return { parsed, errors, warnings }
}
