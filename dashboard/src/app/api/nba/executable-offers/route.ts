import { gunzipSync } from 'node:zlib'
import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@/lib/supabase/server'
import { DFS_BOOKMAKERS, STATE_SPORTSBOOKS } from '@/lib/sportsbook-availability'
import {
  applyBestOpportunityPolicy,
  buildExecutableOfferResult,
  validateBlConfig,
  type CurrentNbaLine,
  type NbaOfferPredictionBase,
} from '@/lib/nba-executable-offers'
import type { NbaExecutableOfferResult } from '@/types/predictions'

export const runtime = 'nodejs'

const MAX_PREDICTIONS = 600
const PAGE_SIZE = 1000
const MAX_PAGES = 12
const PREDICTION_QUERY_CHUNK = 100
const GAME_QUERY_CHUNK = 8
const EXCLUDED_BOOKS = new Set<string>([...DFS_BOOKMAKERS, 'fliff', 'polymarket'])

const STAT_TO_MARKET: Record<string, string> = {
  pts: 'player_points',
  reb: 'player_rebounds',
  ast: 'player_assists',
  stl: 'player_steals',
  blk: 'player_blocks',
  '3pm': 'player_threes',
  pra: 'player_points_rebounds_assists',
  pr: 'player_points_rebounds',
  pa: 'player_points_assists',
  ra: 'player_rebounds_assists',
}

const COMBO_COMPONENTS: Record<string, string[]> = {
  pra: ['pts', 'reb', 'ast'],
  pr: ['pts', 'reb'],
  pa: ['pts', 'ast'],
  ra: ['reb', 'ast'],
}

interface RequestBody {
  predictionIds?: unknown
  stateCode?: unknown
  excludedBooks?: unknown
}

interface PredictionRow {
  id: number
  prediction_date: string
  player_id: number
  game_id: string
  stat: string
  line: number | string
  quote_snapshot: unknown
}

interface SampleRow {
  prediction_date: string
  player_id: number
  game_id: string
  stat: string
  n_samples: number
  samples_gz: unknown
}

function chunks<T>(values: T[], size: number): T[][] {
  const result: T[][] = []
  for (let index = 0; index < values.length; index += size) result.push(values.slice(index, index + size))
  return result
}

function parseQuoteSnapshot(value: unknown): Record<string, unknown> | null {
  if (value && typeof value === 'object') return value as Record<string, unknown>
  if (typeof value !== 'string') return null
  try {
    const parsed = JSON.parse(value)
    return parsed && typeof parsed === 'object' ? parsed as Record<string, unknown> : null
  } catch {
    return null
  }
}

function byteaToBuffer(value: unknown): Buffer | null {
  if (value instanceof Uint8Array) return Buffer.from(value)
  if (typeof value !== 'string') return null
  if (value.startsWith('\\x')) return Buffer.from(value.slice(2), 'hex')
  try {
    return Buffer.from(value, 'base64')
  } catch {
    return null
  }
}

function decodeSamples(row: SampleRow): Float64Array | null {
  if (!Number.isInteger(row.n_samples) || row.n_samples <= 0 || row.n_samples > 100_000) return null
  const compressed = byteaToBuffer(row.samples_gz)
  if (!compressed) return null
  try {
    const raw = gunzipSync(compressed)
    if (raw.byteLength !== row.n_samples * Float64Array.BYTES_PER_ELEMENT) return null
    const copy = Uint8Array.from(raw)
    return new Float64Array(copy.buffer, copy.byteOffset, row.n_samples)
  } catch {
    return null
  }
}

function sampleKey(date: string, playerId: number, gameId: string, stat: string): string {
  return `${date}:${playerId}:${gameId}:${stat}`
}

function samplesForPrediction(row: PredictionRow, sampleMap: Map<string, Float64Array>): Float64Array | null {
  const direct = sampleMap.get(sampleKey(row.prediction_date, row.player_id, row.game_id, row.stat))
  if (direct) return direct
  const components = COMBO_COMPONENTS[row.stat]
  if (!components) return null
  const arrays = components.map(stat => sampleMap.get(sampleKey(row.prediction_date, row.player_id, row.game_id, stat)))
  if (arrays.some(value => !value)) return null
  const typed = arrays as Float64Array[]
  const length = typed[0].length
  if (length === 0 || typed.some(value => value.length !== length)) return null
  const combined = new Float64Array(length)
  for (let index = 0; index < length; index += 1) {
    for (const values of typed) combined[index] += values[index]
  }
  return combined
}

async function fetchPredictionRows(supabase: Awaited<ReturnType<typeof createClient>>, ids: number[]): Promise<PredictionRow[]> {
  const rows: PredictionRow[] = []
  for (const idChunk of chunks(ids, PREDICTION_QUERY_CHUNK)) {
    const { data, error } = await supabase
      .from('daily_predictions')
      .select('id,prediction_date,player_id,game_id,stat,line,quote_snapshot')
      .in('id', idChunk)
      .limit(idChunk.length)
    if (error) throw new Error('Prediction lookup failed')
    rows.push(...((data ?? []) as PredictionRow[]))
  }
  return rows
}

async function fetchCurrentLines(
  supabase: Awaited<ReturnType<typeof createClient>>,
  gameIds: string[],
): Promise<CurrentNbaLine[]> {
  const rows: CurrentNbaLine[] = []
  for (const gameChunk of chunks(gameIds, GAME_QUERY_CHUNK)) {
    for (let page = 0; page < MAX_PAGES; page += 1) {
      const from = page * PAGE_SIZE
      const { data, error } = await supabase
        .rpc('get_sportsbook_lines_by_games', { p_game_ids: gameChunk })
        .order('game_id')
        .order('player_id')
        .order('market_key')
        .order('bookmaker')
        .order('line')
        .range(from, from + PAGE_SIZE - 1)
      if (error) throw new Error('Current quote lookup failed')
      const pageRows = (data ?? []) as Array<Record<string, unknown>>
      for (const value of pageRows) {
        const row: CurrentNbaLine = {
          player_id: Number(value.player_id),
          game_id: String(value.game_id),
          bookmaker: String(value.bookmaker).toLowerCase(),
          market_key: String(value.market_key),
          line: Number(value.line),
          over_odds: Number(value.over_odds),
          under_odds: Number(value.under_odds),
          snapshot_time: String(value.snapshot_time),
        }
        if (Number.isInteger(row.player_id) && typeof value.game_id === 'string' &&
            typeof value.bookmaker === 'string' && value.bookmaker.length > 0 &&
            typeof value.market_key === 'string' &&
            typeof value.snapshot_time === 'string' && Number.isFinite(Date.parse(value.snapshot_time)) &&
            [row.line, row.over_odds, row.under_odds].every(Number.isFinite)) {
          rows.push(row)
        }
      }
      if (pageRows.length < PAGE_SIZE) break
      if (page === MAX_PAGES - 1) throw new Error('Current quote result exceeded safety limit')
    }
  }
  return rows
}

async function fetchSampleRows(
  supabase: Awaited<ReturnType<typeof createClient>>,
  predictions: PredictionRow[],
): Promise<SampleRow[]> {
  const rows: SampleRow[] = []
  const dates = [...new Set(predictions.map(row => row.prediction_date))]
  for (const date of dates) {
    const datePredictions = predictions.filter(row => row.prediction_date === date)
    const gameIds = [...new Set(datePredictions.map(row => row.game_id))]
    const playerIds = [...new Set(datePredictions.map(row => row.player_id))]
    const stats = [...new Set(datePredictions.flatMap(row => COMBO_COMPONENTS[row.stat] ?? [row.stat]))]
    for (const gameChunk of chunks(gameIds, GAME_QUERY_CHUNK)) {
      for (let page = 0; page < MAX_PAGES; page += 1) {
        const from = page * PAGE_SIZE
        const { data, error } = await supabase
          .from('daily_prediction_samples')
          .select('prediction_date,player_id,game_id,stat,n_samples,samples_gz')
          .eq('prediction_date', date)
          .in('game_id', gameChunk)
          .in('player_id', playerIds)
          .in('stat', stats)
          .order('game_id')
          .order('player_id')
          .order('stat')
          .range(from, from + PAGE_SIZE - 1)
        if (error) throw new Error('Prediction sample lookup failed')
        const pageRows = (data ?? []) as SampleRow[]
        rows.push(...pageRows)
        if (pageRows.length < PAGE_SIZE) break
        if (page === MAX_PAGES - 1) throw new Error('Prediction sample result exceeded safety limit')
      }
    }
  }
  return rows
}

export async function POST(request: NextRequest) {
  const supabase = await createClient()
  const { data: { user }, error: authError } = await supabase.auth.getUser()
  if (authError || !user) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })

  let body: RequestBody
  try {
    body = await request.json() as RequestBody
  } catch {
    return NextResponse.json({ error: 'Invalid JSON' }, { status: 400 })
  }

  if (!Array.isArray(body.predictionIds) || body.predictionIds.length === 0 || body.predictionIds.length > MAX_PREDICTIONS) {
    return NextResponse.json({ error: `predictionIds must contain 1-${MAX_PREDICTIONS} IDs` }, { status: 400 })
  }
  const predictionIds = [...new Set(body.predictionIds.map(Number))]
  if (predictionIds.some(id => !Number.isInteger(id) || id <= 0)) {
    return NextResponse.json({ error: 'predictionIds contains an invalid ID' }, { status: 400 })
  }

  const stateCode = typeof body.stateCode === 'string' ? body.stateCode.toUpperCase() : ''
  if (stateCode && !STATE_SPORTSBOOKS[stateCode]) {
    return NextResponse.json({ error: 'Invalid stateCode' }, { status: 400 })
  }
  if (!Array.isArray(body.excludedBooks) || body.excludedBooks.length > 50 ||
      body.excludedBooks.some(value => typeof value !== 'string' || !/^[a-z0-9_]+$/i.test(value))) {
    return NextResponse.json({ error: 'Invalid excludedBooks' }, { status: 400 })
  }
  const userExcluded = new Set((body.excludedBooks as string[]).map(value => value.toLowerCase()))
  const allowedBooks = stateCode ? new Set(STATE_SPORTSBOOKS[stateCode]) : null

  try {
    const predictions = await fetchPredictionRows(supabase, predictionIds)
    if (predictions.length !== predictionIds.length) {
      return NextResponse.json({ error: 'One or more predictions are unavailable' }, { status: 404 })
    }
    const gameIds = [...new Set(predictions.map(row => row.game_id))]
    if (gameIds.length > 32) return NextResponse.json({ error: 'Too many games requested' }, { status: 400 })

    const [allLines, sampleRows] = await Promise.all([
      fetchCurrentLines(supabase, gameIds),
      fetchSampleRows(supabase, predictions),
    ])

    const filteredLines = allLines.filter(line =>
      !EXCLUDED_BOOKS.has(line.bookmaker) &&
      !userExcluded.has(line.bookmaker) &&
      (!allowedBooks || allowedBooks.has(line.bookmaker)))
    const sampleMap = new Map<string, Float64Array>()
    for (const row of sampleRows) {
      const samples = decodeSamples(row)
      if (samples) sampleMap.set(sampleKey(row.prediction_date, row.player_id, row.game_id, row.stat), samples)
    }

    const results: NbaExecutableOfferResult[] = predictions.map(row => {
      const quoteSnapshot = parseQuoteSnapshot(row.quote_snapshot)
      const blConfig = validateBlConfig(quoteSnapshot?.bl_config)
      const marketKey = STAT_TO_MARKET[row.stat]
      const referenceLine = Number(row.line)
      const prediction: NbaOfferPredictionBase = {
        prediction_id: row.id,
        player_id: row.player_id,
        game_id: row.game_id,
        stat: row.stat,
        prediction_date: row.prediction_date,
        reference_line: referenceLine,
      }
      const matching = marketKey
        ? filteredLines.filter(line =>
            line.player_id === row.player_id && line.game_id === row.game_id && line.market_key === marketKey)
        : []
      return buildExecutableOfferResult(
        prediction,
        blConfig,
        matching,
        samplesForPrediction(row, sampleMap),
      )
    })

    applyBestOpportunityPolicy(results)
    return NextResponse.json({ results })
  } catch (error) {
    console.error('NBA executable offer lookup failed:', error instanceof Error ? error.message : 'unknown error')
    return NextResponse.json({ error: 'Current executable offers are unavailable' }, { status: 502 })
  }
}
