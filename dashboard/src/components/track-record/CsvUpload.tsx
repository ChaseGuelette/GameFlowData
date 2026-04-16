'use client'

import { useState, useRef, useCallback } from 'react'
import { createClient } from '@/lib/supabase/client'
import { parseBetsCsv, type ParsedBet } from '@/lib/csv/parseBets'

interface CsvUploadProps {
  onSuccess?: () => void
  onCancel?: () => void
}

type Step = 'upload' | 'preview' | 'importing' | 'done'

const PREVIEW_ROWS = 10

export function CsvUpload({ onSuccess, onCancel }: CsvUploadProps) {
  const [step, setStep] = useState<Step>('upload')
  const [dragging, setDragging] = useState(false)
  const [parsed, setParsed] = useState<ParsedBet[]>([])
  const [parseErrors, setParseErrors] = useState<{ row: number; message: string }[]>([])
  const [parseWarnings, setParseWarnings] = useState<string[]>([])
  const [assumeNegativeOdds, setAssumeNegativeOdds] = useState(true)
  const [importErrors, setImportErrors] = useState<string[]>([])
  const [importedCount, setImportedCount] = useState(0)
  const [rawCsv, setRawCsv] = useState<string>('')
  const fileRef = useRef<HTMLInputElement>(null)

  const processFile = useCallback((file: File) => {
    const reader = new FileReader()
    reader.onload = (e) => {
      const text = e.target?.result as string
      setRawCsv(text)
      const result = parseBetsCsv(text, { assumeNegativeOdds })
      setParsed(result.parsed)
      setParseErrors(result.errors)
      setParseWarnings(result.warnings)
      setStep('preview')
    }
    reader.readAsText(file)
  }, [assumeNegativeOdds])

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setDragging(false)
    const file = e.dataTransfer.files[0]
    if (file) processFile(file)
  }, [processFile])

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (file) processFile(file)
  }

  // Re-parse when toggle changes
  const handleToggleNegOdds = (val: boolean) => {
    setAssumeNegativeOdds(val)
    if (rawCsv) {
      const result = parseBetsCsv(rawCsv, { assumeNegativeOdds: val })
      setParsed(result.parsed)
      setParseErrors(result.errors)
      setParseWarnings(result.warnings)
    }
  }

  const handleImport = async () => {
    if (parsed.length === 0) return
    setStep('importing')
    setImportErrors([])

    try {
      const supabase = createClient()
      const { data: { user } } = await supabase.auth.getUser()
      if (!user) { setImportErrors(['Not authenticated']); setStep('preview'); return }

      const rows = parsed.map(bet => ({
        user_id:       user.id,
        game_date:     bet.game_date,
        player_name:   bet.player_name,
        stat_type:     bet.stat_type,
        line:          bet.line,
        bet_direction: bet.bet_direction,
        odds_at_bet:   bet.odds_at_bet,
        stake:         bet.stake,
        status:        bet.status,
        pnl:           bet.status === 'pending' ? null : bet.pnl,
        book_at_bet:   bet.book_at_bet,
        source:        'csv_import' as const,
        prediction_id: null,
        player_id:     null,
      }))

      // Insert in batches of 100
      const errors: string[] = []
      const BATCH = 100
      let inserted = 0

      for (let i = 0; i < rows.length; i += BATCH) {
        const batch = rows.slice(i, i + BATCH)
        const { error } = await supabase
          .from('user_bets')
          .upsert(batch, { onConflict: 'user_id,game_date,player_name,stat_type,bet_direction' })
        if (error) {
          errors.push(`Batch ${Math.floor(i / BATCH) + 1}: ${error.message}`)
        } else {
          inserted += batch.length
        }
      }

      setImportedCount(inserted)
      setImportErrors(errors)

      // Rebuild daily log
      await supabase.rpc('rebuild_user_daily_log', { target_user_id: user.id })

      setStep('done')
    } catch (err) {
      setImportErrors([err instanceof Error ? err.message : 'Unknown error'])
      setStep('preview')
    }
  }

  // ── Upload step ───────────────────────────────────────────────────────────

  if (step === 'upload') {
    return (
      <div className="space-y-4">
        <p className="text-sm text-slate-400">
          Upload a CSV or TSV export from your sportsbook tracking spreadsheet.
          Required columns: <span className="text-slate-300">Date, Player, Prop Type, Line, Over/Under, Win/Loss</span>.
        </p>

        {/* Assume negative odds toggle */}
        <label className="flex items-center gap-3 cursor-pointer">
          <div
            onClick={() => setAssumeNegativeOdds(!assumeNegativeOdds)}
            className={`relative w-10 h-5 rounded-full transition-colors ${assumeNegativeOdds ? 'bg-blue-600' : 'bg-slate-600'}`}
          >
            <span className={`absolute top-0.5 left-0.5 w-4 h-4 bg-white rounded-full transition-transform ${assumeNegativeOdds ? 'translate-x-5' : ''}`} />
          </div>
          <span className="text-sm text-slate-300">Treat unsigned odds as negative (e.g. 110 → -110)</span>
        </label>

        {/* Drop zone */}
        <div
          onDragOver={e => { e.preventDefault(); setDragging(true) }}
          onDragLeave={() => setDragging(false)}
          onDrop={handleDrop}
          onClick={() => fileRef.current?.click()}
          className={`
            border-2 border-dashed rounded-xl p-10 text-center cursor-pointer transition-colors
            ${dragging
              ? 'border-blue-500 bg-blue-900/20'
              : 'border-slate-600 hover:border-slate-500 hover:bg-slate-700/30'
            }
          `}
        >
          <div className="text-4xl mb-3">📂</div>
          <div className="text-sm text-slate-300 font-medium">Drop your CSV file here</div>
          <div className="text-xs text-slate-500 mt-1">or click to browse</div>
          <input
            ref={fileRef}
            type="file"
            accept=".csv,.tsv,.txt"
            className="hidden"
            onChange={handleFileChange}
          />
        </div>

        {onCancel && (
          <button
            onClick={onCancel}
            className="text-sm text-slate-500 hover:text-slate-300 transition-colors"
          >
            Cancel
          </button>
        )}
      </div>
    )
  }

  // ── Preview step ─────────────────────────────────────────────────────────

  if (step === 'preview') {
    return (
      <div className="space-y-4">
        {/* Summary banner */}
        <div className="flex items-center gap-4 flex-wrap">
          <div className="bg-green-900/30 border border-green-700 rounded-lg px-4 py-2 text-sm text-green-300">
            {parsed.length} bets parsed
          </div>
          {parseErrors.length > 0 && (
            <div className="bg-red-900/30 border border-red-700 rounded-lg px-4 py-2 text-sm text-red-300">
              {parseErrors.length} rows skipped
            </div>
          )}
          {parseWarnings.length > 0 && (
            <div className="bg-amber-900/30 border border-amber-700 rounded-lg px-4 py-2 text-sm text-amber-300">
              {parseWarnings.length} warnings
            </div>
          )}
        </div>

        {/* Negative odds toggle (re-parse) */}
        <label className="flex items-center gap-3 cursor-pointer">
          <div
            onClick={() => handleToggleNegOdds(!assumeNegativeOdds)}
            className={`relative w-10 h-5 rounded-full transition-colors ${assumeNegativeOdds ? 'bg-blue-600' : 'bg-slate-600'}`}
          >
            <span className={`absolute top-0.5 left-0.5 w-4 h-4 bg-white rounded-full transition-transform ${assumeNegativeOdds ? 'translate-x-5' : ''}`} />
          </div>
          <span className="text-sm text-slate-300">Treat unsigned odds as negative</span>
        </label>

        {/* Errors */}
        {parseErrors.length > 0 && (
          <details className="bg-red-900/20 border border-red-800 rounded-lg">
            <summary className="px-4 py-2 text-sm text-red-300 cursor-pointer">
              Rows with errors (skipped)
            </summary>
            <ul className="px-4 pb-3 space-y-1">
              {parseErrors.map((e, i) => (
                <li key={i} className="text-xs text-red-400">Row {e.row}: {e.message}</li>
              ))}
            </ul>
          </details>
        )}

        {/* Warnings */}
        {parseWarnings.length > 0 && (
          <details className="bg-amber-900/20 border border-amber-800 rounded-lg">
            <summary className="px-4 py-2 text-sm text-amber-300 cursor-pointer">
              Warnings
            </summary>
            <ul className="px-4 pb-3 space-y-1">
              {parseWarnings.map((w, i) => (
                <li key={i} className="text-xs text-amber-400">{w}</li>
              ))}
            </ul>
          </details>
        )}

        {/* Preview table */}
        {parsed.length > 0 && (
          <div className="overflow-x-auto rounded-lg border border-slate-700">
            <table className="w-full text-xs">
              <thead>
                <tr className="bg-slate-900 text-slate-400">
                  <th className="px-3 py-2 text-left font-medium">Date</th>
                  <th className="px-3 py-2 text-left font-medium">Player</th>
                  <th className="px-3 py-2 text-left font-medium">Stat</th>
                  <th className="px-3 py-2 text-right font-medium">Line</th>
                  <th className="px-3 py-2 text-center font-medium">Side</th>
                  <th className="px-3 py-2 text-right font-medium">Odds</th>
                  <th className="px-3 py-2 text-right font-medium">Stake</th>
                  <th className="px-3 py-2 text-center font-medium">Result</th>
                  <th className="px-3 py-2 text-right font-medium">P&L</th>
                </tr>
              </thead>
              <tbody>
                {parsed.slice(0, PREVIEW_ROWS).map((bet, i) => (
                  <tr key={i} className="border-t border-slate-700/50">
                    <td className="px-3 py-2 text-slate-300">{bet.game_date}</td>
                    <td className="px-3 py-2 text-slate-300">{bet.player_name}</td>
                    <td className="px-3 py-2 text-slate-400">{bet.stat_type}</td>
                    <td className="px-3 py-2 text-right text-slate-400">{bet.line}</td>
                    <td className="px-3 py-2 text-center">
                      <span className={`px-1.5 py-0.5 rounded text-xs font-medium ${
                        bet.bet_direction === 'over'
                          ? 'bg-blue-900/50 text-blue-300'
                          : 'bg-purple-900/50 text-purple-300'
                      }`}>
                        {bet.bet_direction.toUpperCase()}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-right text-slate-400">
                      {bet.odds_at_bet > 0 ? '+' : ''}{bet.odds_at_bet}
                    </td>
                    <td className="px-3 py-2 text-right text-slate-400">${bet.stake.toFixed(2)}</td>
                    <td className="px-3 py-2 text-center">
                      <span className={`px-1.5 py-0.5 rounded text-xs font-medium ${
                        bet.status === 'won'  ? 'bg-green-900/50 text-green-300' :
                        bet.status === 'lost' ? 'bg-red-900/50 text-red-300' :
                        'bg-slate-700 text-slate-300'
                      }`}>
                        {bet.status.toUpperCase()}
                      </span>
                    </td>
                    <td className={`px-3 py-2 text-right font-medium ${
                      bet.pnl >= 0 ? 'text-green-400' : 'text-red-400'
                    }`}>
                      {bet.pnl >= 0 ? '+' : ''}${bet.pnl.toFixed(2)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {parsed.length > PREVIEW_ROWS && (
              <div className="px-3 py-2 text-xs text-slate-500 bg-slate-900 border-t border-slate-700">
                Showing first {PREVIEW_ROWS} of {parsed.length} rows
              </div>
            )}
          </div>
        )}

        <div className="flex gap-3">
          <button
            onClick={handleImport}
            disabled={parsed.length === 0}
            className="flex-1 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white font-medium py-2 px-4 rounded-lg text-sm transition-colors"
          >
            Import {parsed.length} bet{parsed.length !== 1 ? 's' : ''}
          </button>
          <button
            onClick={() => { setStep('upload'); setRawCsv('') }}
            className="px-4 py-2 rounded-lg text-sm text-slate-400 hover:text-slate-200 hover:bg-slate-700 transition-colors"
          >
            Back
          </button>
          {onCancel && (
            <button
              onClick={onCancel}
              className="px-4 py-2 rounded-lg text-sm text-slate-500 hover:text-slate-300 transition-colors"
            >
              Cancel
            </button>
          )}
        </div>
      </div>
    )
  }

  // ── Importing step ────────────────────────────────────────────────────────

  if (step === 'importing') {
    return (
      <div className="py-10 text-center">
        <div className="animate-spin h-8 w-8 border-2 border-blue-500 border-t-transparent rounded-full mx-auto mb-4" />
        <div className="text-sm text-slate-400">Importing bets and rebuilding daily log...</div>
      </div>
    )
  }

  // ── Done step ─────────────────────────────────────────────────────────────

  return (
    <div className="space-y-4">
      <div className="text-center py-4">
        <div className="text-4xl mb-3">{importErrors.length === 0 ? '✅' : '⚠️'}</div>
        <div className="text-lg font-semibold text-slate-50 mb-1">
          {importErrors.length === 0 ? 'Import complete!' : 'Import finished with errors'}
        </div>
        <div className="text-sm text-slate-400">
          {importedCount} bet{importedCount !== 1 ? 's' : ''} imported successfully
        </div>
      </div>

      {importErrors.length > 0 && (
        <div className="bg-red-900/20 border border-red-800 rounded-lg px-4 py-3 space-y-1">
          {importErrors.map((e, i) => (
            <div key={i} className="text-xs text-red-400">{e}</div>
          ))}
        </div>
      )}

      <div className="flex gap-3">
        <button
          onClick={() => { onSuccess?.() }}
          className="flex-1 bg-blue-600 hover:bg-blue-700 text-white font-medium py-2 px-4 rounded-lg text-sm transition-colors"
        >
          Done
        </button>
        <button
          onClick={() => { setStep('upload'); setRawCsv(''); setParsed([]) }}
          className="px-4 py-2 rounded-lg text-sm text-slate-400 hover:text-slate-200 hover:bg-slate-700 transition-colors"
        >
          Import Another
        </button>
      </div>
    </div>
  )
}
