<#
.SYNOPSIS
    Run the CLV-only audit + ranker diagnostics on an existing ablation sweep.

.DESCRIPTION
    Companion to run_batter_hits_family_ablation.ps1. Use this to resume
    audit + ranker after a sweep has completed (or when the original run
    skipped those stages).

    Auto-discovers decision-grade configs (>=100 bets) by reading each
    config_*/metrics.json or counting bets.csv lines.

.PARAMETER RunLabel
    The ablation run label, e.g. batter_hits_no_prop_line_include_contact_quality_20260526_202333

.PARAMETER Start
    Default 2026-04-13.

.PARAMETER End
    Default 2026-05-17.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [string]$RunLabel,
    [string]$Start = '2026-04-13',
    [string]$End   = '2026-05-17'
)

$ErrorActionPreference = 'Stop'
$venvPython = '.\venv\Scripts\python.exe'

$artifactRoot = "src\models\mlb\artifacts\ablations\$RunLabel"
$sweepDir     = "backtest_results\ablations\${RunLabel}_preferred_book"
$auditDir     = "$sweepDir\audit_suite"
$rankerRoot   = "$auditDir\ranker"

if (-not (Test-Path $artifactRoot)) { throw "Artifact dir not found: $artifactRoot" }
if (-not (Test-Path $sweepDir))     { throw "Sweep dir not found: $sweepDir" }

# Resolve model dir (most recent run dir)
$candidate = Get-ChildItem -Path $artifactRoot -Directory -Filter 'mlb_run_batter_hits_*' -ErrorAction Stop |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $candidate) { throw "No mlb_run_batter_hits_* directory under $artifactRoot" }
$modelDir = $candidate.FullName
Write-Host "Model dir: $modelDir"
Write-Host "Sweep dir: $sweepDir"

# Find decision-grade configs (>=100 bets)
$decisionGradeBetsCsvs = @()
Get-ChildItem -Path $sweepDir -Directory -Filter 'config_*' | ForEach-Object {
    $cfgDir = $_.FullName
    $metricsPath = Join-Path $cfgDir 'metrics.json'
    $betsPath = Join-Path $cfgDir 'bets.csv'
    if (-not (Test-Path $betsPath)) { return }
    $bets = 0
    if (Test-Path $metricsPath) {
        try {
            $m = Get-Content $metricsPath -Raw | ConvertFrom-Json
            if ($m.total_bets) { $bets = [int]$m.total_bets }
        } catch { $bets = 0 }
    }
    if ($bets -lt 100) {
        $bets = (Get-Content $betsPath | Measure-Object -Line).Lines - 1
    }
    if ($bets -ge 100) {
        $decisionGradeBetsCsvs += $betsPath
    }
}
Write-Host "Decision-grade configs (>=100 bets): $($decisionGradeBetsCsvs.Count)"
foreach ($p in $decisionGradeBetsCsvs) { Write-Host "  $p" }

if ($decisionGradeBetsCsvs.Count -eq 0) {
    Write-Warning "No decision-grade configs. Nothing to audit."
    return
}

# --- Audit ---
Write-Host ""
Write-Host "[1/2] CLV-only audit (--skip-dropout-audit)" -ForegroundColor Cyan
$auditArgs = @(
    $venvPython,
    'scripts\run_mlb_quote_clean_audit_suite.py',
    '--local',
    '--skip-dropout-audit',
    '--sweep-output-dir', $sweepDir,
    '--output-dir', $auditDir,
    '--model-dir', $modelDir,
    '--start', $Start,
    '--end', $End,
    '--stats', 'batter_hits',
    '--quote-decision-policy', 'slate_or_tminus',
    '--quote-relative-minutes', '60',
    '--line-source', 'mlb_player_props_clv_snapshots',
    '--snapshots-table', 'mlb_player_props_clv_snapshots',
    '--batch-size', '25'
)
foreach ($p in $decisionGradeBetsCsvs) {
    $auditArgs += '--bets-csv'
    $auditArgs += $p
}
& $auditArgs[0] $auditArgs[1..($auditArgs.Length - 1)]
if ($LASTEXITCODE -ne 0) { throw "Audit failed (exit $LASTEXITCODE)" }

# --- Ranker per decision-grade config ---
Write-Host ""
Write-Host "[2/2] Ranker diagnostics" -ForegroundColor Cyan
foreach ($p in $decisionGradeBetsCsvs) {
    $label = Split-Path -Parent $p | Split-Path -Leaf
    $clv = Join-Path $auditDir "clv\$label\clv_matches.csv"
    $cand = Join-Path (Split-Path -Parent $p) 'bookmaker_candidate_edges.csv'
    if ((Test-Path $clv) -and (Test-Path $cand)) {
        $rankOut = Join-Path $rankerRoot $label
        & $venvPython scripts\analyze_mlb_clv_ranking_diagnostics.py `
            --clv-matches-csv $clv `
            --candidate-edges-csv $cand `
            --score-set all `
            --bootstrap-samples 1000 `
            --min-n 100 `
            --output-dir $rankOut
        if ($LASTEXITCODE -ne 0) {
            Write-Warning "Ranker failed for $label (exit $LASTEXITCODE). Continuing."
        }
    } else {
        Write-Warning "Skipping ranker for $label (missing clv_matches or candidate_edges)."
    }
}

Write-Host ""
Write-Host "Done." -ForegroundColor Green
Write-Host "Audit:  $auditDir"
Write-Host "Ranker: $rankerRoot"
