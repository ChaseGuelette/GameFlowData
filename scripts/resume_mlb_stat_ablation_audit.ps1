<#
.SYNOPSIS
    Generic resume runner for MLB stat ablation CLV audit + ranker diagnostics.
.DESCRIPTION
    Runs the audit/ranker tail on an existing sweep. Accepts batter_hits or
    pitcher_strikeouts profile defaults without hard-coded batter artifact names.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet('batter_hits','pitcher_strikeouts')]
    [string]$Profile,

    [string]$RunLabel,
    [string]$SweepDir,
    [string]$ModelDir,
    [string]$Start = '2026-04-13',
    [string]$End = '2026-05-17',
    [int]$MinDecisionGradeBets = 100,
    [string]$QuoteDecisionPolicy = 'slate_or_tminus',
    [string]$QuoteRelativeMinutes = '60',
    [string]$LineSource = 'mlb_player_props_clv_snapshots',
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'
$venvPython = '.\venv\Scripts\python.exe'

function Join-Command {
    param([object[]]$CommandParts)
    return (($CommandParts | ForEach-Object { [string]$_ }) -join ' ')
}

function Get-ProfileConfig {
    param([string]$Name)
    switch ($Name) {
        'batter_hits' {
            return [pscustomobject]@{
                Name = 'batter_hits'
                SweepStat = 'batter_hits'
                RunPrefix = 'batter_hits'
                ArtifactFilter = 'mlb_run_batter_hits_*'
            }
        }
        'pitcher_strikeouts' {
            return [pscustomobject]@{
                Name = 'pitcher_strikeouts'
                SweepStat = 'pitcher_strikeouts'
                RunPrefix = 'pitcher_strikeouts'
                ArtifactFilter = 'mlb_run_*'
            }
        }
    }
    throw "Unknown profile: $Name"
}

$profileConfig = Get-ProfileConfig $Profile

if (-not $RunLabel -and -not $SweepDir) {
    throw "Specify -RunLabel or -SweepDir."
}

if (-not $SweepDir) { $SweepDir = "backtest_results\ablations\${RunLabel}_preferred_book" }
if (-not $ModelDir) {
    if (-not $RunLabel) { throw "Specify -ModelDir when -RunLabel is omitted." }
    $artifactRoot = "src\models\mlb\artifacts\ablations\$RunLabel"
    if ($DryRun) {
        $ModelDir = "$artifactRoot\<run_dir_after_training>"
    } else {
        if (-not (Test-Path $artifactRoot)) { throw "Artifact root not found: $artifactRoot" }
        $candidate = Get-ChildItem -Path $artifactRoot -Directory -Filter $profileConfig.ArtifactFilter -ErrorAction Stop |
            Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if (-not $candidate) { throw "No $($profileConfig.ArtifactFilter) directory under $artifactRoot" }
        $ModelDir = $candidate.FullName
    }
}

$auditDir = "$SweepDir\audit_suite"
$rankerRoot = "$auditDir\ranker"

if (-not $DryRun) {
    if (-not (Test-Path $SweepDir)) { throw "Sweep dir not found: $SweepDir" }
    if (-not (Test-Path $ModelDir)) { throw "Model dir not found: $ModelDir" }
}

Write-Host ""
Write-Host "=== MLB Stat Ablation Resume Audit ==="
Write-Host "Profile:   $Profile"
Write-Host "Model dir: $ModelDir"
Write-Host "Sweep dir: $SweepDir"
Write-Host "Audit dir: $auditDir"
Write-Host ""

$decisionGradeBetsCsvs = @()
if ($DryRun) {
    $decisionGradeBetsCsvs = @("$SweepDir\config_<decision_grade>\bets.csv")
} else {
    Get-ChildItem -Path $SweepDir -Directory -Filter 'config_*' | ForEach-Object {
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
        if ($bets -lt $MinDecisionGradeBets) {
            $bets = (Get-Content $betsPath | Measure-Object -Line).Lines - 1
        }
        if ($bets -ge $MinDecisionGradeBets) { $decisionGradeBetsCsvs += $betsPath }
    }
}

Write-Host "Decision-grade configs (>=$MinDecisionGradeBets bets): $($decisionGradeBetsCsvs.Count)"
foreach ($p in $decisionGradeBetsCsvs) { Write-Host "  $p" }

if ($decisionGradeBetsCsvs.Count -eq 0) {
    Write-Warning "No decision-grade configs. Nothing to audit."
    return
}

Write-Host ""
Write-Host "[1/2] CLV-only audit (--skip-dropout-audit)" -ForegroundColor Cyan
$auditArgs = @(
    $venvPython,
    'scripts\run_mlb_quote_clean_audit_suite.py',
    '--local',
    '--skip-dropout-audit',
    '--sweep-output-dir', $SweepDir,
    '--output-dir', $auditDir,
    '--model-dir', $ModelDir,
    '--start', $Start,
    '--end', $End,
    '--stats', $profileConfig.SweepStat,
    '--quote-decision-policy', $QuoteDecisionPolicy,
    '--quote-relative-minutes', $QuoteRelativeMinutes,
    '--line-source', $LineSource,
    '--snapshots-table', 'mlb_player_props_clv_snapshots',
    '--batch-size', '25'
)
foreach ($p in $decisionGradeBetsCsvs) { $auditArgs += @('--bets-csv', $p) }
Write-Host "  $(Join-Command $auditArgs)"
if (-not $DryRun) {
    & $auditArgs[0] $auditArgs[1..($auditArgs.Length - 1)]
    if ($LASTEXITCODE -ne 0) { throw "Audit failed (exit $LASTEXITCODE)" }
}

Write-Host ""
Write-Host "[2/2] Ranker diagnostics" -ForegroundColor Cyan
foreach ($p in $decisionGradeBetsCsvs) {
    $label = if ($DryRun) { 'config_<decision_grade>' } else { Split-Path -Parent $p | Split-Path -Leaf }
    $clv = Join-Path $auditDir "clv\$label\clv_matches.csv"
    $cand = Join-Path (Split-Path -Parent $p) 'bookmaker_candidate_edges.csv'
    $rankOut = Join-Path $rankerRoot $label
    $rankArgs = @(
        $venvPython,
        'scripts\analyze_mlb_clv_ranking_diagnostics.py',
        '--clv-matches-csv', $clv,
        '--candidate-edges-csv', $cand,
        '--score-set', 'all',
        '--bootstrap-samples', '1000',
        '--min-n', "$MinDecisionGradeBets",
        '--output-dir', $rankOut
    )
    Write-Host "  $(Join-Command $rankArgs)"
    if (-not $DryRun -and (Test-Path $clv) -and (Test-Path $cand)) {
        & $rankArgs[0] $rankArgs[1..($rankArgs.Length - 1)]
        if ($LASTEXITCODE -ne 0) { Write-Warning "Ranker failed for $label (exit $LASTEXITCODE). Continuing." }
    }
}

Write-Host ""
Write-Host "Done." -ForegroundColor Green
Write-Host "Audit:  $auditDir"
Write-Host "Ranker: $rankerRoot"
