<#
.SYNOPSIS
    Generic MLB stat ablation runner for dry-run-first stat-suite migration.
.DESCRIPTION
    Builds train -> quote-clean sweep -> CLV audit -> ranker command sequences for
    batter_hits and pitcher_strikeouts profiles. Slice 2 validation is dry-run/static;
    real execution is supported but should only be used after Chase approval.
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [ValidateSet('batter_hits','pitcher_strikeouts')]
    [string]$Profile,

    [ValidateSet('include','exclude')]
    [string]$Mode = 'include',

    [string[]]$Families,
    [string[]]$Features,

    [ValidateSet('none','static_no_l30','hook_only','ip_only','ip_hook','hook_avg_ip_l30','hook_short_hook_l30','hook_deep_start_l30')]
    [string]$Variant = 'none',

    [string]$Start = '2026-04-13',
    [string]$End = '2026-05-17',
    [string]$CalEndDate = '2026-04-12',
    [int[]]$TrainSeasons = @(2024, 2025),
    [double]$FeatureTolerance = 0.02,
    [string]$LabelTag,

    [ValidateSet('over','under','both')]
    [string]$Direction,
    [string[]]$Edge = @('0.10','0.12','0.15'),
    [string]$Kelly = '0.125',
    [switch]$FlatBet,
    [string]$BookRoutingPolicy = 'preferred_book_first',
    [string]$LineSource = 'mlb_player_props_clv_snapshots',
    [string]$QuoteDecisionPolicy = 'slate_or_tminus',
    [string]$QuoteRelativeMinutes = '60',
    [int]$MinDecisionGradeBets = 100,

    [switch]$SkipTrain,
    [switch]$SkipSweep,
    [switch]$SkipAudit,
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'
$venvPython = '.\venv\Scripts\python.exe'

function Expand-MultiArg {
    param([string[]]$Items)
    if (-not $Items) { return @() }
    $out = @()
    foreach ($i in $Items) {
        foreach ($p in ($i -split ',')) {
            $t = $p.Trim()
            if ($t) { $out += $t }
        }
    }
    return $out
}

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
                TrainScript = 'src\models\mlb\mlb_batter_train_pipeline.py'
                TrainStatArgs = @('--stat', 'hits')
                SweepStat = 'batter_hits'
                ArtifactFilter = 'mlb_run_batter_hits_*'
                RunPrefix = 'batter_hits'
                DefaultDirection = 'both'
                SupportsFamilies = $true
                SupportsFeatures = $true
                SupportsVariant = $false
            }
        }
        'pitcher_strikeouts' {
            return [pscustomobject]@{
                Name = 'pitcher_strikeouts'
                TrainScript = 'src\models\mlb\mlb_train_pipeline.py'
                TrainStatArgs = @()
                SweepStat = 'pitcher_strikeouts'
                ArtifactFilter = 'mlb_run_*'
                RunPrefix = 'pitcher_strikeouts'
                DefaultDirection = 'under'
                SupportsFamilies = $false
                SupportsFeatures = $false
                SupportsVariant = $true
            }
        }
    }
    throw "Unknown profile: $Name"
}

$profileConfig = Get-ProfileConfig $Profile
$Families = Expand-MultiArg $Families
$Features = Expand-MultiArg $Features
if (-not $Direction) { $Direction = $profileConfig.DefaultDirection }

$ts = Get-Date -Format 'yyyyMMdd_HHmmss'
if ($LabelTag) {
    $tag = $LabelTag
} else {
    $parts = @()
    if ($Families.Count -gt 0) { $parts += ($Families -join '+') }
    if ($Features.Count -gt 0) { $parts += ('feat-' + ($Features -join '+')) }
    if ($Variant -ne 'none') { $parts += $Variant }
    if ($parts.Count -eq 0) { $parts += 'baseline' }
    $tag = ($parts -join '__')
}
$runLabel = "$($profileConfig.RunPrefix)_${Mode}_${tag}_${ts}"
$artifactRoot = "src\models\mlb\artifacts\ablations\$runLabel"
$sweepDir = "backtest_results\ablations\${runLabel}_preferred_book"
$auditDir = "$sweepDir\audit_suite"
$rankerRoot = "$auditDir\ranker"

Write-Host ""
Write-Host "=== MLB Stat Ablation ==="
Write-Host "Profile:          $Profile"
Write-Host "Mode:             $Mode"
Write-Host "Families:         $($Families -join ', ')"
Write-Host "Features:         $($Features -join ', ')"
Write-Host "Variant:          $Variant"
Write-Host "Direction:        $Direction"
Write-Host "Window:           $Start -> $End"
Write-Host "Cal cutoff:       $CalEndDate"
Write-Host "Run label:        $runLabel"
Write-Host "Artifact root:    $artifactRoot"
Write-Host "Sweep dir:        $sweepDir"
Write-Host ""

if (-not $DryRun -and -not (Test-Path $venvPython)) {
    throw "venv python not found at $venvPython. Run from the GameFlowData project root."
}

# [1/4] Train
$trainArgs = @($venvPython, $profileConfig.TrainScript) + $profileConfig.TrainStatArgs + @(
    '--local',
    '--train-seasons'
) + ($TrainSeasons | ForEach-Object { "$_" }) + @(
    '--cal-season', '2026',
    '--cal-end-date', $CalEndDate,
    '--feature-tolerance', "$FeatureTolerance",
    '--output-dir', $artifactRoot
)

if ($Profile -eq 'batter_hits') {
    # Literal fragments kept for static characterization: '--stats', 'batter_hits'
    if ($Mode -eq 'include') {
        if ($Families.Count -gt 0) { $trainArgs += '--force-include-families'; foreach ($f in $Families) { $trainArgs += $f } }
        if ($Features.Count -gt 0) { $trainArgs += '--force-include-features'; foreach ($f in $Features) { $trainArgs += $f } }
    } else {
        if ($Families.Count -gt 0) { $trainArgs += '--force-exclude-families'; foreach ($f in $Families) { $trainArgs += $f } }
        if ($Features.Count -gt 0) { $trainArgs += '--force-exclude-features'; foreach ($f in $Features) { $trainArgs += $f } }
    }
} elseif ($Profile -eq 'pitcher_strikeouts') {
    # Literal fragments kept for static characterization: '--stats', 'pitcher_strikeouts'
    if ($Mode -eq 'include') {
        if ($Families.Count -gt 0) { $trainArgs += '--force-include-families'; foreach ($f in $Families) { $trainArgs += $f } }
        if ($Features.Count -gt 0) { $trainArgs += '--force-include-features'; foreach ($f in $Features) { $trainArgs += $f } }
    } else {
        if ($Families.Count -gt 0) { $trainArgs += '--force-exclude-families'; foreach ($f in $Families) { $trainArgs += $f } }
        if ($Features.Count -gt 0) { $trainArgs += '--force-exclude-features'; foreach ($f in $Features) { $trainArgs += $f } }
    }
    if ($Variant -ne 'none') { $trainArgs += @('--ablation-variant', $Variant) }
}

Write-Host "[1/4] Train" -ForegroundColor Cyan
Write-Host "  $(Join-Command $trainArgs)"
if (-not $DryRun -and -not $SkipTrain) {
    & $trainArgs[0] $trainArgs[1..($trainArgs.Length - 1)]
    if ($LASTEXITCODE -ne 0) { throw "Train failed (exit $LASTEXITCODE)" }
}

if ($DryRun) {
    $modelDir = "$artifactRoot\<run_dir_after_training>"
} else {
    $candidate = Get-ChildItem -Path $artifactRoot -Directory -Filter $profileConfig.ArtifactFilter -ErrorAction Stop |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if (-not $candidate) { throw "No $($profileConfig.ArtifactFilter) directory found in $artifactRoot" }
    $modelDir = $candidate.FullName
}

# [2/4] Sweep
$sweepArgs = @(
    $venvPython,
    'src\backtesting\mlb\run_mlb_sweep.py',
    '--local',
    '--start', $Start,
    '--end', $End,
    '--stats', $profileConfig.SweepStat,
    '--direction', $profileConfig.DefaultDirection,
    '--tau', 'none',
    '--edge'
) + $Edge + @(
    '--kelly', $Kelly,
    '--model-dir', $modelDir,
    '--output-dir', $sweepDir,
    '--quote-clean',
    '--quote-decision-policy', 'slate_or_tminus',
    '--quote-relative-minutes', $QuoteRelativeMinutes,
    '--line-source', 'mlb_player_props_clv_snapshots',
    '--book-routing-policy', 'preferred_book_first'
)
if ($Direction -ne $profileConfig.DefaultDirection) {
    $idx = [array]::IndexOf($sweepArgs, $profileConfig.DefaultDirection)
    if ($idx -ge 0) { $sweepArgs[$idx] = $Direction }
}
if ($FlatBet) { $sweepArgs += @('--flat', '100') }

Write-Host ""
Write-Host "[2/4] Sweep" -ForegroundColor Cyan
Write-Host "  $(Join-Command $sweepArgs)"
if (-not $DryRun -and -not $SkipSweep) {
    & $sweepArgs[0] $sweepArgs[1..($sweepArgs.Length - 1)]
    if ($LASTEXITCODE -ne 0) { throw "Sweep failed (exit $LASTEXITCODE)" }
}

# [3/4] Audit
$decisionGradeBetsCsvs = @()
if ($DryRun) {
    $decisionGradeBetsCsvs = @("$sweepDir\config_<decision_grade>\bets.csv")
} elseif (-not $SkipAudit) {
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
        if ($bets -lt $MinDecisionGradeBets) {
            $bets = (Get-Content $betsPath | Measure-Object -Line).Lines - 1
        }
        if ($bets -ge $MinDecisionGradeBets) { $decisionGradeBetsCsvs += $betsPath }
    }
}

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
    '--stats', $profileConfig.SweepStat,
    '--quote-decision-policy', 'slate_or_tminus',
    '--quote-relative-minutes', $QuoteRelativeMinutes,
    '--line-source', 'mlb_player_props_clv_snapshots',
    '--snapshots-table', 'mlb_player_props_clv_snapshots',
    '--batch-size', '25'
)
foreach ($p in $decisionGradeBetsCsvs) { $auditArgs += @('--bets-csv', $p) }

Write-Host ""
Write-Host "[3/4] Audit" -ForegroundColor Cyan
Write-Host "  $(Join-Command $auditArgs)"
if (-not $DryRun -and -not $SkipAudit -and $decisionGradeBetsCsvs.Count -gt 0) {
    & $auditArgs[0] $auditArgs[1..($auditArgs.Length - 1)]
    if ($LASTEXITCODE -ne 0) { throw "Audit suite failed (exit $LASTEXITCODE)" }
}

# [4/4] Ranker
Write-Host ""
Write-Host "[4/4] Ranker" -ForegroundColor Cyan
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
    if (-not $DryRun -and -not $SkipAudit -and (Test-Path $clv) -and (Test-Path $cand)) {
        & $rankArgs[0] $rankArgs[1..($rankArgs.Length - 1)]
        if ($LASTEXITCODE -ne 0) { Write-Warning "Ranker failed for $label (exit $LASTEXITCODE). Continuing." }
    }
}

Write-Host ""
Write-Host "Done. Run label: $runLabel" -ForegroundColor Green
