<#
.SYNOPSIS
    Run a single MLB batter_hits feature-family ablation end-to-end:
    train -> compact preferred_book_first sweep -> CLV-only audit -> ranker diagnostics.

.DESCRIPTION
    Designed for the iteration pipeline documented in
    docs/development_docs/mlb_batter_hits_ablation_iteration_pipeline.md.

    Does NOT tune hyperparameters by design — ablation discovery must compare
    against frozen baselines under matched hyperparams. Tune only the winner.

    Does NOT run the full dropout/bucket audit — that phase is local-Postgres
    I/O-bound and reserved for finalists. Uses --skip-dropout-audit and only
    audits configs with >=100 bets (decision-grade).

.PARAMETER Family
    Feature family name from BATTER_FORCE_FEATURE_FAMILIES. Valid:
    market, recent_form, contact_quality, matchup_pitcher, bullpen,
    platoon, environment, opportunity.

.PARAMETER Mode
    'include' adds the family via --force-include-families.
    'exclude' removes it via --force-exclude-families.

.PARAMETER Base
    'no_prop_line' (Track A / clean ablation baseline) or
    'with_prop_line' (Track B / paper-candidate attribution).

.PARAMETER Start
    Evaluation window start (default 2026-04-13).

.PARAMETER End
    Evaluation window end (default 2026-05-17).

.PARAMETER CalEndDate
    Calibration cutoff (default 2026-04-12).

.PARAMETER TrainSeasons
    Default 2024, 2025.

.PARAMETER FeatureTolerance
    Default 0.02 — frozen-baseline value. 0.005 made no difference in baselines.

.PARAMETER SkipTrain
    If set, reuses the existing artifact directory under
    src/models/mlb/artifacts/ablations/<run_label>/.

.PARAMETER SkipSweep
    If set, reuses the existing sweep directory under
    backtest_results/ablations/<run_label>_preferred_book/.

.PARAMETER DryRun
    Prints the commands it would run and exits.

.EXAMPLE
    .\scripts\run_batter_hits_family_ablation.ps1 -Family contact_quality -Mode include -Base no_prop_line

.EXAMPLE
    .\scripts\run_batter_hits_family_ablation.ps1 -Family matchup_pitcher -Mode exclude -Base with_prop_line
#>

[CmdletBinding()]
param(
    # One or more feature families (comma-separated or repeated). Optional if -Features is provided.
    [ValidateScript({
        $valid = @('market','recent_form','contact_quality','matchup_pitcher','bullpen','platoon','environment','opportunity')
        foreach ($f in ($_ -split ',')) {
            $f = $f.Trim()
            if ($f -and $valid -notcontains $f) {
                throw "Invalid family '$f'. Valid: $($valid -join ', ')"
            }
        }
        $true
    })]
    [string[]]$Families,

    # One or more exact feature names (comma-separated or repeated). Optional if -Families is provided.
    [string[]]$Features,

    [Parameter(Mandatory=$true)]
    [ValidateSet('include','exclude')]
    [string]$Mode,

    [Parameter(Mandatory=$true)]
    [ValidateSet('no_prop_line','with_prop_line')]
    [string]$Base,

    [string]$Start = '2026-04-13',
    [string]$End   = '2026-05-17',
    [string]$CalEndDate = '2026-04-12',
    [int[]] $TrainSeasons = @(2024, 2025),
    [double]$FeatureTolerance = 0.02,

    # Optional override of the auto-generated run label suffix (the family/feature part).
    # Useful for combos to keep the label short and readable.
    [string]$LabelTag,

    [switch]$SkipTrain,
    [switch]$SkipSweep,
    [switch]$DryRun
)

# Normalize multi-value args (handle comma-separated as well as repeated)
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

$Families = Expand-MultiArg $Families
$Features = Expand-MultiArg $Features

if ($Families.Count -eq 0 -and $Features.Count -eq 0) {
    throw "Specify at least one of -Families or -Features."
}

$ErrorActionPreference = 'Stop'
$venvPython = '.\venv\Scripts\python.exe'

# --- Sanity: must be at project root with venv ---
if (-not (Test-Path $venvPython)) {
    throw "venv python not found at $venvPython. Run from C:\Users\Chase\Projects\GameFlowData."
}

# --- Build a stable run label ---
$ts = Get-Date -Format 'yyyyMMdd_HHmmss'
if ($LabelTag) {
    $tag = $LabelTag
} else {
    $parts = @()
    if ($Families.Count -gt 0) { $parts += ($Families -join '+') }
    if ($Features.Count -gt 0) { $parts += ('feat-' + ($Features -join '+')) }
    $tag = ($parts -join '__')
}
$runLabel = "batter_hits_${Base}_${Mode}_${tag}_${ts}"

$artifactRoot = "src\models\mlb\artifacts\ablations\$runLabel"
$sweepDir     = "backtest_results\ablations\${runLabel}_preferred_book"
$auditDir     = "$sweepDir\audit_suite"
$rankerRoot   = "$auditDir\ranker"
$summaryPath  = "$sweepDir\ablation_summary.md"

Write-Host ""
Write-Host "=== Batter Hits Family Ablation ==="
if ($Families.Count -gt 0) { Write-Host "Families:         $($Families -join ', ')" }
if ($Features.Count -gt 0) { Write-Host "Features:         $($Features -join ', ')" }
Write-Host "Mode:             $Mode"
Write-Host "Base:             $Base"
Write-Host "Window:           $Start -> $End"
Write-Host "Cal cutoff:       $CalEndDate"
Write-Host "Train seasons:    $($TrainSeasons -join ', ')"
Write-Host "Tolerance:        $FeatureTolerance (no tuning)"
Write-Host "Artifact dir:     $artifactRoot"
Write-Host "Sweep dir:        $sweepDir"
Write-Host "Run label:        $runLabel"
Write-Host ""

# --- Build train command ---
$trainArgs = @(
    $venvPython,
    'src\models\mlb\mlb_batter_train_pipeline.py',
    '--local',
    '--stat', 'hits',
    '--train-seasons'
) + ($TrainSeasons | ForEach-Object { "$_" }) + @(
    '--cal-season', '2026',
    '--cal-end-date', $CalEndDate,
    '--feature-tolerance', "$FeatureTolerance",
    '--output-dir', $artifactRoot
)

if ($Base -eq 'no_prop_line') {
    $trainArgs += '--exclude-prop-line'
}

if ($Mode -eq 'include') {
    if ($Families.Count -gt 0) {
        $trainArgs += '--force-include-families'
        foreach ($f in $Families) { $trainArgs += $f }
    }
    if ($Features.Count -gt 0) {
        $trainArgs += '--force-include-features'
        foreach ($f in $Features) { $trainArgs += $f }
    }
} else {
    if ($Families.Count -gt 0) {
        $trainArgs += '--force-exclude-families'
        foreach ($f in $Families) { $trainArgs += $f }
    }
    if ($Features.Count -gt 0) {
        $trainArgs += '--force-exclude-features'
        foreach ($f in $Features) { $trainArgs += $f }
    }
}

# --- Step 1: train ---
Write-Host "[1/4] Train" -ForegroundColor Cyan
$trainCmd = $trainArgs -join ' '
Write-Host "  $trainCmd"
if (-not $DryRun -and -not $SkipTrain) {
    & $trainArgs[0] $trainArgs[1..($trainArgs.Length - 1)]
    if ($LASTEXITCODE -ne 0) { throw "Train failed (exit $LASTEXITCODE)" }
}

# --- Find the trained artifact dir (most recent under $artifactRoot) ---
if ($DryRun) {
    $modelDir = "$artifactRoot\<run_dir_after_training>"
} else {
    $candidate = Get-ChildItem -Path $artifactRoot -Directory -Filter 'mlb_run_batter_hits_*' -ErrorAction Stop |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if (-not $candidate) { throw "No mlb_run_batter_hits_* directory found in $artifactRoot" }
    $modelDir = $candidate.FullName
    Write-Host "  Resolved model dir: $modelDir"
}

# --- Step 2: compact preferred_book_first sweep ---
Write-Host ""
Write-Host "[2/4] Compact sweep (preferred_book_first, no_BL edge 0.10 / 0.12 / 0.15)" -ForegroundColor Cyan

$sweepArgs = @(
    $venvPython,
    'src\backtesting\mlb\run_mlb_sweep.py',
    '--local',
    '--start', $Start,
    '--end', $End,
    '--stats', 'batter_hits',
    '--direction', 'both',
    '--tau', 'none',
    '--edge', '0.10', '0.12', '0.15',
    '--kelly', '0.125',
    '--model-dir', $modelDir,
    '--output-dir', $sweepDir,
    '--quote-clean',
    '--quote-decision-policy', 'slate_or_tminus',
    '--quote-relative-minutes', '60',
    '--line-source', 'mlb_player_props_clv_snapshots',
    '--book-routing-policy', 'preferred_book_first'
)

$sweepCmd = $sweepArgs -join ' '
Write-Host "  $sweepCmd"
if (-not $DryRun -and -not $SkipSweep) {
    & $sweepArgs[0] $sweepArgs[1..($sweepArgs.Length - 1)]
    if ($LASTEXITCODE -ne 0) { throw "Sweep failed (exit $LASTEXITCODE)" }
}

# --- Step 3: identify decision-grade configs (>=100 bets) ---
Write-Host ""
Write-Host "[3/4] CLV-only audit suite (decision-grade configs only, --skip-dropout-audit)" -ForegroundColor Cyan

$decisionGradeBetsCsvs = @()
if (-not $DryRun) {
    # Enumerate config directories on disk; sweep_summary.csv does not carry a label column.
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
            } catch {
                $bets = 0
            }
        }
        if ($bets -lt 100) {
            # Fallback: count rows in bets.csv (header excluded)
            $bets = (Get-Content $betsPath | Measure-Object -Line).Lines - 1
        }
        if ($bets -ge 100) {
            $decisionGradeBetsCsvs += $betsPath
        }
    }
    Write-Host "  Decision-grade configs (>=100 bets): $($decisionGradeBetsCsvs.Count)"
    foreach ($p in $decisionGradeBetsCsvs) { Write-Host "    $p" }
}

if ($decisionGradeBetsCsvs.Count -eq 0 -and -not $DryRun) {
    Write-Warning "No decision-grade configs (>=100 bets). Skipping audit + ranker."
} else {
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
    $auditCmd = $auditArgs -join ' '
    Write-Host "  $auditCmd"
    if (-not $DryRun) {
        & $auditArgs[0] $auditArgs[1..($auditArgs.Length - 1)]
        if ($LASTEXITCODE -ne 0) { throw "Audit suite failed (exit $LASTEXITCODE)" }
    }

    # --- Step 4: ranker diagnostics for each decision-grade config ---
    Write-Host ""
    Write-Host "[4/4] Ranker diagnostics" -ForegroundColor Cyan
    if (-not $DryRun) {
        foreach ($p in $decisionGradeBetsCsvs) {
            $label = Split-Path -Parent $p | Split-Path -Leaf
            $clv = Join-Path $auditDir "clv\$label\clv_matches.csv"
            $cand = Join-Path (Split-Path -Parent $p) 'bookmaker_candidate_edges.csv'
            if ((Test-Path $clv) -and (Test-Path $cand)) {
                $rankOut = Join-Path $rankerRoot $label
                $rankArgs = @(
                    $venvPython,
                    'scripts\analyze_mlb_clv_ranking_diagnostics.py',
                    '--clv-matches-csv', $clv,
                    '--candidate-edges-csv', $cand,
                    '--score-set', 'all',
                    '--bootstrap-samples', '1000',
                    '--min-n', '100',
                    '--output-dir', $rankOut
                )
                Write-Host "  ranker $label"
                & $rankArgs[0] $rankArgs[1..($rankArgs.Length - 1)]
                if ($LASTEXITCODE -ne 0) {
                    Write-Warning "Ranker failed for $label (exit $LASTEXITCODE). Continuing - common on tiny configs."
                }
            } else {
                Write-Warning "Skipping ranker for $label (missing clv_matches or candidate_edges)."
            }
        }
    }
}

# --- Final summary file ---
Write-Host ""
Write-Host "Writing ablation summary -> $summaryPath"
if (-not $DryRun) {
    $trainSeasonsStr = ($TrainSeasons -join ', ')
    $lines = New-Object System.Collections.Generic.List[string]
    $lines.Add("# Ablation summary: $runLabel")
    $lines.Add("")
    $lines.Add("- Families: $($Families -join ', ')")
    $lines.Add("- Features: $($Features -join ', ')")
    $lines.Add("- Mode: $Mode")
    $lines.Add("- Base: $Base")
    $lines.Add("- Window: $Start -> $End (cal cutoff $CalEndDate)")
    $lines.Add("- Train seasons: $trainSeasonsStr")
    $lines.Add("- Feature tolerance: $FeatureTolerance (no tuning)")
    $lines.Add("- Model: $modelDir")
    $lines.Add("- Sweep: $sweepDir")
    $lines.Add("- Audit: $auditDir")
    $lines.Add("")
    $lines.Add("## Frozen baselines for comparison")
    $lines.Add("")
    $lines.Add("- with_prop_line preferred_book edge=0.12: bets=261 ROI=+25.76% mean_clv_low=+0.00346 edge_clv_low=-0.0418")
    $lines.Add("- no_prop_line   preferred_book edge=0.12: bets=455 ROI=+11.94% mean_clv_low=+0.00539 edge_clv_low=-0.1782")
    $lines.Add("- no_prop_line   preferred_book edge=0.10: bets=783 ROI=+11.13% mean_clv_low=+0.00516 edge_clv_low=-0.0970")
    $lines.Add("")
    $lines.Add("## Next steps")
    $lines.Add("")
    $lines.Add("- Compare sweep_summary.csv decision-grade rows to baseline metrics above.")
    $lines.Add("- Compare audit suite_manifest.csv for mean_clv_ci_low and edge_clv_ci_low.")
    $lines.Add("- Compare ranker/<config>/ranking_score_summary.csv raw_edge/model_prob/logit_edge ci_low.")
    $lines.Add("- If edge_clv_ci_low moves materially toward zero, run book sensitivity then independent-window validation.")
    $lines.Add("- If ROI/CLV drops with no ranker improvement, reject this family/mode.")
    New-Item -ItemType Directory -Force -Path (Split-Path $summaryPath -Parent) | Out-Null
    Set-Content -Path $summaryPath -Value ($lines -join [Environment]::NewLine) -Encoding UTF8
}

Write-Host ""
Write-Host "Done. Run label: $runLabel" -ForegroundColor Green
Write-Host "Summary: $summaryPath" -ForegroundColor Green
