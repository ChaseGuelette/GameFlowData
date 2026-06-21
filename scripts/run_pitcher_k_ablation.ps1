<#
.SYNOPSIS
    Thin pitcher K wrapper for the generic MLB stat ablation runner.
.DESCRIPTION
    Delegates all command assembly to scripts\run_mlb_stat_ablation.ps1 with
    -Profile pitcher_strikeouts. Keeps pitcher-friendly parameters without
    cloning the generic lifecycle logic.
#>

[CmdletBinding()]
param(
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
    [string]$Direction = 'under',
    [string[]]$Edge = @('0.10','0.12','0.15'),
    [string]$Kelly = '0.125',
    [switch]$FlatBet,

    [switch]$SkipTrain,
    [switch]$SkipSweep,
    [switch]$SkipAudit,
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'
$runner = Join-Path $PSScriptRoot 'run_mlb_stat_ablation.ps1'

$invoke = @{
    Profile = 'pitcher_strikeouts'
    Mode = $Mode
    Variant = $Variant
    Start = $Start
    End = $End
    CalEndDate = $CalEndDate
    TrainSeasons = $TrainSeasons
    FeatureTolerance = $FeatureTolerance
    Direction = $Direction
    Edge = $Edge
    Kelly = $Kelly
}

if ($Families) { $invoke.Families = $Families }
if ($Features) { $invoke.Features = $Features }
if ($LabelTag) { $invoke.LabelTag = $LabelTag }
if ($FlatBet) { $invoke.FlatBet = $true }
if ($SkipTrain) { $invoke.SkipTrain = $true }
if ($SkipSweep) { $invoke.SkipSweep = $true }
if ($SkipAudit) { $invoke.SkipAudit = $true }
if ($DryRun) { $invoke.DryRun = $true }

& $runner @invoke
exit $LASTEXITCODE
