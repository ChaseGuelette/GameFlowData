# Plan by default. -Run invokes bounded child batches; only -Execute writes DB.
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$StartDate,
    [Parameter(Mandatory = $true)][string]$EndDate,
    [Parameter(Mandatory = $true)][string]$StateDirectory,
    [ValidateRange(1, 7)][int]$WindowDays = 7,
    [ValidateRange(1, 100)][int]$MaxGames = 10,
    [ValidateRange(1, 20)][int]$MaxRequests = 4,
    [ValidateRange(1, 100)][int]$MaxBatchesPerWindow = 20,
    [string[]]$ReportSlot = @('05PM'),
    [switch]$Run,
    [switch]$Execute,
    [switch]$MirrorLocal
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$culture = [System.Globalization.CultureInfo]::InvariantCulture
$start = [datetime]::ParseExact($StartDate, 'yyyy-MM-dd', $culture)
$end = [datetime]::ParseExact($EndDate, 'yyyy-MM-dd', $culture)
if ($end -lt $start) { throw 'EndDate must not precede StartDate.' }
if ($MirrorLocal -and -not $Execute) { throw 'MirrorLocal requires Execute.' }
if ($ReportSlot.Count -lt 1 -or $ReportSlot.Count -gt 4) { throw 'Supply one to four report slots.' }
foreach ($slot in $ReportSlot) {
    if ($slot -notmatch '^[A-Za-z0-9_-]+$') { throw 'Invalid report slot token.' }
}
$root = Split-Path $PSScriptRoot -Parent
$python = Join-Path $root 'venv\Scripts\python.exe'
$script = Join-Path $PSScriptRoot 'recover_nba_injury_history.py'
if (-not (Test-Path -LiteralPath $python)) { throw "Python not found: $python" }
if (-not (Test-Path -LiteralPath $script)) { throw "Recovery CLI not found: $script" }
$stateRoot = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($StateDirectory)
$mode = if ($Execute) { 'execute' } else { 'dry' }
$mirror = if ($MirrorLocal) { 'mirror' } else { 'remote' }
$summary = New-Object System.Collections.Generic.List[object]
$warnings = $false

for ($cursor = $start; $cursor -le $end; $cursor = $stop.AddDays(1)) {
    $stop = $cursor.AddDays($WindowDays - 1)
    if ($stop -gt $end) { $stop = $end }
    $from = $cursor.ToString('yyyy-MM-dd')
    $to = $stop.ToString('yyyy-MM-dd')
    $state = Join-Path $stateRoot "${from}_${to}_${mode}_${mirror}.json"
    $childArgs = @($script, '--start-date', $from, '--end-date', $to,
        '--state-file', $state, '--max-games', "$MaxGames", '--max-requests', "$MaxRequests")
    foreach ($slot in $ReportSlot) { $childArgs += @('--report-slot', $slot) }
    if ($Execute) { $childArgs += '--execute' }
    if ($MirrorLocal) { $childArgs += '--mirror-local' }
    if (-not $Run) {
        $summary.Add([pscustomobject]@{start_date=$from; end_date=$to; mode=$mode;
            mirror_local=[bool]$MirrorLocal; state_file=$state; python=$python; arguments=$childArgs})
        continue
    }
    $finished = $false
    for ($batch = 1; $batch -le $MaxBatchesPerWindow; $batch++) {
        $invocation = @($childArgs)
        if (Test-Path -LiteralPath $state) { $invocation += '--resume' }
        # Windows PowerShell treats native stderr as ErrorRecord. Capture it, then
        # use the native exit code and structured JSON rather than shell policy.
        $oldPreference = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        try {
            $output = @(& $python @invocation 2>&1)
            $code = $LASTEXITCODE
        } finally { $ErrorActionPreference = $oldPreference }
        $rendered = ($output | ForEach-Object { $_.ToString() }) -join "`n"
        try { $receipt = $rendered | ConvertFrom-Json } catch {
            throw "Non-JSON child output for ${from}..${to}; stop and inspect $state (exit $code)."
        }
        $properties = @($receipt.PSObject.Properties.Name)
        if ($properties -contains 'error') {
            if ($receipt.error.message -eq 'no competitive games in the requested stats/date scope') {
                $summary.Add([pscustomobject]@{start_date=$from; end_date=$to; status='no_observed_games'; batches=$batch})
                $finished = $true
                break
            }
            throw "Recovery failed for ${from}..${to}: $($receipt.error.message)"
        }
        if ($code -notin @(0, 3, 4) -or $properties -notcontains 'scope_accounted') {
            throw "Unexpected child contract for ${from}..${to} (exit $code)."
        }
        Write-Host "${from}..${to} batch $batch exit=$code accounted=$($receipt.scope_accounted) blocked=$($receipt.blocked_or_errors)"
        if ($receipt.scope_accounted -and -not $receipt.remaining_due_to_budget) {
            $status = if ($receipt.blocked_or_errors) { 'accounted_with_gaps' } else { 'accounted' }
            if ($receipt.blocked_or_errors) { $warnings = $true }
            $summary.Add([pscustomobject]@{start_date=$from; end_date=$to; status=$status; batches=$batch; state_file=$state})
            $finished = $true
            break
        }
    }
    if (-not $finished) {
        throw "Batch ceiling reached for ${from}..${to}; state is resumable at $state. No later window started."
    }
}
# This is an accounting summary, never a claim that all injury history is usable.
$result = [pscustomobject]@{mode=$(if ($Run) {$mode} else {'plan_only'});
    db_execution_requested=[bool]($Run -and $Execute); completeness_claimed=$false;
    windows=@($summary.ToArray()); has_gaps=$warnings}
$result | ConvertTo-Json -Depth 8
if ($warnings) { exit 4 }
exit 0
