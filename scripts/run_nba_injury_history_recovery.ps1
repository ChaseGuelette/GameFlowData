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
    [switch]$MirrorLocal,
    [switch]$FromBeginning
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
$progressPath = Join-Path $stateRoot "_launcher\${StartDate}_${EndDate}_${WindowDays}_${mode}_${mirror}.json"
$resumeAt = $start

function Save-LauncherProgress([datetime]$NextDate) {
    $directory = Split-Path $progressPath -Parent
    [System.IO.Directory]::CreateDirectory($directory) | Out-Null
    $progress = [ordered]@{version=1; start_date=$StartDate; end_date=$EndDate;
        window_days=$WindowDays; mode=$mode; mirror_local=[bool]$MirrorLocal;
        report_slots=@($ReportSlot); next_date=$NextDate.ToString('yyyy-MM-dd'); has_gaps=$warnings}
    $tempPath = "$progressPath.$PID.tmp"
    [System.IO.File]::WriteAllText($tempPath, ($progress | ConvertTo-Json -Depth 4),
        (New-Object System.Text.UTF8Encoding($false)))
    Move-Item -LiteralPath $tempPath -Destination $progressPath -Force
}

if (-not $FromBeginning) {
    if (Test-Path -LiteralPath $progressPath) {
        $saved = Get-Content -LiteralPath $progressPath -Raw | ConvertFrom-Json
        if ($saved.version -ne 1 -or $saved.start_date -ne $StartDate -or $saved.end_date -ne $EndDate -or
            $saved.window_days -ne $WindowDays -or $saved.mode -ne $mode -or
            $saved.mirror_local -ne [bool]$MirrorLocal -or
            (@($saved.report_slots) -join ',') -cne (@($ReportSlot) -join ',')) {
            throw 'Launcher checkpoint scope differs; keep the original arguments or use -FromBeginning explicitly.'
        }
        $resumeAt = [datetime]::ParseExact($saved.next_date, 'yyyy-MM-dd', $culture)
        if ($resumeAt -lt $start -or $resumeAt -gt $end.AddDays(1) -or
            ($resumeAt -ne $end.AddDays(1) -and (($resumeAt - $start).Days % $WindowDays) -ne 0)) {
            throw 'Invalid launcher checkpoint position.'
        }
        $warnings = [bool]$saved.has_gaps
    } else {
        # Older runs have per-window states but no launcher cursor. Resume the
        # furthest existing window, not the first; its unfinished work is kept.
        $legacyPath = $null
        for ($probe = $start; $probe -le $end; $probe = $probe.AddDays($WindowDays)) {
            $probeEnd = $probe.AddDays($WindowDays - 1)
            if ($probeEnd -gt $end) { $probeEnd = $end }
            $candidate = Join-Path $stateRoot "$($probe.ToString('yyyy-MM-dd'))_$($probeEnd.ToString('yyyy-MM-dd'))_${mode}_${mirror}.json"
            if (Test-Path -LiteralPath $candidate) { $resumeAt = $probe; $legacyPath = $candidate }
        }
        if ($legacyPath) {
            $legacy = Get-Content -LiteralPath $legacyPath -Raw | ConvertFrom-Json
            $legacyMode = if ($Execute) { 'execute' } else { 'dry_run' }
            if ($legacy.scope.start_date -ne $resumeAt.ToString('yyyy-MM-dd') -or
                $legacy.scope.mode -ne $legacyMode -or $legacy.scope.mirror_local -ne [bool]$MirrorLocal -or
                @($legacy.scope.game_ids).Count -ne 0 -or @($legacy.scope.report_urls).Count -ne 0 -or
                (@($legacy.scope.report_slots) -join ',') -cne (@($ReportSlot) -join ',')) {
                throw 'Latest window checkpoint scope differs; do not reuse it with changed arguments.'
            }
        }
    }
}
if ($Run) {
    Save-LauncherProgress $resumeAt
    Write-Host "Resuming at $($resumeAt.ToString('yyyy-MM-dd')); earlier windows are not replayed."
}

for ($cursor = $resumeAt; $cursor -le $end; $cursor = $stop.AddDays(1)) {
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
    Save-LauncherProgress $stop.AddDays(1)
}
# This is an accounting summary, never a claim that all injury history is usable.
$result = [pscustomobject]@{mode=$(if ($Run) {$mode} else {'plan_only'});
    db_execution_requested=[bool]($Run -and $Execute); completeness_claimed=$false;
    resume_start_date=$resumeAt.ToString('yyyy-MM-dd'); earlier_windows_not_rechecked=($resumeAt -gt $start);
    windows=@($summary.ToArray()); has_gaps=$warnings}
$result | ConvertTo-Json -Depth 8
if ($warnings) { exit 4 }
exit 0
