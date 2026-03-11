# GameFlowData - Windows Task Scheduler Setup
# ============================================
# Run this script as Administrator to set up scheduled tasks
# PowerShell: .\scripts\setup_windows_tasks.ps1

param(
    [switch]$Remove,
    [switch]$DryRun
)

$ProjectDir = "C:\Users\Chase\Projects\GameFlowData"
$ScriptsDir = "$ProjectDir\scripts"

# Task definitions
$Tasks = @(
    @{
        Name = "GameFlowData_DailyStats"
        Description = "Daily Stats Job - Scrapes NBA game results and runs processing pipeline"
        Script = "$ScriptsDir\run_daily_stats.bat"
        Time = "06:00"  # 6 AM ET
        Repeat = $false
    },
    @{
        Name = "GameFlowData_Lines_Noon"
        Description = "Lines Job (Noon) - Scrapes player props and injuries"
        Script = "$ScriptsDir\run_lines.bat"
        Time = "12:00"  # 12 PM ET
        Repeat = $false
    },
    @{
        Name = "GameFlowData_Lines_4PM"
        Description = "Lines Job (4 PM) - Afternoon lines update"
        Script = "$ScriptsDir\run_lines.bat"
        Time = "16:00"  # 4 PM ET
        Repeat = $false
    },
    @{
        Name = "GameFlowData_Lines_6PM"
        Description = "Lines Job (6 PM) - Final lines before games"
        Script = "$ScriptsDir\run_lines.bat"
        Time = "18:00"  # 6 PM ET
        Repeat = $false
    },
    @{
        Name = "GameFlowData_Inference"
        Description = "Inference Job - Generates predictions for today's games"
        Script = "$ScriptsDir\run_inference.bat"
        Time = "18:30"  # 6:30 PM ET
        Repeat = $false
    },
    @{
        Name = "GameFlowData_AdvancedScraper"
        Description = "Advanced Stats Scraper - Scrapes advanced box scores from stats.nba.com (local only)"
        Script = "$ScriptsDir\run_advanced_scraper.bat"
        Time = "09:00"  # 9 AM ET
        Repeat = $false
    }
)

function Remove-Tasks {
    Write-Host "`nRemoving GameFlowData scheduled tasks..." -ForegroundColor Yellow

    foreach ($task in $Tasks) {
        $taskName = $task.Name
        $existingTask = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue

        if ($existingTask) {
            if ($DryRun) {
                Write-Host "  [DRY RUN] Would remove: $taskName"
            } else {
                Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
                Write-Host "  Removed: $taskName" -ForegroundColor Green
            }
        } else {
            Write-Host "  Not found: $taskName" -ForegroundColor Gray
        }
    }
}

function Create-Tasks {
    Write-Host "`nCreating GameFlowData scheduled tasks..." -ForegroundColor Cyan

    foreach ($task in $Tasks) {
        $taskName = $task.Name
        $description = $task.Description
        $script = $task.Script
        $time = $task.Time

        Write-Host "`n  Task: $taskName"
        Write-Host "    Time: $time"
        Write-Host "    Script: $script"

        if ($DryRun) {
            Write-Host "    [DRY RUN] Would create task" -ForegroundColor Yellow
            continue
        }

        # Check if script exists
        if (!(Test-Path $script)) {
            Write-Host "    ERROR: Script not found!" -ForegroundColor Red
            continue
        }

        # Remove existing task if present
        $existingTask = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
        if ($existingTask) {
            Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
            Write-Host "    Replaced existing task" -ForegroundColor Yellow
        }

        # Create trigger (daily at specified time)
        $trigger = New-ScheduledTaskTrigger -Daily -At $time

        # Create action
        $action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument "/c `"$script`"" -WorkingDirectory $ProjectDir

        # Create settings
        $settings = New-ScheduledTaskSettingsSet `
            -AllowStartIfOnBatteries `
            -DontStopIfGoingOnBatteries `
            -StartWhenAvailable `
            -ExecutionTimeLimit (New-TimeSpan -Hours 2)

        # Register task
        try {
            Register-ScheduledTask `
                -TaskName $taskName `
                -Description $description `
                -Trigger $trigger `
                -Action $action `
                -Settings $settings `
                -RunLevel Highest | Out-Null

            Write-Host "    Created successfully" -ForegroundColor Green
        } catch {
            Write-Host "    ERROR: $($_.Exception.Message)" -ForegroundColor Red
        }
    }
}

# Main
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "GameFlowData Windows Task Scheduler Setup" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan

if ($Remove) {
    Remove-Tasks
} else {
    Create-Tasks
}

Write-Host "`n================================================" -ForegroundColor Cyan
Write-Host "To view tasks: Get-ScheduledTask -TaskName 'GameFlowData*' | Format-Table -AutoSize" -ForegroundColor Gray
Write-Host "To run a task: Start-ScheduledTask -TaskName 'GameFlowData_DailyStats'" -ForegroundColor Gray
Write-Host "================================================" -ForegroundColor Cyan
