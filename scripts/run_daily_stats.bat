@echo off
REM GameFlowData - Basic Daily Stats (advanced chain has a separate owner)
REM Run once daily (6 AM ET) after previous night's games are final
REM ================================================================

setlocal EnableDelayedExpansion

REM Set project paths
set PROJECT_DIR=C:\Users\Chase\Projects\GameFlowData
set PYTHONPATH=%PROJECT_DIR%
set LOG_FILE=%PROJECT_DIR%\logs\daily_stats.log

REM Change to project directory
cd /d %PROJECT_DIR%
if errorlevel 1 (
    echo [%date% %time%] ERROR: Failed to change to project directory >> %LOG_FILE%
    exit /b 1
)

REM Activate virtual environment
call venv\Scripts\activate
if errorlevel 1 (
    echo [%date% %time%] ERROR: Failed to activate virtual environment >> %LOG_FILE%
    exit /b 1
)

REM Run the job
echo [%date% %time%] Starting Daily Stats Job... >> %LOG_FILE%
set "PYTHONUTF8=1"
python src/orchestration/daily_stats_job.py --basic-only --skip-linker
set EXIT_CODE=%errorlevel%

if %EXIT_CODE% EQU 0 (
    echo [%date% %time%] Daily Stats Job completed successfully >> %LOG_FILE%
) else (
    echo [%date% %time%] Daily Stats Job FAILED with exit code %EXIT_CODE% >> %LOG_FILE%
)

exit /b %EXIT_CODE%
