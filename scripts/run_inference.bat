@echo off
REM GameFlowData - Inference Job (Daily Predictions)
REM Run once daily (6:30 PM ET) after lines_job, before games start
REM ================================================================

setlocal EnableDelayedExpansion

REM Set project paths
set PROJECT_DIR=C:\Users\Chase\Projects\GameFlowData
set PYTHONPATH=%PROJECT_DIR%
set LOG_FILE=%PROJECT_DIR%\logs\inference.log

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

REM Run the job (with optional date argument)
echo [%date% %time%] Starting Inference Job... >> %LOG_FILE%
if "%1"=="" (
    python src/orchestration/inference_job.py
) else (
    python src/orchestration/inference_job.py --date %1
)
set EXIT_CODE=%errorlevel%

if %EXIT_CODE% EQU 0 (
    echo [%date% %time%] Inference Job completed successfully >> %LOG_FILE%
) else (
    echo [%date% %time%] Inference Job FAILED with exit code %EXIT_CODE% >> %LOG_FILE%
)

exit /b %EXIT_CODE%
