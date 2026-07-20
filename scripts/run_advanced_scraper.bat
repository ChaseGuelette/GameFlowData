@echo off
REM Morning local jobs - run after Railway's 9:00/9:20 AM source jobs
REM 1. Advanced Stats Scraper (local only — stats.nba.com blocks Railway)
REM 2. DB sync (Supabase -> local Postgres, incremental)

cd /d C:\Users\Chase\Projects\GameFlowData

if not exist logs\advanced mkdir logs\advanced
if not exist logs\sync mkdir logs\sync

for /f "tokens=1-4 delims=/ " %%a in ('date /t') do set datestamp=%%d-%%b-%%c
set SYNCLOG=logs\sync\sync_%datestamp%.log
set LOGFILE=logs\advanced\advanced_scraper_%datestamp%.log

call venv\Scripts\activate.bat

set RUN_EXIT=0

echo === Advanced Stats Scraper started at %date% %time% === >> %LOGFILE% 2>&1
python src/scrapers/nba_unified_scraper.py --no-proxy --skip-team --skip-traditional >> %LOGFILE% 2>&1
set ADV_EXIT=%ERRORLEVEL%
echo === Advanced Stats Scraper finished at %date% %time% with exit code %ADV_EXIT% === >> %LOGFILE% 2>&1

echo === DB sync started at %date% %time% === >> %SYNCLOG% 2>&1
python scripts\sync_local_db.py >> %SYNCLOG% 2>&1
set SYNC_EXIT=%ERRORLEVEL%
echo === DB sync finished at %date% %time% with exit code %SYNC_EXIT% === >> %SYNCLOG% 2>&1

echo === Advanced Exit=%ADV_EXIT% / Sync Exit=%SYNC_EXIT% === >> %SYNCLOG% 2>&1
echo === Advanced Exit=%ADV_EXIT% / Sync Exit=%SYNC_EXIT% === >> %LOGFILE% 2>&1

if not "%ADV_EXIT%"=="0" set RUN_EXIT=%ADV_EXIT%
if not "%SYNC_EXIT%"=="0" set RUN_EXIT=%SYNC_EXIT%

exit /b %RUN_EXIT%
