@echo off
REM Morning local jobs - Daily 9 AM job
REM 1. DB sync (Supabase -> local Postgres, incremental)
REM 2. Advanced Stats Scraper (local only — stats.nba.com blocks Railway)

cd /d C:\Users\Chase\Projects\GameFlowData

if not exist logs\advanced mkdir logs\advanced
if not exist logs\sync mkdir logs\sync

for /f "tokens=1-4 delims=/ " %%a in ('date /t') do set datestamp=%%d-%%b-%%c
set SYNCLOG=logs\sync\sync_%datestamp%.log
set LOGFILE=logs\advanced\advanced_scraper_%datestamp%.log

call venv\Scripts\activate.bat

echo === DB sync started at %date% %time% === >> %SYNCLOG% 2>&1
python scripts\sync_local_db.py >> %SYNCLOG% 2>&1
echo === DB sync finished at %date% %time% with exit code %ERRORLEVEL% === >> %SYNCLOG% 2>&1

echo === Advanced Stats Scraper started at %date% %time% === >> %LOGFILE% 2>&1
python src/scrapers/nba_unified_scraper.py --no-proxy --skip-team --skip-traditional >> %LOGFILE% 2>&1
echo === Finished at %date% %time% with exit code %ERRORLEVEL% === >> %LOGFILE% 2>&1
