@echo off
REM Advanced Stats Scraper - Daily 9 AM job
REM Runs from local machine to avoid stats.nba.com rate limiting on Railway

cd /d C:\Users\Chase\Projects\GameFlowData

REM Create logs directory if needed
if not exist logs\advanced mkdir logs\advanced

REM Timestamped log file
for /f "tokens=1-4 delims=/ " %%a in ('date /t') do set datestamp=%%d-%%b-%%c
set LOGFILE=logs\advanced\advanced_scraper_%datestamp%.log

echo === Advanced Stats Scraper started at %date% %time% === >> %LOGFILE% 2>&1
call venv\Scripts\activate.bat
python src/scrapers/nba_unified_scraper.py --no-proxy --skip-team --skip-traditional >> %LOGFILE% 2>&1
echo === Finished at %date% %time% with exit code %ERRORLEVEL% === >> %LOGFILE% 2>&1
