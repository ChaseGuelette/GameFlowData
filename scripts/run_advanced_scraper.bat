@echo off
REM Advanced Stats Scraper - Daily 9 AM job
REM Runs from local machine to avoid stats.nba.com rate limiting on Railway

cd /d C:\Users\Chase\Projects\GameFlowData
call venv\Scripts\activate.bat
python src/scrapers/nba_unified_scraper.py --no-proxy --skip-team --skip-traditional
