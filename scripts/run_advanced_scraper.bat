@echo off
setlocal

set "REPO_ROOT=%~dp0.."
set "PYTHON=C:\Users\Chase\Projects\GameFlowData\venv\Scripts\python.exe"
set "LOG_DIR=%REPO_ROOT%\logs\advanced"
set "LOG_FILE=%LOG_DIR%\advanced_scraper.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%PYTHON%" (
  echo ERROR: Python interpreter not found: "%PYTHON%" 1>&2
  exit /b 2
)

pushd "%REPO_ROOT%" || exit /b 2
echo === Advanced Stats Scraper started at %date% %time% ===>>"%LOG_FILE%"
set "PROXY_ARGS=--no-proxy"
if /I "%NBA_ADVANCED_USE_PROXY%"=="true" set "PROXY_ARGS=--use-proxy"
if /I "%NBA_ADVANCED_NO_PROXY%"=="true" set "PROXY_ARGS=--no-proxy"
"%PYTHON%" -X utf8 "src\scrapers\nba_unified_scraper.py" --season current --advanced-limit 120 --request-timeout 30 --max-retries 2 --http-client curl %PROXY_ARGS% --skip-team --skip-traditional >>"%LOG_FILE%" 2>&1
set "ADV_EXIT=%ERRORLEVEL%"
echo === Advanced Stats Scraper finished at %date% %time% with exit code %ADV_EXIT% ===>>"%LOG_FILE%"
if not "%ADV_EXIT%"=="0" (
  popd
  exit /b %ADV_EXIT%
)

echo === Advanced complete; mirror sync is a separate owner-launched step ===>>"%LOG_FILE%"
popd
exit /b %ADV_EXIT%
