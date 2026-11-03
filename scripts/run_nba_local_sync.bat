@echo off
setlocal

set "REPO_ROOT=%~dp0.."
set "PYTHON=C:\Users\Chase\Projects\GameFlowData\venv\Scripts\python.exe"
set "SYNC_LOG_DIR=%REPO_ROOT%\logs\sync"
set "PROPS_LOG=%SYNC_LOG_DIR%\nba_scheduled_props.log"
set "PROPS_STATE=%SYNC_LOG_DIR%\nba_scheduled_props.json"

if not exist "%SYNC_LOG_DIR%" mkdir "%SYNC_LOG_DIR%"
if not exist "%PYTHON%" (
  echo ERROR: Python interpreter not found: "%PYTHON%" 1>&2
  exit /b 2
)

pushd "%REPO_ROOT%" || exit /b 2

echo === NBA props follow started at %date% %time% ===>>"%PROPS_LOG%"
"%PYTHON%" -X utf8 "scripts\sync_nba_props_by_id.py" --execute --follow --batch-size 10000 --max-batches 50 --replay-id-span 200000 --state-file "%PROPS_STATE%" >>"%PROPS_LOG%" 2>&1
set "PROPS_EXIT=%ERRORLEVEL%"
echo === NBA props follow finished at %date% %time% with exit code %PROPS_EXIT% ===>>"%PROPS_LOG%"
popd
exit /b %PROPS_EXIT%
