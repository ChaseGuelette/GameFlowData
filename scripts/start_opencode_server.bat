@echo off
REM Start OpenCode headless server for GLM code generation
REM Run this once before starting a Claude Code session
REM Server listens on http://localhost:4096

cd /d C:\Users\Chase\Projects\GameFlowData

REM Load OPENROUTER_API_KEY from .env
for /f "tokens=1,* delims==" %%a in ('findstr "OPENROUTER_API_KEY" .env') do (
    set raw=%%b
)
REM Strip surrounding quotes
set OPENROUTER_API_KEY=%raw:"=%

REM Kill any existing server on port 4096
for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":4096" ^| findstr "LISTENING" 2^>nul') do (
    echo Killing existing process on port 4096 (PID %%p)
    taskkill /PID %%p /F >nul 2>&1
)

echo.
echo Starting OpenCode server on http://localhost:4096
echo Model: openrouter/z-ai/glm-5.1 (via OpenRouter)
echo Press Ctrl+C to stop
echo.

opencode serve --port 4096
