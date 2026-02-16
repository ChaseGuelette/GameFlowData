@echo off
REM ============================================================
REM GameFlowData Discord Bot Startup Script
REM ============================================================
REM
REM This script starts the Discord bot for GameFlowData.
REM
REM For Windows Task Scheduler:
REM   1. Open Task Scheduler
REM   2. Create Basic Task -> Name: "GameFlow-DiscordBot"
REM   3. Trigger: "When I log on" (or "At startup" for always-on)
REM   4. Action: Start a program
REM   5. Program/script: C:\Users\Chase\Projects\GameFlowData\scripts\run_discord_bot.bat
REM   6. Start in: C:\Users\Chase\Projects\GameFlowData
REM
REM To run manually:
REM   cd C:\Users\Chase\Projects\GameFlowData
REM   scripts\run_discord_bot.bat
REM
REM ============================================================

REM Change to project directory
cd /d C:\Users\Chase\Projects\GameFlowData

REM Set PYTHONPATH for imports
set PYTHONPATH=C:\Users\Chase\Projects\GameFlowData

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Run the bot
python src/discord_bot/run_bot.py

REM Keep window open if there's an error (useful for debugging)
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo Bot exited with error code %ERRORLEVEL%
    pause
)
