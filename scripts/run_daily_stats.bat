@echo off
cd /d C:\Users\Chase\Projects\GameFlowData
call .venv\Scripts\activate
python src/orchestration/daily_stats_job.py >> logs\daily_stats.log 2>&1
