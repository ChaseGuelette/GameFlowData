@echo off
cd /d C:\Users\Chase\Projects\GameFlowData
set PYTHONPATH=C:\Users\Chase\Projects\GameFlowData
call venv\Scripts\activate
python src/orchestration/lines_job.py
