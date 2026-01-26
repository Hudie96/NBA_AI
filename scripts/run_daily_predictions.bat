@echo off
REM Daily Predictions Automation Script for Windows Task Scheduler
REM Runs predictions and logs output
REM
REM Configuration:
REM - Set confidence threshold below (0.75 = 75%+ picks only)
REM - Set Discord webhook URL in .env file
REM - Logs saved to logs\daily_predictions.log

cd /d "C:\Users\Hudak\projects\axiom"

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Run predictions with 75%+ confidence filter
REM Discord webhook auto-loaded from .env if set
python scripts\daily_predictions.py --min-confidence 0.75 >> logs\daily_predictions.log 2>&1

REM Alternative: No filter (all picks)
REM python scripts\daily_predictions.py >> logs\daily_predictions.log 2>&1

REM Alternative: High confidence only (85%+)
REM python scripts\daily_predictions.py --min-confidence 0.85 >> logs\daily_predictions.log 2>&1

REM Deactivate
call deactivate
