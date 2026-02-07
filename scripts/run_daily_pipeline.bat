@echo off
REM AXIOM Pre-Game Pipeline
REM Schedule: 3:00 PM ET daily via Task Scheduler
REM Task: "Axiom Daily Pipeline"

cd /d C:\Users\Hudak\projects\axiom
call venv\Scripts\activate.bat

echo [%date% %time%] Starting daily pipeline...
python scripts\run_daily.py 2>&1 >> logs\pipeline_%date:~-4,4%%date:~-7,2%%date:~-10,2%.log
echo [%date% %time%] Pipeline complete.

REM Task Scheduler Setup (run in PowerShell as Admin):
REM $action = New-ScheduledTaskAction -Execute "C:\Users\Hudak\projects\axiom\scripts\run_daily_pipeline.bat"
REM $trigger = New-ScheduledTaskTrigger -Daily -At 3:00PM
REM Register-ScheduledTask -Action $action -Trigger $trigger -TaskName "Axiom Daily Pipeline" -Description "Run AXIOM betting pipeline pre-game"
