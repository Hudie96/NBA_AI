@echo off
REM AXIOM Post-Game Results Collection
REM Schedule: 12:30 AM ET daily via Task Scheduler
REM Task: "Axiom Post-Game Results"

cd /d C:\Users\Hudak\projects\axiom
call venv\Scripts\activate.bat

echo [%date% %time%] Starting post-game results collection...
python scripts\update_boxscores.py 2>&1 >> logs\post_game_%date:~-4,4%%date:~-7,2%%date:~-10,2%.log
python scripts\auto_results.py 2>&1 >> logs\post_game_%date:~-4,4%%date:~-7,2%%date:~-10,2%.log
echo [%date% %time%] Post-game collection complete.

REM Task Scheduler Setup (run in PowerShell as Admin):
REM $action = New-ScheduledTaskAction -Execute "C:\Users\Hudak\projects\axiom\scripts\run_post_game.bat"
REM $trigger = New-ScheduledTaskTrigger -Daily -At 12:30AM
REM Register-ScheduledTask -Action $action -Trigger $trigger -TaskName "Axiom Post-Game Results" -Description "Collect game results and update performance"
