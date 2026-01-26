# Daily Predictions Automation Setup

## Overview

This automation runs NBA predictions every day at 3:00 PM ET using historical data from the database.

## Files

- `daily_predictions.py` - Main prediction script
- `run_daily_predictions.bat` - Windows batch file wrapper
- `SETUP_DAILY_AUTOMATION.md` - This setup guide

## Output Files

Generated in `outputs/` directory:
- `predictions_YYYY-MM-DD.json` - Machine-readable predictions
- `predictions_YYYY-MM-DD.txt` - Human-readable formatted picks
- `predictions_YYYY-MM-DD.csv` - Spreadsheet format for analysis

## Manual Usage

### Run for today:
```bash
python scripts/daily_predictions.py
```

### Run for specific date:
```bash
python scripts/daily_predictions.py --date 2026-01-25
```

### With confidence filter (75%+ picks only):
```bash
python scripts/daily_predictions.py --min-confidence 0.75
```

### With Discord webhook:
```bash
python scripts/daily_predictions.py --discord-webhook https://discord.com/api/webhooks/...
```

### Combined (filtered picks to Discord):
```bash
python scripts/daily_predictions.py --min-confidence 0.75 --discord-webhook YOUR_WEBHOOK_URL
```

### Custom output directory:
```bash
python scripts/daily_predictions.py --output-dir custom/path
```

## Windows Task Scheduler Setup

### Method 1: Using GUI

1. Open Task Scheduler (search "Task Scheduler" in Start menu)

2. Click "Create Basic Task..." in right panel

3. Name: `Axiom Daily Predictions`
   Description: `Generate NBA predictions at 3pm ET daily`

4. Trigger: Daily
   - Start: Today's date
   - Recur every: 1 days
   - Time: 3:00 PM (15:00)

5. Action: Start a program
   - Program/script: `C:\Users\Hudak\projects\axiom\scripts\run_daily_predictions.bat`
   - Start in: `C:\Users\Hudak\projects\axiom`

6. Click Finish

### Method 2: Using Command Line

Run this in PowerShell (as Administrator):

```powershell
$action = New-ScheduledTaskAction -Execute "C:\Users\Hudak\projects\axiom\scripts\run_daily_predictions.bat" -WorkingDirectory "C:\Users\Hudak\projects\axiom"
$trigger = New-ScheduledTaskTrigger -Daily -At 3pm
Register-ScheduledTask -Action $action -Trigger $trigger -TaskName "Axiom Daily Predictions" -Description "Generate NBA predictions at 3pm ET daily"
```

### Method 3: Import XML

Save this as `task.xml` and import via Task Scheduler GUI:

```xml
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>2026-01-24T15:00:00</StartBoundary>
      <ExecutionTimeLimit>PT1H</ExecutionTimeLimit>
      <Enabled>true</Enabled>
      <ScheduleByDay>
        <DaysInterval>1</DaysInterval>
      </ScheduleByDay>
    </CalendarTrigger>
  </Triggers>
  <Actions Context="Author">
    <Exec>
      <Command>C:\Users\Hudak\projects\axiom\scripts\run_daily_predictions.bat</Command>
      <WorkingDirectory>C:\Users\Hudak\projects\axiom</WorkingDirectory>
    </Exec>
  </Actions>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <ExecutionTimeLimit>PT1H</ExecutionTimeLimit>
    <Priority>7</Priority>
  </Settings>
</Task>
```

## Testing the Automation

### Test manually:
```bash
cd C:\Users\Hudak\projects\axiom
scripts\run_daily_predictions.bat
```

### Check output:
```bash
type outputs\predictions_2026-01-24.txt
```

### View logs:
```bash
type logs\daily_predictions.log
```

## Troubleshooting

### Issue: Script doesn't run
- Check Task Scheduler History tab
- Ensure Python is in PATH
- Check virtual environment is activated in batch file

### Issue: No output files
- Check `outputs/` directory exists
- Check database path in `.env` file
- Run script manually to see errors

### Issue: Wrong predictions
- Check date parameter
- Ensure database has recent data
- Verify team names match database

## Next Steps

After automation is running:
1. Add Discord webhook integration
2. Add Vegas lines comparison
3. Track accuracy in database
4. Create weekly summary reports
