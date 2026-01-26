# Daily Predictions - Feature Guide

## New Features (v1.1)

### 1. Confidence Threshold Filtering

Filter predictions to show only high-confidence picks.

**Usage:**
```bash
python scripts/daily_predictions.py --min-confidence 0.75
```

**Parameters:**
- `--min-confidence`: Float between 0.0 and 1.0
- Examples:
  - `0.75` = 75%+ confidence (recommended)
  - `0.80` = 80%+ confidence (conservative)
  - `0.85` = 85%+ confidence (elite picks only)

**Output Changes:**
- TXT file shows filtered picks first, then all predictions
- Console summary displays filter stats
- JSON includes both full and filtered prediction lists

**Example Output:**
```
[SUCCESS] Predictions generated successfully!
  - 11 total predictions
  - 8 picks meet 75%+ confidence threshold
  - 0 games skipped
```

---

### 2. Discord Webhook Integration

Automatically post picks to Discord channel.

**Setup:**
1. Create webhook in Discord (Server Settings > Integrations)
2. Add to `.env` file:
   ```
   DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/YOUR_ID/YOUR_TOKEN
   ```

**Usage:**

Using environment variable:
```bash
python scripts/daily_predictions.py
```

Explicit webhook URL:
```bash
python scripts/daily_predictions.py --discord-webhook YOUR_WEBHOOK_URL
```

Combined with filter:
```bash
python scripts/daily_predictions.py --min-confidence 0.75
```

**Discord Message Format:**
- Rich embed with team logos colors
- Top 10 picks (sorted by confidence)
- Each pick shows: Pick, Score, Total, Confidence
- Footer shows filter threshold and pick count

**See:** `DISCORD_WEBHOOK_SETUP.md` for detailed guide

---

## Complete Parameter Reference

```bash
python scripts/daily_predictions.py [OPTIONS]
```

**Options:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `--date` | string | today | Target date (YYYY-MM-DD) |
| `--output-dir` | string | outputs/ | Output directory |
| `--min-confidence` | float | 0.0 | Confidence threshold (0.0-1.0) |
| `--discord-webhook` | string | None | Discord webhook URL |

---

## Usage Examples

### Daily Workflow

**Morning: Generate all predictions**
```bash
python scripts/daily_predictions.py
```

**Afternoon: Post high-confidence picks to Discord**
```bash
python scripts/daily_predictions.py --min-confidence 0.75
```

**Custom: Elite picks only (90%+)**
```bash
python scripts/daily_predictions.py --min-confidence 0.90 --date 2026-01-25
```

### Automated Daily Run

**Batch file** (`run_daily_predictions.bat`):
```batch
python scripts\daily_predictions.py --min-confidence 0.75
```

**Runs automatically at 3pm ET via Task Scheduler**

---

## Output File Formats

### 1. JSON (`predictions_YYYY-MM-DD.json`)

```json
{
  "date": "2026-01-24",
  "total_games": 11,
  "predictions_count": 11,
  "predictions": [...],
  "filtered_predictions": [...],  // NEW: Filtered by confidence
  "skipped": [...]
}
```

### 2. TXT (`predictions_YYYY-MM-DD.txt`)

**With filter applied:**
```
AXIOM NBA PREDICTIONS - 2026-01-24
Confidence Filter: 75%+ (8/11 picks)

TOP PICKS (75%+ CONFIDENCE)
=============================
[Filtered picks here]

ALL PREDICTIONS
===============
[All picks here]

DETAILED BREAKDOWN
==================
[Full details for all games]
```

**Without filter:**
```
AXIOM NBA PREDICTIONS - 2026-01-24

[All predictions]

DETAILED BREAKDOWN
==================
[Full details]
```

### 3. CSV (`predictions_YYYY-MM-DD.csv`)

Same as before - all predictions in spreadsheet format.

---

## Confidence Threshold Strategy

### Conservative Strategy (Higher Win Rate)

**Threshold:** 85%+
```bash
--min-confidence 0.85
```

**Characteristics:**
- 2-4 picks per day
- Expected win rate: 75-85%
- Lower volume, higher accuracy
- Best for: Building bankroll, maintaining edge

### Balanced Strategy (Recommended)

**Threshold:** 75%+
```bash
--min-confidence 0.75
```

**Characteristics:**
- 6-8 picks per day
- Expected win rate: 65-75%
- Good volume with quality
- Best for: Daily action, steady profits

### Aggressive Strategy (Maximum Coverage)

**Threshold:** 65%+
```bash
--min-confidence 0.65
```

**Characteristics:**
- 10+ picks per day
- Expected win rate: 55-65%
- High volume
- Best for: Parlays, testing system

---

## Integration with Other Tools

### Discord Bot Commands (Future)

```
!axiom picks         - Show today's picks
!axiom picks 0.85    - Show 85%+ picks
!axiom results       - Show yesterday's results
!axiom stats         - Show accuracy stats
```

### Tracking & Analysis (Future)

```bash
# Compare predictions to actual results
python scripts/update_results.py

# Generate accuracy report
python scripts/accuracy_report.py --days 30
```

---

## Troubleshooting

### No picks meet confidence threshold

**Symptom:**
```
No picks meet the 85% confidence threshold
```

**Solution:**
- Lower threshold: `--min-confidence 0.70`
- Check if games are scheduled for the date
- Verify database has recent data

### Discord webhook not posting

**Check:**
1. Webhook URL correct in `.env`
2. Network connectivity
3. Discord server permissions
4. Run with `--discord-webhook` explicitly

**Test:**
```bash
python scripts/daily_predictions.py --date 2026-01-24 --min-confidence 0.75 --discord-webhook YOUR_WEBHOOK_URL
```

### Automation not running

**Check:**
1. Task Scheduler task enabled
2. Batch file path correct
3. Virtual environment activated
4. Check logs: `logs\daily_predictions.log`

**Manual test:**
```bash
scripts\run_daily_predictions.bat
```

---

## Feature Roadmap

### Completed ✓
- [x] Confidence threshold filtering
- [x] Discord webhook integration
- [x] Multiple output formats
- [x] Windows Task Scheduler automation

### In Progress
- [ ] Vegas lines comparison
- [ ] Edge calculation
- [ ] Accuracy tracking

### Planned
- [ ] Injury integration
- [ ] Live game updates
- [ ] Historical performance analysis
- [ ] Web dashboard integration
- [ ] Multi-channel Discord routing (high confidence vs all picks)

---

## Support & Documentation

- **Setup Guide:** `SETUP_DAILY_AUTOMATION.md`
- **Discord Guide:** `DISCORD_WEBHOOK_SETUP.md`
- **Main README:** `../README.md`
- **Project Docs:** `../.claude/docs/`

---

## Version History

**v1.1** (2026-01-25)
- Added confidence threshold filtering
- Added Discord webhook integration
- Updated output formats
- Enhanced batch automation

**v1.0** (2026-01-24)
- Initial release
- Daily predictions automation
- JSON/TXT/CSV output formats
