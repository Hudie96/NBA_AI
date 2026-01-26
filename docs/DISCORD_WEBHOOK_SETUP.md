# Discord Webhook Integration Guide

## Overview

Automatically post your daily NBA predictions to a Discord channel using webhooks.

## Features

- Posts top picks filtered by confidence threshold
- Rich embed formatting with game details
- Shows predicted scores, totals, and confidence levels
- Configurable confidence filter (only post high-confidence picks)

## Setup Discord Webhook

### Step 1: Create Webhook in Discord

1. Open your Discord server
2. Right-click the channel where you want predictions posted
3. Click **Edit Channel** (or Channel Settings)
4. Go to **Integrations** tab
5. Click **Create Webhook** (or View Webhooks if exists)
6. Click **New Webhook**
7. Name it: `Axiom Sports` (or whatever you prefer)
8. Copy the **Webhook URL** (looks like: `https://discord.com/api/webhooks/1234567890/abc...`)

### Step 2: Configure Webhook URL

**Option A: Environment Variable (Recommended)**

Add to `.env` file:
```bash
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/YOUR_WEBHOOK_ID/YOUR_WEBHOOK_TOKEN
```

**Option B: Command Line Argument**

Pass webhook URL directly:
```bash
python scripts/daily_predictions.py --discord-webhook https://discord.com/api/webhooks/...
```

## Usage Examples

### Basic: Post all picks to Discord
```bash
python scripts/daily_predictions.py --discord-webhook YOUR_WEBHOOK_URL
```

### With Confidence Filter: Only post 75%+ confidence picks
```bash
python scripts/daily_predictions.py --min-confidence 0.75 --discord-webhook YOUR_WEBHOOK_URL
```

### Using Environment Variable
```bash
# Set in .env file first
python scripts/daily_predictions.py --min-confidence 0.80
```

### High Confidence Only (85%+)
```bash
python scripts/daily_predictions.py --min-confidence 0.85 --discord-webhook YOUR_WEBHOOK_URL
```

## Discord Message Format

The webhook posts a rich embed with:

```
🏀 AXIOM NBA PICKS - 2026-01-24

1. SAC @ CLE
Pick: CLE -11.0
Score: 112-123
Total: 235
Confidence: 95.9%

2. IND @ OKC
Pick: OKC -8.2
Score: 113-122
Total: 235
Confidence: 91.2%

...

Confidence threshold: 75% | 8/11 picks
```

## Automation with Discord

### Update Daily Predictions Batch File

Edit `scripts/run_daily_predictions.bat`:

```batch
@echo off
cd /d "C:\Users\Hudak\projects\axiom"
call venv\Scripts\activate.bat

REM Post picks with 75%+ confidence to Discord
python scripts\daily_predictions.py --min-confidence 0.75 >> logs\daily_predictions.log 2>&1

call deactivate
```

### Windows Task Scheduler

The scheduled task will automatically:
1. Generate predictions at 3pm ET daily
2. Filter by confidence threshold (if set)
3. Post to Discord (if webhook configured in .env)
4. Save all outputs to files

## Testing

### Test without posting (dry run)
```bash
# Generate predictions without Discord
python scripts/daily_predictions.py --min-confidence 0.75
```

### Test Discord webhook
```bash
# Post to Discord
python scripts/daily_predictions.py --date 2026-01-24 --min-confidence 0.75 --discord-webhook YOUR_WEBHOOK_URL
```

### Verify in Discord
Check your Discord channel for the posted message. Should appear within seconds.

## Confidence Threshold Guide

| Threshold | Description | Typical Picks |
|-----------|-------------|---------------|
| 0.50 (50%) | All picks | 11/11 games |
| 0.70 (70%) | Medium confidence+ | 8-10 picks |
| 0.75 (75%) | High confidence only | 6-8 picks |
| 0.80 (80%) | Very high confidence | 4-6 picks |
| 0.85 (85%) | Elite picks only | 2-4 picks |
| 0.90 (90%) | Slam dunk picks | 0-2 picks |

## Recommended Settings

### Conservative (High Win Rate)
```bash
--min-confidence 0.85
```
- Only post strongest picks
- Higher win rate expected
- Fewer picks per day

### Balanced (Good Value)
```bash
--min-confidence 0.75
```
- 6-8 picks per day
- Good confidence level
- Reasonable coverage

### Aggressive (More Action)
```bash
--min-confidence 0.65
```
- 10+ picks per day
- Lower win rate expected
- Maximum coverage

## Troubleshooting

### Webhook not posting

**Check webhook URL:**
```bash
# Test with curl (Windows Git Bash)
curl -X POST YOUR_WEBHOOK_URL \
  -H "Content-Type: application/json" \
  -d '{"content": "Test message"}'
```

**Common issues:**
- Webhook URL invalid or deleted
- Discord server permissions
- Network firewall blocking requests
- Confidence threshold too high (no picks meet it)

### "No predictions meet confidence threshold"

Lower the threshold:
```bash
python scripts/daily_predictions.py --min-confidence 0.70
```

### Rate limiting

Discord webhooks allow:
- 30 requests per minute
- Shouldn't be an issue with daily picks (1 post/day)

## Security Notes

- ⚠️ **DO NOT** commit webhook URL to git
- ✅ Keep it in `.env` file (already in .gitignore)
- ✅ Can regenerate webhook if compromised
- ⚠️ Anyone with URL can post to your channel

## Advanced: Custom Formatting

To customize the Discord message format, edit the `send_discord_webhook()` function in `scripts/daily_predictions.py`:

```python
def send_discord_webhook(webhook_url, predictions, target_date, min_confidence=0.0):
    # Customize embed colors, fields, footer, etc.
    embed = {
        "title": f"🏀 YOUR CUSTOM TITLE",
        "color": 0x1E90FF,  # Change color (hex code)
        # ... customize as needed
    }
```

## Next Steps

Once Discord integration is working:
1. Set up daily automation (Task Scheduler)
2. Monitor pick accuracy in Discord
3. Adjust confidence threshold based on results
4. Add Vegas lines comparison when available
5. Track results for ROI analysis
