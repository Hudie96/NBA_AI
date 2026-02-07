"""
Discord Auto-Poster (Tiered Channels)

Posts picks to tier-specific Discord channels:
  - Platinum channel: PLATINUM picks (full details)
  - Gold channel: GOLD picks (full details)
  - Free channel: SILVER spreads + S_TIER props (full details)
  - Results channel: Results + performance summaries

Env vars:
  DISCORD_WEBHOOK_PLATINUM, DISCORD_WEBHOOK_GOLD,
  DISCORD_WEBHOOK_FREE, DISCORD_WEBHOOK_RESULTS

Usage:
    python scripts/discord_poster.py --picks                     # Post today's picks
    python scripts/discord_poster.py --picks --date 2026-02-01   # Post picks for specific date
    python scripts/discord_poster.py --results                   # Post today's results
    python scripts/discord_poster.py --results --date 2026-02-01 # Post results for specific date
    python scripts/discord_poster.py --performance               # Post running performance
"""
import argparse
import csv
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

RESULTS_CSV = PROJECT_ROOT / 'data' / 'results.csv'
PREDICTIONS_DIR = PROJECT_ROOT / 'outputs' / 'predictions'
PERFORMANCE_DIR = PROJECT_ROOT / 'outputs' / 'performance'

# Tier colors for embeds
TIER_COLORS = {
    'PLATINUM': 0x7B68EE,
    'GOLD': 0xFFD700,
    'SILVER': 0xC0C0C0,
    'S_TIER': 0x00FF7F,
    'RESULTS': 0x3498DB,
    'PERFORMANCE': 0x9B59B6,
}


def get_webhooks():
    """Load tier-specific webhook URLs from env."""
    return {
        'PLATINUM': os.getenv('DISCORD_WEBHOOK_PLATINUM'),
        'GOLD': os.getenv('DISCORD_WEBHOOK_GOLD'),
        'FREE': os.getenv('DISCORD_WEBHOOK_FREE'),
        'RESULTS': os.getenv('DISCORD_WEBHOOK_RESULTS'),
    }


def send_webhook(webhook_url, embeds, username="Axiom Sports"):
    """Send embeds to Discord webhook."""
    if not webhook_url:
        print("[WARN] No webhook URL for this channel, printing instead")
        for embed in embeds:
            print(f"\n--- {embed.get('title', 'Embed')} ---")
            if embed.get('description'):
                print(f"  {embed['description']}")
            for field in embed.get('fields', []):
                print(f"  {field['name']}: {field['value']}")
        return False

    payload = {
        "username": username,
        "embeds": embeds[:10]
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        print(f"[SUCCESS] Posted {len(embeds)} embed(s) to Discord")
        return True
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to post to Discord: {type(e).__name__}")
        return False


def _build_spread_field(row):
    """Build embed field for a spread pick."""
    return {
        "name": f"SPREAD | {row['game']}",
        "value": f"**{row['pick']}**\nEdge: {row.get('edge', 'N/A')} | Vegas: {row.get('vegas_line', 'N/A')}",
        "inline": False
    }


def _build_prop_field(row):
    """Build embed field for a prop pick."""
    return {
        "name": f"PROP | {row.get('player', row.get('pick', ''))}",
        "value": f"**{row['pick']}**\nL10: {row.get('l10_avg', '')} | Edge: {row.get('edge', '')}",
        "inline": False
    }


def _build_embed(title, description, color, fields, footer_text="AXIOM | Data-Driven NBA Picks"):
    """Build a Discord embed dict."""
    return {
        "title": title,
        "description": description,
        "color": color,
        "fields": fields[:25],
        "footer": {"text": footer_text},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def post_daily_picks(target_date, webhooks):
    """Post daily picks to tier-specific Discord channels."""
    picks_file = PREDICTIONS_DIR / f"picks_{target_date}.csv"

    if not picks_file.exists():
        print(f"No picks file found: {picks_file}")
        return False

    with open(picks_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        print("No picks found")
        return False

    # Categorize picks
    platinum_picks = [r for r in rows if r.get('tier') == 'PLATINUM']
    gold_picks = [r for r in rows if r.get('tier') == 'GOLD']
    silver_picks = [r for r in rows if r.get('tier') in ('SILVER', 'S_TIER')]
    posted = False

    # --- PLATINUM channel ---
    if platinum_picks:
        fields = []
        for r in platinum_picks:
            if r.get('bet_type') == 'SPREAD':
                fields.append(_build_spread_field(r))
            else:
                fields.append(_build_prop_field(r))

        embed = _build_embed(
            f"PLATINUM PICKS - {target_date}",
            f"**{len(platinum_picks)} pick(s)** | 88.9% backtest win rate",
            TIER_COLORS['PLATINUM'],
            fields
        )
        print(f"\n[PLATINUM] {len(platinum_picks)} pick(s)")
        if send_webhook(webhooks.get('PLATINUM'), [embed]):
            posted = True

    # --- GOLD channel ---
    if gold_picks:
        fields = []
        for r in gold_picks:
            if r.get('bet_type') == 'SPREAD':
                fields.append(_build_spread_field(r))
            else:
                fields.append(_build_prop_field(r))

        embed = _build_embed(
            f"GOLD PICKS - {target_date}",
            f"**{len(gold_picks)} pick(s)** | 82.4% backtest win rate",
            TIER_COLORS['GOLD'],
            fields
        )
        print(f"\n[GOLD] {len(gold_picks)} pick(s)")
        if send_webhook(webhooks.get('GOLD'), [embed]):
            posted = True

    # --- FREE channel (SILVER spreads + S_TIER props) ---
    if silver_picks:
        fields = []
        for r in silver_picks:
            if r.get('bet_type') == 'SPREAD':
                fields.append(_build_spread_field(r))
            else:
                fields.append(_build_prop_field(r))

        embed = _build_embed(
            f"FREE PICKS - {target_date}",
            f"**{len(silver_picks)} pick(s)** | Full details below",
            TIER_COLORS['SILVER'],
            fields
        )
        print(f"\n[FREE] {len(silver_picks)} pick(s)")
        if send_webhook(webhooks.get('FREE'), [embed]):
            posted = True

    # --- Also post teasers to FREE channel for premium tiers ---
    premium_count = len(platinum_picks) + len(gold_picks)
    if premium_count > 0:
        tease_lines = []
        if platinum_picks:
            tease_lines.append(f"**{len(platinum_picks)} PLATINUM** pick(s)")
        if gold_picks:
            tease_lines.append(f"**{len(gold_picks)} GOLD** pick(s)")
        tease_lines.append("\nUpgrade for full access to premium picks!")

        teaser = _build_embed(
            f"PREMIUM PICKS AVAILABLE - {target_date}",
            "\n".join(tease_lines),
            0xF39C12,
            [],
            "Upgrade to Gold or Platinum for full picks"
        )
        send_webhook(webhooks.get('FREE'), [teaser])

    if not posted and not silver_picks:
        print("No picks to post today")
        return False

    # Post summary to results channel
    total = len(platinum_picks) + len(gold_picks) + len(silver_picks)
    if total > 0:
        summary = _build_embed(
            f"DAILY CARD - {target_date}",
            f"**{total} total pick(s)** posted across channels\n"
            f"Platinum: {len(platinum_picks)} | Gold: {len(gold_picks)} | Free: {len(silver_picks)}",
            TIER_COLORS['RESULTS'],
            [],
            "Check your tier channel for details"
        )
        send_webhook(webhooks.get('RESULTS'), [summary])

    return True


def post_results_update(target_date, webhooks):
    """Post results to the results channel."""
    if not RESULTS_CSV.exists():
        print("No results.csv found")
        return False

    with open(RESULTS_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        results = [r for r in reader if r['date'] == target_date and r.get('result')]

    if not results:
        print(f"No resolved results for {target_date}")
        return False

    wins = sum(1 for r in results if r['result'] == 'W')
    losses = sum(1 for r in results if r['result'] == 'L')
    pushes = sum(1 for r in results if r['result'] == 'P')

    fields = []
    for r in results:
        result = r['result']
        emoji = {"W": "W", "L": "L", "P": "P"}.get(result, "?")
        bet_type = r.get('bet_type', 'PROP')
        actual = r.get('actual', '')
        tier = r.get('tier', '')
        fields.append({
            "name": f"{emoji} {bet_type} [{tier}] | {r.get('pick', '')}",
            "value": f"Actual: {actual}" if actual else "Result recorded",
            "inline": True
        })

    color = 0x2ECC71 if wins > losses else 0xE74C3C if losses > wins else 0xF39C12
    record = f"**{wins}-{losses}**"
    if pushes:
        record += f" ({pushes}P)"

    embed = _build_embed(
        f"RESULTS - {target_date}",
        f"{record} on the day",
        color,
        fields,
        "AXIOM | Track Record"
    )

    webhook_url = webhooks.get('RESULTS')
    return send_webhook(webhook_url, [embed])


def post_performance_summary(webhooks):
    """Post running performance summary to the results channel."""
    if not RESULTS_CSV.exists():
        print("No results.csv found")
        return False

    with open(RESULTS_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        results = [r for r in reader if r.get('result') in ['W', 'L']]

    if not results:
        print("No resolved results found")
        return False

    spread_w = sum(1 for r in results if r.get('bet_type') == 'SPREAD' and r['result'] == 'W')
    spread_l = sum(1 for r in results if r.get('bet_type') == 'SPREAD' and r['result'] == 'L')
    prop_w = sum(1 for r in results if r.get('bet_type', 'PROP') == 'PROP' and r['result'] == 'W')
    prop_l = sum(1 for r in results if r.get('bet_type', 'PROP') == 'PROP' and r['result'] == 'L')

    total_w = spread_w + prop_w
    total_l = spread_l + prop_l
    total_pct = (total_w / (total_w + total_l) * 100) if (total_w + total_l) > 0 else 0

    profit = (total_w * 100) - (total_l * 110)
    risked = (total_w + total_l) * 110
    roi = (profit / risked * 100) if risked > 0 else 0

    fields = [
        {"name": "Overall", "value": f"**{total_w}-{total_l}** ({total_pct:.1f}%)", "inline": True},
        {"name": "ROI", "value": f"**{roi:+.1f}%**", "inline": True},
        {"name": "Spreads", "value": f"{spread_w}-{spread_l}", "inline": True},
        {"name": "Props", "value": f"{prop_w}-{prop_l}", "inline": True},
    ]

    embed = _build_embed(
        "AXIOM PERFORMANCE",
        f"Running record across {total_w + total_l} graded picks",
        TIER_COLORS['PERFORMANCE'],
        fields,
        "AXIOM | Data-Driven NBA Picks"
    )

    webhook_url = webhooks.get('RESULTS')
    return send_webhook(webhook_url, [embed])


def main():
    parser = argparse.ArgumentParser(description='Post to Discord (tiered channels)')
    parser.add_argument('--picks', action='store_true', help='Post daily picks')
    parser.add_argument('--results', action='store_true', help='Post results')
    parser.add_argument('--performance', action='store_true', help='Post performance summary')
    parser.add_argument('--date', type=str, default=None, help='Target date (YYYY-MM-DD)')
    args = parser.parse_args()

    target_date = args.date or date.today().isoformat()
    webhooks = get_webhooks()

    any_webhook = any(webhooks.values())
    if not any_webhook:
        print("[INFO] No Discord webhooks configured - will print output instead")

    if args.picks:
        post_daily_picks(target_date, webhooks)
    if args.results:
        post_results_update(target_date, webhooks)
    if args.performance:
        post_performance_summary(webhooks)

    if not (args.picks or args.results or args.performance):
        print("Specify --picks, --results, or --performance")
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
