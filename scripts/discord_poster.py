"""
Discord Auto-Poster

Posts daily picks and results to a Discord webhook.
Reads from picks CSV and results.csv, formats into Discord embeds.

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

RESULTS_CSV = PROJECT_ROOT / 'data' / 'results.csv'
PREDICTIONS_DIR = PROJECT_ROOT / 'outputs' / 'predictions'
PERFORMANCE_DIR = PROJECT_ROOT / 'outputs' / 'performance'

# Tier colors for embeds
TIER_COLORS = {
    'PLATINUM': 0x7B68EE,  # Medium slate blue
    'GOLD': 0xFFD700,      # Gold
    'SILVER': 0xC0C0C0,    # Silver
    'S_TIER': 0x00FF7F,    # Spring green
    'FREE': 0x2ECC71,      # Green
    'PREMIUM': 0xF39C12,   # Orange
    'RESULTS': 0x3498DB,   # Blue
    'PERFORMANCE': 0x9B59B6,  # Purple
}


def send_webhook(webhook_url, embeds, username="Axiom Sports"):
    """Send embeds to Discord webhook."""
    if not webhook_url:
        print("[WARN] No DISCORD_WEBHOOK_URL set, printing instead")
        for embed in embeds:
            print(f"\n--- {embed.get('title', 'Embed')} ---")
            for field in embed.get('fields', []):
                print(f"  {field['name']}: {field['value']}")
        return False

    payload = {
        "username": username,
        "embeds": embeds[:10]  # Discord limit: 10 embeds per message
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        print(f"[SUCCESS] Posted {len(embeds)} embed(s) to Discord")
        return True
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to post to Discord: {e}")
        return False


def post_daily_picks(target_date, webhook_url):
    """Post daily picks to Discord. Free picks (SILVER) shown in full, premium teased."""
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

    # Separate by tier
    spreads = [r for r in rows if r.get('bet_type') == 'SPREAD' and r.get('tier') != 'SKIP']
    props = [r for r in rows if r.get('bet_type') == 'PROP']

    silver_spreads = [r for r in spreads if r.get('tier') == 'SILVER']
    silver_props = [r for r in props if r.get('tier') == 'SILVER']
    premium_spreads = [r for r in spreads if r.get('tier') in ('GOLD', 'PLATINUM')]
    premium_props = [r for r in props if r.get('tier') in ('GOLD', 'PLATINUM')]

    embeds = []

    # Free picks embed (SILVER tier - full details)
    if silver_spreads or silver_props:
        fields = []
        for s in silver_spreads[:3]:
            fields.append({
                "name": f"SPREAD | {s['game']}",
                "value": f"**{s['pick']}**\nEdge: {s.get('edge', 'N/A')}",
                "inline": True
            })
        for p in silver_props[:5]:
            fields.append({
                "name": f"PROP | {p.get('player', '')}",
                "value": f"**{p['pick']}**\nL10: {p.get('l10_avg', '')} | Edge: {p.get('edge', '')}",
                "inline": True
            })

        embeds.append({
            "title": f"FREE PICKS - {target_date}",
            "description": "SILVER tier picks (full details)",
            "color": TIER_COLORS['FREE'],
            "fields": fields,
            "footer": {"text": "AXIOM | Data-Driven NBA Picks"},
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

    # Premium teaser embed (GOLD/PLATINUM - counts only)
    if premium_spreads or premium_props:
        tease_lines = []
        if premium_spreads:
            tease_lines.append(f"**{len(premium_spreads)} SPREAD pick(s)** rated GOLD/PLATINUM")
        if premium_props:
            tease_lines.append(f"**{len(premium_props)} PROP pick(s)** rated GOLD/PLATINUM")
            # Tease player names without picks
            for p in premium_props[:3]:
                tier = p.get('tier', '')
                tease_lines.append(f"  {p.get('player', '')} ({tier}) - Edge: {p.get('edge', '')}")

        embeds.append({
            "title": f"PREMIUM PICKS AVAILABLE - {target_date}",
            "description": "\n".join(tease_lines),
            "color": TIER_COLORS['PREMIUM'],
            "footer": {"text": "Full picks available for premium members"},
            "timestamp": datetime.now(timezone.utc).isoformat()
        })

    if not embeds:
        print("No picks to post (no SILVER or higher tier picks)")
        return False

    return send_webhook(webhook_url, embeds)


def post_results_update(target_date, webhook_url):
    """Post results for a specific date to Discord."""
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

    fields = []
    for r in results:
        emoji = "W" if r['result'] == 'W' else "L"
        bet_type = r.get('bet_type', 'PROP')
        actual = r.get('actual', '')
        fields.append({
            "name": f"{emoji} {bet_type} | {r.get('pick', '')}",
            "value": f"Actual: {actual}" if actual else "Result recorded",
            "inline": True
        })

    color = 0x2ECC71 if wins > losses else 0xE74C3C if losses > wins else 0xF39C12

    embed = {
        "title": f"RESULTS - {target_date}",
        "description": f"**{wins}-{losses}** on the day",
        "color": color,
        "fields": fields[:25],  # Discord limit
        "footer": {"text": "AXIOM | Track Record"},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    return send_webhook(webhook_url, [embed])


def post_performance_summary(webhook_url):
    """Post running performance summary to Discord."""
    if not RESULTS_CSV.exists():
        print("No results.csv found")
        return False

    with open(RESULTS_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        results = [r for r in reader if r.get('result') in ['W', 'L']]

    if not results:
        print("No resolved results found")
        return False

    # Calculate totals
    spread_w = sum(1 for r in results if r.get('bet_type') == 'SPREAD' and r['result'] == 'W')
    spread_l = sum(1 for r in results if r.get('bet_type') == 'SPREAD' and r['result'] == 'L')
    prop_w = sum(1 for r in results if r.get('bet_type', 'PROP') == 'PROP' and r['result'] == 'W')
    prop_l = sum(1 for r in results if r.get('bet_type', 'PROP') == 'PROP' and r['result'] == 'L')

    total_w = spread_w + prop_w
    total_l = spread_l + prop_l
    total_pct = (total_w / (total_w + total_l) * 100) if (total_w + total_l) > 0 else 0

    # ROI (-110 juice)
    profit = (total_w * 100) - (total_l * 110)
    risked = (total_w + total_l) * 110
    roi = (profit / risked * 100) if risked > 0 else 0

    fields = [
        {"name": "Overall", "value": f"**{total_w}-{total_l}** ({total_pct:.1f}%)", "inline": True},
        {"name": "ROI", "value": f"**{roi:+.1f}%**", "inline": True},
        {"name": "Spreads", "value": f"{spread_w}-{spread_l}", "inline": True},
        {"name": "Props", "value": f"{prop_w}-{prop_l}", "inline": True},
    ]

    embed = {
        "title": "AXIOM PERFORMANCE",
        "description": f"Running record across {total_w + total_l} graded picks",
        "color": TIER_COLORS['PERFORMANCE'],
        "fields": fields,
        "footer": {"text": "AXIOM | Data-Driven NBA Picks"},
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    return send_webhook(webhook_url, [embed])


def main():
    parser = argparse.ArgumentParser(description='Post to Discord')
    parser.add_argument('--picks', action='store_true', help='Post daily picks')
    parser.add_argument('--results', action='store_true', help='Post results')
    parser.add_argument('--performance', action='store_true', help='Post performance summary')
    parser.add_argument('--date', type=str, default=None, help='Target date (YYYY-MM-DD)')
    args = parser.parse_args()

    target_date = args.date or date.today().isoformat()
    webhook_url = os.getenv('DISCORD_WEBHOOK_URL')

    if not webhook_url:
        print("[INFO] DISCORD_WEBHOOK_URL not set - will print output instead of posting")

    if args.picks:
        post_daily_picks(target_date, webhook_url)
    if args.results:
        post_results_update(target_date, webhook_url)
    if args.performance:
        post_performance_summary(webhook_url)

    if not (args.picks or args.results or args.performance):
        print("Specify --picks, --results, or --performance")
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())
