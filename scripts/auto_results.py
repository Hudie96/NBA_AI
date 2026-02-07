"""
Auto-Results Collection

Automatically resolves pending picks in results.csv by checking ESPN API
for final scores and querying PlayerBox for prop stat lines.

Usage:
    python scripts/auto_results.py                    # Resolve all pending picks
    python scripts/auto_results.py --date 2026-02-01  # Resolve picks for specific date
    python scripts/auto_results.py --dry-run           # Show what would be updated without writing
"""
import argparse
import csv
import re
import sqlite3
import subprocess
import sys
from datetime import date
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]
RESULTS_CSV = PROJECT_ROOT / 'data' / 'results.csv'
CANONICAL_FIELDS = ['date', 'game', 'bet_type', 'pick', 'line', 'vegas_line', 'tier', 'edge', 'result', 'actual']

# ESPN to standard abbreviation mapping (same as run_daily.py)
ESPN_TO_STD = {
    'UTAH': 'UTA', 'WSH': 'WAS', 'SA': 'SAS', 'NY': 'NYK',
    'GS': 'GSW', 'NO': 'NOP', 'PHO': 'PHX', 'PHOE': 'PHX'
}

# Stat column mapping for prop resolution
STAT_MAP = {
    'PTS': ['pts'],
    'REB': ['reb'],
    'AST': ['ast'],
    'PRA': ['pts', 'reb', 'ast'],
    'PA': ['pts', 'ast'],
    'RA': ['reb', 'ast'],
    'PR': ['pts', 'reb'],
    '3PM': ['fg3m'],
}


def fetch_espn_scoreboard(target_date):
    """Fetch ESPN scoreboard for a date. Returns list of game dicts."""
    date_fmt = target_date.replace('-', '')
    url = f'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_fmt}'

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  [ERROR] ESPN API failed: {e}")
        return []

    games = []
    for event in data.get('events', []):
        competition = event.get('competitions', [{}])[0]
        status = competition.get('status', {}).get('type', {})
        is_final = status.get('completed', False)

        competitors = competition.get('competitors', [])
        home = next((c for c in competitors if c.get('homeAway') == 'home'), {})
        away = next((c for c in competitors if c.get('homeAway') == 'away'), {})

        home_abbrev = home.get('team', {}).get('abbreviation', '')
        away_abbrev = away.get('team', {}).get('abbreviation', '')
        home_abbrev = ESPN_TO_STD.get(home_abbrev, home_abbrev)
        away_abbrev = ESPN_TO_STD.get(away_abbrev, away_abbrev)

        home_score = int(home.get('score', 0)) if home.get('score') else 0
        away_score = int(away.get('score', 0)) if away.get('score') else 0

        games.append({
            'home_team': home_abbrev,
            'away_team': away_abbrev,
            'home_score': home_score,
            'away_score': away_score,
            'is_final': is_final,
        })

    return games


def extract_team_from_pick(pick_str):
    """Extract team abbreviation from a spread pick like 'BOS -17.7' or 'WAS -6.9'."""
    match = re.match(r'^([A-Z]{2,4})\s+[+-]?\d', pick_str)
    if match:
        return match.group(1)
    return None


def extract_spread_from_pick(pick_str):
    """Extract spread value from pick like 'BOS -17.7' -> -17.7."""
    match = re.match(r'^[A-Z]{2,4}\s+([+-]?\d+\.?\d*)', pick_str)
    if match:
        return float(match.group(1))
    return None


def resolve_spread(pick_str, line, espn_games):
    """
    Resolve a SPREAD pick against ESPN scores.

    Returns (result, actual) or (None, None) if game not found/not final.
    """
    team = extract_team_from_pick(pick_str)
    if not team:
        return None, None

    # Find game involving this team
    game = None
    for g in espn_games:
        if g['home_team'] == team or g['away_team'] == team:
            game = g
            break

    if not game or not game['is_final']:
        return None, None

    # Calculate margin from our picked team's perspective
    if team == game['home_team']:
        margin = game['home_score'] - game['away_score']
    else:
        margin = game['away_score'] - game['home_score']

    # The line in the CSV is the absolute spread value
    # The pick string has the sign: "BOS -17.7" means we need BOS to win by 17.7+
    spread = extract_spread_from_pick(pick_str)
    if spread is None:
        # Try using the line column
        try:
            spread = -float(line)  # line is positive, spread is negative for favorite
        except (ValueError, TypeError):
            return None, None

    # For spread picks: margin must exceed the negative spread to cover
    # "BOS -17.7" -> spread = -17.7, need margin > 17.7 to cover
    covers = margin > abs(spread)
    result = 'W' if covers else 'L'

    return result, str(margin)


def parse_prop_pick(pick_str):
    """
    Parse a prop pick string.
    'Tyler Kolek OVER 2.4 RA' -> (player_name, direction, line, stat)
    'Matas Buzelis OVER 13.1 PRA' -> (player_name, direction, line, stat)
    """
    match = re.match(r'^(.+?)\s+(OVER|UNDER)\s+(\d+\.?\d*)\s+(\w+)$', pick_str)
    if match:
        return match.group(1), match.group(2), float(match.group(3)), match.group(4)
    return None, None, None, None


def resolve_prop(pick_str, game_str, pick_date, conn, espn_games):
    """
    Resolve a PROP pick by querying PlayerBox.

    Returns (result, actual) or (None, None) if data not available.
    """
    player_name, direction, line, stat = parse_prop_pick(pick_str)
    if not player_name:
        return None, None

    if stat not in STAT_MAP:
        print(f"  [WARN] Unknown stat type: {stat} for {pick_str}")
        return None, None

    # Check if the game is final first
    # Extract opponent from game_str (either "vs OPP" or "AWAY @ HOME")
    opponent = None
    if game_str.startswith('vs '):
        opponent = game_str[3:]
    elif ' @ ' in game_str:
        parts = game_str.split(' @ ')
        opponent = parts[0]  # Could be either team

    # Check ESPN for game completion
    game_final = False
    for g in espn_games:
        if opponent and (g['home_team'] == opponent or g['away_team'] == opponent):
            game_final = g['is_final']
            break
        # Also check if player's team is in the game
        # We'll check PlayerBox anyway if we can't match by opponent

    if not game_final and opponent:
        return None, None

    # Query PlayerBox for the actual stat
    cols = STAT_MAP[stat]
    col_expr = ' + '.join(f'pb.{c}' for c in cols)

    query = f'''
        SELECT {col_expr} as stat_value
        FROM PlayerBox pb
        JOIN Games g ON pb.game_id = g.game_id
        WHERE pb.player_name = ?
          AND DATE(g.date_time_utc) = ?
        LIMIT 1
    '''

    try:
        result = conn.execute(query, (player_name, pick_date)).fetchone()
    except Exception as e:
        print(f"  [ERROR] DB query failed for {player_name}: {e}")
        return None, None

    if not result or result[0] is None:
        return None, None

    actual = result[0]

    if direction == 'OVER':
        pick_result = 'W' if actual > line else 'L'
    else:
        pick_result = 'W' if actual < line else 'L'

    return pick_result, str(int(actual) if actual == int(actual) else actual)


def run_auto_results(target_date=None, dry_run=False):
    """Main auto-results logic."""
    if not RESULTS_CSV.exists():
        print("No results.csv found")
        return 0

    # Read all rows
    with open(RESULTS_CSV, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Find pending picks
    pending = [(i, row) for i, row in enumerate(rows) if not row.get('result')]
    if target_date:
        pending = [(i, row) for i, row in pending if row['date'] == target_date]

    if not pending:
        print("No pending picks to resolve")
        return 0

    # Get unique dates from pending picks
    dates = sorted(set(row['date'] for _, row in pending))
    print(f"Resolving {len(pending)} pending picks across {len(dates)} date(s)")

    conn = sqlite3.connect(DB_PATH)
    updated = 0

    for pick_date in dates:
        print(f"\n--- {pick_date} ---")

        # Fetch ESPN scoreboard for this date
        espn_games = fetch_espn_scoreboard(pick_date)
        final_count = sum(1 for g in espn_games if g['is_final'])
        print(f"  ESPN: {len(espn_games)} games, {final_count} final")

        if not espn_games:
            continue

        date_picks = [(i, row) for i, row in pending if row['date'] == pick_date]

        for idx, row in date_picks:
            bet_type = row.get('bet_type', 'PROP')
            pick = row.get('pick', '')

            if bet_type == 'SPREAD':
                result, actual = resolve_spread(pick, row.get('line', ''), espn_games)
            elif bet_type == 'PROP':
                result, actual = resolve_prop(pick, row.get('game', ''), pick_date, conn, espn_games)
            else:
                continue

            if result:
                if dry_run:
                    print(f"  [DRY RUN] {pick} -> {result} (actual: {actual})")
                else:
                    rows[idx]['result'] = result
                    rows[idx]['actual'] = actual
                    print(f"  {result} | {pick} (actual: {actual})")
                updated += 1
            else:
                print(f"  [SKIP] {pick} - game not final or data missing")

    conn.close()

    # Write back
    if updated > 0 and not dry_run:
        with open(RESULTS_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=CANONICAL_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        print(f"\nUpdated {updated} picks in results.csv")

        # Update performance tracker
        try:
            from scripts.generate_daily_output import update_performance_tracker
            perf_result = update_performance_tracker()
            if perf_result:
                print(f"Performance tracker updated: {perf_result[0]}")
        except Exception as e:
            print(f"[WARN] Could not update performance tracker: {e}")

        # Post results to Discord if webhooks configured
        try:
            import os
            if os.getenv('DISCORD_WEBHOOK_RESULTS'):
                from scripts.discord_poster import get_webhooks, post_results_update
                webhooks = get_webhooks()
                for d in dates:
                    post_results_update(d, webhooks)
        except Exception as e:
            print(f"[WARN] Discord posting failed: {e}")
    elif dry_run:
        print(f"\n[DRY RUN] Would update {updated} picks")
    else:
        print("\nNo picks could be resolved")

    return updated


def main():
    parser = argparse.ArgumentParser(description='Auto-collect game results')
    parser.add_argument('--date', type=str, default=None, help='Target date (YYYY-MM-DD)')
    parser.add_argument('--dry-run', action='store_true', help='Show changes without writing')
    args = parser.parse_args()

    print("=" * 60)
    print("  AXIOM AUTO-RESULTS COLLECTOR")
    print("=" * 60)

    updated = run_auto_results(target_date=args.date, dry_run=args.dry_run)

    return 0 if updated >= 0 else 1


if __name__ == '__main__':
    sys.exit(main())
