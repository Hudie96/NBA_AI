"""
Update PlayerBox with recent game boxscores.

Fetches boxscore data for completed games that are missing from PlayerBox.
Uses slower rate limiting to avoid NBA API throttling.
"""

import sqlite3
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from nba_api.stats.endpoints import BoxScoreTraditionalV3

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]


def get_missing_games(conn, days_back=30):
    """Get completed games that are missing boxscore data."""
    cutoff_date = (date.today() - timedelta(days=days_back)).isoformat()

    cur = conn.cursor()
    cur.execute('''
        SELECT g.game_id, DATE(g.date_time_utc) as game_date
        FROM Games g
        LEFT JOIN PlayerBox pb ON g.game_id = pb.game_id
        WHERE DATE(g.date_time_utc) >= ?
          AND DATE(g.date_time_utc) < DATE('now')
          AND (g.status = '3' OR g.status = 'Final')
          AND pb.game_id IS NULL
        ORDER BY g.date_time_utc
    ''', (cutoff_date,))

    return cur.fetchall()


def fetch_and_save_boxscore(conn, game_id):
    """Fetch boxscore for a single game and save to database."""
    try:
        boxscore = BoxScoreTraditionalV3(game_id=game_id, timeout=60)
        data = boxscore.get_dict()

        if 'boxScoreTraditional' not in data:
            return False

        players = data['boxScoreTraditional'].get('homeTeam', {}).get('players', [])
        players += data['boxScoreTraditional'].get('awayTeam', {}).get('players', [])

        cur = conn.cursor()
        count = 0

        for p in players:
            stats = p.get('statistics', {})
            min_str = stats.get('minutes', '0:00')

            # Skip players with 0 minutes
            if min_str == '0:00' or not min_str:
                continue

            # Convert minutes
            if ':' in str(min_str):
                parts = str(min_str).split(':')
                mins = int(parts[0]) + int(parts[1]) / 60
            else:
                mins = float(min_str) if min_str else 0

            cur.execute('''
                INSERT OR REPLACE INTO PlayerBox
                (player_id, game_id, team_id, player_name, position, min,
                 pts, reb, ast, stl, blk, tov, pf, oreb, dreb,
                 fga, fgm, fg_pct, fg3a, fg3m, fg3_pct, fta, ftm, ft_pct, plus_minus)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                p.get('personId'), game_id, p.get('teamId'),
                f"{p.get('firstName', '')} {p.get('familyName', '')}",
                p.get('position', ''), mins,
                stats.get('points', 0), stats.get('reboundsTotal', 0),
                stats.get('assists', 0), stats.get('steals', 0),
                stats.get('blocks', 0), stats.get('turnovers', 0),
                stats.get('foulsPersonal', 0), stats.get('reboundsOffensive', 0),
                stats.get('reboundsDefensive', 0), stats.get('fieldGoalsAttempted', 0),
                stats.get('fieldGoalsMade', 0), stats.get('fieldGoalsPercentage', 0),
                stats.get('threePointersAttempted', 0), stats.get('threePointersMade', 0),
                stats.get('threePointersPercentage', 0), stats.get('freeThrowsAttempted', 0),
                stats.get('freeThrowsMade', 0), stats.get('freeThrowsPercentage', 0),
                stats.get('plusMinusPoints', 0)
            ))
            count += 1

        conn.commit()
        return count

    except Exception as e:
        print(f"    Error: {str(e)[:50]}")
        return False


def main():
    print("=" * 60)
    print("  AXIOM BOXSCORE UPDATER")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    # Check current status
    cur = conn.cursor()
    cur.execute('''
        SELECT MAX(DATE(g.date_time_utc)) as max_date
        FROM PlayerBox pb
        JOIN Games g ON pb.game_id = g.game_id
    ''')
    current_max = cur.fetchone()[0]
    print(f"\nCurrent PlayerBox latest date: {current_max}")

    # Get missing games
    missing = get_missing_games(conn, days_back=30)

    if not missing:
        print("No missing boxscores found!")
        conn.close()
        return 0

    print(f"Found {len(missing)} games missing boxscores")
    print("-" * 40)

    success = 0
    failed = 0

    for game_id, game_date in missing:
        result = fetch_and_save_boxscore(conn, game_id)
        if result:
            success += 1
            if success % 10 == 0:
                print(f"  Progress: {success}/{len(missing)} ({game_date})")
        else:
            failed += 1

        # Rate limiting
        time.sleep(1.2)

    # Final status
    cur.execute('''
        SELECT MAX(DATE(g.date_time_utc)) as max_date, COUNT(DISTINCT pb.game_id)
        FROM PlayerBox pb
        JOIN Games g ON pb.game_id = g.game_id
    ''')
    new_max, total_games = cur.fetchone()

    print("-" * 40)
    print(f"Complete: {success} added, {failed} failed")
    print(f"PlayerBox now: {new_max} ({total_games} games)")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
