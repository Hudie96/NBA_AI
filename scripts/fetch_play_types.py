"""
AXIOM: Play Type Data Fetcher
Fetches play type efficiency (ISO, PnR, Post-Up, Transition, etc.)

Usage:
    python scripts/fetch_play_types.py
    python scripts/fetch_play_types.py --season 2024-25
"""

import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]

try:
    from nba_api.stats.endpoints import synergyplaytypes
    from nba_api.stats.static import teams
    NBA_API_AVAILABLE = True
except ImportError:
    NBA_API_AVAILABLE = False
    print("Warning: nba_api not installed")


def safe_print(text):
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))


def create_tables(conn):
    """Create play type tables."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS TeamPlayTypes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER,
            team_abbrev TEXT,
            team_name TEXT,
            season TEXT,
            play_type TEXT,
            gp INTEGER,
            poss_pct REAL,
            ppp REAL,
            fg_pct REAL,
            efg_pct REAL,
            turnover_pct REAL,
            score_pct REAL,
            percentile REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(team_id, season, play_type)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS PlayerPlayTypes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER,
            player_name TEXT,
            team_abbrev TEXT,
            season TEXT,
            play_type TEXT,
            gp INTEGER,
            poss_pct REAL,
            ppp REAL,
            fg_pct REAL,
            efg_pct REAL,
            turnover_pct REAL,
            score_pct REAL,
            percentile REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(player_id, season, play_type)
        )
    """)
    conn.commit()
    safe_print("Created PlayTypes tables")


def get_team_abbrev_map():
    if not NBA_API_AVAILABLE:
        return {}
    team_list = teams.get_teams()
    return {t['id']: t['abbreviation'] for t in team_list}


PLAY_TYPES = [
    'Isolation',
    'Transition',
    'PRBallHandler',
    'PRRollman',
    'Postup',
    'Spotup',
    'Handoff',
    'Cut',
    'OffScreen',
    'OffRebound'
]


def fetch_team_play_types(conn, season="2024-25"):
    """Fetch team play type data."""
    if not NBA_API_AVAILABLE:
        safe_print("SKIPPED: nba_api not available")
        return 0

    safe_print(f"Fetching team play types for {season}...")
    abbrev_map = get_team_abbrev_map()
    total = 0

    for play_type in PLAY_TYPES:
        try:
            safe_print(f"  Fetching {play_type}...")
            stats = synergyplaytypes.SynergyPlayTypes(
                season=season,
                play_type_nullable=play_type,
                type_grouping_nullable='offensive',
                player_or_team_abbreviation='T'
            )
            time.sleep(1.5)

            df = stats.get_data_frames()[0]

            for _, row in df.iterrows():
                team_id = row.get('TEAM_ID')
                conn.execute("""
                    INSERT OR REPLACE INTO TeamPlayTypes
                    (team_id, team_abbrev, team_name, season, play_type,
                     gp, poss_pct, ppp, fg_pct, efg_pct, turnover_pct,
                     score_pct, percentile, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    team_id,
                    abbrev_map.get(team_id, ''),
                    row.get('TEAM_NAME', ''),
                    season,
                    play_type,
                    row.get('GP'),
                    row.get('POSS_PCT'),
                    row.get('PPP'),
                    row.get('FG_PCT'),
                    row.get('EFG_PCT'),
                    row.get('TOV_PCT'),
                    row.get('SCORE_PCT'),
                    row.get('PERCENTILE'),
                    datetime.now().isoformat()
                ))
                total += 1

            conn.commit()

        except Exception as e:
            safe_print(f"    Error fetching {play_type}: {e}")

    safe_print(f"Saved {total} team play type records")
    return total


def fetch_player_play_types(conn, season="2024-25", top_n=100):
    """Fetch player play type data (top players by possessions)."""
    if not NBA_API_AVAILABLE:
        safe_print("SKIPPED: nba_api not available")
        return 0

    safe_print(f"Fetching player play types for {season}...")
    total = 0

    # Only fetch key play types for players
    key_play_types = ['Isolation', 'Transition', 'PRBallHandler', 'Postup', 'Spotup']

    for play_type in key_play_types:
        try:
            safe_print(f"  Fetching {play_type}...")
            stats = synergyplaytypes.SynergyPlayTypes(
                season=season,
                play_type_nullable=play_type,
                type_grouping_nullable='offensive',
                player_or_team_abbreviation='P'
            )
            time.sleep(1.5)

            df = stats.get_data_frames()[0]
            df = df.head(top_n)  # Top N players

            for _, row in df.iterrows():
                conn.execute("""
                    INSERT OR REPLACE INTO PlayerPlayTypes
                    (player_id, player_name, team_abbrev, season, play_type,
                     gp, poss_pct, ppp, fg_pct, efg_pct, turnover_pct,
                     score_pct, percentile, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row.get('PLAYER_ID'),
                    row.get('PLAYER_NAME', ''),
                    row.get('TEAM_ABBREVIATION', ''),
                    season,
                    play_type,
                    row.get('GP'),
                    row.get('POSS_PCT'),
                    row.get('PPP'),
                    row.get('FG_PCT'),
                    row.get('EFG_PCT'),
                    row.get('TOV_PCT'),
                    row.get('SCORE_PCT'),
                    row.get('PERCENTILE'),
                    datetime.now().isoformat()
                ))
                total += 1

            conn.commit()

        except Exception as e:
            safe_print(f"    Error fetching {play_type}: {e}")

    safe_print(f"Saved {total} player play type records")
    return total


def display_insights(conn, season="2024-25"):
    """Display play type insights."""
    safe_print("\n" + "=" * 60)
    safe_print(f"PLAY TYPE INSIGHTS - {season}")
    safe_print("=" * 60)

    # Best transition teams
    safe_print("\nBEST TRANSITION TEAMS (PPP):")
    trans = conn.execute("""
        SELECT team_abbrev, ppp, poss_pct, percentile
        FROM TeamPlayTypes
        WHERE season = ? AND play_type = 'Transition'
        ORDER BY ppp DESC LIMIT 10
    """, (season,)).fetchall()
    for team, ppp, poss, pctl in trans:
        safe_print(f"  {team}: {ppp:.3f} PPP ({poss*100:.1f}% of poss, {pctl:.0f}th percentile)")

    # Best ISO players
    safe_print("\nBEST ISO PLAYERS (PPP, min 50 poss):")
    iso = conn.execute("""
        SELECT player_name, team_abbrev, ppp, poss_pct, percentile
        FROM PlayerPlayTypes
        WHERE season = ? AND play_type = 'Isolation'
        ORDER BY ppp DESC LIMIT 10
    """, (season,)).fetchall()
    for name, team, ppp, poss, pctl in iso:
        clean_name = name.encode('ascii', 'replace').decode('ascii')
        safe_print(f"  {clean_name} ({team}): {ppp:.3f} PPP ({pctl:.0f}th pctl)")

    # Best PnR ball handlers
    safe_print("\nBEST PnR BALL HANDLERS (PPP):")
    pnr = conn.execute("""
        SELECT player_name, team_abbrev, ppp, poss_pct, percentile
        FROM PlayerPlayTypes
        WHERE season = ? AND play_type = 'PRBallHandler'
        ORDER BY ppp DESC LIMIT 10
    """, (season,)).fetchall()
    for name, team, ppp, poss, pctl in pnr:
        clean_name = name.encode('ascii', 'replace').decode('ascii')
        safe_print(f"  {clean_name} ({team}): {ppp:.3f} PPP ({pctl:.0f}th pctl)")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", default="2024-25")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)

    fetch_team_play_types(conn, args.season)
    fetch_player_play_types(conn, args.season)
    display_insights(conn, args.season)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
