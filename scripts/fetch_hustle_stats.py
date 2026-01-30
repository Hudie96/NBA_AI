"""
AXIOM: Hustle Stats Fetcher
Fetches deflections, contested shots, loose balls, charges, screen assists.

Usage:
    python scripts/fetch_hustle_stats.py
    python scripts/fetch_hustle_stats.py --season 2024-25
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
    from nba_api.stats.endpoints import leaguehustlestatsplayer, leaguehustlestatsteam
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
    """Create hustle stats tables."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS TeamHustleStats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER,
            team_abbrev TEXT,
            team_name TEXT,
            season TEXT,
            gp INTEGER,
            minutes REAL,
            contested_shots REAL,
            contested_shots_3pt REAL,
            contested_shots_2pt REAL,
            deflections REAL,
            charges_drawn REAL,
            screen_assists REAL,
            screen_ast_pts REAL,
            loose_balls_recovered REAL,
            box_outs REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(team_id, season)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS PlayerHustleStats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER,
            player_name TEXT,
            team_abbrev TEXT,
            season TEXT,
            gp INTEGER,
            minutes REAL,
            contested_shots REAL,
            contested_shots_3pt REAL,
            contested_shots_2pt REAL,
            deflections REAL,
            charges_drawn REAL,
            screen_assists REAL,
            screen_ast_pts REAL,
            loose_balls_recovered REAL,
            box_outs REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(player_id, season)
        )
    """)
    conn.commit()
    safe_print("Created HustleStats tables")


def get_team_abbrev_map():
    if not NBA_API_AVAILABLE:
        return {}
    team_list = teams.get_teams()
    return {t['id']: t['abbreviation'] for t in team_list}


def fetch_team_hustle(conn, season="2024-25"):
    """Fetch team hustle stats."""
    if not NBA_API_AVAILABLE:
        safe_print("SKIPPED: nba_api not available")
        return 0

    safe_print(f"Fetching team hustle stats for {season}...")

    try:
        stats = leaguehustlestatsteam.LeagueHustleStatsTeam(
            season=season,
            per_mode_time='PerGame'
        )
        time.sleep(1)

        df = stats.get_data_frames()[0]
        abbrev_map = get_team_abbrev_map()

        count = 0
        for _, row in df.iterrows():
            team_id = row.get('TEAM_ID')
            conn.execute("""
                INSERT OR REPLACE INTO TeamHustleStats
                (team_id, team_abbrev, team_name, season, gp, minutes,
                 contested_shots, contested_shots_3pt, contested_shots_2pt,
                 deflections, charges_drawn, screen_assists, screen_ast_pts,
                 loose_balls_recovered, box_outs, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                team_id,
                abbrev_map.get(team_id, ''),
                row.get('TEAM_NAME', ''),
                season,
                row.get('G'),
                row.get('MIN'),
                row.get('CONTESTED_SHOTS'),
                row.get('CONTESTED_SHOTS_3PT'),
                row.get('CONTESTED_SHOTS_2PT'),
                row.get('DEFLECTIONS'),
                row.get('CHARGES_DRAWN'),
                row.get('SCREEN_ASSISTS'),
                row.get('SCREEN_AST_PTS'),
                row.get('LOOSE_BALLS_RECOVERED'),
                row.get('BOX_OUTS'),
                datetime.now().isoformat()
            ))
            count += 1

        conn.commit()
        safe_print(f"Saved {count} team hustle records")
        return count

    except Exception as e:
        safe_print(f"Error: {e}")
        return 0


def fetch_player_hustle(conn, season="2024-25", top_n=150):
    """Fetch player hustle stats."""
    if not NBA_API_AVAILABLE:
        safe_print("SKIPPED: nba_api not available")
        return 0

    safe_print(f"Fetching player hustle stats for {season}...")

    try:
        stats = leaguehustlestatsplayer.LeagueHustleStatsPlayer(
            season=season,
            per_mode_time='PerGame'
        )
        time.sleep(1)

        df = stats.get_data_frames()[0]
        df = df.sort_values('MIN', ascending=False).head(top_n)

        count = 0
        for _, row in df.iterrows():
            conn.execute("""
                INSERT OR REPLACE INTO PlayerHustleStats
                (player_id, player_name, team_abbrev, season, gp, minutes,
                 contested_shots, contested_shots_3pt, contested_shots_2pt,
                 deflections, charges_drawn, screen_assists, screen_ast_pts,
                 loose_balls_recovered, box_outs, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get('PLAYER_ID'),
                row.get('PLAYER_NAME', ''),
                row.get('TEAM_ABBREVIATION', ''),
                season,
                row.get('G'),
                row.get('MIN'),
                row.get('CONTESTED_SHOTS'),
                row.get('CONTESTED_SHOTS_3PT'),
                row.get('CONTESTED_SHOTS_2PT'),
                row.get('DEFLECTIONS'),
                row.get('CHARGES_DRAWN'),
                row.get('SCREEN_ASSISTS'),
                row.get('SCREEN_AST_PTS'),
                row.get('LOOSE_BALLS_RECOVERED'),
                row.get('BOX_OUTS'),
                datetime.now().isoformat()
            ))
            count += 1

        conn.commit()
        safe_print(f"Saved {count} player hustle records")
        return count

    except Exception as e:
        safe_print(f"Error: {e}")
        return 0


def display_insights(conn, season="2024-25"):
    """Display hustle insights."""
    safe_print("\n" + "=" * 60)
    safe_print(f"HUSTLE STATS INSIGHTS - {season}")
    safe_print("=" * 60)

    # Best defensive hustle teams
    safe_print("\nBEST DEFENSIVE HUSTLE TEAMS:")
    safe_print("-" * 50)
    teams_data = conn.execute("""
        SELECT team_abbrev, contested_shots, deflections, charges_drawn, loose_balls_recovered
        FROM TeamHustleStats
        WHERE season = ?
        ORDER BY contested_shots DESC
        LIMIT 10
    """, (season,)).fetchall()

    safe_print(f"{'Team':<6} {'Contested':<12} {'Deflect':<10} {'Charges':<10} {'Loose':<10}")
    for team, cont, defl, chrg, loose in teams_data:
        safe_print(f"{team:<6} {cont or 0:.1f}        {defl or 0:.1f}       {chrg or 0:.1f}        {loose or 0:.1f}")

    # Best screen setters
    safe_print("\nBEST SCREEN SETTERS (Screen Assists):")
    screens = conn.execute("""
        SELECT player_name, team_abbrev, screen_assists, screen_ast_pts
        FROM PlayerHustleStats
        WHERE season = ? AND screen_assists IS NOT NULL
        ORDER BY screen_assists DESC
        LIMIT 10
    """, (season,)).fetchall()

    for name, team, sa, pts in screens:
        clean_name = name.encode('ascii', 'replace').decode('ascii')
        safe_print(f"  {clean_name:<22} ({team}): {sa:.1f} screen ast, {pts:.1f} pts created")

    # Best deflection players
    safe_print("\nBEST DEFLECTION PLAYERS:")
    defl = conn.execute("""
        SELECT player_name, team_abbrev, deflections, contested_shots
        FROM PlayerHustleStats
        WHERE season = ?
        ORDER BY deflections DESC
        LIMIT 10
    """, (season,)).fetchall()

    for name, team, d, c in defl:
        clean_name = name.encode('ascii', 'replace').decode('ascii')
        safe_print(f"  {clean_name:<22} ({team}): {d:.1f} deflections, {c:.1f} contested")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", default="2024-25")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)

    fetch_team_hustle(conn, args.season)
    fetch_player_hustle(conn, args.season)
    display_insights(conn, args.season)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
