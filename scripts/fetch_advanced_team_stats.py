"""
AXIOM Phase 1: Advanced Team Stats Fetcher
Fetches Pace, ORTG, DRTG, Net Rating for all teams.

Usage:
    python scripts/fetch_advanced_team_stats.py
    python scripts/fetch_advanced_team_stats.py --season 2024-25
"""

import sqlite3
import sys
from datetime import datetime
from pathlib import Path
import time

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]

# Try to import nba_api
try:
    from nba_api.stats.endpoints import leaguedashteamstats
    from nba_api.stats.static import teams
    NBA_API_AVAILABLE = True
except ImportError:
    NBA_API_AVAILABLE = False
    print("Warning: nba_api not installed. Run: pip install nba_api")


def create_advanced_stats_table(conn):
    """Create table for advanced team stats."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS TeamAdvancedStats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER NOT NULL,
            team_abbrev TEXT NOT NULL,
            team_name TEXT NOT NULL,
            season TEXT NOT NULL,
            games_played INTEGER,
            pace REAL,
            off_rating REAL,
            def_rating REAL,
            net_rating REAL,
            ast_pct REAL,
            ast_to_ratio REAL,
            oreb_pct REAL,
            dreb_pct REAL,
            reb_pct REAL,
            ts_pct REAL,
            efg_pct REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(team_id, season)
        )
    """)
    conn.commit()
    print("Created/verified TeamAdvancedStats table")


def get_team_abbrev_map():
    """Get mapping of team ID to abbreviation."""
    if not NBA_API_AVAILABLE:
        return {}

    team_list = teams.get_teams()
    return {t['id']: t['abbreviation'] for t in team_list}


def fetch_team_advanced_stats(season="2024-25"):
    """Fetch advanced stats from NBA API."""
    if not NBA_API_AVAILABLE:
        print("ERROR: nba_api not available")
        return None

    print(f"Fetching advanced team stats for {season}...")

    try:
        # Get advanced stats
        stats = leaguedashteamstats.LeagueDashTeamStats(
            season=season,
            measure_type_detailed_defense='Advanced',
            per_mode_detailed='PerGame'
        )
        time.sleep(1)  # Rate limiting

        df = stats.get_data_frames()[0]

        # Add team abbreviations
        abbrev_map = get_team_abbrev_map()
        df['TEAM_ABBREV'] = df['TEAM_ID'].map(abbrev_map)

        print(f"Retrieved stats for {len(df)} teams")
        return df

    except Exception as e:
        print(f"Error fetching stats: {e}")
        return None


def save_team_stats(conn, df, season):
    """Save team stats to database."""
    if df is None or df.empty:
        return 0

    count = 0
    for _, row in df.iterrows():
        try:
            conn.execute("""
                INSERT OR REPLACE INTO TeamAdvancedStats
                (team_id, team_abbrev, team_name, season, games_played,
                 pace, off_rating, def_rating, net_rating,
                 ast_pct, ast_to_ratio, oreb_pct, dreb_pct, reb_pct,
                 ts_pct, efg_pct, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get('TEAM_ID'),
                row.get('TEAM_ABBREV', ''),
                row.get('TEAM_NAME', ''),
                season,
                row.get('GP', 0),
                row.get('E_PACE') or row.get('PACE'),
                row.get('OFF_RATING') or row.get('E_OFF_RATING'),
                row.get('DEF_RATING') or row.get('E_DEF_RATING'),
                row.get('NET_RATING') or row.get('E_NET_RATING'),
                row.get('AST_PCT'),
                row.get('AST_TO') or row.get('AST_TOV'),
                row.get('OREB_PCT'),
                row.get('DREB_PCT'),
                row.get('REB_PCT'),
                row.get('TS_PCT'),
                row.get('EFG_PCT'),
                datetime.now().isoformat()
            ))
            count += 1
        except Exception as e:
            print(f"Error saving {row.get('TEAM_NAME')}: {e}")

    conn.commit()
    return count


def get_pace_rankings(conn, season="2024-25"):
    """Get teams ranked by pace."""
    df = conn.execute("""
        SELECT team_abbrev, pace, off_rating, def_rating, net_rating
        FROM TeamAdvancedStats
        WHERE season = ?
        ORDER BY pace DESC
    """, (season,)).fetchall()

    return df


def display_stats(conn, season="2024-25"):
    """Display fetched stats."""
    print("\n" + "=" * 60)
    print(f"TEAM ADVANCED STATS - {season}")
    print("=" * 60)

    # Pace rankings
    print("\nPACE RANKINGS (Fastest to Slowest):")
    print("-" * 50)
    print(f"{'Rank':<5} {'Team':<6} {'Pace':<8} {'ORTG':<8} {'DRTG':<8} {'Net':<8}")
    print("-" * 50)

    rankings = get_pace_rankings(conn, season)
    for i, (team, pace, ortg, drtg, net) in enumerate(rankings, 1):
        pace_str = f"{pace:.1f}" if pace else "N/A"
        ortg_str = f"{ortg:.1f}" if ortg else "N/A"
        drtg_str = f"{drtg:.1f}" if drtg else "N/A"
        net_str = f"{net:+.1f}" if net else "N/A"
        print(f"{i:<5} {team:<6} {pace_str:<8} {ortg_str:<8} {drtg_str:<8} {net_str:<8}")

    # Key insights for betting
    print("\n" + "=" * 60)
    print("BETTING INSIGHTS")
    print("=" * 60)

    # Slowest pace teams (UNDER candidates)
    slow_teams = [r for r in rankings if r[1] and r[1] < 98]
    if slow_teams:
        print("\nSLOW PACE TEAMS (Target UNDERs):")
        for team, pace, _, _, _ in slow_teams[:5]:
            print(f"  {team}: {pace:.1f}")

    # Best offenses
    best_off = sorted([r for r in rankings if r[2]], key=lambda x: x[2], reverse=True)[:5]
    print("\nBEST OFFENSES (ORTG):")
    for team, _, ortg, _, _ in best_off:
        print(f"  {team}: {ortg:.1f}")

    # Worst defenses
    worst_def = sorted([r for r in rankings if r[3]], key=lambda x: x[3], reverse=True)[:5]
    print("\nWORST DEFENSES (High DRTG = bad):")
    for team, _, _, drtg, _ in worst_def:
        print(f"  {team}: {drtg:.1f}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", default="2024-25", help="Season (e.g., 2024-25)")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)

    # Create table
    create_advanced_stats_table(conn)

    # Fetch and save stats
    df = fetch_team_advanced_stats(args.season)

    if df is not None and not df.empty:
        count = save_team_stats(conn, df, args.season)
        print(f"\nSaved {count} team records")

        # Display results
        display_stats(conn, args.season)
    else:
        print("\nNo data fetched. Check if nba_api is installed.")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
