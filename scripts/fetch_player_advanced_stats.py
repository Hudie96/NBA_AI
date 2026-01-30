"""
AXIOM Phase 2: Player Advanced Stats Fetcher
Fetches TS%, eFG%, Usage Rate, and other advanced metrics for players.

Usage:
    python scripts/fetch_player_advanced_stats.py
    python scripts/fetch_player_advanced_stats.py --season 2024-25
    python scripts/fetch_player_advanced_stats.py --top 100  # Top 100 by minutes
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
    from nba_api.stats.endpoints import leaguedashplayerstats
    from nba_api.stats.static import players
    NBA_API_AVAILABLE = True
except ImportError:
    NBA_API_AVAILABLE = False
    print("Warning: nba_api not installed. Run: pip install nba_api")


def create_player_advanced_table(conn):
    """Create table for player advanced stats."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS PlayerAdvancedStats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            player_name TEXT NOT NULL,
            team_abbrev TEXT,
            season TEXT NOT NULL,
            games_played INTEGER,
            minutes REAL,
            ts_pct REAL,
            efg_pct REAL,
            usg_pct REAL,
            ast_pct REAL,
            reb_pct REAL,
            oreb_pct REAL,
            dreb_pct REAL,
            tov_pct REAL,
            off_rating REAL,
            def_rating REAL,
            net_rating REAL,
            pie REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(player_id, season)
        )
    """)
    conn.commit()
    print("Created/verified PlayerAdvancedStats table")


def fetch_player_advanced_stats(season="2024-25", top_n=200):
    """Fetch advanced stats from NBA API."""
    if not NBA_API_AVAILABLE:
        print("ERROR: nba_api not available")
        return None

    print(f"Fetching player advanced stats for {season}...")

    try:
        # Get advanced stats for all players
        stats = leaguedashplayerstats.LeagueDashPlayerStats(
            season=season,
            measure_type_detailed_defense='Advanced',
            per_mode_detailed='PerGame'
        )
        time.sleep(1)  # Rate limiting

        df = stats.get_data_frames()[0]

        # Sort by minutes and take top N
        df = df.sort_values('MIN', ascending=False).head(top_n)

        print(f"Retrieved stats for {len(df)} players")
        return df

    except Exception as e:
        print(f"Error fetching stats: {e}")
        return None


def save_player_stats(conn, df, season):
    """Save player stats to database."""
    if df is None or df.empty:
        return 0

    count = 0
    for _, row in df.iterrows():
        try:
            conn.execute("""
                INSERT OR REPLACE INTO PlayerAdvancedStats
                (player_id, player_name, team_abbrev, season, games_played,
                 minutes, ts_pct, efg_pct, usg_pct, ast_pct, reb_pct,
                 oreb_pct, dreb_pct, tov_pct, off_rating, def_rating,
                 net_rating, pie, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get('PLAYER_ID'),
                row.get('PLAYER_NAME', ''),
                row.get('TEAM_ABBREVIATION', ''),
                season,
                row.get('GP', 0),
                row.get('MIN', 0),
                row.get('TS_PCT'),
                row.get('EFG_PCT'),
                row.get('USG_PCT'),
                row.get('AST_PCT'),
                row.get('REB_PCT'),
                row.get('OREB_PCT'),
                row.get('DREB_PCT'),
                row.get('TM_TOV_PCT'),
                row.get('OFF_RATING') or row.get('E_OFF_RATING'),
                row.get('DEF_RATING') or row.get('E_DEF_RATING'),
                row.get('NET_RATING') or row.get('E_NET_RATING'),
                row.get('PIE'),
                datetime.now().isoformat()
            ))
            count += 1
        except Exception as e:
            print(f"Error saving {row.get('PLAYER_NAME')}: {e}")

    conn.commit()
    return count


def get_efficiency_leaders(conn, season="2024-25", min_games=20):
    """Get players ranked by true shooting percentage."""
    df = conn.execute("""
        SELECT player_name, team_abbrev, ts_pct, efg_pct, usg_pct, games_played
        FROM PlayerAdvancedStats
        WHERE season = ? AND games_played >= ?
        ORDER BY ts_pct DESC
        LIMIT 30
    """, (season, min_games)).fetchall()

    return df


def get_usage_leaders(conn, season="2024-25", min_games=20):
    """Get players ranked by usage rate."""
    df = conn.execute("""
        SELECT player_name, team_abbrev, usg_pct, ts_pct, net_rating, games_played
        FROM PlayerAdvancedStats
        WHERE season = ? AND games_played >= ?
        ORDER BY usg_pct DESC
        LIMIT 20
    """, (season, min_games)).fetchall()

    return df


def get_inefficient_high_usage(conn, season="2024-25", min_games=20):
    """Find high usage players with poor efficiency - fade candidates."""
    df = conn.execute("""
        SELECT player_name, team_abbrev, usg_pct, ts_pct, net_rating
        FROM PlayerAdvancedStats
        WHERE season = ?
          AND games_played >= ?
          AND usg_pct >= 0.22
          AND ts_pct < 0.55
        ORDER BY usg_pct DESC
    """, (season, min_games)).fetchall()

    return df


def safe_print(text):
    """Print with encoding safety for Windows console."""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))


def display_stats(conn, season="2024-25"):
    """Display fetched stats."""
    safe_print("\n" + "=" * 70)
    safe_print(f"PLAYER ADVANCED STATS - {season}")
    safe_print("=" * 70)

    # TS% leaders
    safe_print("\nTRUE SHOOTING % LEADERS (min 20 games):")
    safe_print("-" * 60)
    safe_print(f"{'Rank':<5} {'Player':<22} {'Team':<5} {'TS%':<8} {'eFG%':<8} {'USG%':<8}")
    safe_print("-" * 60)

    leaders = get_efficiency_leaders(conn, season)
    for i, (name, team, ts, efg, usg, gp) in enumerate(leaders[:15], 1):
        ts_str = f"{ts*100:.1f}%" if ts else "N/A"
        efg_str = f"{efg*100:.1f}%" if efg else "N/A"
        usg_str = f"{usg*100:.1f}%" if usg else "N/A"
        # Clean name for console output
        clean_name = name.encode('ascii', 'replace').decode('ascii')[:21]
        safe_print(f"{i:<5} {clean_name:<22} {team:<5} {ts_str:<8} {efg_str:<8} {usg_str:<8}")

    # Usage leaders
    safe_print("\n" + "-" * 60)
    safe_print("USAGE RATE LEADERS:")
    safe_print("-" * 60)
    safe_print(f"{'Rank':<5} {'Player':<22} {'Team':<5} {'USG%':<8} {'TS%':<8} {'Net':<8}")
    safe_print("-" * 60)

    usage = get_usage_leaders(conn, season)
    for i, (name, team, usg, ts, net, gp) in enumerate(usage, 1):
        usg_str = f"{usg*100:.1f}%" if usg else "N/A"
        ts_str = f"{ts*100:.1f}%" if ts else "N/A"
        net_str = f"{net:+.1f}" if net else "N/A"
        clean_name = name.encode('ascii', 'replace').decode('ascii')[:21]
        safe_print(f"{i:<5} {clean_name:<22} {team:<5} {usg_str:<8} {ts_str:<8} {net_str:<8}")

    # Betting insights - inefficient high usage players
    safe_print("\n" + "=" * 70)
    safe_print("BETTING INSIGHTS")
    safe_print("=" * 70)

    inefficient = get_inefficient_high_usage(conn, season)
    if inefficient:
        safe_print("\nFADE CANDIDATES (High Usage + Low Efficiency):")
        safe_print("These players take a lot of shots but don't convert efficiently")
        safe_print("-" * 60)
        for name, team, usg, ts, net in inefficient:
            usg_str = f"{usg*100:.1f}%" if usg else "N/A"
            ts_str = f"{ts*100:.1f}%" if ts else "N/A"
            clean_name = name.encode('ascii', 'replace').decode('ascii')
            safe_print(f"  {clean_name:<22} ({team}) USG: {usg_str}, TS: {ts_str}")
    else:
        safe_print("\nNo clear fade candidates found")

    # Super efficient players
    safe_print("\nTARGET PLAYERS (Elite Efficiency):")
    super_eff = conn.execute("""
        SELECT player_name, team_abbrev, ts_pct, usg_pct
        FROM PlayerAdvancedStats
        WHERE season = ?
          AND games_played >= 20
          AND ts_pct >= 0.62
          AND usg_pct >= 0.20
        ORDER BY ts_pct DESC
    """, (season,)).fetchall()

    if super_eff:
        for name, team, ts, usg in super_eff:
            clean_name = name.encode('ascii', 'replace').decode('ascii')
            safe_print(f"  {clean_name:<22} ({team}) TS: {ts*100:.1f}%, USG: {usg*100:.1f}%")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", default="2024-25", help="Season (e.g., 2024-25)")
    parser.add_argument("--top", type=int, default=200, help="Top N players by minutes")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)

    # Create table
    create_player_advanced_table(conn)

    # Fetch and save stats
    df = fetch_player_advanced_stats(args.season, args.top)

    if df is not None and not df.empty:
        count = save_player_stats(conn, df, args.season)
        print(f"\nSaved {count} player records")

        # Display results
        display_stats(conn, args.season)
    else:
        print("\nNo data fetched. Check if nba_api is installed.")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
