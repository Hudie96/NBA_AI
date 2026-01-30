"""
AXIOM Phase 3: Clutch Stats Fetcher
Fetches clutch performance (last 5 min, within 5 pts) for teams and players.

Usage:
    python scripts/fetch_clutch_stats.py
    python scripts/fetch_clutch_stats.py --season 2024-25
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
    from nba_api.stats.endpoints import leaguedashteamclutch, leaguedashplayerclutch
    from nba_api.stats.static import teams
    NBA_API_AVAILABLE = True
except ImportError:
    NBA_API_AVAILABLE = False
    print("Warning: nba_api not installed. Run: pip install nba_api")


def safe_print(text):
    """Print with encoding safety for Windows console."""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))


def create_clutch_tables(conn):
    """Create tables for clutch stats."""
    # Team clutch stats
    conn.execute("""
        CREATE TABLE IF NOT EXISTS TeamClutchStats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER NOT NULL,
            team_abbrev TEXT NOT NULL,
            team_name TEXT NOT NULL,
            season TEXT NOT NULL,
            clutch_gp INTEGER,
            clutch_wins INTEGER,
            clutch_losses INTEGER,
            clutch_win_pct REAL,
            clutch_pts REAL,
            clutch_fgm REAL,
            clutch_fga REAL,
            clutch_fg_pct REAL,
            clutch_fg3m REAL,
            clutch_fg3a REAL,
            clutch_fg3_pct REAL,
            clutch_ftm REAL,
            clutch_fta REAL,
            clutch_ft_pct REAL,
            clutch_plus_minus REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(team_id, season)
        )
    """)

    # Player clutch stats
    conn.execute("""
        CREATE TABLE IF NOT EXISTS PlayerClutchStats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            player_name TEXT NOT NULL,
            team_abbrev TEXT,
            season TEXT NOT NULL,
            clutch_gp INTEGER,
            clutch_mins REAL,
            clutch_pts REAL,
            clutch_fgm REAL,
            clutch_fga REAL,
            clutch_fg_pct REAL,
            clutch_fg3m REAL,
            clutch_fg3a REAL,
            clutch_fg3_pct REAL,
            clutch_ftm REAL,
            clutch_fta REAL,
            clutch_ft_pct REAL,
            clutch_plus_minus REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(player_id, season)
        )
    """)

    conn.commit()
    print("Created/verified TeamClutchStats and PlayerClutchStats tables")


def get_team_abbrev_map():
    """Get mapping of team ID to abbreviation."""
    if not NBA_API_AVAILABLE:
        return {}
    team_list = teams.get_teams()
    return {t['id']: t['abbreviation'] for t in team_list}


def fetch_team_clutch_stats(season="2024-25"):
    """Fetch team clutch stats from NBA API."""
    if not NBA_API_AVAILABLE:
        print("ERROR: nba_api not available")
        return None

    print(f"Fetching team clutch stats for {season}...")

    try:
        stats = leaguedashteamclutch.LeagueDashTeamClutch(
            season=season,
            clutch_time='Last 5 Minutes',
            ahead_behind='Ahead or Behind',
            point_diff=5,
            per_mode_detailed='PerGame'
        )
        time.sleep(1)

        df = stats.get_data_frames()[0]

        # Add abbreviations
        abbrev_map = get_team_abbrev_map()
        df['TEAM_ABBREV'] = df['TEAM_ID'].map(abbrev_map)

        print(f"Retrieved clutch stats for {len(df)} teams")
        return df

    except Exception as e:
        print(f"Error fetching team clutch stats: {e}")
        return None


def fetch_player_clutch_stats(season="2024-25", top_n=100):
    """Fetch player clutch stats from NBA API."""
    if not NBA_API_AVAILABLE:
        print("ERROR: nba_api not available")
        return None

    print(f"Fetching player clutch stats for {season}...")

    try:
        stats = leaguedashplayerclutch.LeagueDashPlayerClutch(
            season=season,
            clutch_time='Last 5 Minutes',
            ahead_behind='Ahead or Behind',
            point_diff=5,
            per_mode_detailed='PerGame'
        )
        time.sleep(1)

        df = stats.get_data_frames()[0]

        # Sort by clutch minutes and take top N
        df = df.sort_values('MIN', ascending=False).head(top_n)

        print(f"Retrieved clutch stats for {len(df)} players")
        return df

    except Exception as e:
        print(f"Error fetching player clutch stats: {e}")
        return None


def save_team_clutch_stats(conn, df, season):
    """Save team clutch stats to database."""
    if df is None or df.empty:
        return 0

    count = 0
    for _, row in df.iterrows():
        try:
            conn.execute("""
                INSERT OR REPLACE INTO TeamClutchStats
                (team_id, team_abbrev, team_name, season, clutch_gp,
                 clutch_wins, clutch_losses, clutch_win_pct, clutch_pts,
                 clutch_fgm, clutch_fga, clutch_fg_pct, clutch_fg3m, clutch_fg3a,
                 clutch_fg3_pct, clutch_ftm, clutch_fta, clutch_ft_pct,
                 clutch_plus_minus, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get('TEAM_ID'),
                row.get('TEAM_ABBREV', ''),
                row.get('TEAM_NAME', ''),
                season,
                row.get('GP', 0),
                row.get('W', 0),
                row.get('L', 0),
                row.get('W_PCT'),
                row.get('PTS'),
                row.get('FGM'),
                row.get('FGA'),
                row.get('FG_PCT'),
                row.get('FG3M'),
                row.get('FG3A'),
                row.get('FG3_PCT'),
                row.get('FTM'),
                row.get('FTA'),
                row.get('FT_PCT'),
                row.get('PLUS_MINUS'),
                datetime.now().isoformat()
            ))
            count += 1
        except Exception as e:
            print(f"Error saving {row.get('TEAM_NAME')}: {e}")

    conn.commit()
    return count


def save_player_clutch_stats(conn, df, season):
    """Save player clutch stats to database."""
    if df is None or df.empty:
        return 0

    count = 0
    for _, row in df.iterrows():
        try:
            conn.execute("""
                INSERT OR REPLACE INTO PlayerClutchStats
                (player_id, player_name, team_abbrev, season, clutch_gp,
                 clutch_mins, clutch_pts, clutch_fgm, clutch_fga, clutch_fg_pct,
                 clutch_fg3m, clutch_fg3a, clutch_fg3_pct, clutch_ftm, clutch_fta,
                 clutch_ft_pct, clutch_plus_minus, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get('PLAYER_ID'),
                row.get('PLAYER_NAME', ''),
                row.get('TEAM_ABBREVIATION', ''),
                season,
                row.get('GP', 0),
                row.get('MIN'),
                row.get('PTS'),
                row.get('FGM'),
                row.get('FGA'),
                row.get('FG_PCT'),
                row.get('FG3M'),
                row.get('FG3A'),
                row.get('FG3_PCT'),
                row.get('FTM'),
                row.get('FTA'),
                row.get('FT_PCT'),
                row.get('PLUS_MINUS'),
                datetime.now().isoformat()
            ))
            count += 1
        except Exception as e:
            print(f"Error saving {row.get('PLAYER_NAME')}: {e}")

    conn.commit()
    return count


def display_stats(conn, season="2024-25"):
    """Display clutch stats."""
    safe_print("\n" + "=" * 70)
    safe_print(f"CLUTCH STATS - {season}")
    safe_print("Last 5 minutes, within 5 points")
    safe_print("=" * 70)

    # Team clutch rankings
    safe_print("\nTEAM CLUTCH WIN % RANKINGS:")
    safe_print("-" * 60)
    safe_print(f"{'Rank':<5} {'Team':<6} {'GP':<5} {'W-L':<10} {'WIN%':<8} {'+/-':<8}")
    safe_print("-" * 60)

    team_rankings = conn.execute("""
        SELECT team_abbrev, clutch_gp, clutch_wins, clutch_losses,
               clutch_win_pct, clutch_plus_minus
        FROM TeamClutchStats
        WHERE season = ?
        ORDER BY clutch_win_pct DESC
    """, (season,)).fetchall()

    for i, (team, gp, w, l, pct, pm) in enumerate(team_rankings, 1):
        pct_str = f"{pct*100:.1f}%" if pct else "N/A"
        pm_str = f"{pm:+.1f}" if pm else "N/A"
        safe_print(f"{i:<5} {team:<6} {gp or 0:<5} {f'{w or 0}-{l or 0}':<10} {pct_str:<8} {pm_str:<8}")

    # Player clutch leaders
    safe_print("\n" + "-" * 60)
    safe_print("PLAYER CLUTCH PPG LEADERS (min 20 games):")
    safe_print("-" * 60)
    safe_print(f"{'Rank':<5} {'Player':<22} {'Team':<5} {'GP':<5} {'PPG':<8} {'FG%':<8}")
    safe_print("-" * 60)

    player_rankings = conn.execute("""
        SELECT player_name, team_abbrev, clutch_gp, clutch_pts, clutch_fg_pct
        FROM PlayerClutchStats
        WHERE season = ? AND clutch_gp >= 20
        ORDER BY clutch_pts DESC
        LIMIT 15
    """, (season,)).fetchall()

    for i, (name, team, gp, pts, fg_pct) in enumerate(player_rankings, 1):
        pts_str = f"{pts:.1f}" if pts else "N/A"
        fg_str = f"{fg_pct*100:.1f}%" if fg_pct else "N/A"
        clean_name = name.encode('ascii', 'replace').decode('ascii')[:21]
        safe_print(f"{i:<5} {clean_name:<22} {team:<5} {gp:<5} {pts_str:<8} {fg_str:<8}")

    # Betting insights
    safe_print("\n" + "=" * 70)
    safe_print("BETTING INSIGHTS")
    safe_print("=" * 70)

    # Best clutch teams
    best_clutch = conn.execute("""
        SELECT team_abbrev, clutch_win_pct, clutch_plus_minus
        FROM TeamClutchStats
        WHERE season = ? AND clutch_win_pct >= 0.55
        ORDER BY clutch_win_pct DESC
    """, (season,)).fetchall()

    if best_clutch:
        safe_print("\nBEST CLUTCH TEAMS (55%+ win rate):")
        safe_print("Target these teams to cover in close games")
        for team, pct, pm in best_clutch:
            safe_print(f"  {team}: {pct*100:.1f}% clutch win rate ({pm:+.1f} +/-)")

    # Worst clutch teams
    worst_clutch = conn.execute("""
        SELECT team_abbrev, clutch_win_pct, clutch_plus_minus
        FROM TeamClutchStats
        WHERE season = ? AND clutch_win_pct < 0.45
        ORDER BY clutch_win_pct ASC
    """, (season,)).fetchall()

    if worst_clutch:
        safe_print("\nWORST CLUTCH TEAMS (<45% win rate):")
        safe_print("Fade these teams in close game scenarios")
        for team, pct, pm in worst_clutch:
            safe_print(f"  {team}: {pct*100:.1f}% clutch win rate ({pm:+.1f} +/-)")

    # Clutch shooting leaders
    safe_print("\nCLUTCH SHOOTERS (50%+ FG, 40%+ 3PT, min 20 games):")
    clutch_shooters = conn.execute("""
        SELECT player_name, team_abbrev, clutch_fg_pct, clutch_fg3_pct, clutch_pts
        FROM PlayerClutchStats
        WHERE season = ?
          AND clutch_gp >= 20
          AND clutch_fg_pct >= 0.50
          AND clutch_fg3_pct >= 0.40
        ORDER BY clutch_pts DESC
    """, (season,)).fetchall()

    if clutch_shooters:
        for name, team, fg, fg3, pts in clutch_shooters:
            clean_name = name.encode('ascii', 'replace').decode('ascii')
            safe_print(f"  {clean_name:<22} ({team}) FG: {fg*100:.1f}%, 3PT: {fg3*100:.1f}%")
    else:
        safe_print("  No players meet elite clutch shooting criteria")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", default="2024-25", help="Season (e.g., 2024-25)")
    parser.add_argument("--top", type=int, default=100, help="Top N players by clutch minutes")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)

    # Create tables
    create_clutch_tables(conn)

    # Fetch and save team clutch stats
    team_df = fetch_team_clutch_stats(args.season)
    if team_df is not None and not team_df.empty:
        count = save_team_clutch_stats(conn, team_df, args.season)
        print(f"Saved {count} team clutch records")

    # Fetch and save player clutch stats
    player_df = fetch_player_clutch_stats(args.season, args.top)
    if player_df is not None and not player_df.empty:
        count = save_player_clutch_stats(conn, player_df, args.season)
        print(f"Saved {count} player clutch records")

    # Display results
    display_stats(conn, args.season)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
