"""
AXIOM: Shooting Zone Data Fetcher
Fetches shot distribution by zone (paint, mid-range, corner 3, above break 3).

Usage:
    python scripts/fetch_shooting_zones.py
    python scripts/fetch_shooting_zones.py --season 2024-25
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
    from nba_api.stats.endpoints import leaguedashteamshotlocations, leaguedashplayershotlocations
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
    """Create shooting zone tables."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS TeamShootingZones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER,
            team_abbrev TEXT,
            team_name TEXT,
            season TEXT,
            -- Restricted Area (paint)
            ra_fgm REAL,
            ra_fga REAL,
            ra_fg_pct REAL,
            -- In The Paint (Non-RA)
            paint_fgm REAL,
            paint_fga REAL,
            paint_fg_pct REAL,
            -- Mid-Range
            mid_fgm REAL,
            mid_fga REAL,
            mid_fg_pct REAL,
            -- Left Corner 3
            lc3_fgm REAL,
            lc3_fga REAL,
            lc3_fg_pct REAL,
            -- Right Corner 3
            rc3_fgm REAL,
            rc3_fga REAL,
            rc3_fg_pct REAL,
            -- Above the Break 3
            ab3_fgm REAL,
            ab3_fga REAL,
            ab3_fg_pct REAL,
            -- Backcourt (heaves)
            bc_fgm REAL,
            bc_fga REAL,
            bc_fg_pct REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(team_id, season)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS PlayerShootingZones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER,
            player_name TEXT,
            team_abbrev TEXT,
            season TEXT,
            -- Restricted Area
            ra_fgm REAL,
            ra_fga REAL,
            ra_fg_pct REAL,
            -- Paint (Non-RA)
            paint_fgm REAL,
            paint_fga REAL,
            paint_fg_pct REAL,
            -- Mid-Range
            mid_fgm REAL,
            mid_fga REAL,
            mid_fg_pct REAL,
            -- Corner 3 (combined)
            c3_fgm REAL,
            c3_fga REAL,
            c3_fg_pct REAL,
            -- Above Break 3
            ab3_fgm REAL,
            ab3_fga REAL,
            ab3_fg_pct REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(player_id, season)
        )
    """)
    conn.commit()
    safe_print("Created ShootingZones tables")


def get_team_abbrev_map():
    if not NBA_API_AVAILABLE:
        return {}
    team_list = teams.get_teams()
    return {t['id']: t['abbreviation'] for t in team_list}


def fetch_team_shot_zones(conn, season="2024-25"):
    """Fetch team shooting by zone."""
    if not NBA_API_AVAILABLE:
        safe_print("SKIPPED: nba_api not available")
        return 0

    safe_print(f"Fetching team shooting zones for {season}...")

    try:
        stats = leaguedashteamshotlocations.LeagueDashTeamShotLocations(
            season=season,
            per_mode_detailed='PerGame',
            distance_range='By Zone'
        )
        time.sleep(1)

        dfs = stats.get_data_frames()
        if not dfs:
            safe_print("  No data returned")
            return 0

        df = dfs[0]
        abbrev_map = get_team_abbrev_map()

        # Flatten multi-index columns
        df.columns = ['_'.join(col).strip('_') if isinstance(col, tuple) else col for col in df.columns]

        count = 0
        for _, row in df.iterrows():
            # Handle multi-index column access
            team_id = row.get('_TEAM_ID') or row.get('TEAM_ID')
            team_name = row.get('_TEAM_NAME') or row.get('TEAM_NAME', '')

            conn.execute("""
                INSERT OR REPLACE INTO TeamShootingZones
                (team_id, team_abbrev, team_name, season,
                 ra_fgm, ra_fga, ra_fg_pct,
                 paint_fgm, paint_fga, paint_fg_pct,
                 mid_fgm, mid_fga, mid_fg_pct,
                 lc3_fgm, lc3_fga, lc3_fg_pct,
                 rc3_fgm, rc3_fga, rc3_fg_pct,
                 ab3_fgm, ab3_fga, ab3_fg_pct,
                 bc_fgm, bc_fga, bc_fg_pct,
                 updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                team_id,
                abbrev_map.get(team_id, ''),
                team_name,
                season,
                row.get('Restricted Area_FGM'), row.get('Restricted Area_FGA'), row.get('Restricted Area_FG_PCT'),
                row.get('In The Paint (Non-RA)_FGM'), row.get('In The Paint (Non-RA)_FGA'), row.get('In The Paint (Non-RA)_FG_PCT'),
                row.get('Mid-Range_FGM'), row.get('Mid-Range_FGA'), row.get('Mid-Range_FG_PCT'),
                row.get('Left Corner 3_FGM'), row.get('Left Corner 3_FGA'), row.get('Left Corner 3_FG_PCT'),
                row.get('Right Corner 3_FGM'), row.get('Right Corner 3_FGA'), row.get('Right Corner 3_FG_PCT'),
                row.get('Above the Break 3_FGM'), row.get('Above the Break 3_FGA'), row.get('Above the Break 3_FG_PCT'),
                row.get('Backcourt_FGM'), row.get('Backcourt_FGA'), row.get('Backcourt_FG_PCT'),
                datetime.now().isoformat()
            ))
            count += 1

        conn.commit()
        safe_print(f"Saved {count} team shooting zone records")
        return count

    except Exception as e:
        safe_print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 0


def display_insights(conn, season="2024-25"):
    """Display shooting zone insights."""
    safe_print("\n" + "=" * 60)
    safe_print(f"SHOOTING ZONE INSIGHTS - {season}")
    safe_print("=" * 60)

    # Best paint teams
    safe_print("\nBEST PAINT SCORING TEAMS:")
    paint = conn.execute("""
        SELECT team_abbrev, ra_fga, ra_fg_pct, paint_fga, paint_fg_pct
        FROM TeamShootingZones
        WHERE season = ? AND ra_fga IS NOT NULL
        ORDER BY (ra_fga + COALESCE(paint_fga, 0)) DESC
        LIMIT 10
    """, (season,)).fetchall()

    for team, ra_a, ra_p, p_a, p_p in paint:
        total_paint = (ra_a or 0) + (p_a or 0)
        safe_print(f"  {team}: {total_paint:.1f} paint FGA/g (RA: {ra_p*100 if ra_p else 0:.1f}%)")

    # Best 3PT teams
    safe_print("\nBEST 3PT VOLUME TEAMS:")
    three = conn.execute("""
        SELECT team_abbrev,
               COALESCE(lc3_fga, 0) + COALESCE(rc3_fga, 0) + COALESCE(ab3_fga, 0) as total_3pa,
               ab3_fg_pct
        FROM TeamShootingZones
        WHERE season = ?
        ORDER BY total_3pa DESC
        LIMIT 10
    """, (season,)).fetchall()

    for team, fga, pct in three:
        safe_print(f"  {team}: {fga:.1f} 3PA/g ({pct*100 if pct else 0:.1f}% above break)")

    # Corner 3 specialists
    safe_print("\nBEST CORNER 3 TEAMS:")
    corner = conn.execute("""
        SELECT team_abbrev,
               COALESCE(lc3_fga, 0) + COALESCE(rc3_fga, 0) as corner_fga,
               (COALESCE(lc3_fg_pct, 0) + COALESCE(rc3_fg_pct, 0)) / 2 as corner_pct
        FROM TeamShootingZones
        WHERE season = ?
        ORDER BY corner_fga DESC
        LIMIT 10
    """, (season,)).fetchall()

    for team, fga, pct in corner:
        safe_print(f"  {team}: {fga:.1f} corner 3PA/g ({pct*100 if pct else 0:.1f}%)")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", default="2024-25")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)

    fetch_team_shot_zones(conn, args.season)
    display_insights(conn, args.season)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
