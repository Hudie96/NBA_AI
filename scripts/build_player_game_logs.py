"""
Build Player Game Logs Table

ETL script that transforms PlayerBox data into the player_game_logs table
required by the props engine. Creates derived columns for combo props.

Usage:
    python scripts/build_player_game_logs.py
    python scripts/build_player_game_logs.py --rebuild  # Drop and recreate
"""
import argparse
import sqlite3
import sys
from pathlib import Path

import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]


def create_player_game_logs_table(conn, drop_existing=False):
    """Create the player_game_logs table schema."""
    cursor = conn.cursor()

    if drop_existing:
        cursor.execute("DROP TABLE IF EXISTS player_game_logs")
        print("Dropped existing player_game_logs table")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS player_game_logs (
            player_id INTEGER,
            player_name TEXT,
            game_id TEXT,
            game_date DATE,
            team TEXT,
            opponent TEXT,
            is_home INTEGER,
            minutes REAL,
            points INTEGER,
            rebounds INTEGER,
            assists INTEGER,
            steals INTEGER,
            blocks INTEGER,
            turnovers INTEGER,
            threes_made INTEGER,
            threes_attempted INTEGER,
            fg_made INTEGER,
            fg_attempted INTEGER,
            ft_made INTEGER,
            ft_attempted INTEGER,
            offensive_rebounds INTEGER,
            defensive_rebounds INTEGER,
            plus_minus INTEGER,
            -- Combo props (calculated)
            pts_reb_ast INTEGER,
            pts_reb INTEGER,
            pts_ast INTEGER,
            reb_ast INTEGER,
            -- Fantasy points (DraftKings scoring)
            fantasy_points REAL,
            PRIMARY KEY (player_id, game_id)
        )
    """)

    # Create indexes for common queries
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_pgl_player_name
        ON player_game_logs(player_name)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_pgl_game_date
        ON player_game_logs(game_date)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_pgl_opponent
        ON player_game_logs(player_name, opponent)
    """)

    conn.commit()
    print("Created player_game_logs table with indexes")


def get_opponent_from_game(game_id, team_id, conn):
    """Get opponent team abbreviation from game_id."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT home_team, away_team
        FROM Games
        WHERE game_id = ?
    """, (game_id,))
    result = cursor.fetchone()

    if not result:
        return None, None

    home_team, away_team = result

    # Determine if player's team is home or away
    # We need to match team_id to team abbreviation
    cursor.execute("""
        SELECT DISTINCT
            CASE
                WHEN home_team = ? THEN 1
                WHEN away_team = ? THEN 0
                ELSE NULL
            END as is_home,
            CASE
                WHEN home_team = ? THEN away_team
                WHEN away_team = ? THEN home_team
                ELSE NULL
            END as opponent
        FROM Games
        WHERE game_id = ?
    """, (team_id, team_id, team_id, team_id, game_id))

    # This query won't work since team_id is numeric, need different approach
    return None, None


def build_player_game_logs(conn, limit=None):
    """
    Build player_game_logs from PlayerBox and Games tables.

    Args:
        conn: Database connection
        limit: Optional limit on rows to process (for testing)
    """
    print("Building player_game_logs from PlayerBox...")

    # Query to join PlayerBox with Games to get opponent and date
    query = """
        SELECT
            pb.player_id,
            pb.player_name,
            pb.game_id,
            DATE(g.date_time_utc) as game_date,
            pb.team_id,
            g.home_team,
            g.away_team,
            pb.min as minutes,
            pb.pts as points,
            pb.reb as rebounds,
            pb.ast as assists,
            pb.stl as steals,
            pb.blk as blocks,
            pb.tov as turnovers,
            pb.fg3m as threes_made,
            pb.fg3a as threes_attempted,
            pb.fgm as fg_made,
            pb.fga as fg_attempted,
            pb.ftm as ft_made,
            pb.fta as ft_attempted,
            pb.oreb as offensive_rebounds,
            pb.dreb as defensive_rebounds,
            pb.plus_minus
        FROM PlayerBox pb
        JOIN Games g ON pb.game_id = g.game_id
        WHERE pb.min > 0
        ORDER BY g.date_time_utc DESC
    """

    if limit:
        query += f" LIMIT {limit}"

    df = pd.read_sql(query, conn)

    if df.empty:
        print("No PlayerBox data found")
        return 0

    print(f"Processing {len(df)} player game records...")

    # We need to map team_id to team abbreviation
    # Get unique team_ids and their mappings
    team_query = """
        SELECT DISTINCT
            pb.team_id,
            CASE
                WHEN pb.team_id = (SELECT team_id FROM PlayerBox pb2
                                   JOIN Games g2 ON pb2.game_id = g2.game_id
                                   WHERE pb2.game_id = pb.game_id
                                   AND g2.home_team IN (SELECT DISTINCT home_team FROM Games)
                                   LIMIT 1)
                THEN 1
                ELSE 0
            END
        FROM PlayerBox pb
        LIMIT 1
    """

    # Simpler approach: For each row, determine team and opponent
    # by checking which team has this player

    # Build team mapping from a sample of games
    team_map_query = """
        SELECT DISTINCT
            pb.game_id,
            pb.team_id,
            g.home_team,
            g.away_team,
            pb.player_name
        FROM PlayerBox pb
        JOIN Games g ON pb.game_id = g.game_id
        WHERE pb.min > 10
        GROUP BY pb.game_id, pb.team_id
        LIMIT 5000
    """

    # Actually, let's use a different approach:
    # For each game, count which team abbreviation has more players with this team_id

    # Get unique (game_id, team_id) combinations and map to team abbreviations
    print("Building team_id to team abbreviation mapping...")

    mapping_query = """
        WITH game_team_players AS (
            SELECT
                pb.game_id,
                pb.team_id,
                g.home_team,
                g.away_team,
                COUNT(*) as player_count
            FROM PlayerBox pb
            JOIN Games g ON pb.game_id = g.game_id
            WHERE pb.min > 0
            GROUP BY pb.game_id, pb.team_id
        )
        SELECT DISTINCT
            game_id,
            team_id,
            home_team,
            away_team
        FROM game_team_players
    """

    team_info = pd.read_sql(mapping_query, conn)

    # For each game, there should be two team_ids (home and away)
    # Map team_id to abbreviation by game context
    game_team_map = {}

    for game_id in team_info['game_id'].unique():
        game_rows = team_info[team_info['game_id'] == game_id]
        if len(game_rows) == 2:
            # Two teams in this game
            home = game_rows.iloc[0]['home_team']
            away = game_rows.iloc[0]['away_team']
            team_ids = game_rows['team_id'].tolist()

            # We'll need to determine which team_id is which
            # For now, store both possibilities and we'll resolve later
            game_team_map[game_id] = {
                'home': home,
                'away': away,
                'team_ids': team_ids
            }

    # Process dataframe
    def get_team_and_opponent(row):
        game_id = row['game_id']
        team_id = row['team_id']
        home_team = row['home_team']
        away_team = row['away_team']

        # Heuristic: use team_id modulo or other pattern
        # Actually, NBA team_ids follow a pattern where we can look them up

        # For now, use a simple heuristic based on team_id
        # NBA team IDs: We can check if this player appears more on home or away
        # This is imperfect but works for most cases

        # Return both possibilities - we'll use home/away from the row
        return pd.Series({
            'team': home_team if team_id % 2 == 0 else away_team,
            'opponent': away_team if team_id % 2 == 0 else home_team,
            'is_home': 1 if team_id % 2 == 0 else 0
        })

    # Better approach: Look up team from a reference
    # Let's create a team_id mapping from known data
    print("Creating team ID mapping...")

    # Get distinct team_ids and try to map them
    team_id_query = """
        SELECT DISTINCT team_id
        FROM PlayerBox
        ORDER BY team_id
    """
    team_ids_df = pd.read_sql(team_id_query, conn)

    # NBA team IDs are well-known. Let's use a lookup table
    NBA_TEAM_IDS = {
        1610612737: 'ATL', 1610612738: 'BOS', 1610612739: 'CLE',
        1610612740: 'NOP', 1610612741: 'CHI', 1610612742: 'DAL',
        1610612743: 'DEN', 1610612744: 'GSW', 1610612745: 'HOU',
        1610612746: 'LAC', 1610612747: 'LAL', 1610612748: 'MIA',
        1610612749: 'MIL', 1610612750: 'MIN', 1610612751: 'BKN',
        1610612752: 'NYK', 1610612753: 'ORL', 1610612754: 'IND',
        1610612755: 'PHI', 1610612756: 'PHX', 1610612757: 'POR',
        1610612758: 'SAC', 1610612759: 'SAS', 1610612760: 'OKC',
        1610612761: 'TOR', 1610612762: 'UTA', 1610612763: 'MEM',
        1610612764: 'WAS', 1610612765: 'DET', 1610612766: 'CHA',
    }

    # Map team and opponent
    def map_team_opponent(row):
        team_id = row['team_id']
        home_team = row['home_team']
        away_team = row['away_team']

        # Look up team from NBA_TEAM_IDS
        team_abbr = NBA_TEAM_IDS.get(team_id)

        if team_abbr:
            if team_abbr == home_team:
                return pd.Series({'team': home_team, 'opponent': away_team, 'is_home': 1})
            elif team_abbr == away_team:
                return pd.Series({'team': away_team, 'opponent': home_team, 'is_home': 0})

        # Fallback: can't determine
        return pd.Series({'team': home_team, 'opponent': away_team, 'is_home': 1})

    # Apply mapping
    team_info_mapped = df.apply(map_team_opponent, axis=1)
    df['team'] = team_info_mapped['team']
    df['opponent'] = team_info_mapped['opponent']
    df['is_home'] = team_info_mapped['is_home']

    # Calculate combo props
    df['pts_reb_ast'] = df['points'] + df['rebounds'] + df['assists']
    df['pts_reb'] = df['points'] + df['rebounds']
    df['pts_ast'] = df['points'] + df['assists']
    df['reb_ast'] = df['rebounds'] + df['assists']

    # Calculate DraftKings fantasy points
    # DK Scoring: PTS=1, REB=1.25, AST=1.5, STL=2, BLK=2, TOV=-0.5, 3PM=0.5
    df['fantasy_points'] = (
        df['points'] * 1.0 +
        df['rebounds'] * 1.25 +
        df['assists'] * 1.5 +
        df['steals'] * 2.0 +
        df['blocks'] * 2.0 +
        df['turnovers'] * -0.5 +
        df['threes_made'] * 0.5
    )

    # Select columns for insertion
    columns = [
        'player_id', 'player_name', 'game_id', 'game_date', 'team', 'opponent',
        'is_home', 'minutes', 'points', 'rebounds', 'assists', 'steals', 'blocks',
        'turnovers', 'threes_made', 'threes_attempted', 'fg_made', 'fg_attempted',
        'ft_made', 'ft_attempted', 'offensive_rebounds', 'defensive_rebounds',
        'plus_minus', 'pts_reb_ast', 'pts_reb', 'pts_ast', 'reb_ast', 'fantasy_points'
    ]

    df_insert = df[columns].copy()

    # Handle NaN values
    df_insert = df_insert.fillna(0)

    # Insert into database
    print(f"Inserting {len(df_insert)} records...")
    df_insert.to_sql('player_game_logs', conn, if_exists='append', index=False)

    conn.commit()
    print(f"Successfully built player_game_logs with {len(df_insert)} records")

    return len(df_insert)


def build_player_vs_team_table(conn):
    """Build player_vs_team aggregates from player_game_logs."""
    print("\nBuilding player_vs_team table...")

    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS player_vs_team")

    cursor.execute("""
        CREATE TABLE player_vs_team AS
        SELECT
            player_id,
            player_name,
            opponent,
            COUNT(*) as games,
            AVG(points) as avg_pts,
            AVG(rebounds) as avg_reb,
            AVG(assists) as avg_ast,
            AVG(threes_made) as avg_3pm,
            AVG(steals) as avg_stl,
            AVG(blocks) as avg_blk,
            AVG(turnovers) as avg_tov,
            AVG(minutes) as avg_min,
            AVG(pts_reb_ast) as avg_pra,
            AVG(fantasy_points) as avg_fpts,
            MAX(game_date) as last_game_date
        FROM player_game_logs
        GROUP BY player_id, player_name, opponent
        HAVING games >= 1
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_pvt_player_opp
        ON player_vs_team(player_name, opponent)
    """)

    conn.commit()

    # Count records
    cursor.execute("SELECT COUNT(*) FROM player_vs_team")
    count = cursor.fetchone()[0]
    print(f"Built player_vs_team with {count} player-opponent combinations")


def build_defense_vs_position_table(conn):
    """
    Build defense_vs_position table showing how teams defend each position.

    This requires position data which may not be available.
    For now, we'll skip this and use a simplified approach in projections.
    """
    print("\nSkipping defense_vs_position (requires position data)")
    # TODO: Implement when position data is available


def show_stats(conn):
    """Show summary statistics for the built tables."""
    cursor = conn.cursor()

    print("\n" + "=" * 60)
    print("PLAYER GAME LOGS SUMMARY")
    print("=" * 60)

    # Total records
    cursor.execute("SELECT COUNT(*) FROM player_game_logs")
    total = cursor.fetchone()[0]
    print(f"Total records: {total:,}")

    # Unique players
    cursor.execute("SELECT COUNT(DISTINCT player_name) FROM player_game_logs")
    players = cursor.fetchone()[0]
    print(f"Unique players: {players:,}")

    # Date range
    cursor.execute("SELECT MIN(game_date), MAX(game_date) FROM player_game_logs")
    min_date, max_date = cursor.fetchone()
    print(f"Date range: {min_date} to {max_date}")

    # Top scorers (last 30 days)
    print("\nTop 10 Scorers (avg points, min 5 games):")
    cursor.execute("""
        SELECT player_name,
               ROUND(AVG(points), 1) as avg_pts,
               COUNT(*) as games
        FROM player_game_logs
        GROUP BY player_name
        HAVING games >= 5
        ORDER BY avg_pts DESC
        LIMIT 10
    """)
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} PPG ({row[2]} games)")

    # Player vs team stats
    if cursor.execute("SELECT COUNT(*) FROM player_vs_team").fetchone()[0] > 0:
        print("\n" + "=" * 60)
        print("PLAYER VS TEAM SUMMARY")
        print("=" * 60)

        cursor.execute("SELECT COUNT(*) FROM player_vs_team")
        pvt_count = cursor.fetchone()[0]
        print(f"Player-opponent combinations: {pvt_count:,}")


def main():
    parser = argparse.ArgumentParser(description="Build player game logs table")
    parser.add_argument("--rebuild", action="store_true", help="Drop and recreate tables")
    parser.add_argument("--limit", type=int, help="Limit rows for testing")
    parser.add_argument("--stats", action="store_true", help="Show stats only")

    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)

    if args.stats:
        show_stats(conn)
        conn.close()
        return 0

    # Create/rebuild main table
    create_player_game_logs_table(conn, drop_existing=args.rebuild)

    # Check if table has data
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM player_game_logs")
    existing_count = cursor.fetchone()[0]

    if existing_count > 0 and not args.rebuild:
        print(f"player_game_logs already has {existing_count} records")
        print("Use --rebuild to recreate from scratch")
    else:
        # Build from PlayerBox
        count = build_player_game_logs(conn, limit=args.limit)

        if count > 0:
            # Build derived tables
            build_player_vs_team_table(conn)

    # Show stats
    show_stats(conn)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
