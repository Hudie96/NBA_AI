"""
Build Player vs Team History Table

Task 1.3 from AXIOM_ACTION_PLAN_v2.md
Aggregates how each player performs vs specific opponents.

Usage:
    python scripts/build_player_vs_team.py
    python scripts/build_player_vs_team.py --test  # Preview without saving
    python scripts/build_player_vs_team.py --min-games 5  # Require 5+ games
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


def build_player_vs_team(db_path=DB_PATH, min_games=3, season="2024-25"):
    """
    Aggregate player stats vs each opponent from game logs.

    Args:
        db_path: Path to database
        min_games: Minimum games required for inclusion
        season: Season string (e.g., '2024-25') for date filtering

    Returns:
        DataFrame with player vs team averages
    """
    # Parse season to date range (e.g., '2024-25' -> Oct 2024 to Jun 2025)
    start_year = int(season.split("-")[0])
    season_start = f"{start_year}-10-01"
    season_end = f"{start_year + 1}-06-30"

    print(f"Building player vs team history for {season} (min {min_games} games)...")
    print(f"  Date range: {season_start} to {season_end}")

    conn = sqlite3.connect(db_path)

    query = f"""
    SELECT
        player_id,
        player_name,
        opponent,
        COUNT(*) as games,
        ROUND(AVG(points), 1) as avg_pts,
        ROUND(AVG(rebounds), 1) as avg_reb,
        ROUND(AVG(assists), 1) as avg_ast,
        ROUND(AVG(threes_made), 1) as avg_3pm,
        ROUND(AVG(minutes), 1) as avg_min,
        MAX(game_date) as last_game_date
    FROM player_game_logs
    WHERE game_date >= '{season_start}' AND game_date <= '{season_end}'
    GROUP BY player_id, player_name, opponent
    HAVING COUNT(*) >= {min_games}
    ORDER BY player_name, opponent
    """

    df = pd.read_sql(query, conn)
    conn.close()

    print(f"Generated {len(df)} player-opponent matchup records")
    print(f"Unique players: {df['player_id'].nunique()}")
    print(f"Average games per matchup: {df['games'].mean():.1f}")

    return df


def save_to_db(df, db_path=DB_PATH):
    """Save player vs team data to database."""
    print(f"Saving {len(df)} rows to database...")

    conn = sqlite3.connect(db_path)

    df.to_sql(
        "player_vs_team",
        conn,
        if_exists="replace",
        index=False
    )

    # Verify
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM player_vs_team")
    count = cursor.fetchone()[0]

    conn.close()

    print(f"Saved {count} rows to player_vs_team table")
    return count


def test_mode(df):
    """Display sample data without saving."""
    print("\n=== TEST MODE ===\n")

    # Find Tatum's stats vs Miami (success criteria)
    tatum = df[df["player_name"].str.contains("Tatum", case=False)]
    if not tatum.empty:
        print("Jayson Tatum's averages by opponent:")
        print(tatum[["opponent", "games", "avg_pts", "avg_reb", "avg_ast"]].head(10).to_string(index=False))

        miami = tatum[tatum["opponent"] == "MIA"]
        if not miami.empty:
            print(f"\nTatum vs Miami: {miami.iloc[0]['avg_pts']} PPG in {miami.iloc[0]['games']} games")
    else:
        print("Tatum not found in data")

    # Show top scorers vs specific teams (filter to ASCII names to avoid encoding issues)
    print("\n\nHighest PPG vs specific opponents (min 3 games):")
    df_ascii = df[df["player_name"].str.encode("ascii", errors="ignore").str.decode("ascii") == df["player_name"]]
    top_scorers = df_ascii.nlargest(10, "avg_pts")[["player_name", "opponent", "games", "avg_pts", "avg_reb", "avg_ast"]]
    print(top_scorers.to_string(index=False))

    print(f"\n\nTotal matchup records: {len(df)}")
    print(f"Unique players: {df['player_id'].nunique()}")

    print("\n[TEST MODE] Not saving to database. Run without --test to save.")


def main():
    parser = argparse.ArgumentParser(description="Build player vs team history table")
    parser.add_argument("--test", action="store_true", help="Test mode (no save)")
    parser.add_argument("--min-games", type=int, default=3, help="Minimum games required")
    parser.add_argument("--season", type=str, default="2024-25", help="Season (e.g., 2024-25)")

    args = parser.parse_args()

    print(f"=== Building Player vs Team History ({args.season}) ===\n")

    # Build aggregations
    df = build_player_vs_team(min_games=args.min_games, season=args.season)

    if args.test:
        test_mode(df)
        return 0

    # Save to database
    count = save_to_db(df)

    print(f"\n=== SUCCESS ===")
    print(f"Saved {count} player-opponent matchup records")

    return 0


if __name__ == "__main__":
    sys.exit(main())
