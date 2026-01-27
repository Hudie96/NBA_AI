"""
Fetch Player Game Logs from NBA.com Stats API

Task 1.1 from AXIOM_ACTION_PLAN_v2.md
Fetches historical player stats and calculates B2B/rest days.

Usage:
    python scripts/fetch_player_logs.py
    python scripts/fetch_player_logs.py --season 2024-25
    python scripts/fetch_player_logs.py --test  # Test with small sample
"""
import argparse
import sqlite3
import sys
import time
from pathlib import Path

import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

# Use nba_api for reliable NBA.com access
from nba_api.stats.endpoints import playergamelogs

DB_PATH = config["database"]["path"]


def fetch_player_game_logs(season="2024-25", season_type="Regular Season"):
    """
    Fetch all player game logs for a season from NBA.com

    Args:
        season: Season string (e.g., '2024-25')
        season_type: 'Regular Season' or 'Playoffs'

    Returns:
        DataFrame with player game logs
    """
    print(f"Fetching player game logs for {season} {season_type}...")

    try:
        # Use nba_api which handles headers and rate limiting
        logs = playergamelogs.PlayerGameLogs(
            season_nullable=season,
            season_type_nullable=season_type
        )
        df = logs.get_data_frames()[0]
        print(f"Fetched {len(df)} player game log rows")
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None


def transform_game_logs(df):
    """
    Transform raw NBA.com data to our schema.

    Args:
        df: Raw DataFrame from API

    Returns:
        Transformed DataFrame matching player_game_logs schema
    """
    # Extract opponent from matchup string
    def get_opponent(matchup):
        return matchup.split(" ")[-1]

    def get_home_away(matchup):
        return "HOME" if "vs." in matchup else "AWAY"

    # Map columns
    transformed = pd.DataFrame({
        "player_id": df["PLAYER_ID"].astype(str),
        "player_name": df["PLAYER_NAME"],
        "game_id": df["GAME_ID"],
        "game_date": pd.to_datetime(df["GAME_DATE"]).dt.strftime("%Y-%m-%d"),
        "team": df["TEAM_ABBREVIATION"],
        "opponent": df["MATCHUP"].apply(get_opponent),
        "home_away": df["MATCHUP"].apply(get_home_away),
        "minutes": df["MIN"],
        "points": df["PTS"],
        "rebounds": df["REB"],
        "assists": df["AST"],
        "steals": df["STL"],
        "blocks": df["BLK"],
        "turnovers": df["TOV"],
        "threes_made": df["FG3M"],
        "threes_attempted": df["FG3A"],
        "fg_made": df["FGM"],
        "fg_attempted": df["FGA"],
        "ft_made": df["FTM"],
        "ft_attempted": df["FTA"],
        "plus_minus": df["PLUS_MINUS"],
    })

    return transformed


def calculate_rest_days(df):
    """
    Calculate is_b2b and days_rest for each player game.

    Args:
        df: DataFrame with player_id, game_date columns

    Returns:
        DataFrame with is_b2b and days_rest columns added
    """
    print("Calculating rest days and B2B flags...")

    # Sort by player and date
    df = df.sort_values(["player_id", "game_date"]).copy()

    # Convert game_date to datetime for calculation
    df["game_date_dt"] = pd.to_datetime(df["game_date"])

    # Calculate days since last game for each player
    df["prev_game_date"] = df.groupby("player_id")["game_date_dt"].shift(1)
    df["days_rest"] = (df["game_date_dt"] - df["prev_game_date"]).dt.days

    # Fill NaN (first game of season) with reasonable default
    df["days_rest"] = df["days_rest"].fillna(3).astype(int)

    # Cap at reasonable max (14 days = likely injury/absence)
    df["days_rest"] = df["days_rest"].clip(upper=14)

    # B2B = played yesterday (days_rest = 1)
    df["is_b2b"] = (df["days_rest"] == 1).astype(int)

    # Clean up temp columns
    df = df.drop(columns=["game_date_dt", "prev_game_date"])

    b2b_count = df["is_b2b"].sum()
    print(f"Found {b2b_count} back-to-back games ({b2b_count/len(df)*100:.1f}%)")

    return df


def save_to_db(df, db_path=DB_PATH):
    """
    Save player game logs to database.

    Args:
        df: DataFrame to save
        db_path: Path to SQLite database

    Returns:
        Number of rows inserted
    """
    print(f"Saving {len(df)} rows to database...")

    conn = sqlite3.connect(db_path)

    # Insert or replace (handles duplicates)
    df.to_sql(
        "player_game_logs",
        conn,
        if_exists="replace",
        index=False
    )

    # Verify
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM player_game_logs")
    count = cursor.fetchone()[0]

    conn.close()

    print(f"Saved {count} rows to player_game_logs table")
    return count


def test_fetch():
    """Test fetch with a small sample."""
    print("=== TEST MODE ===")

    # Fetch data
    df = fetch_player_game_logs(season="2024-25")
    if df is None:
        print("Failed to fetch data")
        return False

    # Transform
    df = transform_game_logs(df)

    # Show sample
    print("\nSample data:")
    sample_cols = ["player_name", "game_date", "team", "opponent", "points", "rebounds", "assists"]
    print(df[sample_cols].head(10))

    # Calculate rest
    df = calculate_rest_days(df)

    # Show B2B sample
    b2b_games = df[df["is_b2b"] == 1]
    print(f"\nB2B games sample:")
    print(b2b_games[["player_name", "game_date", "team", "days_rest", "is_b2b"]].head(5))

    # Don't save in test mode
    print("\n[TEST MODE] Not saving to database. Run without --test to save.")

    return True


def main():
    parser = argparse.ArgumentParser(description="Fetch player game logs from NBA.com")
    parser.add_argument("--season", type=str, default="2024-25", help="Season (e.g., 2024-25)")
    parser.add_argument("--test", action="store_true", help="Test mode (no save)")

    args = parser.parse_args()

    if args.test:
        success = test_fetch()
        return 0 if success else 1

    # Full fetch
    print(f"=== Fetching {args.season} Player Game Logs ===\n")

    # Fetch
    df = fetch_player_game_logs(season=args.season)
    if df is None:
        print("Failed to fetch data")
        return 1

    # Transform
    df = transform_game_logs(df)

    # Calculate rest days
    df = calculate_rest_days(df)

    # Save
    count = save_to_db(df)

    # Summary
    print(f"\n=== SUCCESS ===")
    print(f"Loaded {count} player game logs for {args.season}")
    print(f"Unique players: {df['player_id'].nunique()}")
    print(f"Date range: {df['game_date'].min()} to {df['game_date'].max()}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
