"""
Fetch Defense vs Position (DVP) Rankings

Task 1.2 from AXIOM_ACTION_PLAN_v2.md
Calculates which defenses are weak vs which positions.

Usage:
    python scripts/fetch_dvp.py
    python scripts/fetch_dvp.py --test  # Test with sample output
"""
import argparse
import sqlite3
import sys
import time
from datetime import date
from pathlib import Path

import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config
from nba_api.stats.endpoints import commonteamroster
from nba_api.stats.static import teams

DB_PATH = config["database"]["path"]

# Position mapping: normalize various position strings to standard 5
POSITION_MAP = {
    "G": "PG",      # Guard -> Point Guard (default)
    "G-F": "SG",    # Guard-Forward -> Shooting Guard
    "F-G": "SF",    # Forward-Guard -> Small Forward
    "F": "SF",      # Forward -> Small Forward (default)
    "F-C": "PF",    # Forward-Center -> Power Forward
    "C-F": "PF",    # Center-Forward -> Power Forward
    "C": "C",       # Center
    "PG": "PG",
    "SG": "SG",
    "SF": "SF",
    "PF": "PF",
}


def fetch_all_rosters(season="2024-25"):
    """
    Fetch rosters for all 30 NBA teams to get player positions.

    Returns:
        DataFrame with player_id, player_name, position columns
    """
    print("Fetching team rosters for player positions...")

    all_teams = teams.get_teams()
    all_rosters = []

    for i, team in enumerate(all_teams):
        team_id = team["id"]
        team_abbr = team["abbreviation"]

        try:
            roster = commonteamroster.CommonTeamRoster(
                team_id=team_id,
                season=season
            )
            df = roster.get_data_frames()[0]
            df["team"] = team_abbr
            all_rosters.append(df[["PLAYER_ID", "PLAYER", "POSITION", "team"]])

            # Rate limiting - NBA.com can block rapid requests
            if i < len(all_teams) - 1:
                time.sleep(0.6)

        except Exception as e:
            print(f"  Warning: Could not fetch {team_abbr} roster: {e}")
            continue

    if not all_rosters:
        print("Failed to fetch any rosters")
        return None

    combined = pd.concat(all_rosters, ignore_index=True)
    combined.columns = ["player_id", "player_name", "position_raw", "team"]
    combined["player_id"] = combined["player_id"].astype(str)

    # Normalize positions to standard 5
    combined["position"] = combined["position_raw"].map(
        lambda x: POSITION_MAP.get(x, "SF")  # Default to SF if unknown
    )

    print(f"Fetched {len(combined)} players from {len(all_rosters)} teams")
    return combined[["player_id", "player_name", "position"]]


def calculate_dvp(player_positions, db_path=DB_PATH, season="2024-25"):
    """
    Calculate Defense vs Position stats from player game logs.

    Args:
        player_positions: DataFrame with player_id, position
        db_path: Path to database with player_game_logs
        season: Season string for date filtering

    Returns:
        DataFrame with DVP stats
    """
    # Parse season to date range
    start_year = int(season.split("-")[0])
    season_start = f"{start_year}-10-01"
    season_end = f"{start_year + 1}-06-30"

    print(f"Calculating defense vs position stats for {season}...")
    print(f"  Date range: {season_start} to {season_end}")

    conn = sqlite3.connect(db_path)

    # Get player game logs for current season only
    logs = pd.read_sql(f"""
        SELECT player_id, opponent, points, rebounds, assists, threes_made
        FROM player_game_logs
        WHERE game_date >= '{season_start}' AND game_date <= '{season_end}'
    """, conn)
    conn.close()

    # Join with positions
    logs["player_id"] = logs["player_id"].astype(str)
    logs = logs.merge(player_positions, on="player_id", how="left")

    # Drop players without position data
    logs = logs.dropna(subset=["position"])
    print(f"  Matched {len(logs)} game logs with positions")

    # Calculate stats allowed by each team (opponent) vs each position
    # Group by opponent (defending team) and position
    dvp_stats = logs.groupby(["opponent", "position"]).agg({
        "points": "mean",
        "rebounds": "mean",
        "assists": "mean",
        "threes_made": "mean"
    }).reset_index()

    dvp_stats.columns = ["team", "position", "pts_allowed", "reb_allowed", "ast_allowed", "3pm_allowed"]

    return dvp_stats


def calculate_rankings(dvp_stats):
    """
    Calculate league averages, diff from avg, and rankings.

    Args:
        dvp_stats: DataFrame with team, position, and stat columns

    Returns:
        DataFrame in final schema format
    """
    print("Calculating rankings...")

    stats = ["pts", "reb", "ast", "3pm"]
    stat_cols = [f"{s}_allowed" for s in stats]

    # Calculate league averages by position
    league_avgs = dvp_stats.groupby("position")[stat_cols].mean()

    results = []

    for stat in stats:
        col = f"{stat}_allowed"

        for position in dvp_stats["position"].unique():
            pos_data = dvp_stats[dvp_stats["position"] == position].copy()
            league_avg = league_avgs.loc[position, col]

            pos_data["diff_from_avg"] = pos_data[col] - league_avg

            # Rank: 1 = allows most (worst defense)
            pos_data["rank"] = pos_data[col].rank(ascending=False, method="min").astype(int)

            for _, row in pos_data.iterrows():
                results.append({
                    "team": row["team"],
                    "position": position,
                    "stat": stat.upper().replace("3PM", "3PM"),
                    "avg_allowed": round(row[col], 2),
                    "league_avg": round(league_avg, 2),
                    "diff_from_avg": round(row["diff_from_avg"], 2),
                    "rank": row["rank"],
                    "updated_date": date.today().isoformat()
                })

    return pd.DataFrame(results)


def save_to_db(df, db_path=DB_PATH):
    """Save DVP data to database."""
    print(f"Saving {len(df)} rows to database...")

    conn = sqlite3.connect(db_path)

    df.to_sql(
        "defense_vs_position",
        conn,
        if_exists="replace",
        index=False
    )

    # Verify
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM defense_vs_position")
    count = cursor.fetchone()[0]

    conn.close()

    print(f"Saved {count} rows to defense_vs_position table")
    return count


def test_mode(df):
    """Display sample data without saving."""
    print("\n=== TEST MODE ===\n")

    # Show worst defenses vs PG scoring
    print("Worst defenses vs PG scoring (PTS):")
    pg_pts = df[(df["position"] == "PG") & (df["stat"] == "PTS")].sort_values("rank")
    print(pg_pts[["team", "avg_allowed", "league_avg", "diff_from_avg", "rank"]].head(10).to_string(index=False))

    print("\nBest defenses vs C scoring (PTS):")
    c_pts = df[(df["position"] == "C") & (df["stat"] == "PTS")].sort_values("rank", ascending=False)
    print(c_pts[["team", "avg_allowed", "league_avg", "diff_from_avg", "rank"]].head(10).to_string(index=False))

    print(f"\nTotal rows: {len(df)}")
    print(f"Teams: {df['team'].nunique()}")
    print(f"Positions: {df['position'].unique().tolist()}")
    print(f"Stats: {df['stat'].unique().tolist()}")

    print("\n[TEST MODE] Not saving to database. Run without --test to save.")


def main():
    parser = argparse.ArgumentParser(description="Fetch Defense vs Position rankings")
    parser.add_argument("--test", action="store_true", help="Test mode (no save)")
    parser.add_argument("--season", type=str, default="2024-25", help="Season")

    args = parser.parse_args()

    print(f"=== Fetching DVP Rankings for {args.season} ===\n")

    # Step 1: Get player positions from rosters
    positions = fetch_all_rosters(season=args.season)
    if positions is None:
        return 1

    # Step 2: Calculate DVP from game logs
    dvp_stats = calculate_dvp(positions, season=args.season)

    # Step 3: Calculate rankings
    final_df = calculate_rankings(dvp_stats)

    if args.test:
        test_mode(final_df)
        return 0

    # Step 4: Save to database
    count = save_to_db(final_df)

    print(f"\n=== SUCCESS ===")
    print(f"Saved {count} DVP records")
    print(f"Expected: 30 teams x 5 positions x 4 stats = 600 rows")

    return 0


if __name__ == "__main__":
    sys.exit(main())
