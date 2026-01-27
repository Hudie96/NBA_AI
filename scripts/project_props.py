"""
Player Props Projections

Task 2.1 from AXIOM_ACTION_PLAN_v2.md
Projects player stat lines based on recent form + matchup.

Projection Formula:
- Last 10 games average (40%)
- Season average (30%)
- vs This Opponent average (20%)
- DvP adjustment (10%)

Usage:
    python scripts/project_props.py --player "LeBron James" --opponent LAC
    python scripts/project_props.py --games-today  # Project all players in today's games
    python scripts/project_props.py --test  # Test with sample projections
"""
import argparse
import sqlite3
import sys
from datetime import date
from pathlib import Path

import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]

# Stats we project (core + combo props)
STATS = ["PTS", "REB", "AST", "3PM", "PRA", "PR", "PA", "RA"]

# Extended stats available for projection
STATS_EXTENDED = [
    "PTS", "REB", "AST", "3PM",       # Core props
    "STL", "BLK", "TOV",              # Secondary props
    "PRA", "PR", "PA", "RA",          # Combo props
    "OREB", "DREB",                   # Rebound splits
    "FPT",                            # Fantasy points
]

# Column mapping for stats
STAT_COLS = {
    "PTS": "points",
    "REB": "rebounds",
    "AST": "assists",
    "3PM": "threes_made",
    "STL": "steals",
    "BLK": "blocks",
    "TOV": "turnovers",
    "OREB": "offensive_rebounds",
    "DREB": "defensive_rebounds",
    "PRA": "pts_reb_ast",
    "PR": "pts_reb",
    "PA": "pts_ast",
    "RA": "reb_ast",
    "FPT": "fantasy_points",
}


def get_player_position(player_name, conn):
    """Get player's position from roster data or infer from game logs."""
    # Try to find in a players table if exists, otherwise use a simple heuristic
    # For now, we'll need to fetch positions - let's store them
    try:
        df = pd.read_sql("""
            SELECT position FROM player_positions WHERE player_name = ?
        """, conn, params=(player_name,))
        if not df.empty:
            return df.iloc[0]["position"]
    except:
        pass

    # Default fallback - could be improved
    return "SF"


def get_last_n_games_avg(player_name, stat, conn, n=10):
    """Get player's average over last N games."""
    col = STAT_COLS.get(stat, stat.lower())

    df = pd.read_sql(f"""
        SELECT {col} as val
        FROM player_game_logs
        WHERE player_name = ?
        ORDER BY game_date DESC
        LIMIT ?
    """, conn, params=(player_name, n))

    if df.empty:
        return None
    return df["val"].mean()


def get_season_avg(player_name, stat, conn):
    """Get player's season average."""
    col = STAT_COLS.get(stat, stat.lower())

    df = pd.read_sql(f"""
        SELECT AVG({col}) as val
        FROM player_game_logs
        WHERE player_name = ?
    """, conn, params=(player_name,))

    if df.empty or df.iloc[0]["val"] is None:
        return None
    return df.iloc[0]["val"]


def get_vs_opponent_avg(player_name, opponent, stat, conn):
    """Get player's average vs specific opponent."""
    # Map stat to column(s) in player_vs_team
    stat_map = {
        "PTS": "avg_pts",
        "REB": "avg_reb",
        "AST": "avg_ast",
        "3PM": "avg_3pm",
    }

    # Handle combo stats
    if stat in ["PRA", "PR", "PA", "RA"]:
        df = pd.read_sql("""
            SELECT avg_pts, avg_reb, avg_ast, games
            FROM player_vs_team
            WHERE player_name = ? AND opponent = ?
        """, conn, params=(player_name, opponent))

        if df.empty:
            return None, 0

        row = df.iloc[0]
        if stat == "PRA":
            val = row["avg_pts"] + row["avg_reb"] + row["avg_ast"]
        elif stat == "PR":
            val = row["avg_pts"] + row["avg_reb"]
        elif stat == "PA":
            val = row["avg_pts"] + row["avg_ast"]
        elif stat == "RA":
            val = row["avg_reb"] + row["avg_ast"]
        return val, row["games"]

    # Individual stat
    col = stat_map.get(stat)
    if col is None:
        return None, 0

    df = pd.read_sql(f"""
        SELECT {col} as val, games
        FROM player_vs_team
        WHERE player_name = ? AND opponent = ?
    """, conn, params=(player_name, opponent))

    if df.empty:
        return None, 0
    return df.iloc[0]["val"], df.iloc[0]["games"]


def get_dvp_adjustment(opponent, position, stat, conn):
    """
    Get Defense vs Position adjustment.
    Returns the diff_from_avg (positive = opponent allows more than average).
    """
    # Handle combo stats by summing components
    if stat == "PRA":
        pts_adj = get_dvp_adjustment(opponent, position, "PTS", conn)
        reb_adj = get_dvp_adjustment(opponent, position, "REB", conn)
        ast_adj = get_dvp_adjustment(opponent, position, "AST", conn)
        return pts_adj + reb_adj + ast_adj
    elif stat == "PR":
        pts_adj = get_dvp_adjustment(opponent, position, "PTS", conn)
        reb_adj = get_dvp_adjustment(opponent, position, "REB", conn)
        return pts_adj + reb_adj
    elif stat == "PA":
        pts_adj = get_dvp_adjustment(opponent, position, "PTS", conn)
        ast_adj = get_dvp_adjustment(opponent, position, "AST", conn)
        return pts_adj + ast_adj
    elif stat == "RA":
        reb_adj = get_dvp_adjustment(opponent, position, "REB", conn)
        ast_adj = get_dvp_adjustment(opponent, position, "AST", conn)
        return reb_adj + ast_adj

    df = pd.read_sql("""
        SELECT diff_from_avg
        FROM defense_vs_position
        WHERE team = ? AND position = ? AND stat = ?
    """, conn, params=(opponent, position, stat))

    if df.empty:
        return 0.0
    return df.iloc[0]["diff_from_avg"]


def project_player_prop(player_name, opponent, stat, conn, position=None):
    """
    Generate projection for a player prop.

    Weights:
    - Last 10 games: 40%
    - Season avg: 30%
    - vs Opponent: 20%
    - DvP adjustment: 10%

    Returns:
        dict with projection details
    """
    # Get position if not provided
    if position is None:
        position = get_player_position(player_name, conn)

    # Get components
    last_10 = get_last_n_games_avg(player_name, stat, conn, n=10)
    season = get_season_avg(player_name, stat, conn)
    vs_opp, vs_opp_games = get_vs_opponent_avg(player_name, opponent, stat, conn)
    dvp_adj = get_dvp_adjustment(opponent, position, stat, conn)

    # Can't project without basic data
    if last_10 is None or season is None:
        return None

    # Build projection with available data
    # If no vs_opp data, redistribute weight to last_10 and season
    if vs_opp is None:
        # 40% last_10, 30% season, 0% vs_opp -> redistribute 20% to others
        # New weights: 50% last_10, 40% season, 10% dvp
        projection = (
            last_10 * 0.50 +
            season * 0.40 +
            (season + dvp_adj) * 0.10
        )
        vs_opp_weight = 0
    else:
        projection = (
            last_10 * 0.40 +
            season * 0.30 +
            vs_opp * 0.20 +
            (season + dvp_adj) * 0.10
        )
        vs_opp_weight = 0.20

    return {
        "player_name": player_name,
        "opponent": opponent,
        "position": position,
        "stat": stat,
        "projection": round(projection, 1),
        "last_10_avg": round(last_10, 1) if last_10 else None,
        "season_avg": round(season, 1) if season else None,
        "vs_opp_avg": round(vs_opp, 1) if vs_opp else None,
        "vs_opp_games": vs_opp_games,
        "dvp_adj": round(dvp_adj, 2),
        "dvp_rank": None,  # Could add rank lookup
    }


def get_top_usage_players(conn, limit=50):
    """Get top players by total minutes played (proxy for usage)."""
    df = pd.read_sql(f"""
        SELECT player_name,
               SUM(minutes) as total_min,
               COUNT(*) as games,
               AVG(points) as avg_pts
        FROM player_game_logs
        GROUP BY player_name
        HAVING games >= 20
        ORDER BY total_min DESC
        LIMIT ?
    """, conn, params=(limit,))
    return df


def project_all_stats(player_name, opponent, conn, position=None):
    """Project all stats for a player."""
    results = []
    for stat in STATS:
        proj = project_player_prop(player_name, opponent, stat, conn, position)
        if proj:
            results.append(proj)
    return results


def build_player_positions_table(conn):
    """Build player positions table from roster data if needed."""
    # Check if table exists
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='player_positions'
    """)

    if cursor.fetchone() is None:
        print("Building player_positions table from rosters...")
        from nba_api.stats.endpoints import commonteamroster
        from nba_api.stats.static import teams
        import time

        POSITION_MAP = {
            "G": "PG", "G-F": "SG", "F-G": "SF", "F": "SF",
            "F-C": "PF", "C-F": "PF", "C": "C",
            "PG": "PG", "SG": "SG", "SF": "SF", "PF": "PF"
        }

        all_teams = teams.get_teams()
        all_rosters = []

        for i, team in enumerate(all_teams):
            try:
                roster = commonteamroster.CommonTeamRoster(
                    team_id=team["id"],
                    season="2024-25"
                )
                df = roster.get_data_frames()[0]
                all_rosters.append(df[["PLAYER_ID", "PLAYER", "POSITION"]])
                if i < len(all_teams) - 1:
                    time.sleep(0.6)
            except:
                continue

        if all_rosters:
            combined = pd.concat(all_rosters, ignore_index=True)
            combined.columns = ["player_id", "player_name", "position_raw"]
            combined["position"] = combined["position_raw"].map(
                lambda x: POSITION_MAP.get(x, "SF")
            )
            combined[["player_id", "player_name", "position"]].to_sql(
                "player_positions", conn, if_exists="replace", index=False
            )
            print(f"  Saved {len(combined)} player positions")


def backtest_projections(conn, num_players=50, num_games=10):
    """
    Backtest projections against actual results.

    For each player's last N games:
    - Use only data available BEFORE that game to project
    - Compare projection to actual
    - Calculate accuracy metrics
    """
    print(f"=== BACKTESTING (top {num_players} players, last {num_games} games each) ===\n")

    build_player_positions_table(conn)

    # Get top usage players
    top_players = get_top_usage_players(conn, limit=num_players)

    results = []

    for _, player_row in top_players.iterrows():
        player_name = player_row["player_name"]

        # Get player's recent games
        games = pd.read_sql("""
            SELECT game_date, opponent, points, rebounds, assists, threes_made
            FROM player_game_logs
            WHERE player_name = ?
            ORDER BY game_date DESC
            LIMIT ?
        """, conn, params=(player_name, num_games + 10))  # Get extra for history

        if len(games) < num_games + 5:
            continue  # Need enough history

        # Get position
        pos = get_player_position(player_name, conn)

        # For each of the last N games, project using prior data
        for i in range(num_games):
            if i >= len(games) - 10:
                break

            game = games.iloc[i]
            game_date = game["game_date"]
            opponent = game["opponent"]

            # Get stats BEFORE this game for projection
            prior_games = games.iloc[i+1:i+11]  # 10 games before this one

            if len(prior_games) < 5:
                continue

            for stat in STATS:
                col = STAT_COLS[stat]
                actual = game[col]

                # Calculate projection components from prior data
                last_10_avg = prior_games[col].mean()
                season_avg = games.iloc[i+1:][col].mean()  # All prior games

                # Get vs opponent avg (simplified - use all historical)
                vs_opp_df = pd.read_sql("""
                    SELECT AVG(""" + col + """) as avg_val
                    FROM player_game_logs
                    WHERE player_name = ? AND opponent = ? AND game_date < ?
                """, conn, params=(player_name, opponent, game_date))

                vs_opp = vs_opp_df.iloc[0]["avg_val"] if not vs_opp_df.empty and vs_opp_df.iloc[0]["avg_val"] else None

                # DVP adjustment (static for simplicity)
                dvp_adj = get_dvp_adjustment(opponent, pos, stat, conn)

                # Calculate projection
                if vs_opp is not None:
                    projection = (
                        last_10_avg * 0.40 +
                        season_avg * 0.30 +
                        vs_opp * 0.20 +
                        (season_avg + dvp_adj) * 0.10
                    )
                else:
                    projection = (
                        last_10_avg * 0.50 +
                        season_avg * 0.40 +
                        (season_avg + dvp_adj) * 0.10
                    )

                # Calculate error
                if actual > 0:
                    error_pct = abs(projection - actual) / actual * 100
                else:
                    error_pct = 0 if projection < 1 else 100

                results.append({
                    "player": player_name,
                    "stat": stat,
                    "projection": round(projection, 1),
                    "actual": actual,
                    "error_pct": round(error_pct, 1),
                    "within_15": error_pct <= 15
                })

    if not results:
        print("No backtest results generated")
        return

    df = pd.DataFrame(results)

    # Summary stats
    print("=== BACKTEST RESULTS ===\n")

    for stat in STATS:
        stat_df = df[df["stat"] == stat]
        if stat_df.empty:
            continue

        within_15 = stat_df["within_15"].mean() * 100
        avg_error = stat_df["error_pct"].mean()
        median_error = stat_df["error_pct"].median()

        print(f"{stat}:")
        print(f"  Samples: {len(stat_df)}")
        print(f"  Within 15%: {within_15:.1f}%")
        print(f"  Avg Error: {avg_error:.1f}%")
        print(f"  Median Error: {median_error:.1f}%")
        print()

    # Overall
    overall_within_15 = df["within_15"].mean() * 100
    overall_avg_error = df["error_pct"].mean()

    print(f"OVERALL:")
    print(f"  Total samples: {len(df)}")
    print(f"  Within 15%: {overall_within_15:.1f}%")
    print(f"  Avg Error: {overall_avg_error:.1f}%")

    return df


def test_projections(conn):
    """Test projections with sample players."""
    print("=== TEST MODE ===\n")

    # Build positions table if needed
    build_player_positions_table(conn)

    # Test with known players
    test_cases = [
        ("LeBron James", "LAC"),
        ("Stephen Curry", "LAL"),
        ("Jayson Tatum", "MIA"),
        ("Luka Doncic", "PHX"),
    ]

    for player, opp in test_cases:
        print(f"\n{player} vs {opp}:")
        projections = project_all_stats(player, opp, conn)
        if projections:
            for p in projections:
                print(f"  {p['stat']}: {p['projection']} "
                      f"(L10: {p['last_10_avg']}, Szn: {p['season_avg']}, "
                      f"vsOpp: {p['vs_opp_avg']} in {p['vs_opp_games']}g, "
                      f"DVP: {p['dvp_adj']:+.1f})")
        else:
            print("  No data available")

    # Show top usage players
    print("\n\nTop 10 Usage Players (by minutes):")
    top = get_top_usage_players(conn, limit=10)
    print(top.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(description="Project player props")
    parser.add_argument("--player", type=str, help="Player name")
    parser.add_argument("--opponent", type=str, help="Opponent team abbreviation")
    parser.add_argument("--stat", type=str, default="PTS", help="Stat to project")
    parser.add_argument("--test", action="store_true", help="Test mode")
    parser.add_argument("--backtest", action="store_true", help="Run backtest")
    parser.add_argument("--top", type=int, default=50, help="Project top N players")

    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)

    if args.backtest:
        backtest_projections(conn, num_players=args.top, num_games=10)
        conn.close()
        return 0

    if args.test:
        test_projections(conn)
        conn.close()
        return 0

    if args.player and args.opponent:
        # Single player projection
        build_player_positions_table(conn)
        projections = project_all_stats(args.player, args.opponent, conn)

        if projections:
            print(f"\n{args.player} vs {args.opponent} Projections:")
            print("-" * 50)
            for p in projections:
                print(f"{p['stat']:>4}: {p['projection']:>5.1f}  "
                      f"(L10: {p['last_10_avg']}, Szn: {p['season_avg']}, "
                      f"DVP: {p['dvp_adj']:+.1f})")
        else:
            print(f"No data available for {args.player}")
    else:
        print("Usage: python project_props.py --player 'LeBron James' --opponent LAC")
        print("       python project_props.py --test")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
