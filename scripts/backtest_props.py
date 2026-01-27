"""
Props Model Backtest

Task 4.2 from AXIOM_ACTION_PLAN_v2.md
Validates props model using historical data with NO lookahead bias.

For each game:
1. Generate projection using ONLY data available before that game
2. Compare to actual result
3. Calculate hit rates by stat type and confidence level

Usage:
    python scripts/backtest_props.py
    python scripts/backtest_props.py --players 100 --games 20
"""
import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from scipy import stats
import numpy as np

import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]

STATS = ["PTS", "REB", "AST", "3PM"]
STAT_COLS = {
    "PTS": "points",
    "REB": "rebounds",
    "AST": "assists",
    "3PM": "threes_made"
}


def get_projection_components(player_name, game_date, opponent, stat, conn):
    """
    Get projection components using ONLY data before game_date.
    Returns None if insufficient data.
    """
    col = STAT_COLS[stat]

    # Get all games BEFORE this date
    prior_games = pd.read_sql(f"""
        SELECT game_date, opponent, {col} as value
        FROM player_game_logs
        WHERE player_name = ? AND game_date < ?
        ORDER BY game_date DESC
    """, conn, params=(player_name, game_date))

    if len(prior_games) < 10:
        return None  # Need at least 10 games for projection

    # Last 10 games average
    last_10 = prior_games.head(10)["value"].mean()

    # Season average (all prior games)
    season_avg = prior_games["value"].mean()

    # vs Opponent average
    vs_opp = prior_games[prior_games["opponent"] == opponent]
    vs_opp_avg = vs_opp["value"].mean() if len(vs_opp) >= 1 else None
    vs_opp_games = len(vs_opp)

    # DVP adjustment (static - would need historical DVP for true backtest)
    # For simplicity, using 0 since we don't have point-in-time DVP
    dvp_adj = 0

    return {
        "last_10": last_10,
        "season_avg": season_avg,
        "vs_opp_avg": vs_opp_avg,
        "vs_opp_games": vs_opp_games,
        "dvp_adj": dvp_adj,
        "total_games": len(prior_games)
    }


def calculate_projection(components):
    """Calculate projection using our weighted formula."""
    if components is None:
        return None

    last_10 = components["last_10"]
    season = components["season_avg"]
    vs_opp = components["vs_opp_avg"]
    dvp_adj = components["dvp_adj"]

    if vs_opp is not None and components["vs_opp_games"] >= 2:
        # Full formula with vs opponent data
        projection = (
            last_10 * 0.40 +
            season * 0.30 +
            vs_opp * 0.20 +
            (season + dvp_adj) * 0.10
        )
    else:
        # Simplified formula without vs opponent
        projection = (
            last_10 * 0.50 +
            season * 0.40 +
            (season + dvp_adj) * 0.10
        )

    return projection


def calculate_confidence(edge_pct, vs_opp_games, total_games):
    """Calculate confidence level for a projection."""
    edge_pct = abs(edge_pct)

    if edge_pct >= 10 and vs_opp_games >= 3 and total_games >= 30:
        return "HIGH"
    elif edge_pct >= 5 and total_games >= 20:
        return "MEDIUM"
    elif edge_pct >= 3:
        return "LOW"
    else:
        return "NONE"


def run_backtest(conn, num_players=50, games_per_player=15):
    """
    Run backtest on top players.

    For each game, we simulate:
    - What our projection would have been (using only prior data)
    - What a "market line" would be (using season avg as proxy)
    - Whether OVER or UNDER would have hit
    """
    print(f"=== PROPS MODEL BACKTEST ===")
    print(f"Players: {num_players} | Games per player: {games_per_player}")
    print(f"Using only data available BEFORE each game (no lookahead)\n")

    # Get top players by games played
    top_players = pd.read_sql(f"""
        SELECT player_name, COUNT(*) as games
        FROM player_game_logs
        GROUP BY player_name
        HAVING games >= 30
        ORDER BY games DESC
        LIMIT ?
    """, conn, params=(num_players,))

    results = []

    for _, player_row in top_players.iterrows():
        player_name = player_row["player_name"]

        # Get player's games (need enough history)
        all_games = pd.read_sql("""
            SELECT game_date, opponent, points, rebounds, assists, threes_made
            FROM player_game_logs
            WHERE player_name = ?
            ORDER BY game_date DESC
        """, conn, params=(player_name,))

        if len(all_games) < 25:
            continue

        # Test on most recent N games (excluding first 10 which we need for history)
        test_games = all_games.iloc[:games_per_player]

        for _, game in test_games.iterrows():
            game_date = game["game_date"]
            opponent = game["opponent"]

            for stat in STATS:
                col = STAT_COLS[stat]
                actual = game[col]

                # Get projection using only prior data
                components = get_projection_components(
                    player_name, game_date, opponent, stat, conn
                )

                if components is None:
                    continue

                projection = calculate_projection(components)

                # Use season average as proxy for "market line"
                # (In reality, lines are set by sharp money, but this is our best proxy)
                market_line = components["season_avg"]

                # Calculate edge
                edge = projection - market_line
                edge_pct = (edge / market_line * 100) if market_line > 0 else 0

                # Determine confidence
                confidence = calculate_confidence(
                    edge_pct,
                    components["vs_opp_games"],
                    components["total_games"]
                )

                # Determine pick direction based on projection vs line
                if abs(edge_pct) < 3:
                    pick = "NO_BET"  # Edge too small
                elif projection > market_line:
                    pick = "OVER"
                else:
                    pick = "UNDER"

                # Determine result
                if pick == "NO_BET":
                    result = "NO_BET"
                elif pick == "OVER":
                    result = "WIN" if actual > market_line else "LOSS"
                else:  # UNDER
                    result = "WIN" if actual < market_line else "LOSS"

                # Calculate projection accuracy
                if actual > 0:
                    accuracy_pct = abs(projection - actual) / actual * 100
                else:
                    accuracy_pct = 100 if projection > 1 else 0

                results.append({
                    "player": player_name,
                    "game_date": game_date,
                    "opponent": opponent,
                    "stat": stat,
                    "actual": actual,
                    "projection": round(projection, 1),
                    "market_line": round(market_line, 1),
                    "edge": round(edge, 1),
                    "edge_pct": round(edge_pct, 1),
                    "pick": pick,
                    "confidence": confidence,
                    "result": result,
                    "accuracy_pct": round(accuracy_pct, 1),
                    "within_10pct": accuracy_pct <= 10,
                    "within_15pct": accuracy_pct <= 15,
                    "within_20pct": accuracy_pct <= 20,
                })

    return pd.DataFrame(results)


def analyze_results(df):
    """Analyze backtest results and calculate statistics."""

    print("=" * 60)
    print("BACKTEST RESULTS")
    print("=" * 60)

    # Filter to actual bets (exclude NO_BET)
    bets = df[df["pick"] != "NO_BET"].copy()

    # 1. Overall Projection Accuracy
    print("\n1. PROJECTION ACCURACY (all projections)")
    print("-" * 40)
    print(f"Total projections: {len(df)}")
    print(f"Within 10% of actual: {df['within_10pct'].mean()*100:.1f}%")
    print(f"Within 15% of actual: {df['within_15pct'].mean()*100:.1f}%")
    print(f"Within 20% of actual: {df['within_20pct'].mean()*100:.1f}%")
    print(f"Mean absolute error: {df['accuracy_pct'].mean():.1f}%")
    print(f"Median absolute error: {df['accuracy_pct'].median():.1f}%")

    # 2. Over/Under Hit Rate
    print("\n2. OVER/UNDER HIT RATE (edge >= 3%)")
    print("-" * 40)

    if len(bets) > 0:
        wins = (bets["result"] == "WIN").sum()
        losses = (bets["result"] == "LOSS").sum()
        total = wins + losses
        win_pct = wins / total * 100 if total > 0 else 0

        print(f"Total bets: {total}")
        print(f"Record: {wins}W - {losses}L ({win_pct:.1f}%)")
        print(f"Break-even (vs -110): 52.4%")
        print(f"Edge vs break-even: {win_pct - 52.4:+.1f}%")

        # Statistical significance
        if total >= 30:
            # Binomial test against 52.4% (break-even)
            try:
                result = stats.binomtest(wins, total, 0.524, alternative='greater')
                p_value = result.pvalue
            except AttributeError:
                # Older scipy version
                p_value = stats.binom_test(wins, total, 0.524, alternative='greater')
            print(f"P-value (vs 52.4%): {p_value:.4f}")
            print(f"Statistically significant: {'YES' if p_value < 0.05 else 'NO'}")
    else:
        print("No bets met edge threshold")

    # 3. Hit Rate by Stat Type
    print("\n3. HIT RATE BY STAT TYPE")
    print("-" * 40)

    for stat in STATS:
        stat_bets = bets[bets["stat"] == stat]
        if len(stat_bets) > 0:
            wins = (stat_bets["result"] == "WIN").sum()
            losses = (stat_bets["result"] == "LOSS").sum()
            total = wins + losses
            win_pct = wins / total * 100 if total > 0 else 0

            # Accuracy for this stat
            stat_all = df[df["stat"] == stat]
            within_15 = stat_all["within_15pct"].mean() * 100

            print(f"{stat:>4}: {wins}W-{losses}L ({win_pct:.1f}%) | Accuracy within 15%: {within_15:.1f}% | n={total}")

    # 4. Hit Rate by Confidence Level
    print("\n4. HIT RATE BY CONFIDENCE LEVEL")
    print("-" * 40)

    for conf in ["HIGH", "MEDIUM", "LOW"]:
        conf_bets = bets[bets["confidence"] == conf]
        if len(conf_bets) > 0:
            wins = (conf_bets["result"] == "WIN").sum()
            losses = (conf_bets["result"] == "LOSS").sum()
            total = wins + losses
            win_pct = wins / total * 100 if total > 0 else 0

            # Average edge size
            avg_edge = conf_bets["edge_pct"].abs().mean()

            print(f"{conf:>6}: {wins}W-{losses}L ({win_pct:.1f}%) | Avg edge: {avg_edge:.1f}% | n={total}")

    # 5. Hit Rate by Edge Size
    print("\n5. HIT RATE BY EDGE SIZE")
    print("-" * 40)

    edge_buckets = [
        ("3-5%", 3, 5),
        ("5-10%", 5, 10),
        ("10-15%", 10, 15),
        ("15%+", 15, 100),
    ]

    for label, min_edge, max_edge in edge_buckets:
        bucket = bets[(bets["edge_pct"].abs() >= min_edge) & (bets["edge_pct"].abs() < max_edge)]
        if len(bucket) > 0:
            wins = (bucket["result"] == "WIN").sum()
            losses = (bucket["result"] == "LOSS").sum()
            total = wins + losses
            win_pct = wins / total * 100 if total > 0 else 0
            print(f"{label:>6}: {wins}W-{losses}L ({win_pct:.1f}%) | n={total}")

    # 6. Verdict
    print("\n" + "=" * 60)
    print("VERDICT")
    print("=" * 60)

    if len(bets) >= 100:
        overall_win_pct = (bets["result"] == "WIN").sum() / len(bets) * 100

        if overall_win_pct >= 55:
            print("EDGE DETECTED: Model shows profitable signal")
            print(f"Expected ROI: ~{(overall_win_pct - 52.4) * 2:.1f}% (assuming -110 odds)")
        elif overall_win_pct >= 52.4:
            print("MARGINAL EDGE: Model slightly beats break-even")
            print("Recommend: More data needed, be selective")
        else:
            print("NO EDGE: Model does not beat break-even")
            print("Recommend: Refine model or find different signals")
    else:
        print("INSUFFICIENT DATA: Need 100+ bets for reliable conclusion")

    return bets


def main():
    parser = argparse.ArgumentParser(description="Backtest props model")
    parser.add_argument("--players", type=int, default=50, help="Number of players to test")
    parser.add_argument("--games", type=int, default=15, help="Games per player to test")

    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)

    # Run backtest
    results_df = run_backtest(conn, args.players, args.games)

    if results_df.empty:
        print("No results generated. Check data.")
        conn.close()
        return 1

    # Analyze
    analyze_results(results_df)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
