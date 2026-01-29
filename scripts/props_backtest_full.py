"""
AXIOM Full Props Backtest

Comprehensive backtest of player props at 15%+ edge threshold.
Tests: PTS, AST, combo props (PRA, PR, PA, RA)

Reports:
- Hit rate by stat type
- Monthly breakdown (decay check)
- ROI at -110
- Statistical significance
"""

import sqlite3
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]

# Stats to test (user-requested focus)
STAT_COLS = {
    "PTS": "points",
    "AST": "assists",
    "PRA": "pts_reb_ast",
    "PR": "pts_reb",
    "PA": "pts_ast",
    "RA": "reb_ast",
}

# Categorize stats
S_TIER = ["PTS", "AST"]  # Most reliable
A_TIER = ["PRA", "PR", "PA", "RA"]  # Combo props


def get_projection(player_name, game_date, opponent, stat, conn):
    """Get projection using only data before game_date."""
    col = STAT_COLS[stat]

    prior_games = pd.read_sql(f"""
        SELECT game_date, opponent, {col} as value
        FROM player_game_logs
        WHERE player_name = ? AND game_date < ?
        ORDER BY game_date DESC
    """, conn, params=(player_name, game_date))

    if len(prior_games) < 10:
        return None, None

    # Components
    last_10 = prior_games.head(10)["value"].mean()
    season_avg = prior_games["value"].mean()

    vs_opp = prior_games[prior_games["opponent"] == opponent]
    vs_opp_avg = vs_opp["value"].mean() if len(vs_opp) >= 2 else None
    vs_opp_games = len(vs_opp)

    # Projection formula
    if vs_opp_avg is not None:
        projection = last_10 * 0.40 + season_avg * 0.30 + vs_opp_avg * 0.20 + season_avg * 0.10
    else:
        projection = last_10 * 0.50 + season_avg * 0.50

    # Use season avg as market line proxy
    market_line = season_avg

    return projection, market_line


def run_full_backtest():
    """Run comprehensive props backtest."""
    conn = sqlite3.connect(DB_PATH)

    print("=" * 70)
    print("AXIOM FULL PROPS BACKTEST")
    print("=" * 70)

    # Get date range
    date_range = pd.read_sql("""
        SELECT MIN(game_date) as min_date, MAX(game_date) as max_date
        FROM player_game_logs
    """, conn)
    print(f"\nData range: {date_range.iloc[0]['min_date']} to {date_range.iloc[0]['max_date']}")

    # Get top players (enough games for reliable projection)
    top_players = pd.read_sql("""
        SELECT player_name, COUNT(*) as games
        FROM player_game_logs
        GROUP BY player_name
        HAVING games >= 30
        ORDER BY games DESC
        LIMIT 150
    """, conn)

    print(f"Testing {len(top_players)} players with 30+ games")
    print(f"Stats: {list(STAT_COLS.keys())}")
    print(f"Edge threshold: 15%+")
    print()

    results = []

    for idx, player_row in top_players.iterrows():
        player_name = player_row["player_name"]

        # Get all games for this player
        all_games = pd.read_sql("""
            SELECT game_date, opponent, points, assists,
                   pts_reb_ast, pts_reb, pts_ast, reb_ast
            FROM player_game_logs
            WHERE player_name = ?
            ORDER BY game_date
        """, conn, params=(player_name,))

        if len(all_games) < 25:
            continue

        # Test on games 15+ (need history)
        test_games = all_games.iloc[15:]

        for _, game in test_games.iterrows():
            game_date = game["game_date"]
            opponent = game["opponent"]

            for stat in STAT_COLS.keys():
                col = STAT_COLS[stat]
                actual = game[col]

                projection, market_line = get_projection(
                    player_name, game_date, opponent, stat, conn
                )

                if projection is None or market_line is None or market_line == 0:
                    continue

                # Calculate edge
                edge_pct = (projection - market_line) / market_line * 100

                # Only 15%+ edges
                if abs(edge_pct) < 15:
                    continue

                # Determine pick and result
                if edge_pct > 0:
                    pick = "OVER"
                    hit = actual > market_line
                else:
                    pick = "UNDER"
                    hit = actual < market_line

                # Get month for decay analysis
                month = game_date[:7] if isinstance(game_date, str) else game_date.strftime("%Y-%m")

                tier = "S_TIER" if stat in S_TIER else "A_TIER"

                results.append({
                    "player": player_name,
                    "game_date": game_date,
                    "month": month,
                    "stat": stat,
                    "tier": tier,
                    "actual": actual,
                    "projection": projection,
                    "line": market_line,
                    "edge_pct": edge_pct,
                    "pick": pick,
                    "hit": hit
                })

    df = pd.DataFrame(results)

    if df.empty:
        print("No results found!")
        conn.close()
        return

    # ==================== RESULTS ====================
    print("=" * 70)
    print("RESULTS: 15%+ EDGE PROPS")
    print("=" * 70)

    total = len(df)
    wins = df["hit"].sum()
    hit_rate = wins / total * 100

    # Break-even at -110 is 52.38%
    breakeven = 52.38
    roi = (hit_rate / 100 * 2.0909 - 1) * 100  # payout at -110 is 1.909

    # P-value
    p_value = stats.binomtest(wins, total, 0.5, alternative='greater').pvalue

    print(f"\n[OVERALL]")
    print(f"  Total bets: {total}")
    print(f"  Record: {wins}W - {total-wins}L ({hit_rate:.1f}%)")
    print(f"  Break-even: {breakeven:.1f}%")
    print(f"  ROI at -110: {roi:+.1f}%")
    print(f"  P-value: {p_value:.4f}")
    print(f"  Significant (p<0.05): {'YES' if p_value < 0.05 else 'NO'}")

    # By stat type
    print(f"\n[BY STAT TYPE]")
    print(f"{'Stat':<8} {'N':>6} {'W':>6} {'L':>6} {'Hit%':>8} {'ROI':>8} {'p-val':>8}")
    print("-" * 54)

    stat_results = []
    for stat in STAT_COLS.keys():
        stat_df = df[df["stat"] == stat]
        if len(stat_df) == 0:
            continue
        n = len(stat_df)
        w = stat_df["hit"].sum()
        l = n - w
        hr = w / n * 100
        stat_roi = (hr / 100 * 2.0909 - 1) * 100
        stat_p = stats.binomtest(w, n, 0.5, alternative='greater').pvalue

        print(f"{stat:<8} {n:>6} {w:>6} {l:>6} {hr:>7.1f}% {stat_roi:>+7.1f}% {stat_p:>8.4f}")
        stat_results.append({"stat": stat, "n": n, "wins": w, "hit_rate": hr, "roi": stat_roi, "p": stat_p})

    # By tier
    print(f"\n[BY TIER]")
    print(f"{'Tier':<10} {'N':>6} {'W':>6} {'L':>6} {'Hit%':>8} {'ROI':>8}")
    print("-" * 48)

    for tier in ["S_TIER", "A_TIER"]:
        tier_df = df[df["tier"] == tier]
        if len(tier_df) == 0:
            continue
        n = len(tier_df)
        w = tier_df["hit"].sum()
        l = n - w
        hr = w / n * 100
        tier_roi = (hr / 100 * 2.0909 - 1) * 100
        print(f"{tier:<10} {n:>6} {w:>6} {l:>6} {hr:>7.1f}% {tier_roi:>+7.1f}%")

    # Monthly breakdown (decay check)
    print(f"\n[MONTHLY BREAKDOWN] (Decay Check)")
    print(f"{'Month':<10} {'N':>6} {'W':>6} {'L':>6} {'Hit%':>8} {'ROI':>8}")
    print("-" * 48)

    for month in sorted(df["month"].unique()):
        month_df = df[df["month"] == month]
        n = len(month_df)
        w = month_df["hit"].sum()
        l = n - w
        hr = w / n * 100
        month_roi = (hr / 100 * 2.0909 - 1) * 100
        print(f"{month:<10} {n:>6} {w:>6} {l:>6} {hr:>7.1f}% {month_roi:>+7.1f}%")

    # Edge size breakdown
    print(f"\n[BY EDGE SIZE]")
    print(f"{'Edge':<12} {'N':>6} {'W':>6} {'L':>6} {'Hit%':>8} {'ROI':>8}")
    print("-" * 50)

    for edge_min, edge_max, label in [(15, 20, "15-20%"), (20, 25, "20-25%"), (25, 30, "25-30%"), (30, 100, "30%+")]:
        edge_df = df[(abs(df["edge_pct"]) >= edge_min) & (abs(df["edge_pct"]) < edge_max)]
        if len(edge_df) == 0:
            continue
        n = len(edge_df)
        w = edge_df["hit"].sum()
        l = n - w
        hr = w / n * 100
        edge_roi = (hr / 100 * 2.0909 - 1) * 100
        print(f"{label:<12} {n:>6} {w:>6} {l:>6} {hr:>7.1f}% {edge_roi:>+7.1f}%")

    # OVER vs UNDER
    print(f"\n[OVER vs UNDER]")
    for pick in ["OVER", "UNDER"]:
        pick_df = df[df["pick"] == pick]
        if len(pick_df) == 0:
            continue
        n = len(pick_df)
        w = pick_df["hit"].sum()
        hr = w / n * 100
        pick_roi = (hr / 100 * 2.0909 - 1) * 100
        print(f"  {pick}: {w}W-{n-w}L ({hr:.1f}%) ROI: {pick_roi:+.1f}%")

    # Best individual stats
    print(f"\n[TOP PERFORMERS BY STAT]")
    for stat in ["PTS", "AST"]:
        stat_df = df[df["stat"] == stat]
        if len(stat_df) >= 50:
            w = stat_df["hit"].sum()
            n = len(stat_df)
            hr = w/n*100
            p = stats.binomtest(w, n, 0.5, alternative='greater').pvalue
            sig = "***" if p < 0.01 else "**" if p < 0.05 else "*" if p < 0.10 else ""
            print(f"  {stat}: {w}W-{n-w}L ({hr:.1f}%) p={p:.4f} {sig}")

    print("\n" + "=" * 70)
    print("VERDICT")
    print("=" * 70)

    if hit_rate > 54 and p_value < 0.05:
        print(f"\n  STRONG EDGE CONFIRMED")
        print(f"  -> {hit_rate:.1f}% hit rate on 15%+ edges")
        print(f"  -> Statistically significant (p={p_value:.4f})")
    elif hit_rate > 52.5:
        print(f"\n  MARGINAL EDGE")
        print(f"  -> {hit_rate:.1f}% hit rate beats break-even ({breakeven:.1f}%)")
        print(f"  -> More data needed for significance")
    else:
        print(f"\n  NO EDGE")
        print(f"  -> {hit_rate:.1f}% does not beat break-even")

    conn.close()
    return df


if __name__ == "__main__":
    run_full_backtest()
