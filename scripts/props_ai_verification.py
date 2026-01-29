"""
AXIOM Props AI Verification

Applies AI-style filters to prop picks to improve hit rate.

FILTERS:
1. Blowout risk: spread > 12 for OVER props (player sits Q4)
2. B2B fatigue: player on back-to-back
3. Minutes consistency: check recent minutes variance
4. Injury/lineup: teammate out affecting usage

Backtests RAW vs AI-VERIFIED props.
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

STAT_COLS = {
    "PTS": "points",
    "AST": "assists",
    "PRA": "pts_reb_ast",
    "PR": "pts_reb",
    "PA": "pts_ast",
    "RA": "reb_ast",
}

S_TIER = ["PTS", "AST"]
A_TIER = ["PRA", "PR", "PA", "RA"]


def get_player_context(player_name, game_date, conn):
    """Get context for AI verification."""
    # Recent games for minutes consistency
    recent = pd.read_sql("""
        SELECT minutes, is_b2b, days_rest
        FROM player_game_logs
        WHERE player_name = ? AND game_date < ?
        ORDER BY game_date DESC
        LIMIT 10
    """, conn, params=(player_name, game_date))

    if recent.empty:
        return None

    return {
        "avg_minutes": recent["minutes"].mean(),
        "min_minutes": recent["minutes"].min(),
        "max_minutes": recent["minutes"].max(),
        "minutes_std": recent["minutes"].std(),
        "recent_b2b_count": recent["is_b2b"].sum(),
    }


def get_game_spread(game_date, team, conn):
    """Get spread for the game (proxy using betting data if available)."""
    # Try to match game and get spread
    spread_df = pd.read_sql("""
        SELECT b.espn_closing_spread, g.home_team, g.away_team
        FROM Games g
        JOIN Betting b ON g.game_id = b.game_id
        WHERE DATE(g.date_time_utc) = ?
        AND (g.home_team = ? OR g.away_team = ?)
        LIMIT 1
    """, conn, params=(game_date, team, team))

    if spread_df.empty or pd.isna(spread_df.iloc[0]["espn_closing_spread"]):
        return None

    spread = spread_df.iloc[0]["espn_closing_spread"]
    is_home = spread_df.iloc[0]["home_team"] == team

    # Spread is from home perspective, positive = home is underdog
    if is_home:
        return -spread  # flip: negative = favorite
    else:
        return spread


def ai_verify_prop(row, player_context, game_spread, conn):
    """
    Apply AI verification rules to a prop pick.

    Returns: (verdict, reason)
    """
    stat = row["stat"]
    pick = row["pick"]
    edge_pct = abs(row["edge_pct"])
    player = row["player"]
    game_date = row["game_date"]

    # Rule 1: Blowout risk for OVER props
    # If team is heavy favorite (spread > 12), starters may sit Q4
    if game_spread is not None and pick == "OVER":
        if game_spread < -12:  # Team is 12+ point favorite
            return "REJECT", f"Blowout risk: team is {abs(game_spread):.1f} pt favorite, starters may rest"

    # Rule 2: UNDER on blowout games can work (garbage time boost for other team)
    # But OVER is risky

    # Rule 3: B2B check
    # Get if this specific game is a B2B
    b2b_check = pd.read_sql("""
        SELECT is_b2b FROM player_game_logs
        WHERE player_name = ? AND game_date = ?
    """, conn, params=(player, game_date))

    if not b2b_check.empty and b2b_check.iloc[0]["is_b2b"] == 1:
        # Player is on B2B
        if pick == "OVER":
            # OVER on B2B is risky for high-minute players
            if player_context and player_context["avg_minutes"] > 32:
                return "FLAG", "B2B game for high-minute player - fatigue risk for OVER"

    # Rule 4: Minutes consistency
    if player_context:
        minutes_std = player_context["minutes_std"]
        if minutes_std > 6:
            # High variance in minutes = unpredictable
            return "FLAG", f"High minutes variance (std={minutes_std:.1f}) - unpredictable role"

        # Check for recent low minutes (injury concern or rotation change)
        if player_context["min_minutes"] < 20 and player_context["avg_minutes"] > 28:
            return "FLAG", "Recent game with low minutes - possible injury/rotation concern"

    # Rule 5: Edge strength
    # Very high edges (30%+) might indicate something unusual
    if edge_pct > 35:
        return "FLAG", f"Unusually high edge ({edge_pct:.1f}%) - verify projection"

    # Rule 6: UNDER on combo props is safer
    if stat in A_TIER and pick == "UNDER" and edge_pct > 20:
        # High confidence UNDER on combo - good signal
        pass  # Confirm

    return "CONFIRM", "Passed all AI verification checks"


def run_verification_backtest():
    """Compare RAW vs AI-verified props."""
    conn = sqlite3.connect(DB_PATH)

    print("=" * 70)
    print("AXIOM PROPS AI VERIFICATION BACKTEST")
    print("=" * 70)

    # Get top players
    top_players = pd.read_sql("""
        SELECT player_name, team, COUNT(*) as games
        FROM player_game_logs
        GROUP BY player_name
        HAVING games >= 30
        ORDER BY games DESC
        LIMIT 150
    """, conn)

    results = []

    for _, player_row in top_players.iterrows():
        player_name = player_row["player_name"]
        team = player_row["team"]

        all_games = pd.read_sql("""
            SELECT game_date, opponent, team, points, assists,
                   pts_reb_ast, pts_reb, pts_ast, reb_ast, minutes, is_b2b
            FROM player_game_logs
            WHERE player_name = ?
            ORDER BY game_date
        """, conn, params=(player_name,))

        if len(all_games) < 25:
            continue

        test_games = all_games.iloc[15:]

        for _, game in test_games.iterrows():
            game_date = game["game_date"]
            opponent = game["opponent"]
            current_team = game["team"]

            for stat in STAT_COLS.keys():
                col = STAT_COLS[stat]
                actual = game[col]

                # Get projection (simplified - reuse from full backtest logic)
                prior_games = pd.read_sql(f"""
                    SELECT {col} as value
                    FROM player_game_logs
                    WHERE player_name = ? AND game_date < ?
                    ORDER BY game_date DESC
                """, conn, params=(player_name, game_date))

                if len(prior_games) < 10:
                    continue

                last_10 = prior_games.head(10)["value"].mean()
                season_avg = prior_games["value"].mean()
                projection = last_10 * 0.50 + season_avg * 0.50
                market_line = season_avg

                if market_line == 0:
                    continue

                edge_pct = (projection - market_line) / market_line * 100

                if abs(edge_pct) < 15:
                    continue

                if edge_pct > 0:
                    pick = "OVER"
                    hit = actual > market_line
                else:
                    pick = "UNDER"
                    hit = actual < market_line

                tier = "S_TIER" if stat in S_TIER else "A_TIER"

                results.append({
                    "player": player_name,
                    "team": current_team,
                    "game_date": game_date,
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
        print("No results!")
        conn.close()
        return

    # Apply AI verification
    print(f"\n[STEP 1] Running AI verification on {len(df)} picks...")

    verdicts = []
    for _, row in df.iterrows():
        player_context = get_player_context(row["player"], row["game_date"], conn)
        game_spread = get_game_spread(row["game_date"], row["team"], conn)

        verdict, reason = ai_verify_prop(row, player_context, game_spread, conn)
        verdicts.append({
            "verdict": verdict,
            "reason": reason
        })

    df["ai_verdict"] = [v["verdict"] for v in verdicts]
    df["ai_reason"] = [v["reason"] for v in verdicts]

    confirmed = df[df["ai_verdict"] == "CONFIRM"]
    flagged = df[df["ai_verdict"] == "FLAG"]
    rejected = df[df["ai_verdict"] == "REJECT"]

    print(f"  CONFIRM: {len(confirmed)}")
    print(f"  FLAG: {len(flagged)}")
    print(f"  REJECT: {len(rejected)}")

    # Compare results
    print("\n" + "=" * 70)
    print("RESULTS: RAW vs AI-VERIFIED")
    print("=" * 70)

    def calc_stats(subset, label):
        n = len(subset)
        if n == 0:
            return
        w = subset["hit"].sum()
        hr = w / n * 100
        roi = (hr / 100 * 2.0909 - 1) * 100
        p = stats.binomtest(w, n, 0.5, alternative='greater').pvalue
        print(f"  {label}: {w}W-{n-w}L ({hr:.1f}%) ROI: {roi:+.1f}% p={p:.4f}")

    print(f"\n[RAW (All picks)]")
    calc_stats(df, "ALL")

    print(f"\n[AI-VERIFIED (CONFIRM only)]")
    calc_stats(confirmed, "CONFIRM")

    print(f"\n[FLAGGED (Proceed with caution)]")
    calc_stats(flagged, "FLAG")

    print(f"\n[REJECTED (Would have skipped)]")
    calc_stats(rejected, "REJECT")

    # Detailed comparison
    print("\n" + "=" * 70)
    print("IMPROVEMENT ANALYSIS")
    print("=" * 70)

    raw_hr = df["hit"].mean() * 100
    verified_hr = confirmed["hit"].mean() * 100 if len(confirmed) > 0 else 0
    rejected_hr = rejected["hit"].mean() * 100 if len(rejected) > 0 else 0

    raw_roi = (raw_hr / 100 * 2.0909 - 1) * 100
    verified_roi = (verified_hr / 100 * 2.0909 - 1) * 100

    print(f"\n  RAW: {len(df)} bets, {raw_hr:.1f}% hit rate, {raw_roi:+.1f}% ROI")
    print(f"  VERIFIED: {len(confirmed)} bets, {verified_hr:.1f}% hit rate, {verified_roi:+.1f}% ROI")
    print(f"  REJECTED: {len(rejected)} bets, {rejected_hr:.1f}% hit rate (would have lost)")

    print(f"\n  Hit rate improvement: {verified_hr - raw_hr:+.1f} pp")
    print(f"  ROI improvement: {verified_roi - raw_roi:+.1f} pp")
    print(f"  Bets filtered: {len(df) - len(confirmed)} ({(len(df) - len(confirmed))/len(df)*100:.1f}%)")

    # Rejection reason analysis
    print("\n" + "=" * 70)
    print("REJECTION ANALYSIS")
    print("=" * 70)

    all_non_confirmed = pd.concat([flagged, rejected])
    for reason, group in all_non_confirmed.groupby("ai_reason"):
        n = len(group)
        w = group["hit"].sum()
        hr = w / n * 100
        print(f"\n  \"{reason}\"")
        print(f"    {n} picks, {w} would have won ({hr:.1f}%)")

    # Verdict
    print("\n" + "=" * 70)
    print("VERDICT")
    print("=" * 70)

    if verified_roi > raw_roi:
        print(f"\n  AI VERIFICATION HELPS")
        print(f"  -> Improves ROI from {raw_roi:+.1f}% to {verified_roi:+.1f}%")
        print(f"  -> Filtered bets hit at {rejected_hr:.1f}% (correctly avoided)")
    else:
        print(f"\n  AI VERIFICATION DOES NOT HELP (with current rules)")
        print(f"  -> May be filtering good bets")

    conn.close()
    return df


if __name__ == "__main__":
    run_verification_backtest()
