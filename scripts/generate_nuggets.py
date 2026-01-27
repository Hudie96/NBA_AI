"""
Stat Nugget Generator

Task 3.1 from AXIOM_ACTION_PLAN_v2.md
Auto-generates tweetable stats from player data.

Nugget Types:
- Player streaks (hit over/under X straight)
- Home/Away splits (significant differences)
- Matchup dominance (avg 30+ vs specific team)
- DVP extremes (team allows most/least to position)
- B2B impact (players who drop significantly on B2B)

Usage:
    python scripts/generate_nuggets.py
    python scripts/generate_nuggets.py --type streaks
    python scripts/generate_nuggets.py --player "LeBron James"
    python scripts/generate_nuggets.py --top 10
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

STAT_LABELS = {
    "points": "PTS",
    "rebounds": "REB",
    "assists": "AST",
    "threes_made": "3PM"
}


def find_streaks(conn, min_streak=5, stat="points"):
    """
    Find players on over/under streaks.

    Returns players who have hit over or under their season avg
    for X consecutive games.
    """
    nuggets = []

    # Get players with enough games
    players = pd.read_sql(f"""
        SELECT DISTINCT player_name,
               AVG({stat}) as season_avg,
               COUNT(*) as games
        FROM player_game_logs
        GROUP BY player_name
        HAVING games >= 20
    """, conn)

    for _, player in players.iterrows():
        player_name = player["player_name"]
        season_avg = player["season_avg"]

        # Get recent games
        recent = pd.read_sql(f"""
            SELECT game_date, {stat} as value, opponent
            FROM player_game_logs
            WHERE player_name = ?
            ORDER BY game_date DESC
            LIMIT 15
        """, conn, params=(player_name,))

        if len(recent) < min_streak:
            continue

        # Check for over streak
        over_streak = 0
        for _, game in recent.iterrows():
            if game["value"] > season_avg:
                over_streak += 1
            else:
                break

        if over_streak >= min_streak:
            avg_in_streak = recent.head(over_streak)["value"].mean()
            nuggets.append({
                "type": "streak",
                "player": player_name,
                "stat": STAT_LABELS.get(stat, stat),
                "direction": "OVER",
                "streak": over_streak,
                "threshold": round(season_avg, 1),
                "avg_in_streak": round(avg_in_streak, 1),
                "hook": f"{player_name} has gone OVER {season_avg:.1f} {STAT_LABELS.get(stat, stat)} in {over_streak} straight games",
                "detail": f"Averaging {avg_in_streak:.1f} during streak (season avg: {season_avg:.1f})",
                "score": over_streak * 10 + (avg_in_streak - season_avg)
            })

        # Check for under streak
        under_streak = 0
        for _, game in recent.iterrows():
            if game["value"] < season_avg:
                under_streak += 1
            else:
                break

        if under_streak >= min_streak:
            avg_in_streak = recent.head(under_streak)["value"].mean()
            nuggets.append({
                "type": "streak",
                "player": player_name,
                "stat": STAT_LABELS.get(stat, stat),
                "direction": "UNDER",
                "streak": under_streak,
                "threshold": round(season_avg, 1),
                "avg_in_streak": round(avg_in_streak, 1),
                "hook": f"{player_name} has gone UNDER {season_avg:.1f} {STAT_LABELS.get(stat, stat)} in {under_streak} straight games",
                "detail": f"Averaging {avg_in_streak:.1f} during streak (season avg: {season_avg:.1f})",
                "score": under_streak * 10 + (season_avg - avg_in_streak)
            })

    return sorted(nuggets, key=lambda x: x["score"], reverse=True)


def find_home_away_splits(conn, min_diff_pct=20):
    """
    Find players with extreme home/away splits.
    """
    nuggets = []

    splits = pd.read_sql("""
        SELECT
            player_name,
            AVG(CASE WHEN home_away = 'HOME' THEN points END) as home_pts,
            AVG(CASE WHEN home_away = 'AWAY' THEN points END) as away_pts,
            COUNT(CASE WHEN home_away = 'HOME' THEN 1 END) as home_games,
            COUNT(CASE WHEN home_away = 'AWAY' THEN 1 END) as away_games
        FROM player_game_logs
        GROUP BY player_name
        HAVING home_games >= 10 AND away_games >= 10
    """, conn)

    for _, row in splits.iterrows():
        home_pts = row["home_pts"]
        away_pts = row["away_pts"]

        if home_pts is None or away_pts is None:
            continue

        diff = home_pts - away_pts
        diff_pct = abs(diff) / max(home_pts, away_pts) * 100

        if diff_pct >= min_diff_pct:
            if diff > 0:
                hook = f"{row['player_name']} averages {home_pts:.1f} PTS at home vs {away_pts:.1f} on the road"
                detail = f"That's {diff_pct:.0f}% better at home ({row['home_games']} home, {row['away_games']} away games)"
                location = "HOME"
            else:
                hook = f"{row['player_name']} averages {away_pts:.1f} PTS on the road vs {home_pts:.1f} at home"
                detail = f"That's {diff_pct:.0f}% better on the road ({row['away_games']} away, {row['home_games']} home games)"
                location = "AWAY"

            nuggets.append({
                "type": "split",
                "player": row["player_name"],
                "stat": "PTS",
                "home_avg": round(home_pts, 1),
                "away_avg": round(away_pts, 1),
                "diff_pct": round(diff_pct, 1),
                "better_location": location,
                "hook": hook,
                "detail": detail,
                "score": diff_pct
            })

    return sorted(nuggets, key=lambda x: x["score"], reverse=True)


def find_matchup_dominance(conn, min_avg=25, min_games=3):
    """
    Find players who dominate specific opponents.
    """
    nuggets = []

    matchups = pd.read_sql(f"""
        SELECT
            player_name,
            opponent,
            AVG(points) as avg_pts,
            AVG(rebounds) as avg_reb,
            AVG(assists) as avg_ast,
            COUNT(*) as games,
            MAX(points) as max_pts
        FROM player_game_logs
        GROUP BY player_name, opponent
        HAVING games >= {min_games}
    """, conn)

    # Get season averages for comparison
    season_avgs = pd.read_sql("""
        SELECT player_name, AVG(points) as season_pts
        FROM player_game_logs
        GROUP BY player_name
    """, conn)
    season_avgs = dict(zip(season_avgs["player_name"], season_avgs["season_pts"]))

    for _, row in matchups.iterrows():
        if row["avg_pts"] >= min_avg:
            season_avg = season_avgs.get(row["player_name"], row["avg_pts"])
            diff = row["avg_pts"] - season_avg
            diff_pct = diff / season_avg * 100 if season_avg > 0 else 0

            if diff_pct >= 15:  # At least 15% above season avg
                nuggets.append({
                    "type": "matchup",
                    "player": row["player_name"],
                    "opponent": row["opponent"],
                    "avg_pts": round(row["avg_pts"], 1),
                    "season_avg": round(season_avg, 1),
                    "games": int(row["games"]),
                    "max_pts": int(row["max_pts"]),
                    "hook": f"{row['player_name']} averages {row['avg_pts']:.1f} PTS vs {row['opponent']} ({row['games']} games)",
                    "detail": f"That's {diff_pct:.0f}% above his season avg of {season_avg:.1f}. Career high vs them: {row['max_pts']}",
                    "score": row["avg_pts"] + diff_pct
                })

    return sorted(nuggets, key=lambda x: x["score"], reverse=True)


def find_dvp_extremes(conn, top_n=5):
    """
    Find the most extreme DVP matchups.
    """
    nuggets = []

    # Worst defenses (rank 1-5 = allows most)
    worst = pd.read_sql(f"""
        SELECT team, position, stat, avg_allowed, league_avg, diff_from_avg, rank
        FROM defense_vs_position
        WHERE rank <= {top_n}
        ORDER BY rank
    """, conn)

    for _, row in worst.iterrows():
        nuggets.append({
            "type": "dvp_bad",
            "team": row["team"],
            "position": row["position"],
            "stat": row["stat"],
            "avg_allowed": round(row["avg_allowed"], 1),
            "league_avg": round(row["league_avg"], 1),
            "diff": round(row["diff_from_avg"], 1),
            "rank": int(row["rank"]),
            "hook": f"{row['team']} allows {row['avg_allowed']:.1f} {row['stat']} to {row['position']}s (#{row['rank']} worst in NBA)",
            "detail": f"League avg: {row['league_avg']:.1f}. They allow {row['diff_from_avg']:.1f} more than average.",
            "score": row["diff_from_avg"]
        })

    # Best defenses (rank 26-30 = allows least)
    best = pd.read_sql(f"""
        SELECT team, position, stat, avg_allowed, league_avg, diff_from_avg, rank
        FROM defense_vs_position
        WHERE rank >= 26
        ORDER BY rank DESC
    """, conn)

    for _, row in best.iterrows():
        nuggets.append({
            "type": "dvp_good",
            "team": row["team"],
            "position": row["position"],
            "stat": row["stat"],
            "avg_allowed": round(row["avg_allowed"], 1),
            "league_avg": round(row["league_avg"], 1),
            "diff": round(row["diff_from_avg"], 1),
            "rank": int(row["rank"]),
            "hook": f"{row['team']} allows just {row['avg_allowed']:.1f} {row['stat']} to {row['position']}s (#{row['rank']} best D in NBA)",
            "detail": f"League avg: {row['league_avg']:.1f}. They allow {abs(row['diff_from_avg']):.1f} less than average.",
            "score": abs(row["diff_from_avg"])
        })

    return sorted(nuggets, key=lambda x: x["score"], reverse=True)


def find_b2b_impact(conn, min_drop_pct=20):
    """
    Find players who drop significantly on back-to-backs.
    """
    nuggets = []

    b2b_stats = pd.read_sql("""
        SELECT
            player_name,
            AVG(CASE WHEN is_b2b = 0 THEN points END) as rest_pts,
            AVG(CASE WHEN is_b2b = 1 THEN points END) as b2b_pts,
            COUNT(CASE WHEN is_b2b = 0 THEN 1 END) as rest_games,
            COUNT(CASE WHEN is_b2b = 1 THEN 1 END) as b2b_games
        FROM player_game_logs
        GROUP BY player_name
        HAVING rest_games >= 15 AND b2b_games >= 5
    """, conn)

    for _, row in b2b_stats.iterrows():
        if row["rest_pts"] is None or row["b2b_pts"] is None:
            continue

        drop = row["rest_pts"] - row["b2b_pts"]
        drop_pct = drop / row["rest_pts"] * 100 if row["rest_pts"] > 0 else 0

        if drop_pct >= min_drop_pct:
            nuggets.append({
                "type": "b2b",
                "player": row["player_name"],
                "rest_avg": round(row["rest_pts"], 1),
                "b2b_avg": round(row["b2b_pts"], 1),
                "drop": round(drop, 1),
                "drop_pct": round(drop_pct, 1),
                "b2b_games": int(row["b2b_games"]),
                "hook": f"{row['player_name']} drops from {row['rest_pts']:.1f} to {row['b2b_pts']:.1f} PTS on back-to-backs",
                "detail": f"That's a {drop_pct:.0f}% drop ({row['b2b_games']} B2B games this season)",
                "score": drop_pct
            })

    return sorted(nuggets, key=lambda x: x["score"], reverse=True)


def format_tweet(nugget):
    """Format a nugget as a tweet-ready string."""
    tweet = f"{nugget['hook']}\n\n{nugget['detail']}"
    return tweet


def generate_all_nuggets(conn, top_per_type=5):
    """Generate all types of nuggets."""
    all_nuggets = []

    print("Finding streaks...")
    for stat in ["points", "assists", "rebounds"]:
        streaks = find_streaks(conn, min_streak=5, stat=stat)
        all_nuggets.extend(streaks[:top_per_type])

    print("Finding home/away splits...")
    splits = find_home_away_splits(conn, min_diff_pct=15)
    all_nuggets.extend(splits[:top_per_type])

    print("Finding matchup dominance...")
    matchups = find_matchup_dominance(conn, min_avg=25, min_games=3)
    all_nuggets.extend(matchups[:top_per_type])

    print("Finding DVP extremes...")
    dvp = find_dvp_extremes(conn, top_n=5)
    all_nuggets.extend(dvp[:top_per_type * 2])

    print("Finding B2B impact...")
    b2b = find_b2b_impact(conn, min_drop_pct=15)
    all_nuggets.extend(b2b[:top_per_type])

    return all_nuggets


def display_nuggets(nuggets, show_tweets=True):
    """Display nuggets in a formatted way."""

    # Group by type
    by_type = {}
    for n in nuggets:
        t = n["type"]
        if t not in by_type:
            by_type[t] = []
        by_type[t].append(n)

    type_labels = {
        "streak": "PLAYER STREAKS",
        "split": "HOME/AWAY SPLITS",
        "matchup": "MATCHUP DOMINANCE",
        "dvp_bad": "DVP - WORST DEFENSES",
        "dvp_good": "DVP - BEST DEFENSES",
        "b2b": "BACK-TO-BACK IMPACT"
    }

    for ntype, label in type_labels.items():
        if ntype in by_type:
            print(f"\n{'='*50}")
            print(f"  {label}")
            print(f"{'='*50}")

            for n in by_type[ntype][:5]:
                print(f"\n  {n['hook']}")
                print(f"  -> {n['detail']}")

                if show_tweets:
                    print(f"\n  [TWEET READY]")
                    print(f"  {'-'*40}")
                    tweet = format_tweet(n)
                    for line in tweet.split('\n'):
                        print(f"  {line}")


def main():
    parser = argparse.ArgumentParser(description="Generate stat nuggets")
    parser.add_argument("--type", type=str, help="Nugget type (streaks, splits, matchups, dvp, b2b)")
    parser.add_argument("--player", type=str, help="Filter to specific player")
    parser.add_argument("--top", type=int, default=5, help="Top N per category")
    parser.add_argument("--tweets", action="store_true", help="Show tweet-ready format")

    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)

    print("=" * 50)
    print("  AXIOM STAT NUGGET GENERATOR")
    print("=" * 50)

    nuggets = generate_all_nuggets(conn, top_per_type=args.top)

    # Filter by player if specified
    if args.player:
        nuggets = [n for n in nuggets if args.player.lower() in n.get("player", "").lower()]

    # Filter by type if specified
    if args.type:
        type_map = {
            "streaks": "streak",
            "splits": "split",
            "matchups": "matchup",
            "dvp": ["dvp_bad", "dvp_good"],
            "b2b": "b2b"
        }
        filter_types = type_map.get(args.type, args.type)
        if isinstance(filter_types, list):
            nuggets = [n for n in nuggets if n["type"] in filter_types]
        else:
            nuggets = [n for n in nuggets if n["type"] == filter_types]

    display_nuggets(nuggets, show_tweets=args.tweets)

    print(f"\n\nTotal nuggets found: {len(nuggets)}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
