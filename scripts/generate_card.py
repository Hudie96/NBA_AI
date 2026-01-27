"""
Daily Pick Card Generator

Task 3.2 from AXIOM_ACTION_PLAN_v2.md
Creates formatted pick cards for social media.

Output formats:
- Console (pretty print)
- Text file (for copy/paste)
- JSON (for programmatic use)

Usage:
    python scripts/generate_card.py
    python scripts/generate_card.py --format twitter
    python scripts/generate_card.py --output card.txt
"""
import argparse
import json
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]


def get_props_picks(conn, target_date=None):
    """Get prop picks for today."""
    if target_date is None:
        target_date = date.today().isoformat()

    picks = pd.read_sql("""
        SELECT player_name, opponent, prop_type, line, projection, edge_pct, pick,
               confidence, stat_tier, is_top_play
        FROM props_edges
        WHERE date = ?
        ORDER BY
            is_top_play DESC,
            CASE stat_tier
                WHEN 'S_TIER' THEN 1
                WHEN 'A_TIER' THEN 2
                ELSE 3
            END,
            CASE confidence
                WHEN 'HIGH' THEN 1
                WHEN 'MEDIUM' THEN 2
                WHEN 'LOW' THEN 3
            END,
            ABS(edge_pct) DESC
    """, conn, params=(target_date,))

    return picks


def get_props_results(conn):
    """Get props results summary."""
    results = pd.read_sql("""
        SELECT
            confidence,
            COUNT(*) as total,
            SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN result = 'LOSS' THEN 1 ELSE 0 END) as losses
        FROM props_results
        WHERE result IS NOT NULL
        GROUP BY confidence
    """, conn)

    # Overall
    overall = pd.read_sql("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) as wins,
            SUM(CASE WHEN result = 'LOSS' THEN 1 ELSE 0 END) as losses
        FROM props_results
        WHERE result IS NOT NULL
    """, conn)

    return results, overall


def get_top_nugget(conn):
    """Get a featured stat nugget for the card."""
    # Get a matchup dominance nugget (most compelling)
    try:
        from scripts.generate_nuggets import find_matchup_dominance, find_streaks

        matchups = find_matchup_dominance(conn, min_avg=28, min_games=3)
        if matchups:
            return matchups[0]

        streaks = find_streaks(conn, min_streak=7, stat="points")
        if streaks:
            return streaks[0]
    except:
        pass

    return None


def generate_console_card(picks, results, overall, nugget, target_date):
    """Generate a console-formatted card."""
    date_str = datetime.strptime(target_date, "%Y-%m-%d").strftime("%b %d, %Y")

    lines = []
    lines.append("")
    lines.append("=" * 50)
    lines.append(f"     AXIOM DAILY CARD - {date_str}")
    lines.append("=" * 50)
    lines.append("")

    # Check for stat_tier column (new schema)
    has_tiers = "stat_tier" in picks.columns if not picks.empty else False

    if has_tiers:
        # TOP PLAYS Section (S-TIER combo props with 15%+ edge)
        top_plays = picks[picks["is_top_play"] == 1] if "is_top_play" in picks.columns else picks[picks["stat_tier"] == "S_TIER"]

        if len(top_plays) > 0:
            lines.append("[TOP PLAYS] S-Tier Combos - 60% hit rate on 15%+ edges")
            lines.append("-" * 50)
            for _, p in top_plays.head(3).iterrows():
                lines.append(f"  [***] {p['player_name']} {p['pick']} {p['line']} {p['prop_type']}")
                lines.append(f"        vs {p['opponent']} | Proj: {p['projection']} | Edge: {p['edge_pct']:+.1f}%")
            lines.append("")

        # Other S-TIER picks (not top plays)
        s_tier = picks[(picks["stat_tier"] == "S_TIER") & (picks.get("is_top_play", 0) != 1)]
        if len(s_tier) > 0:
            lines.append("[COMBO PROPS] S-Tier - 58% avg hit rate")
            lines.append("-" * 50)
            for _, p in s_tier.head(3).iterrows():
                conf_marker = {"HIGH": "***", "MEDIUM": "**", "LOW": "*"}.get(p["confidence"], "*")
                lines.append(f"  [{conf_marker}] {p['player_name']} {p['pick']} {p['line']} {p['prop_type']}")
                lines.append(f"        vs {p['opponent']} | Proj: {p['projection']} | Edge: {p['edge_pct']:+.1f}%")
            lines.append("")

        # A-TIER picks (individual props)
        a_tier = picks[picks["stat_tier"] == "A_TIER"]
        if len(a_tier) > 0:
            lines.append("[INDIVIDUAL PROPS] A-Tier - 55% avg hit rate")
            lines.append("-" * 50)
            for _, p in a_tier.head(4).iterrows():
                conf_marker = {"HIGH": "***", "MEDIUM": "**", "LOW": "*"}.get(p["confidence"], "*")
                lines.append(f"  [{conf_marker}] {p['player_name']} {p['pick']} {p['line']} {p['prop_type']}")
                lines.append(f"        vs {p['opponent']} | Proj: {p['projection']} | Edge: {p['edge_pct']:+.1f}%")
            lines.append("")

        if len(top_plays) == 0 and len(s_tier) == 0 and len(a_tier) == 0:
            lines.append("[PLAYER PROPS]")
            lines.append("-" * 50)
            lines.append("  No qualifying picks today")
            lines.append("  (Require 10%+ edge on validated stats)")
            lines.append("")
    else:
        # Fallback to old format if no tiers
        lines.append("[PLAYER PROPS] Backtest: 56.4% on 15%+ edges")
        lines.append("-" * 50)

        high_picks = picks[picks["confidence"] == "HIGH"]
        med_picks = picks[picks["confidence"] == "MEDIUM"]

        if len(high_picks) > 0:
            for _, p in high_picks.head(3).iterrows():
                lines.append(f"  [***] {p['player_name']} {p['pick']} {p['line']} {p['prop_type']}")
                lines.append(f"        vs {p['opponent']} | Proj: {p['projection']} | Edge: {p['edge_pct']:+.1f}%")
            lines.append("")

        if len(med_picks) > 0:
            for _, p in med_picks.head(3).iterrows():
                lines.append(f"  [**] {p['player_name']} {p['pick']} {p['line']} {p['prop_type']}")
                lines.append(f"        vs {p['opponent']} | Proj: {p['projection']} | Edge: {p['edge_pct']:+.1f}%")
            lines.append("")

        if len(high_picks) == 0 and len(med_picks) == 0:
            lines.append("  No HIGH/MEDIUM confidence picks today")
            lines.append("  (Require 15%+ edge on PTS/AST)")
            lines.append("")

    # Stat of the Day
    if nugget:
        lines.append("[STAT OF THE DAY]")
        lines.append("-" * 50)
        lines.append(f"  \"{nugget['hook']}\"")
        lines.append(f"  {nugget['detail']}")
        lines.append("")

    # Results
    lines.append("[SEASON RECORD]")
    lines.append("-" * 50)

    if overall.iloc[0]["total"] > 0:
        wins = int(overall.iloc[0]["wins"])
        losses = int(overall.iloc[0]["losses"])
        total = wins + losses
        pct = wins / total * 100 if total > 0 else 0
        lines.append(f"  Props: {wins}W-{losses}L ({pct:.1f}%)")

        # By confidence
        for _, r in results.iterrows():
            conf = r["confidence"]
            w = int(r["wins"])
            l = int(r["losses"])
            t = w + l
            p = w / t * 100 if t > 0 else 0
            lines.append(f"    {conf}: {w}W-{l}L ({p:.1f}%)")
    else:
        lines.append("  Props: No results yet")

    lines.append("")
    lines.append("=" * 50)
    lines.append("  @AXIOM_Picks | Data-driven NBA betting")
    lines.append("=" * 50)
    lines.append("")

    return "\n".join(lines)


def generate_twitter_card(picks, results, overall, nugget, target_date):
    """Generate Twitter-optimized card (280 char limit per tweet)."""
    date_str = datetime.strptime(target_date, "%Y-%m-%d").strftime("%b %d")

    tweets = []

    # Main picks tweet
    tweet1_lines = [f"AXIOM PICKS - {date_str}", ""]

    high_picks = picks[picks["confidence"] == "HIGH"]
    med_picks = picks[picks["confidence"] == "MEDIUM"]

    for _, p in high_picks.head(2).iterrows():
        tweet1_lines.append(f"[***] {p['player_name']} {p['pick']} {p['line']} {p['prop_type']}")

    for _, p in med_picks.head(2).iterrows():
        tweet1_lines.append(f"[**] {p['player_name']} {p['pick']} {p['line']} {p['prop_type']}")

    if overall.iloc[0]["total"] > 0:
        wins = int(overall.iloc[0]["wins"])
        losses = int(overall.iloc[0]["losses"])
        pct = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
        tweet1_lines.append("")
        tweet1_lines.append(f"Season: {wins}W-{losses}L ({pct:.1f}%)")

    tweets.append("\n".join(tweet1_lines))

    # Stat nugget tweet
    if nugget:
        tweet2 = f"STAT OF THE DAY\n\n{nugget['hook']}\n\n{nugget['detail']}"
        tweets.append(tweet2)

    return tweets


def generate_discord_card(picks, results, overall, nugget, target_date):
    """Generate Discord embed-style card."""
    date_str = datetime.strptime(target_date, "%Y-%m-%d").strftime("%b %d, %Y")

    # Discord uses markdown
    lines = []
    lines.append(f"# AXIOM DAILY CARD - {date_str}")
    lines.append("")
    lines.append("## Player Props")
    lines.append("*Backtest validated: 56.4% on 15%+ edges (PTS/AST)*")
    lines.append("")

    high_picks = picks[picks["confidence"] == "HIGH"]
    med_picks = picks[picks["confidence"] == "MEDIUM"]

    if len(high_picks) > 0 or len(med_picks) > 0:
        lines.append("```")
        for _, p in high_picks.head(3).iterrows():
            lines.append(f"[***] {p['player_name']} {p['pick']} {p['line']} {p['prop_type']} (+{p['edge_pct']:.0f}%)")

        for _, p in med_picks.head(3).iterrows():
            lines.append(f"[**]  {p['player_name']} {p['pick']} {p['line']} {p['prop_type']} (+{p['edge_pct']:.0f}%)")
        lines.append("```")
    else:
        lines.append("*No HIGH/MEDIUM picks today*")

    lines.append("")

    if nugget:
        lines.append("## Stat of the Day")
        lines.append(f"> {nugget['hook']}")
        lines.append(f"> {nugget['detail']}")
        lines.append("")

    lines.append("## Season Record")
    if overall.iloc[0]["total"] > 0:
        wins = int(overall.iloc[0]["wins"])
        losses = int(overall.iloc[0]["losses"])
        pct = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
        lines.append(f"**Props: {wins}W-{losses}L ({pct:.1f}%)**")
    else:
        lines.append("*No results yet*")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Generate daily pick card")
    parser.add_argument("--format", type=str, default="console",
                        choices=["console", "twitter", "discord", "json"],
                        help="Output format")
    parser.add_argument("--date", type=str, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, help="Output file path")

    args = parser.parse_args()

    target_date = args.date or date.today().isoformat()

    conn = sqlite3.connect(DB_PATH)

    # Get data
    picks = get_props_picks(conn, target_date)
    results, overall = get_props_results(conn)
    nugget = get_top_nugget(conn)

    # Generate card
    if args.format == "console":
        card = generate_console_card(picks, results, overall, nugget, target_date)
        print(card)

    elif args.format == "twitter":
        tweets = generate_twitter_card(picks, results, overall, nugget, target_date)
        print("=== TWITTER THREAD ===\n")
        for i, tweet in enumerate(tweets, 1):
            print(f"--- Tweet {i} ({len(tweet)} chars) ---")
            print(tweet)
            print()

    elif args.format == "discord":
        card = generate_discord_card(picks, results, overall, nugget, target_date)
        print(card)

    elif args.format == "json":
        card_data = {
            "date": target_date,
            "picks": picks.to_dict("records") if not picks.empty else [],
            "results": {
                "by_confidence": results.to_dict("records") if not results.empty else [],
                "overall": overall.to_dict("records")[0] if not overall.empty else {}
            },
            "nugget": nugget
        }
        print(json.dumps(card_data, indent=2, default=str))

    # Save to file if specified
    if args.output:
        if args.format == "json":
            with open(args.output, "w") as f:
                json.dump(card_data, f, indent=2, default=str)
        else:
            with open(args.output, "w") as f:
                f.write(card if args.format != "twitter" else "\n\n".join(tweets))
        print(f"\nSaved to {args.output}")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
