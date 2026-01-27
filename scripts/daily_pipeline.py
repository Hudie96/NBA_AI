"""
AXIOM Unified Daily Pipeline

Task 4.1 from AXIOM_ACTION_PLAN_v2.md
One command to run the entire daily workflow.

Steps:
1. Update player game logs (yesterday's games)
2. Refresh DVP rankings
3. Rebuild player vs team history
4. Run team spread picks (existing system)
5. Run props edge finder (PTS/AST, 15%+ edges)
6. Output daily card

Usage:
    python scripts/daily_pipeline.py
    python scripts/daily_pipeline.py --date 2025-01-26
    python scripts/daily_pipeline.py --skip-update  # Skip data refresh
    python scripts/daily_pipeline.py --props-only   # Only run props
"""
import argparse
import sqlite3
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]


def print_header(text):
    """Print a formatted section header."""
    print("\n" + "=" * 60)
    print(f"  {text}")
    print("=" * 60 + "\n")


def print_step(num, text):
    """Print a step indicator."""
    print(f"\n[Step {num}] {text}")
    print("-" * 40)


def run_script(script_name, args=None, capture=False):
    """Run a Python script and return success status."""
    cmd = [sys.executable, f"scripts/{script_name}"]
    if args:
        cmd.extend(args)

    try:
        if capture:
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT)
            return result.returncode == 0, result.stdout
        else:
            result = subprocess.run(cmd, cwd=PROJECT_ROOT)
            return result.returncode == 0, None
    except Exception as e:
        print(f"  Error running {script_name}: {e}")
        return False, None


def update_player_data(season="2024-25"):
    """Step 1-3: Update player game logs, DVP, and player vs team."""
    print_step(1, "Updating Player Game Logs")

    success, _ = run_script("fetch_player_logs.py", ["--season", season])
    if not success:
        print("  Warning: Failed to update player game logs")
        return False

    print_step(2, "Refreshing Defense vs Position Rankings")

    success, _ = run_script("fetch_dvp.py", ["--season", season])
    if not success:
        print("  Warning: Failed to refresh DVP rankings")
        return False

    print_step(3, "Rebuilding Player vs Team History")

    success, _ = run_script("build_player_vs_team.py", ["--season", season])
    if not success:
        print("  Warning: Failed to rebuild player vs team")
        return False

    return True


def run_team_spreads(target_date=None):
    """Step 4: Run team spread predictions."""
    print_step(4, "Running Team Spread Predictions")

    args = []
    if target_date:
        args.extend(["--date", target_date])

    success, output = run_script("daily_predictions.py", args, capture=True)

    if success and output:
        # Extract key info from output
        lines = output.strip().split("\n")
        for line in lines:
            if "GREEN" in line or "YELLOW" in line or "predictions" in line.lower():
                print(f"  {line}")

    return success


def run_props_finder(target_date=None):
    """Step 5: Run props edge finder."""
    print_step(5, "Finding Props Edges (PTS/AST, 15%+ validated)")

    # Import the edge finder functions directly
    try:
        from scripts.find_edges import (
            test_edge_finder, build_player_positions_table,
            save_edges_to_db, PROFITABLE_STATS
        )

        conn = sqlite3.connect(DB_PATH)
        build_player_positions_table(conn)

        # Run edge finder - this will print results
        edges = test_edge_finder(conn, all_stats=False)

        # Save edges to database
        if edges:
            count = save_edges_to_db(edges, conn)
            print(f"\n  Saved {count} edges to database")

        conn.close()
        return True, edges

    except Exception as e:
        print(f"  Error running props finder: {e}")
        return False, []


def generate_daily_card(spread_picks=None, prop_edges=None, target_date=None):
    """Step 6: Generate the daily pick card."""
    print_step(6, "Generating Daily Card")

    if target_date is None:
        target_date = date.today().strftime("%b %d")
    else:
        target_date = datetime.strptime(target_date, "%Y-%m-%d").strftime("%b %d")

    card = []
    card.append(f"=== AXIOM DAILY CARD - {target_date} ===")
    card.append("")

    # Team Spread Section
    card.append("[SPREAD PICKS] (Team System)")
    card.append("-" * 35)

    conn = sqlite3.connect(DB_PATH)

    # Try to get today's spread picks from flag system output
    try:
        # Check for recent flagged picks
        from scripts.flag_system import FLAG_THRESHOLDS
        card.append("  Run daily_predictions.py for spread picks")
        card.append("  GREEN = 8+ score, YELLOW = 5-7 score")
    except:
        card.append("  No spread picks available")

    card.append("")

    # Props Section
    card.append("[PROP PICKS] (Player System - Backtest Validated)")
    card.append("-" * 35)

    if prop_edges:
        # Sort by confidence
        sorted_edges = sorted(prop_edges, key=lambda x: x["confidence_score"], reverse=True)

        high_edges = [e for e in sorted_edges if e["confidence"] == "HIGH"]
        med_edges = [e for e in sorted_edges if e["confidence"] == "MEDIUM"]

        # Show HIGH confidence first
        for edge in high_edges[:3]:
            stars = "[***]"
            card.append(f"  {stars} {edge['player_name']} {edge['pick']} {edge['line']} {edge['prop_type']}")
            card.append(f"      vs {edge['opponent']} | Proj: {edge['projection']} | Edge: {edge['edge_pct']:+.1f}%")
            card.append("")

        # Show MEDIUM confidence
        for edge in med_edges[:3]:
            stars = "[**]"
            card.append(f"  {stars} {edge['player_name']} {edge['pick']} {edge['line']} {edge['prop_type']}")
            card.append(f"      vs {edge['opponent']} | Proj: {edge['projection']} | Edge: {edge['edge_pct']:+.1f}%")
            card.append("")

        if not high_edges and not med_edges:
            card.append("  No HIGH/MEDIUM confidence edges found")
            card.append("  (Require 15%+ edge on PTS/AST)")
    else:
        card.append("  No prop edges available")

    card.append("")

    # Results tracking
    card.append("[RESULTS TRACKING]")
    card.append("-" * 35)

    try:
        # Get props results summary
        results = pd.read_sql("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result = 'LOSS' THEN 1 ELSE 0 END) as losses
            FROM props_results
            WHERE result IS NOT NULL
        """, conn)

        if results.iloc[0]["total"] > 0:
            wins = results.iloc[0]["wins"]
            losses = results.iloc[0]["losses"]
            total = wins + losses
            pct = wins / total * 100 if total > 0 else 0
            card.append(f"  Props: {wins}W-{losses}L ({pct:.1f}%)")
        else:
            card.append("  Props: No results yet")
    except:
        card.append("  Props: No results yet")

    card.append("")
    card.append("=" * 40)
    card.append("Backtest: 15%+ edges on PTS/AST = 56.4% hit rate")
    card.append("=" * 40)

    conn.close()

    # Print the card
    print("\n")
    for line in card:
        print(line)

    return card


def main():
    parser = argparse.ArgumentParser(description="AXIOM Daily Pipeline")
    parser.add_argument("--date", type=str, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--skip-update", action="store_true", help="Skip data refresh")
    parser.add_argument("--props-only", action="store_true", help="Only run props analysis")
    parser.add_argument("--spreads-only", action="store_true", help="Only run spreads analysis")
    parser.add_argument("--season", type=str, default="2024-25", help="Season")

    args = parser.parse_args()

    target_date = args.date or date.today().isoformat()

    print_header(f"AXIOM DAILY PIPELINE - {target_date}")

    prop_edges = []

    # Step 1-3: Update data (unless skipped)
    if not args.skip_update and not args.spreads_only:
        print("Updating player data (this may take a few minutes)...")
        update_player_data(args.season)

    # Step 4: Team spreads (unless props-only)
    if not args.props_only:
        run_team_spreads(target_date)

    # Step 5: Props edges (unless spreads-only)
    if not args.spreads_only:
        success, prop_edges = run_props_finder(target_date)

    # Step 6: Generate daily card
    generate_daily_card(prop_edges=prop_edges, target_date=target_date)

    print_header("PIPELINE COMPLETE")

    return 0


if __name__ == "__main__":
    sys.exit(main())
