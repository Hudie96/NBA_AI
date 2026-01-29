"""
AXIOM Unified Daily Pipeline

Task 4.1 from AXIOM_ACTION_PLAN_v2.md
One command to run the entire daily workflow.

Steps:
1. Update player game logs (yesterday's games)
2. Refresh DVP rankings
3. Rebuild player vs team history
4. Run SPREAD & TOTAL EDGES (validated backtested edges)
5. Run team spread picks (existing system)
6. Run props edge finder (PTS/AST, 15%+ edges)
7. Generate stat nuggets (optional)
8. Output daily card

Usage:
    python scripts/daily_pipeline.py
    python scripts/daily_pipeline.py --date 2025-01-26
    python scripts/daily_pipeline.py --skip-update  # Skip data refresh
    python scripts/daily_pipeline.py --props-only   # Only run props
    python scripts/daily_pipeline.py --with-nuggets # Include stat nuggets
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


def run_spread_total_edges(target_date=None):
    """Step 4: Run validated spread & total edges."""
    print_step(4, "Running Spread & Total Edges (Backtested)")

    try:
        from scripts.spread_total_edges import SpreadTotalEdges
        from datetime import datetime

        analyzer = SpreadTotalEdges()

        # Check if date is in the past - if so, include final games
        include_final = False
        if target_date:
            target = datetime.strptime(target_date, '%Y-%m-%d').date()
            today = datetime.now().date()
            if target < today:
                include_final = True

        # Get games with appropriate filter
        games = analyzer.get_todays_games(target_date, include_final=include_final)

        if not games:
            print(f"  No games found for {target_date or 'today'}")
            analyzer.close()
            return True, []

        # Analyze each game
        picks = []
        for game in games:
            analysis = analyzer.analyze_game(game)

            if analysis['spread_edge']:
                picks.append({
                    'type': 'SPREAD',
                    'game': f"{analysis['away_team']} @ {analysis['home_team']}",
                    'pick': f"{analysis['underdog']} +{abs(analysis['spread']):.1f}",
                    'edge': analysis['spread_edge']['edge_type'],
                    'confidence': analysis['spread_edge']['confidence'],
                    'hit_rate': analysis['spread_edge']['historical']['hit_rate'],
                    'multi_edge': analysis['multi_edge']
                })

            if analysis['total_edge']:
                picks.append({
                    'type': 'TOTAL',
                    'game': f"{analysis['away_team']} @ {analysis['home_team']}",
                    'pick': f"UNDER {analysis['total_line']:.1f}" if analysis['total_line'] else "UNDER",
                    'edge': analysis['total_edge']['edge_type'],
                    'confidence': analysis['total_edge']['confidence'],
                    'hit_rate': analysis['total_edge']['historical']['hit_rate'],
                    'multi_edge': analysis['multi_edge'],
                    'warning': analysis['total_edge'].get('warning')
                })

        # Print summary
        multi_edge = [p for p in picks if p.get('multi_edge')]
        spread_plays = [p for p in picks if p['type'] == 'SPREAD']
        total_plays = [p for p in picks if p['type'] == 'TOTAL']

        print(f"\n  Found {len(games)} games")
        print(f"  Multi-edge plays: {len(set(p['game'] for p in multi_edge))}")
        print(f"  Underdog plays: {len(spread_plays)}")
        print(f"  Under plays: {len(total_plays)}")

        # Show top picks
        if multi_edge:
            print(f"\n  TOP MULTI-EDGE:")
            games_shown = set()
            for p in multi_edge[:4]:
                if p['game'] not in games_shown:
                    games_shown.add(p['game'])
                    print(f"    {p['game']}")

        analyzer.close()
        return True, picks

    except Exception as e:
        print(f"  Error running spread/total edges: {e}")
        import traceback
        traceback.print_exc()
        return False, []


def run_team_spreads(target_date=None):
    """Step 5: Run team spread predictions (legacy system)."""
    print_step(5, "Running Team Spread Predictions (Legacy)")

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
    """Step 6: Run props edge finder."""
    print_step(6, "Finding Props Edges (PTS/AST, 15%+ validated)")

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


def generate_stat_nuggets(conn):
    """Step 7: Generate stat nuggets (optional)."""
    print_step(7, "Generating Stat Nuggets")

    try:
        from scripts.generate_nuggets import generate_all_nuggets, display_nuggets

        nuggets = generate_all_nuggets(conn, top_per_type=3)
        print(f"\n  Found {len(nuggets)} stat nuggets")

        # Show top 5 nuggets
        if nuggets:
            print("\n  TOP NUGGETS:")
            for n in sorted(nuggets, key=lambda x: x.get("score", 0), reverse=True)[:5]:
                print(f"    - {n['hook']}")

        return nuggets
    except Exception as e:
        print(f"  Warning: Could not generate nuggets: {e}")
        return []


def generate_daily_card(target_date=None, card_format="console", spread_picks=None):
    """Step 8: Generate the daily pick card using generate_card module."""
    print_step(8, "Generating Daily Card")

    try:
        from scripts.generate_card import (
            get_props_picks, get_props_results, get_top_nugget,
            generate_console_card, generate_discord_card
        )

        conn = sqlite3.connect(DB_PATH)

        # Use ISO format for database queries
        if target_date is None:
            target_date = date.today().isoformat()

        # Get data
        picks = get_props_picks(conn, target_date)
        results, overall = get_props_results(conn)
        nugget = get_top_nugget(conn)

        # Generate card
        if card_format == "discord":
            card = generate_discord_card(picks, results, overall, nugget, target_date)
        else:
            card = generate_console_card(picks, results, overall, nugget, target_date)

        print(card)

        conn.close()
        return card

    except Exception as e:
        print(f"  Warning: Could not generate card: {e}")
        # Fallback to simple output
        print("\n  Run 'python scripts/generate_card.py' for full card")
        return None


def run_ai_verification(target_date):
    """Step 6b: Run AI verification on picks (optional)."""
    print_step("6b", "Running AI Pick Verification")

    try:
        from scripts.ai_verify_picks import verify_picks, display_results

        conn = sqlite3.connect(DB_PATH)
        results = verify_picks(conn, target_date=target_date, dry_run=False, verbose=True)

        if results:
            display_results(results)

        conn.close()
        return True, results
    except Exception as e:
        print(f"  Warning: AI verification failed: {e}")
        return False, []


def main():
    parser = argparse.ArgumentParser(description="AXIOM Daily Pipeline")
    parser.add_argument("--date", type=str, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--skip-update", action="store_true", help="Skip data refresh")
    parser.add_argument("--props-only", action="store_true", help="Only run props analysis")
    parser.add_argument("--spreads-only", action="store_true", help="Only run spreads analysis")
    parser.add_argument("--with-nuggets", action="store_true", help="Include stat nuggets generation")
    parser.add_argument("--with-verification", action="store_true", help="Run AI verification on picks")
    parser.add_argument("--format", type=str, default="console", choices=["console", "discord"],
                        help="Card output format")
    parser.add_argument("--season", type=str, default="2024-25", help="Season")

    args = parser.parse_args()

    target_date = args.date or date.today().isoformat()

    print_header(f"AXIOM DAILY PIPELINE - {target_date}")

    prop_edges = []
    spread_picks = []

    # Step 1-3: Update data (unless skipped)
    if not args.skip_update and not args.spreads_only:
        print("Updating player data (this may take a few minutes)...")
        update_player_data(args.season)

    # Step 4: Spread & Total edges (unless props-only)
    if not args.props_only:
        success, spread_picks = run_spread_total_edges(target_date)

    # Step 5: Team spreads - legacy (unless props-only)
    if not args.props_only and not args.skip_update:
        run_team_spreads(target_date)

    # Step 6: Props edges (unless spreads-only)
    if not args.spreads_only:
        success, prop_edges = run_props_finder(target_date)

    # Step 7: Stat nuggets (optional)
    if args.with_nuggets and not args.spreads_only:
        conn = sqlite3.connect(DB_PATH)
        generate_stat_nuggets(conn)
        conn.close()

    # Step 7b: AI verification (optional)
    if args.with_verification and not args.spreads_only:
        run_ai_verification(target_date)

    # Step 7c: Build parlay
    if not args.props_only and not args.spreads_only:
        print_step("7c", "Building Daily Parlay")
        try:
            from scripts.build_parlay import get_todays_games, get_todays_props, find_parlay_legs, build_optimal_parlay, calculate_parlay_odds
            games = get_todays_games(target_date)
            props = get_todays_props(target_date)
            legs = find_parlay_legs(games, props)
            parlay = build_optimal_parlay(legs, 5)
            if parlay:
                prob, payout, roi = calculate_parlay_odds(parlay)
                print(f"\n  {len(parlay)}-leg parlay found")
                print(f"  Combined: {prob*100:.1f}% | Payout: {payout:.1f}x | ROI: {roi:+.0f}%")
                for i, leg in enumerate(parlay[:3], 1):
                    print(f"    Leg {i}: {leg['pick'][:40]} ({leg['hit_rate']*100:.0f}%)")
                if len(parlay) > 3:
                    print(f"    ... +{len(parlay)-3} more legs")
            else:
                print("  No qualifying parlay today")
        except Exception as e:
            print(f"  Parlay builder error: {e}")

    # Step 8: Generate daily card
    generate_daily_card(target_date=target_date, card_format=args.format, spread_picks=spread_picks)

    print_header("PIPELINE COMPLETE")

    return 0


if __name__ == "__main__":
    sys.exit(main())
