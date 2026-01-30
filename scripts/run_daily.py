"""
AXIOM Daily Runner
Master script that runs the full daily pipeline in order.

Usage:
    python scripts/run_daily.py                  # Full pipeline (spreads + props)
    python scripts/run_daily.py --quick          # Skip slow data fetches
    python scripts/run_daily.py --spreads-only   # Spreads only, skip props
    python scripts/run_daily.py --props-only     # Props only, skip spreads
    python scripts/run_daily.py --date 2026-01-30  # Specific date
"""

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"


def run_script(name: str, args: list = None, required: bool = True) -> bool:
    """Run a script and return success status."""
    script_path = SCRIPTS_DIR / name
    cmd = [sys.executable, str(script_path)] + (args or [])

    print(f"\n{'='*60}")
    print(f"  Running: {name} {' '.join(args or [])}")
    print(f"{'='*60}\n")

    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))

    if result.returncode != 0:
        if required:
            print(f"\n[ERROR] {name} failed with code {result.returncode}")
            return False
        else:
            print(f"\n[WARN] {name} failed (optional, continuing)")

    return True


def main():
    parser = argparse.ArgumentParser(description="AXIOM Daily Runner")
    parser.add_argument("--quick", action="store_true", help="Skip slow data fetches")
    parser.add_argument("--predictions", action="store_true", help="Skip data refresh")
    parser.add_argument("--spreads-only", action="store_true", help="Spreads only, skip props")
    parser.add_argument("--props-only", action="store_true", help="Props only, skip spreads")
    parser.add_argument("--content", action="store_true", help="Generate social content")
    parser.add_argument("--date", type=str, default=None, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--verify", action="store_true", help="Run AI verification")
    args = parser.parse_args()

    target_date = args.date or date.today().isoformat()

    print("\n" + "="*60)
    print(f"  AXIOM DAILY PIPELINE - {target_date}")
    print("="*60)

    bet_types = []
    if not args.props_only:
        bet_types.append("SPREADS")
    if not args.spreads_only:
        bet_types.append("PROPS")
    print(f"  Bet Types: {', '.join(bet_types)}")
    print("="*60)

    # Step 1: Data Refresh
    if not args.predictions:
        refresh_args = ["--quick"] if args.quick else []
        if not run_script("refresh_all_data.py", refresh_args, required=False):
            print("[WARN] Data refresh had issues, continuing with existing data")

    # Step 2: Generate Spread Predictions
    if not args.props_only:
        pred_args = ["--date", target_date]
        if not run_script("daily_predictions.py", pred_args):
            print("[ERROR] Spread predictions failed")
            if args.spreads_only:
                return 1

    # Step 3: Props Edge Finder (now default)
    if not args.spreads_only:
        props_args = ["--today", "--date", target_date]
        run_script("find_edges.py", props_args, required=False)

    # Step 4: AI Verification (optional)
    if args.verify:
        verify_args = ["--date", target_date]
        run_script("ai_verify_picks.py", verify_args, required=False)

    # Step 5: Content Generation (optional)
    if args.content:
        content_args = ["--date", target_date]
        run_script("generate_daily_report.py", content_args, required=False)
        run_script("generate_card.py", content_args, required=False)
        run_script("generate_nuggets.py", content_args, required=False)

    # Summary
    print("\n" + "="*60)
    print("  PIPELINE COMPLETE")
    print("="*60)
    print(f"\nOutputs in: {PROJECT_ROOT / 'outputs'}")

    if not args.props_only:
        print(f"\n  SPREADS:")
        print(f"    - ai_review_{target_date}.txt  (PLATINUM/GOLD/SILVER)")
        print(f"    - predictions_{target_date}.json")

    if not args.spreads_only:
        print(f"\n  PROPS:")
        print(f"    - Props edges printed above (S_TIER = top plays)")

    if args.content:
        print(f"\n  CONTENT:")
        print(f"    - daily_report_{target_date}.md")
        print(f"    - betting_card_{target_date}.png")

    print("\n" + "="*60)
    print("  BET TYPES SUMMARY")
    print("="*60)
    print("""
  SPREADS (Tier System):
    - PLATINUM: 84.4% win rate (GREEN + Edge >= +7)
    - GOLD: 78.9% win rate (GREEN + Edge >= +5)
    - SILVER: 74.4% win rate (Edge >= +5)

  PROPS (S_TIER):
    - High confidence player props
    - Based on L10, season avg, vs opponent history
""")

    print("Next steps:")
    print("  1. Review ai_review for spread picks")
    print("  2. Review S_TIER props above")
    print("  3. After games: python scripts/update_result.py <date> <game> <W/L> <margin>")

    return 0


if __name__ == "__main__":
    sys.exit(main())
