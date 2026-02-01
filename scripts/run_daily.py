"""
AXIOM Daily Runner
Master script that runs the full daily pipeline in order.

Usage:
    python scripts/run_daily.py                  # Full pipeline (spreads + props + verify + output)
    python scripts/run_daily.py --quick          # Skip slow data fetches
    python scripts/run_daily.py --spreads-only   # Spreads only, skip props
    python scripts/run_daily.py --props-only     # Props only, skip spreads
    python scripts/run_daily.py --skip-verify    # Skip AI verification step
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
    parser.add_argument("--date", type=str, default=None, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--skip-verify", action="store_true", help="Skip AI verification step")
    parser.add_argument("--skip-output", action="store_true", help="Skip output generation")
    parser.add_argument("--skip-data-check", action="store_true", help="Skip data verification step")
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

    # Step 0: Data Verification (default, skip with --skip-data-check)
    if not args.skip_data_check:
        verify_args = ["--date", target_date]
        if not run_script("verify_data.py", verify_args, required=False):
            print("[WARN] Data verification found issues - review above")

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

    # Step 4: AI Verification (default, skip with --skip-verify)
    if not args.skip_verify:
        verify_args = ["--date", target_date]
        if not run_script("ai_verify_picks.py", verify_args, required=False):
            print("[WARN] AI verification skipped (check API key in .env)")

    # Step 5: Generate Daily Output (default, skip with --skip-output)
    if not args.skip_output:
        output_args = ["--date", target_date, "--skip-betting"]
        run_script("generate_daily_output.py", output_args, required=False)

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

    if not args.skip_output:
        print(f"\n  OUTPUT FILES:")
        print(f"    - outputs/predictions/picks_{target_date}.csv")
        print(f"    - outputs/social/posts_{target_date}.txt")
        print(f"    - outputs/performance/performance_tracker.csv")

    if not args.skip_verify:
        print(f"\n  AI VERIFICATION:")
        print(f"    - Picks verified through Claude API")

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
