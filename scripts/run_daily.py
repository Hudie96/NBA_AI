"""
AXIOM Daily Runner
Master script that runs the full daily pipeline in order.

Usage:
    python scripts/run_daily.py                  # Full pipeline
    python scripts/run_daily.py --quick          # Skip slow data fetches
    python scripts/run_daily.py --predictions    # Predictions only (skip data refresh)
    python scripts/run_daily.py --content        # Include content generation
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
    parser.add_argument("--predictions", action="store_true", help="Predictions only, skip data refresh")
    parser.add_argument("--content", action="store_true", help="Generate social content")
    parser.add_argument("--props", action="store_true", help="Run props edge finder")
    parser.add_argument("--date", type=str, default=None, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--verify", action="store_true", help="Run AI verification")
    args = parser.parse_args()

    target_date = args.date or date.today().isoformat()

    print("\n" + "="*60)
    print(f"  AXIOM DAILY PIPELINE - {target_date}")
    print("="*60)

    # Step 1: Data Refresh
    if not args.predictions:
        refresh_args = ["--quick"] if args.quick else []
        if not run_script("refresh_all_data.py", refresh_args, required=False):
            print("[WARN] Data refresh had issues, continuing with existing data")

    # Step 2: Generate Predictions (core pipeline)
    pred_args = ["--date", target_date]
    if not run_script("daily_predictions.py", pred_args):
        print("[FATAL] Predictions failed")
        return 1

    # Step 3: Props Edge Finder (optional)
    if args.props:
        props_args = ["--date", target_date]
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
    print(f"  - ai_review_{target_date}.txt  (PLATINUM/GOLD/SILVER picks)")
    print(f"  - predictions_{target_date}.json")
    print(f"  - predictions_{target_date}.txt")

    if args.content:
        print(f"  - daily_report_{target_date}.md")
        print(f"  - betting_card_{target_date}.png")

    print("\nNext steps:")
    print("  1. Review ai_review file for today's plays")
    print("  2. After games: python scripts/update_result.py <date> <game> <W/L> <margin>")

    return 0


if __name__ == "__main__":
    sys.exit(main())
