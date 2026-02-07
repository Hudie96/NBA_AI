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
import os
import subprocess
import sys
import sqlite3
import requests
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"

sys.path.insert(0, str(PROJECT_ROOT))
from src.config import config

DB_PATH = config["database"]["path"]

# ESPN to standard abbreviation mapping
ESPN_TO_STD = {
    'UTAH': 'UTA', 'WSH': 'WAS', 'SA': 'SAS', 'NY': 'NYK',
    'GS': 'GSW', 'NO': 'NOP', 'PHO': 'PHX', 'PHOE': 'PHX'
}


def fetch_betting_lines(target_date: str):
    """Fetch betting lines from ESPN for target date."""
    print(f"\n{'='*60}")
    print(f"  Fetching betting lines for {target_date}")
    print(f"{'='*60}\n")

    conn = sqlite3.connect(DB_PATH)
    date_fmt = target_date.replace('-', '')

    try:
        url = f'https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_fmt}'
        resp = requests.get(url, timeout=30)
        data = resp.json()
        events = data.get('events', [])

        saved = 0
        for event in events:
            competition = event.get('competitions', [{}])[0]
            competitors = competition.get('competitors', [])
            home = next((c for c in competitors if c.get('homeAway') == 'home'), {})
            away = next((c for c in competitors if c.get('homeAway') == 'away'), {})

            home_abbrev = ESPN_TO_STD.get(home.get('team', {}).get('abbreviation', ''),
                                          home.get('team', {}).get('abbreviation', ''))
            away_abbrev = ESPN_TO_STD.get(away.get('team', {}).get('abbreviation', ''),
                                          away.get('team', {}).get('abbreviation', ''))

            game = conn.execute('''
                SELECT game_id FROM Games
                WHERE home_team = ? AND away_team = ? AND date(date_time_utc) = ?
            ''', (home_abbrev, away_abbrev, target_date)).fetchone()

            if not game:
                continue

            game_id = game[0]
            odds = competition.get('odds', [{}])
            if not odds:
                continue

            odds_data = odds[0]
            spread = odds_data.get('spread')
            total = odds_data.get('overUnder')
            home_ml = odds_data.get('homeTeamOdds', {}).get('moneyLine')
            away_ml = odds_data.get('awayTeamOdds', {}).get('moneyLine')

            exists = conn.execute('SELECT 1 FROM Betting WHERE game_id = ?', (game_id,)).fetchone()

            if exists:
                conn.execute('''
                    UPDATE Betting
                    SET espn_current_spread = ?, espn_current_total = ?,
                        espn_current_ml_home = ?, espn_current_ml_away = ?,
                        updated_at = datetime('now')
                    WHERE game_id = ?
                ''', (spread, total, home_ml, away_ml, game_id))
            else:
                conn.execute('''
                    INSERT INTO Betting
                    (game_id, espn_current_spread, espn_current_total, espn_current_ml_home, espn_current_ml_away, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                ''', (game_id, spread, total, home_ml, away_ml))

            print(f"  {away_abbrev} @ {home_abbrev}: spread={spread}, total={total}")
            saved += 1

        conn.commit()
        print(f"\n  Saved {saved} betting lines")

    except Exception as e:
        print(f"  [WARN] Failed to fetch betting lines: {e}")
    finally:
        conn.close()


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
    parser.add_argument("--results", action="store_true", help="Run auto-results collection after pipeline")
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

    # Step 1: Data Refresh (team stats, betting lines, etc)
    if not args.predictions:
        refresh_args = ["--quick"] if args.quick else []
        if not run_script("refresh_all_data.py", refresh_args, required=False):
            print("[WARN] Data refresh had issues, continuing with existing data")

    # Step 1b: Update PlayerBox (boxscores for recent games)
    if not args.predictions and not args.quick:
        if not run_script("update_boxscores.py", [], required=False):
            print("[WARN] Boxscore update had issues")

    # Step 1c: Fetch betting lines from ESPN
    if not args.predictions:
        fetch_betting_lines(target_date)

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

    # Step 6: Discord posting (if any webhook configured)
    if any(os.getenv(k) for k in ['DISCORD_WEBHOOK_PLATINUM', 'DISCORD_WEBHOOK_GOLD', 'DISCORD_WEBHOOK_FREE', 'DISCORD_WEBHOOK_RESULTS']):
        run_script("discord_poster.py", ["--picks", "--date", target_date], required=False)

    # Step 7: Auto-results collection (with --results flag)
    if args.results:
        run_script("auto_results.py", ["--date", target_date], required=False)

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
    - PLATINUM: Model edge >= +7 pts vs Vegas
    - GOLD: Model edge >= +5 pts vs Vegas
    - SILVER: Model edge >= +3 pts vs Vegas

  PROPS (Star Players Only):
    - PLATINUM: Edge >= 25%
    - GOLD: Edge >= 20%
    - SILVER: Edge >= 15%
    - Based on L10 avg, season avg, vs opponent history
""")

    print("Next steps:")
    print("  1. Review ai_review for spread picks")
    print("  2. Review S_TIER props above")
    print("  3. After games: python scripts/update_result.py <date> <game> <W/L> <margin>")

    return 0


if __name__ == "__main__":
    sys.exit(main())
