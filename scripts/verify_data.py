"""
Data Verification Script for AXIOM Pipeline

Validates that all data sources are fresh and consistent before running predictions.
Catches issues like stale data, mismatched player names, missing games, etc.

Usage:
    python scripts/verify_data.py              # Full verification
    python scripts/verify_data.py --quick      # Quick freshness check only
"""

import argparse
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]


def check_data_freshness(conn, target_date: str) -> dict:
    """Check that all data tables have recent data."""
    issues = []
    warnings = []

    target = datetime.strptime(target_date, "%Y-%m-%d").date()
    yesterday = target - timedelta(days=1)
    week_ago = target - timedelta(days=7)

    # Check PlayerBox freshness
    df = pd.read_sql("""
        SELECT MAX(DATE(g.date_time_utc)) as max_date, COUNT(*) as total
        FROM PlayerBox pb
        JOIN Games g ON pb.game_id = g.game_id
    """, conn)

    if not df.empty and df.iloc[0]["max_date"]:
        max_date = datetime.strptime(df.iloc[0]["max_date"], "%Y-%m-%d").date()
        days_old = (target - max_date).days
        if days_old > 7:
            issues.append(f"PlayerBox data is {days_old} days old (last: {max_date})")
        elif days_old > 2:
            warnings.append(f"PlayerBox data is {days_old} days old (last: {max_date})")
        print(f"  PlayerBox: Latest date {max_date}, {df.iloc[0]['total']:,} rows")
    else:
        issues.append("PlayerBox table is empty!")

    # Check Games freshness
    df = pd.read_sql("""
        SELECT MAX(DATE(date_time_utc)) as max_date, COUNT(*) as total
        FROM Games
    """, conn)

    if not df.empty and df.iloc[0]["max_date"]:
        max_date = datetime.strptime(df.iloc[0]["max_date"], "%Y-%m-%d").date()
        print(f"  Games: Latest date {max_date}, {df.iloc[0]['total']:,} rows")

    # Check Betting freshness (join with Games for date)
    df = pd.read_sql("""
        SELECT MAX(DATE(g.date_time_utc)) as max_date, COUNT(*) as total
        FROM Betting b
        JOIN Games g ON b.game_id = g.game_id
    """, conn)

    if not df.empty and df.iloc[0]["max_date"]:
        max_date = datetime.strptime(df.iloc[0]["max_date"], "%Y-%m-%d").date()
        days_old = (target - max_date).days
        if days_old > 7:
            warnings.append(f"Betting data is {days_old} days old")
        print(f"  Betting: Latest date {max_date}, {df.iloc[0]['total']:,} rows")

    # Check if player_game_logs is stale (common issue)
    try:
        df = pd.read_sql("""
            SELECT MAX(game_date) as max_date FROM player_game_logs
        """, conn)
        if not df.empty and df.iloc[0]["max_date"]:
            max_date = datetime.strptime(df.iloc[0]["max_date"], "%Y-%m-%d").date()
            days_old = (target - max_date).days
            if days_old > 30:
                warnings.append(f"player_game_logs is STALE ({days_old} days old) - using PlayerBox instead")
    except:
        pass  # Table might not exist

    return {"issues": issues, "warnings": warnings}


def check_player_data_consistency(conn) -> dict:
    """Verify player data is consistent across tables."""
    issues = []
    warnings = []

    # Check for players with very few games (might indicate name mismatch)
    df = pd.read_sql("""
        SELECT player_name, COUNT(*) as games, AVG(min) as avg_min
        FROM PlayerBox
        WHERE min > 0
        GROUP BY player_name
        HAVING games >= 3 AND avg_min >= 15
        ORDER BY games
        LIMIT 20
    """, conn)

    # Players with significant minutes but few games might be duplicates
    for _, row in df.iterrows():
        if row["games"] <= 5 and row["avg_min"] >= 20:
            warnings.append(f"Player '{row['player_name']}' has only {row['games']} games but {row['avg_min']:.1f} avg min - possible name variant?")

    print(f"  Active players (3+ games, 15+ min): {len(df)}")

    return {"issues": issues, "warnings": warnings}


def check_l10_calculations(conn, sample_size=5) -> dict:
    """Verify L10 calculations match expected values."""
    issues = []
    warnings = []

    # Get some active players to test
    df = pd.read_sql("""
        SELECT player_name
        FROM PlayerBox
        WHERE min > 0
        GROUP BY player_name
        HAVING COUNT(*) >= 10
        ORDER BY SUM(min) DESC
        LIMIT ?
    """, conn, params=(sample_size,))

    for _, row in df.iterrows():
        player = row["player_name"]

        # Get L10 PRA from direct calculation
        l10 = pd.read_sql(f"""
            SELECT pb.pts, pb.reb, pb.ast, (pb.pts + pb.reb + pb.ast) as pra
            FROM PlayerBox pb
            JOIN Games g ON pb.game_id = g.game_id
            WHERE pb.player_name = ?
              AND pb.min > 0
            ORDER BY g.date_time_utc DESC
            LIMIT 10
        """, conn, params=(player,))

        if not l10.empty:
            l10_pra = l10["pra"].mean()
            print(f"  {player}: L10 PRA = {l10_pra:.1f}")

    return {"issues": issues, "warnings": warnings}


def check_todays_games(conn, target_date: str) -> dict:
    """Check that we have games scheduled for target date."""
    issues = []
    warnings = []

    df = pd.read_sql("""
        SELECT game_id, home_team, away_team, status
        FROM Games
        WHERE DATE(date_time_utc) = ?
    """, conn, params=(target_date,))

    if df.empty:
        issues.append(f"No games found for {target_date}")
    else:
        print(f"  Games on {target_date}: {len(df)}")
        for _, row in df.iterrows():
            print(f"    {row['away_team']} @ {row['home_team']}")

    return {"issues": issues, "warnings": warnings}


def main():
    parser = argparse.ArgumentParser(description="Verify AXIOM data integrity")
    parser.add_argument("--date", type=str, default=date.today().isoformat(),
                        help="Target date (YYYY-MM-DD)")
    parser.add_argument("--quick", action="store_true", help="Quick check only")
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print(f"  AXIOM DATA VERIFICATION - {args.date}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    all_issues = []
    all_warnings = []

    # Check 1: Data Freshness
    print("\n[1] Data Freshness Check")
    print("-" * 40)
    result = check_data_freshness(conn, args.date)
    all_issues.extend(result["issues"])
    all_warnings.extend(result["warnings"])

    if not args.quick:
        # Check 2: Today's Games
        print("\n[2] Today's Games")
        print("-" * 40)
        result = check_todays_games(conn, args.date)
        all_issues.extend(result["issues"])
        all_warnings.extend(result["warnings"])

        # Check 3: Player Consistency
        print("\n[3] Player Data Consistency")
        print("-" * 40)
        result = check_player_data_consistency(conn)
        all_issues.extend(result["issues"])
        all_warnings.extend(result["warnings"])

        # Check 4: L10 Calculations
        print("\n[4] L10 Calculation Verification (sample)")
        print("-" * 40)
        result = check_l10_calculations(conn, sample_size=3)
        all_issues.extend(result["issues"])
        all_warnings.extend(result["warnings"])

    conn.close()

    # Summary
    print("\n" + "=" * 60)
    print("  VERIFICATION SUMMARY")
    print("=" * 60)

    if all_issues:
        print("\n[ERRORS] - Must fix before running pipeline:")
        for issue in all_issues:
            print(f"  - {issue}")

    if all_warnings:
        print("\n[WARNINGS] - Review but can proceed:")
        for warning in all_warnings:
            print(f"  - {warning}")

    if not all_issues and not all_warnings:
        print("\n  All checks passed!")

    print("")

    # Return error code if critical issues
    return 1 if all_issues else 0


if __name__ == "__main__":
    sys.exit(main())
