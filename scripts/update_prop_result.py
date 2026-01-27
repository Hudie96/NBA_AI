"""
Update Player Prop Results

Task 2.3 from AXIOM_ACTION_PLAN_v2.md
Updates prop picks with actual results after games complete.

Usage:
    python scripts/update_prop_result.py --player "LeBron James" --stat PTS --actual 28
    python scripts/update_prop_result.py --date 2025-01-25 --auto  # Auto-fetch from game logs
    python scripts/update_prop_result.py --pending  # Show pending picks to update
"""
import argparse
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]

# Stat column mapping
STAT_COLS = {
    "PTS": "points",
    "REB": "rebounds",
    "AST": "assists",
    "3PM": "threes_made"
}


def get_pending_picks(conn, pick_date=None):
    """Get picks that need results updated."""
    if pick_date:
        query = """
            SELECT * FROM props_results
            WHERE result IS NULL AND date = ?
            ORDER BY player_name
        """
        return pd.read_sql(query, conn, params=(pick_date,))
    else:
        query = """
            SELECT * FROM props_results
            WHERE result IS NULL
            ORDER BY date DESC, player_name
        """
        return pd.read_sql(query, conn)


def get_actual_stat(player_name, game_date, stat, conn):
    """
    Get actual stat from player_game_logs.

    Args:
        player_name: Player name
        game_date: Date of the game
        stat: Stat type (PTS, REB, AST, 3PM)
        conn: Database connection

    Returns:
        Actual stat value or None if not found
    """
    col = STAT_COLS.get(stat.upper(), stat.lower())

    df = pd.read_sql(f"""
        SELECT {col} as actual
        FROM player_game_logs
        WHERE player_name = ? AND game_date = ?
    """, conn, params=(player_name, game_date))

    if df.empty:
        return None
    return df.iloc[0]["actual"]


def determine_result(pick, line, actual):
    """
    Determine WIN/LOSS/PUSH based on pick and actual.

    Args:
        pick: 'OVER' or 'UNDER'
        line: Betting line
        actual: Actual stat value

    Returns:
        'WIN', 'LOSS', or 'PUSH'
    """
    if actual == line:
        return "PUSH"

    if pick == "OVER":
        return "WIN" if actual > line else "LOSS"
    else:  # UNDER
        return "WIN" if actual < line else "LOSS"


def update_result(player_name, prop_type, actual, conn, pick_date=None):
    """
    Update a prop pick with actual result.

    Args:
        player_name: Player name
        prop_type: Stat type
        actual: Actual stat value
        conn: Database connection
        pick_date: Date of the pick (defaults to today)

    Returns:
        Result string or None if pick not found
    """
    if pick_date is None:
        pick_date = date.today().isoformat()

    # Get the pick
    pick_df = pd.read_sql("""
        SELECT * FROM props_results
        WHERE player_name = ? AND prop_type = ? AND date = ?
    """, conn, params=(player_name, prop_type, pick_date))

    if pick_df.empty:
        print(f"  Pick not found: {player_name} {prop_type} on {pick_date}")
        return None

    pick_row = pick_df.iloc[0]
    result = determine_result(pick_row["pick"], pick_row["line"], actual)

    # Update
    conn.execute("""
        UPDATE props_results
        SET actual = ?, result = ?
        WHERE player_name = ? AND prop_type = ? AND date = ?
    """, (actual, result, player_name, prop_type, pick_date))
    conn.commit()

    # Display
    marker = {"WIN": "[W]", "LOSS": "[L]", "PUSH": "[P]"}.get(result, "[?]")
    print(f"  {marker} {player_name} {pick_row['pick']} {pick_row['line']} {prop_type}: Actual {actual}")

    return result


def auto_update_results(conn, pick_date=None):
    """
    Auto-update results from player_game_logs.

    Args:
        conn: Database connection
        pick_date: Date to update (defaults to yesterday)

    Returns:
        Number of picks updated
    """
    if pick_date is None:
        # Default to yesterday since today's games may not be complete
        pick_date = (date.today() - timedelta(days=1)).isoformat()

    pending = get_pending_picks(conn, pick_date)

    if pending.empty:
        print(f"No pending picks for {pick_date}")
        return 0

    print(f"Auto-updating {len(pending)} picks for {pick_date}...\n")

    updated = 0
    for _, pick in pending.iterrows():
        actual = get_actual_stat(
            pick["player_name"],
            pick["date"],
            pick["prop_type"],
            conn
        )

        if actual is not None:
            result = update_result(
                pick["player_name"],
                pick["prop_type"],
                actual,
                conn,
                pick["date"]
            )
            if result:
                updated += 1
        else:
            print(f"  No game data: {pick['player_name']} on {pick['date']}")

    return updated


def show_pending(conn):
    """Display pending picks that need results."""
    pending = get_pending_picks(conn)

    if pending.empty:
        print("No pending picks to update")
        return

    print(f"\n=== PENDING RESULTS ({len(pending)}) ===\n")

    current_date = None
    for _, pick in pending.iterrows():
        if pick["date"] != current_date:
            current_date = pick["date"]
            print(f"\n{current_date}:")

        conf_marker = {"HIGH": "[***]", "MEDIUM": "[**]", "LOW": "[*]"}.get(pick["confidence"], "[ ]")
        print(f"  {conf_marker} {pick['player_name']} {pick['pick']} {pick['line']} {pick['prop_type']} vs {pick['opponent']}")


def show_recent_results(conn, days=7):
    """Show recent results."""
    cutoff = (date.today() - timedelta(days=days)).isoformat()

    results = pd.read_sql("""
        SELECT date, player_name, prop_type, line, pick, actual, result, confidence
        FROM props_results
        WHERE result IS NOT NULL AND date >= ?
        ORDER BY date DESC, player_name
    """, conn, params=(cutoff,))

    if results.empty:
        print(f"No results in last {days} days")
        return

    print(f"\n=== RECENT RESULTS (last {days} days) ===\n")

    for _, row in results.iterrows():
        marker = {"WIN": "[W]", "LOSS": "[L]", "PUSH": "[P]"}.get(row["result"], "[?]")
        print(f"{row['date']} {marker} {row['player_name']} {row['pick']} {row['line']} {row['prop_type']}: {row['actual']}")


def main():
    parser = argparse.ArgumentParser(description="Update prop results")
    parser.add_argument("--player", type=str, help="Player name")
    parser.add_argument("--stat", type=str, help="Stat type")
    parser.add_argument("--actual", type=float, help="Actual stat value")
    parser.add_argument("--date", type=str, help="Pick date (YYYY-MM-DD)")
    parser.add_argument("--auto", action="store_true", help="Auto-update from game logs")
    parser.add_argument("--pending", action="store_true", help="Show pending picks")
    parser.add_argument("--recent", action="store_true", help="Show recent results")

    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)

    if args.pending:
        show_pending(conn)
        conn.close()
        return 0

    if args.recent:
        show_recent_results(conn)
        conn.close()
        return 0

    if args.auto:
        count = auto_update_results(conn, args.date)
        print(f"\nUpdated {count} results")
        conn.close()
        return 0

    if args.player and args.stat and args.actual is not None:
        update_result(
            args.player,
            args.stat.upper(),
            args.actual,
            conn,
            args.date
        )
        conn.close()
        return 0

    print("Usage:")
    print("  python update_prop_result.py --player 'LeBron James' --stat PTS --actual 28")
    print("  python update_prop_result.py --date 2025-01-25 --auto")
    print("  python update_prop_result.py --pending")
    print("  python update_prop_result.py --recent")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
