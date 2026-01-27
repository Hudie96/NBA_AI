"""
Log Player Prop Picks

Task 2.3 from AXIOM_ACTION_PLAN_v2.md
Logs prop predictions for tracking results.

Usage:
    python scripts/log_prop_result.py --player "LeBron James" --stat PTS --line 24.5 --pick OVER --confidence MEDIUM
    python scripts/log_prop_result.py --from-edge  # Log from last edge finder run
    python scripts/log_prop_result.py --show  # Show pending picks
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


def create_props_results_table(conn):
    """Create props_results table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS props_results (
            date TEXT,
            player_name TEXT,
            opponent TEXT,
            prop_type TEXT,
            line REAL,
            projection REAL,
            edge REAL,
            edge_pct REAL,
            pick TEXT,
            confidence TEXT,
            actual REAL,
            result TEXT,
            PRIMARY KEY (date, player_name, prop_type)
        )
    """)
    conn.commit()


def log_prop_pick(player_name, opponent, prop_type, line, projection, edge,
                  edge_pct, pick, confidence, conn, pick_date=None):
    """
    Log a prop pick to the database.

    Returns:
        True if logged successfully, False if duplicate
    """
    if pick_date is None:
        pick_date = date.today().isoformat()

    # Check for duplicate
    existing = pd.read_sql("""
        SELECT * FROM props_results
        WHERE date = ? AND player_name = ? AND prop_type = ?
    """, conn, params=(pick_date, player_name, prop_type))

    if not existing.empty:
        print(f"  Already logged: {player_name} {prop_type} on {pick_date}")
        return False

    # Insert
    conn.execute("""
        INSERT INTO props_results
        (date, player_name, opponent, prop_type, line, projection, edge, edge_pct, pick, confidence, actual, result)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
    """, (pick_date, player_name, opponent, prop_type, line, projection, edge, edge_pct, pick, confidence))
    conn.commit()

    print(f"  Logged: {player_name} {pick} {line} {prop_type} ({confidence})")
    return True


def log_from_edges(conn, min_confidence="LOW"):
    """
    Log picks from the props_edges table.

    Args:
        conn: Database connection
        min_confidence: Minimum confidence level to log (LOW, MEDIUM, HIGH)
    """
    confidence_order = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
    min_level = confidence_order.get(min_confidence.upper(), 1)

    # Get today's edges
    today = date.today().isoformat()

    edges = pd.read_sql("""
        SELECT * FROM props_edges
        WHERE date = ?
        ORDER BY confidence_score DESC
    """, conn, params=(today,))

    if edges.empty:
        print(f"No edges found for {today}")
        print("Run: python scripts/find_edges.py --test --save")
        return 0

    logged = 0
    for _, edge in edges.iterrows():
        conf_level = confidence_order.get(edge["confidence"], 0)
        if conf_level >= min_level:
            success = log_prop_pick(
                player_name=edge["player_name"],
                opponent=edge["opponent"],
                prop_type=edge["prop_type"],
                line=edge["line"],
                projection=edge["projection"],
                edge=edge["edge"],
                edge_pct=edge["edge_pct"],
                pick=edge["pick"],
                confidence=edge["confidence"],
                conn=conn,
                pick_date=today
            )
            if success:
                logged += 1

    return logged


def show_pending_picks(conn):
    """Show picks that don't have results yet."""
    pending = pd.read_sql("""
        SELECT date, player_name, opponent, prop_type, line, pick, confidence, projection, edge_pct
        FROM props_results
        WHERE result IS NULL
        ORDER BY date DESC, confidence DESC
    """, conn)

    if pending.empty:
        print("No pending picks")
        return

    print(f"\n=== PENDING PICKS ({len(pending)}) ===\n")
    for _, row in pending.iterrows():
        conf_marker = {"HIGH": "[***]", "MEDIUM": "[**]", "LOW": "[*]"}.get(row["confidence"], "[ ]")
        print(f"{row['date']} {conf_marker} {row['player_name']} {row['pick']} {row['line']} {row['prop_type']} vs {row['opponent']}")
        print(f"         Projection: {row['projection']} | Edge: {row['edge_pct']:+.1f}%")


def show_results_summary(conn):
    """Show results summary by confidence level."""
    results = pd.read_sql("""
        SELECT confidence,
               COUNT(*) as total,
               SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN result = 'LOSS' THEN 1 ELSE 0 END) as losses,
               SUM(CASE WHEN result = 'PUSH' THEN 1 ELSE 0 END) as pushes
        FROM props_results
        WHERE result IS NOT NULL
        GROUP BY confidence
        ORDER BY
            CASE confidence
                WHEN 'HIGH' THEN 1
                WHEN 'MEDIUM' THEN 2
                WHEN 'LOW' THEN 3
            END
    """, conn)

    if results.empty:
        print("\nNo completed results yet")
        return

    print("\n=== RESULTS BY CONFIDENCE ===\n")
    for _, row in results.iterrows():
        total = row["total"]
        wins = row["wins"]
        losses = row["losses"]
        pushes = row["pushes"]
        win_pct = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0

        print(f"{row['confidence']:>6}: {wins}W-{losses}L-{pushes}P ({win_pct:.1f}%)")

    # Overall
    total_wins = results["wins"].sum()
    total_losses = results["losses"].sum()
    total_pushes = results["pushes"].sum()
    overall_pct = (total_wins / (total_wins + total_losses) * 100) if (total_wins + total_losses) > 0 else 0

    print(f"\n{'OVERALL':>6}: {total_wins}W-{total_losses}L-{total_pushes}P ({overall_pct:.1f}%)")


def main():
    parser = argparse.ArgumentParser(description="Log prop picks")
    parser.add_argument("--player", type=str, help="Player name")
    parser.add_argument("--opponent", type=str, help="Opponent abbreviation")
    parser.add_argument("--stat", type=str, help="Stat type (PTS, REB, AST, 3PM)")
    parser.add_argument("--line", type=float, help="Betting line")
    parser.add_argument("--pick", type=str, choices=["OVER", "UNDER"], help="Pick direction")
    parser.add_argument("--confidence", type=str, default="MEDIUM", help="Confidence level")
    parser.add_argument("--projection", type=float, help="Our projection")
    parser.add_argument("--from-edge", action="store_true", help="Log from edge finder results")
    parser.add_argument("--min-confidence", type=str, default="MEDIUM", help="Min confidence for --from-edge")
    parser.add_argument("--show", action="store_true", help="Show pending picks")
    parser.add_argument("--summary", action="store_true", help="Show results summary")

    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    create_props_results_table(conn)

    if args.show:
        show_pending_picks(conn)
        conn.close()
        return 0

    if args.summary:
        show_results_summary(conn)
        conn.close()
        return 0

    if args.from_edge:
        print(f"Logging picks from edge finder (min: {args.min_confidence})...\n")
        count = log_from_edges(conn, min_confidence=args.min_confidence)
        print(f"\nLogged {count} picks")
        conn.close()
        return 0

    if args.player and args.stat and args.line and args.pick:
        # Calculate edge if projection provided
        edge = 0
        edge_pct = 0
        if args.projection:
            edge = args.projection - args.line
            edge_pct = (edge / args.line) * 100 if args.line > 0 else 0

        log_prop_pick(
            player_name=args.player,
            opponent=args.opponent or "UNK",
            prop_type=args.stat.upper(),
            line=args.line,
            projection=args.projection or args.line,
            edge=round(edge, 1),
            edge_pct=round(edge_pct, 1),
            pick=args.pick.upper(),
            confidence=args.confidence.upper(),
            conn=conn
        )
        conn.close()
        return 0

    print("Usage:")
    print("  python log_prop_result.py --player 'LeBron James' --opponent LAC --stat PTS --line 24.5 --pick OVER")
    print("  python log_prop_result.py --from-edge --min-confidence MEDIUM")
    print("  python log_prop_result.py --show")
    print("  python log_prop_result.py --summary")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
