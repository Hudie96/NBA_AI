"""
Props Edge Finder

Task 2.2 from AXIOM_ACTION_PLAN_v2.md
Flags props where our projection differs significantly from the line.

BACKTEST-OPTIMIZED (30-day backtest, 21,000+ bets):

S-TIER (combo props) - 15%+ edges hit at 60.1%:
  RA: 58.6%, PRA: 57.8%, PA: 57.3%, PR: 56.1%

A-TIER (individual props) - solid performers:
  PTS: 56.2%, REB: 55.3%, AST: 54.6%, 3PM: 54.3%

DROPPED (not profitable after vig):
  STL: 52.5%, BLK: 53.2%

Usage:
    python scripts/find_edges.py --player "LeBron James" --opponent LAC --stat PTS --line 24.5
    python scripts/find_edges.py --file lines.csv  # Batch process from file
    python scripts/find_edges.py --test  # Test with sample data
"""
import argparse
import json
import sqlite3
import sys
from datetime import date
from pathlib import Path

import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config
from scripts.project_props import (
    project_player_prop,
    get_player_position,
    get_dvp_adjustment,
    get_vs_opponent_avg,
    build_player_positions_table,
    STATS,
    STAT_COLS
)
from scripts.props_validator import (
    validate_prop,
    get_valid_players_for_props,
    get_todays_games,
    is_player_playing_today,
)

DB_PATH = config["database"]["path"]

# =============================================================================
# STAT TIERS (based on 30-day backtest, 21,000+ bets)
# =============================================================================

# S-TIER: Combo props - best performers, 15%+ edges hit at 60.1%
S_TIER_STATS = ["RA", "PRA", "PA", "PR"]

# A-TIER: Individual props - solid performers
A_TIER_STATS = ["PTS", "REB", "AST", "3PM"]

# All profitable stats (S-TIER + A-TIER)
PROFITABLE_STATS = S_TIER_STATS + A_TIER_STATS

# DROPPED: STL (52.5%), BLK (53.2%) - not profitable after vig

# =============================================================================
# CONFIDENCE THRESHOLDS BY TIER
# =============================================================================
# S-TIER combos are more profitable at every edge level:
#   15%+ = HIGH (60%), 10-15% = MEDIUM (58%), 5-10% = LOW (54%)
# A-TIER individual props:
#   15%+ = MEDIUM (56%), 10-15% = LOW (55%)
EDGE_THRESHOLDS = {
    "S_TIER": {
        "HIGH": {"edge_pct": 15, "min_games": 15},     # 60%+ hit rate
        "MEDIUM": {"edge_pct": 10, "min_games": 10},   # ~58% hit rate
        "LOW": {"edge_pct": 5, "min_games": 10},       # ~54% hit rate
    },
    "A_TIER": {
        "MEDIUM": {"edge_pct": 15, "min_games": 20},   # ~56% hit rate
        "LOW": {"edge_pct": 10, "min_games": 15},      # ~55% hit rate
    }
}


def get_stat_tier(stat):
    """Get the tier for a stat (S_TIER or A_TIER)."""
    if stat in S_TIER_STATS:
        return "S_TIER"
    elif stat in A_TIER_STATS:
        return "A_TIER"
    return None


def calculate_confidence(edge_pct, vs_opp_games, dvp_rank, season_games, stat=None):
    """
    Calculate confidence level based on tiered backtest-validated thresholds.

    S-TIER (combo props): 15%+ = HIGH, 10-15% = MEDIUM
    A-TIER (individual):  15%+ = MEDIUM, 10-15% = LOW

    Returns:
        tuple: (confidence_level, confidence_score, is_top_play)
    """
    edge_pct = abs(edge_pct)
    tier = get_stat_tier(stat)
    is_top_play = False

    # Calculate a numeric confidence score (0-100)
    score = 0

    # Edge size is the primary driver
    if edge_pct >= 20:
        score += 50
    elif edge_pct >= 15:
        score += 40
    elif edge_pct >= 10:
        score += 25
    elif edge_pct >= 5:
        score += 15

    # Tier bonus - S-TIER gets significant boost
    if tier == "S_TIER":
        score += 25  # Combo props are proven best
        is_top_play = edge_pct >= 15
    elif tier == "A_TIER":
        score += 10
    else:
        score -= 20  # Penalty for non-profitable stats

    # Sample size contribution (up to 15 points)
    if season_games >= 40:
        score += 15
    elif season_games >= 25:
        score += 10
    elif season_games >= 15:
        score += 5

    # vs opponent history (up to 10 points)
    if vs_opp_games >= 4:
        score += 10
    elif vs_opp_games >= 2:
        score += 5

    # Determine confidence level based on TIER
    level = "NONE"

    if tier == "S_TIER":
        # S-TIER: 15%+ = HIGH, 10-15% = MEDIUM, 5-10% = LOW
        if edge_pct >= 15 and season_games >= 15:
            level = "HIGH"
        elif edge_pct >= 10 and season_games >= 10:
            level = "MEDIUM"
        elif edge_pct >= 5 and season_games >= 10:
            level = "LOW"
    elif tier == "A_TIER":
        # A-TIER: 15%+ = MEDIUM, 10-15% = LOW
        if edge_pct >= 15 and season_games >= 20:
            level = "MEDIUM"
        elif edge_pct >= 10 and season_games >= 15:
            level = "LOW"

    return level, score, is_top_play


def get_dvp_rank(opponent, position, stat, conn):
    """Get DVP rank for opponent vs position for stat."""
    df = pd.read_sql("""
        SELECT rank FROM defense_vs_position
        WHERE team = ? AND position = ? AND stat = ?
    """, conn, params=(opponent, position, stat))

    if df.empty:
        return None
    return int(df.iloc[0]["rank"])


def get_player_season_games(player_name, conn):
    """Get number of games player has played this season."""
    df = pd.read_sql("""
        SELECT COUNT(*) as games FROM player_game_logs
        WHERE player_name = ?
    """, conn, params=(player_name,))
    return df.iloc[0]["games"]


def find_edge(player_name, opponent, stat, line, conn, target_date=None, validate=True):
    """
    Find edge between our projection and the line.

    Args:
        player_name: Player name
        opponent: Opponent team abbreviation
        stat: Stat type (PTS, REB, AST, etc.)
        line: Betting line
        conn: Database connection
        target_date: Date to validate against (None = today)
        validate: If True, validate player is playing on target_date

    Returns:
        dict with edge details or None if no edge
    """
    # Validate that player is actually playing on this date
    if validate:
        is_valid, error = validate_prop(player_name, opponent, conn, target_date)
        if not is_valid:
            return None

    # Get projection
    position = get_player_position(player_name, conn)
    proj = project_player_prop(player_name, opponent, stat, conn, position)

    if proj is None:
        return None

    projection = proj["projection"]

    # Calculate edge
    edge = projection - line
    if line > 0:
        edge_pct = (edge / line) * 100
    else:
        edge_pct = 0

    # Get supporting data for confidence
    vs_opp_avg, vs_opp_games = get_vs_opponent_avg(player_name, opponent, stat, conn)
    dvp_rank = get_dvp_rank(opponent, position, stat, conn)
    season_games = get_player_season_games(player_name, conn)

    # Calculate confidence (pass stat for tier-based scoring)
    confidence, confidence_score, is_top_play = calculate_confidence(
        edge_pct, vs_opp_games, dvp_rank, season_games, stat
    )

    # Get stat tier for labeling
    stat_tier = get_stat_tier(stat)

    # Build factors JSON (convert numpy types to native Python)
    factors = {
        "last_10_avg": float(proj["last_10_avg"]) if proj["last_10_avg"] else None,
        "season_avg": float(proj["season_avg"]) if proj["season_avg"] else None,
        "vs_opp_avg": float(vs_opp_avg) if vs_opp_avg else None,
        "vs_opp_games": int(vs_opp_games) if vs_opp_games else 0,
        "dvp_adj": float(proj["dvp_adj"]) if proj["dvp_adj"] else 0,
        "dvp_rank": int(dvp_rank) if dvp_rank else None,
        "position": position,
        "season_games": int(season_games)
    }

    # Determine pick direction
    pick = "OVER" if edge > 0 else "UNDER"

    return {
        "date": date.today().isoformat(),
        "player_name": player_name,
        "opponent": opponent,
        "prop_type": stat,
        "line": line,
        "projection": projection,
        "edge": round(edge, 1),
        "edge_pct": round(edge_pct, 1),
        "pick": pick,
        "confidence": confidence,
        "confidence_score": confidence_score,
        "stat_tier": stat_tier,
        "is_top_play": is_top_play,
        "factors": json.dumps(factors)
    }


def find_all_edges(player_name, opponent, lines_dict, conn, profitable_only=True):
    """
    Find edges for all stats for a player.

    Args:
        player_name: Player name
        opponent: Opponent abbreviation
        lines_dict: Dict of {stat: line} e.g., {"PTS": 24.5, "REB": 7.5}
        conn: Database connection
        profitable_only: If True, only consider PTS/AST (backtest-validated)

    Returns:
        List of edge dicts
    """
    edges = []
    for stat, line in lines_dict.items():
        stat_upper = stat.upper()

        # Filter to profitable stats if requested
        if profitable_only and stat_upper not in PROFITABLE_STATS:
            continue

        if stat_upper in STATS:
            edge = find_edge(player_name, opponent, stat_upper, line, conn)
            if edge and edge["confidence"] != "NONE":
                edges.append(edge)
    return edges


def save_edges_to_db(edges, conn):
    """Save edges to database."""
    if not edges:
        return 0

    df = pd.DataFrame(edges)

    # Append to existing or create new
    df.to_sql("props_edges", conn, if_exists="append", index=False)

    return len(edges)


def display_edge(edge):
    """Pretty print an edge."""
    conf_marker = {"HIGH": "[***]", "MEDIUM": "[**]", "LOW": "[*]"}.get(edge["confidence"], "[ ]")

    factors = json.loads(edge["factors"])

    # Show tier and top play status
    tier = edge.get("stat_tier", "")
    tier_label = f" [{tier}]" if tier else ""
    top_play = " >>> TOP PLAY <<<" if edge.get("is_top_play") else ""

    print(f"\n{conf_marker} {edge['confidence']} CONFIDENCE{tier_label}{top_play}")
    print(f"   {edge['player_name']} {edge['pick']} {edge['line']} {edge['prop_type']}")
    print(f"   vs {edge['opponent']}")
    print(f"   Projection: {edge['projection']} | Edge: {edge['edge']:+.1f} ({edge['edge_pct']:+.1f}%)")
    print(f"   L10: {factors['last_10_avg']} | Szn: {factors['season_avg']} | vsOpp: {factors['vs_opp_avg']} ({factors['vs_opp_games']}g)")
    print(f"   DVP Rank: {factors['dvp_rank']} | DVP Adj: {factors['dvp_adj']:+.1f}")


def find_edges_for_today(conn, target_date=None, all_stats=False, min_games=20):
    """
    Find edges for all players playing on target date.

    This is the VALIDATED way to generate props - only includes players
    whose teams are actually playing on the target date.

    Args:
        conn: Database connection
        target_date: Date to find edges for (None = today)
        all_stats: If True, include non-profitable stats
        min_games: Minimum games for player to be included

    Returns:
        List of edge dicts
    """
    from datetime import date as dt
    if target_date is None:
        target_date = dt.today().isoformat()

    print(f"=== FINDING PROPS EDGES FOR {target_date} ===\n")

    # Get games for this date
    games = get_todays_games(conn, target_date)
    if not games:
        print(f"No games found for {target_date}")
        return []

    print(f"Games: {len(games)}")
    for g in games:
        print(f"  {g['away_team']} @ {g['home_team']}")
    print()

    # Get valid players
    valid_players = get_valid_players_for_props(conn, target_date, min_games)
    print(f"Players with {min_games}+ games: {len(valid_players)}")

    build_player_positions_table(conn)

    # Sample lines for each player (in production, these come from sportsbook API)
    # For now, we use their season averages as proxy lines
    all_edges = []

    for player_info in valid_players:
        player_name = player_info["player_name"]
        opponent = player_info["opponent"]

        # Get player's season averages to use as proxy lines
        position = get_player_position(player_name, conn)

        for stat in PROFITABLE_STATS if not all_stats else STATS:
            proj = project_player_prop(player_name, opponent, stat, conn, position)
            if proj and proj["season_avg"]:
                # Use season avg as proxy line (in production, use real lines)
                line = proj["season_avg"]

                edge = find_edge(
                    player_name, opponent, stat, line, conn,
                    target_date=target_date, validate=False  # Already validated
                )

                if edge and edge["confidence"] != "NONE":
                    all_edges.append(edge)

    # Sort by confidence score
    all_edges.sort(key=lambda x: x["confidence_score"], reverse=True)

    print(f"\nFound {len(all_edges)} edges")
    return all_edges


def test_edge_finder(conn, all_stats=False):
    """Test edge finder with sample data (uses hardcoded matchups - for testing only)."""
    print("=== EDGE FINDER TEST (STATIC MATCHUPS - FOR TESTING ONLY) ===")
    print(f"Mode: {'All stats' if all_stats else 'Validated stats (PTS/REB/AST/3PM + combos)'}\n")
    print("WARNING: These are hardcoded test matchups, not validated for today's games.\n")

    build_player_positions_table(conn)

    # Sample lines (would come from sportsbook in production)
    # Includes individual stats and combo props (PRA, PR, PA, RA)
    test_cases = [
        # Player, Opponent, {stat: line}
        ("LeBron James", "LAC", {"PTS": 22.5, "AST": 7.5, "REB": 6.5, "PRA": 36.5}),
        ("Stephen Curry", "LAL", {"PTS": 25.5, "AST": 6.5, "3PM": 4.5, "PA": 32.5}),
        ("Jayson Tatum", "MIA", {"PTS": 27.5, "AST": 5.5, "REB": 8.5, "PRA": 41.5}),
        ("Anthony Edwards", "DEN", {"PTS": 26.5, "AST": 4.5, "PR": 32.5}),
        ("Devin Booker", "DAL", {"PTS": 24.5, "AST": 6.5, "3PM": 2.5, "PA": 31.5}),
        ("Trae Young", "CHI", {"PTS": 24.5, "AST": 10.5, "PA": 35.5, "RA": 14.5}),
        ("James Harden", "MEM", {"PTS": 20.5, "AST": 8.5, "PRA": 35.5}),
    ]

    all_edges = []

    for player, opp, lines in test_cases:
        edges = find_all_edges(player, opp, lines, conn, profitable_only=not all_stats)
        all_edges.extend(edges)

    # Sort by confidence score
    all_edges.sort(key=lambda x: x["confidence_score"], reverse=True)

    print(f"Found {len(all_edges)} edges:\n")

    for edge in all_edges:
        display_edge(edge)

    # Summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    high = len([e for e in all_edges if e["confidence"] == "HIGH"])
    med = len([e for e in all_edges if e["confidence"] == "MEDIUM"])
    low = len([e for e in all_edges if e["confidence"] == "LOW"])
    print(f"HIGH: {high} | MEDIUM: {med} | LOW: {low}")

    # Backtest reminder
    print("\nBacktest results (Task 4.2):")
    print("  HIGH (15%+ edge, PTS/AST): 56.4% hit rate")
    print("  Expected ROI: ~7.6% per bet")

    return all_edges


def process_lines_file(filepath, conn):
    """
    Process lines from a CSV file.

    Expected format:
    player_name,opponent,stat,line
    LeBron James,LAC,PTS,24.5
    """
    df = pd.read_csv(filepath)
    all_edges = []

    for _, row in df.iterrows():
        edge = find_edge(
            row["player_name"],
            row["opponent"],
            row["stat"].upper(),
            float(row["line"]),
            conn
        )
        if edge and edge["confidence"] != "NONE":
            all_edges.append(edge)

    return all_edges


def main():
    parser = argparse.ArgumentParser(description="Find props edges (backtest-optimized for PTS/AST)")
    parser.add_argument("--player", type=str, help="Player name")
    parser.add_argument("--opponent", type=str, help="Opponent abbreviation")
    parser.add_argument("--stat", type=str, help="Stat type (PTS, REB, AST, 3PM)")
    parser.add_argument("--line", type=float, help="Betting line")
    parser.add_argument("--file", type=str, help="CSV file with lines")
    parser.add_argument("--test", action="store_true", help="Test mode (static matchups)")
    parser.add_argument("--today", action="store_true", help="Find edges for today's games (validated)")
    parser.add_argument("--date", type=str, help="Target date for --today (YYYY-MM-DD)")
    parser.add_argument("--save", action="store_true", help="Save edges to database")
    parser.add_argument("--all-stats", action="store_true", help="Include all stats (not just PTS/AST)")

    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    build_player_positions_table(conn)

    if args.today:
        edges = find_edges_for_today(
            conn,
            target_date=args.date,
            all_stats=args.all_stats
        )
        for edge in edges[:20]:  # Show top 20
            display_edge(edge)

        print("\n" + "=" * 50)
        print("SUMMARY")
        print("=" * 50)
        high = len([e for e in edges if e["confidence"] == "HIGH"])
        med = len([e for e in edges if e["confidence"] == "MEDIUM"])
        low = len([e for e in edges if e["confidence"] == "LOW"])
        print(f"HIGH: {high} | MEDIUM: {med} | LOW: {low}")

        if args.save:
            count = save_edges_to_db(edges, conn)
            print(f"\nSaved {count} edges to database")
        conn.close()
        return 0

    if args.test:
        edges = test_edge_finder(conn, all_stats=args.all_stats)
        if args.save:
            count = save_edges_to_db(edges, conn)
            print(f"\nSaved {count} edges to database")
        conn.close()
        return 0

    if args.file:
        edges = process_lines_file(args.file, conn)
        edges.sort(key=lambda x: x["confidence_score"], reverse=True)
        print(f"Found {len(edges)} edges from file:\n")
        for edge in edges:
            display_edge(edge)
        if args.save:
            count = save_edges_to_db(edges, conn)
            print(f"\nSaved {count} edges to database")
        conn.close()
        return 0

    if args.player and args.opponent and args.stat and args.line:
        edge = find_edge(args.player, args.opponent, args.stat.upper(), args.line, conn)
        if edge and edge["confidence"] != "NONE":
            display_edge(edge)
            if args.save:
                save_edges_to_db([edge], conn)
                print("\nSaved edge to database")
        else:
            print(f"No significant edge found for {args.player} {args.stat} {args.line}")
        conn.close()
        return 0

    print("Usage:")
    print("  python find_edges.py --test")
    print("  python find_edges.py --player 'LeBron James' --opponent LAC --stat PTS --line 24.5")
    print("  python find_edges.py --file lines.csv")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
