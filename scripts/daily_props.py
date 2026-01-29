"""
Daily Player Props Predictions Generator

Generates player prop projections for today's NBA games.
Compares projections against market lines to find edges.
Outputs to JSON, TXT, and CSV formats.

Usage:
    python scripts/daily_props.py
    python scripts/daily_props.py --date 2026-01-29
    python scripts/daily_props.py --min-edge 10  # 10% minimum edge
    python scripts/daily_props.py --stats PTS,AST,REB  # Specific stats only
    python scripts/daily_props.py --top-players 20  # Top N players per game
"""
import argparse
import csv
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config
from scripts.props_flag_system import (
    calculate_props_flag_score,
    categorize_prop,
    get_confidence_level,
    generate_props_ai_review_file
)
from scripts.rest_detection import get_team_rest_info

DB_PATH = config["database"]["path"]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs"

# Stats we project
CORE_STATS = ["PTS", "REB", "AST", "3PM"]
COMBO_STATS = ["PRA", "PR", "PA", "RA"]
SECONDARY_STATS = ["STL", "BLK", "TOV"]
ALL_STATS = CORE_STATS + COMBO_STATS

# Column mapping for database
STAT_COLS = {
    "PTS": "points",
    "REB": "rebounds",
    "AST": "assists",
    "3PM": "threes_made",
    "STL": "steals",
    "BLK": "blocks",
    "TOV": "turnovers",
    "PRA": "pts_reb_ast",
    "PR": "pts_reb",
    "PA": "pts_ast",
    "RA": "reb_ast",
    "FPT": "fantasy_points",
}

# Minimum games required for projection
MIN_GAMES_REQUIRED = 10


def get_todays_games(target_date: str, conn) -> pd.DataFrame:
    """Get all games scheduled for target date."""
    query = """
        SELECT game_id, home_team, away_team, date_time_utc, status_text
        FROM Games
        WHERE DATE(date_time_utc) = ?
        ORDER BY date_time_utc
    """
    return pd.read_sql(query, conn, params=(target_date,))


def get_game_players(game_id: str, home_team: str, away_team: str, conn, top_n: int = 15) -> pd.DataFrame:
    """
    Get players likely to play in a game based on recent appearances.

    Returns top N players by minutes from each team.
    """
    # Get players who have played recently for these teams
    query = """
        SELECT
            player_id,
            player_name,
            team,
            AVG(minutes) as avg_minutes,
            AVG(points) as avg_pts,
            COUNT(*) as games_played
        FROM player_game_logs
        WHERE team IN (?, ?)
        AND game_date >= date('now', '-30 days')
        GROUP BY player_id, player_name, team
        HAVING games_played >= 3 AND avg_minutes >= 15
        ORDER BY avg_minutes DESC
    """

    df = pd.read_sql(query, conn, params=(home_team, away_team))

    if df.empty:
        # Fallback: try without date filter
        query_fallback = """
            SELECT
                player_id,
                player_name,
                team,
                AVG(minutes) as avg_minutes,
                AVG(points) as avg_pts,
                COUNT(*) as games_played
            FROM player_game_logs
            WHERE team IN (?, ?)
            GROUP BY player_id, player_name, team
            HAVING games_played >= 5 AND avg_minutes >= 15
            ORDER BY avg_minutes DESC
        """
        df = pd.read_sql(query_fallback, conn, params=(home_team, away_team))

    # Return top N per team
    home_players = df[df['team'] == home_team].head(top_n)
    away_players = df[df['team'] == away_team].head(top_n)

    return pd.concat([home_players, away_players])


def get_player_projection_data(player_name: str, opponent: str, stat: str, conn) -> Optional[Dict]:
    """
    Get projection components for a player-stat combination.

    Returns dict with:
        - last_10_avg: Average over last 10 games
        - season_avg: Season average
        - vs_opp_avg: Average vs this opponent (if available)
        - vs_opp_games: Number of games vs opponent
        - variance: Standard deviation of stat
        - avg_minutes: Average minutes played
    """
    col = STAT_COLS.get(stat)
    if not col:
        return None

    # Get last 10 games
    last_10_query = f"""
        SELECT {col} as value, minutes, game_date
        FROM player_game_logs
        WHERE player_name = ?
        ORDER BY game_date DESC
        LIMIT 10
    """
    last_10_df = pd.read_sql(last_10_query, conn, params=(player_name,))

    if len(last_10_df) < MIN_GAMES_REQUIRED:
        return None  # Insufficient data

    last_10_avg = last_10_df['value'].mean()
    variance = last_10_df['value'].std()
    avg_minutes = last_10_df['minutes'].mean()

    # Get season average
    season_query = f"""
        SELECT AVG({col}) as value
        FROM player_game_logs
        WHERE player_name = ?
    """
    season_df = pd.read_sql(season_query, conn, params=(player_name,))
    season_avg = season_df.iloc[0]['value'] if not season_df.empty else last_10_avg

    # Get vs opponent average
    vs_opp_query = f"""
        SELECT AVG({col}) as value, COUNT(*) as games
        FROM player_game_logs
        WHERE player_name = ? AND opponent = ?
    """
    vs_opp_df = pd.read_sql(vs_opp_query, conn, params=(player_name, opponent))

    vs_opp_avg = None
    vs_opp_games = 0
    if not vs_opp_df.empty and vs_opp_df.iloc[0]['value'] is not None:
        vs_opp_avg = vs_opp_df.iloc[0]['value']
        vs_opp_games = int(vs_opp_df.iloc[0]['games'])

    return {
        'last_10_avg': last_10_avg,
        'season_avg': season_avg,
        'vs_opp_avg': vs_opp_avg,
        'vs_opp_games': vs_opp_games,
        'variance': variance,
        'avg_minutes': avg_minutes,
        'total_games': len(last_10_df)
    }


def calculate_projection(data: Dict) -> float:
    """
    Calculate weighted projection.

    Weights:
    - Last 10 games: 40%
    - Season average: 30%
    - vs Opponent: 20% (if available)
    - Baseline (season): 10%

    If vs opponent data not available, redistributes weight.
    """
    last_10 = data['last_10_avg']
    season = data['season_avg']
    vs_opp = data['vs_opp_avg']
    vs_opp_games = data['vs_opp_games']

    if vs_opp is not None and vs_opp_games >= 2:
        # Full formula
        projection = (
            last_10 * 0.40 +
            season * 0.30 +
            vs_opp * 0.20 +
            season * 0.10
        )
    else:
        # Simplified (no vs opponent data)
        projection = (
            last_10 * 0.50 +
            season * 0.40 +
            season * 0.10
        )

    return projection


def generate_player_props(
    player_name: str,
    team: str,
    opponent: str,
    conn,
    stats: List[str] = None,
    market_lines: Dict = None
) -> List[Dict]:
    """
    Generate prop projections for a single player.

    Args:
        player_name: Player's full name
        team: Player's team abbreviation
        opponent: Opponent team abbreviation
        conn: Database connection
        stats: List of stats to project (default: ALL_STATS)
        market_lines: Optional dict of market lines {stat: line}

    Returns:
        List of prop prediction dicts
    """
    if stats is None:
        stats = CORE_STATS  # Default to core stats only

    if market_lines is None:
        market_lines = {}

    props = []

    for stat in stats:
        # Get projection data
        data = get_player_projection_data(player_name, opponent, stat, conn)

        if data is None:
            continue

        # Calculate projection
        projection = calculate_projection(data)

        # Get market line (use season avg as proxy if not provided)
        line = market_lines.get(stat, data['season_avg'])

        # Calculate edge
        edge = projection - line
        edge_pct = (edge / line * 100) if line > 0 else 0

        # Determine pick direction
        if abs(edge_pct) < 5:
            pick = "NO_BET"
        elif projection > line:
            pick = "OVER"
        else:
            pick = "UNDER"

        prop = {
            'player_name': player_name,
            'team': team,
            'opponent': opponent,
            'prop_type': stat,
            'line': round(line, 1),
            'projection': round(projection, 1),
            'edge': round(edge, 1),
            'edge_pct': round(edge_pct, 1),
            'pick': pick,
            'last_10_avg': round(data['last_10_avg'], 1),
            'season_avg': round(data['season_avg'], 1),
            'vs_opp_avg': round(data['vs_opp_avg'], 1) if data['vs_opp_avg'] else None,
            'vs_opp_games': data['vs_opp_games'],
            'variance': round(data['variance'], 2),
            'avg_minutes': round(data['avg_minutes'], 1),
            'player_is_b2b': False,  # Will be set later
        }

        # Calculate flag score and confidence
        flag_score = calculate_props_flag_score(prop)
        confidence = get_confidence_level(edge_pct, flag_score)

        prop['flag_score'] = flag_score
        prop['confidence'] = confidence
        prop['zone'] = categorize_prop(prop)

        props.append(prop)

    return props


def generate_daily_props(
    target_date: str = None,
    output_dir: str = None,
    min_edge: float = 5.0,
    stats: List[str] = None,
    top_players: int = 12
) -> Dict:
    """
    Generate prop projections for all games on a date.

    Args:
        target_date: Date string (YYYY-MM-DD), defaults to today
        output_dir: Output directory path
        min_edge: Minimum edge percentage to include (default 5%)
        stats: List of stats to project
        top_players: Number of top players per team to project

    Returns:
        Dict with results and file paths
    """
    if target_date is None:
        target_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    if stats is None:
        stats = CORE_STATS

    conn = sqlite3.connect(DB_PATH)

    # Check if player_game_logs table exists
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='player_game_logs'
    """)
    if not cursor.fetchone():
        print("ERROR: player_game_logs table not found!")
        print("Run: python scripts/build_player_game_logs.py")
        conn.close()
        return None

    # Get today's games
    games_df = get_todays_games(target_date, conn)

    if len(games_df) == 0:
        print(f"No games found for {target_date}")
        conn.close()
        return None

    print(f"Found {len(games_df)} games for {target_date}")
    print(f"Projecting stats: {', '.join(stats)}")
    print(f"Top {top_players} players per team")
    print()

    all_props = []
    games_processed = 0

    for _, game in games_df.iterrows():
        home_team = game['home_team']
        away_team = game['away_team']
        game_id = game['game_id']

        print(f"Processing: {away_team} @ {home_team}")

        # Get players for this game
        players_df = get_game_players(game_id, home_team, away_team, conn, top_n=top_players)

        if players_df.empty:
            print(f"  No player data found, skipping")
            continue

        # Get rest info for teams
        try:
            home_rest = get_team_rest_info(home_team, target_date, conn)
            away_rest = get_team_rest_info(away_team, target_date, conn)
        except Exception:
            home_rest = {'is_b2b': False}
            away_rest = {'is_b2b': False}

        game_props = []

        for _, player_row in players_df.iterrows():
            player_name = player_row['player_name']
            team = player_row['team']
            opponent = away_team if team == home_team else home_team

            # Set B2B status
            is_b2b = home_rest['is_b2b'] if team == home_team else away_rest['is_b2b']

            # Generate props for this player
            player_props = generate_player_props(
                player_name=player_name,
                team=team,
                opponent=opponent,
                conn=conn,
                stats=stats
            )

            # Update B2B status
            for prop in player_props:
                prop['player_is_b2b'] = is_b2b
                prop['game_id'] = game_id
                # Recalculate flag score with B2B info
                prop['flag_score'] = calculate_props_flag_score(prop)
                prop['confidence'] = get_confidence_level(prop['edge_pct'], prop['flag_score'])
                prop['zone'] = categorize_prop(prop)

            game_props.extend(player_props)

        if game_props:
            print(f"  Generated {len(game_props)} props")
            all_props.extend(game_props)
            games_processed += 1

    conn.close()

    if not all_props:
        print("No props generated")
        return None

    # Filter by edge threshold
    filtered_props = [p for p in all_props if abs(p['edge_pct']) >= min_edge and p['pick'] != 'NO_BET']

    # Sort by edge (highest first)
    all_props.sort(key=lambda x: abs(x['edge_pct']), reverse=True)
    filtered_props.sort(key=lambda x: abs(x['edge_pct']), reverse=True)

    # Categorize by zone
    green_props = [p for p in filtered_props if p['zone'] == 'GREEN']
    yellow_props = [p for p in filtered_props if p['zone'] == 'YELLOW']

    print(f"\n{'=' * 60}")
    print(f"PROPS SUMMARY - {target_date}")
    print(f"{'=' * 60}")
    print(f"Games processed: {games_processed}")
    print(f"Total props analyzed: {len(all_props)}")
    print(f"Props with {min_edge}%+ edge: {len(filtered_props)}")
    print(f"  GREEN (Best): {len(green_props)}")
    print(f"  YELLOW (Signal): {len(yellow_props)}")

    # Save outputs
    # JSON output (all props)
    json_file = output_dir / f"props_{target_date}.json"
    with open(json_file, 'w') as f:
        json.dump({
            'date': target_date,
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'games_processed': games_processed,
            'total_props': len(all_props),
            'filtered_props': len(filtered_props),
            'min_edge': min_edge,
            'stats_projected': stats,
            'props': filtered_props,
            'all_props': all_props
        }, f, indent=2)

    # TXT output (human readable)
    txt_file = output_dir / f"props_{target_date}.txt"
    with open(txt_file, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write(f"AXIOM PROPS PROJECTIONS - {target_date}\n")
        f.write(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC\n")
        f.write(f"Edge threshold: {min_edge}%+\n")
        f.write("=" * 80 + "\n\n")

        # Top picks
        f.write("TOP PROPS (by edge)\n")
        f.write("-" * 80 + "\n")
        f.write(f"{'Player':<25} {'Prop':<8} {'Pick':<8} {'Line':<8} {'Proj':<8} {'Edge':<10} {'Zone':<8}\n")
        f.write("-" * 80 + "\n")

        for prop in filtered_props[:30]:
            f.write(f"{prop['player_name'][:24]:<25} "
                    f"{prop['prop_type']:<8} "
                    f"{prop['pick']:<8} "
                    f"{prop['line']:<8.1f} "
                    f"{prop['projection']:<8.1f} "
                    f"{prop['edge_pct']:>+7.1f}% "
                    f"{prop['zone']:<8}\n")

        f.write("\n" + "=" * 80 + "\n\n")

        # Detailed breakdown by stat
        for stat in stats:
            stat_props = [p for p in filtered_props if p['prop_type'] == stat]
            if stat_props:
                f.write(f"\n{stat} PROPS ({len(stat_props)} with edge)\n")
                f.write("-" * 60 + "\n")
                for prop in stat_props[:10]:
                    f.write(f"  {prop['player_name']}: {prop['pick']} {prop['line']} "
                            f"(Proj: {prop['projection']}, Edge: {prop['edge_pct']:+.1f}%)\n")
                    f.write(f"    L10: {prop['last_10_avg']} | Season: {prop['season_avg']}")
                    if prop['vs_opp_avg']:
                        f.write(f" | vs {prop['opponent']}: {prop['vs_opp_avg']}")
                    f.write("\n")

    # CSV output
    csv_file = output_dir / f"props_{target_date}.csv"
    with open(csv_file, 'w', newline='') as f:
        if filtered_props:
            fieldnames = filtered_props[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(filtered_props)

    # AI Review file
    ai_review_file = None
    if filtered_props:
        ai_review_file = generate_props_ai_review_file(filtered_props, target_date, output_dir)

    print(f"\nOutput files:")
    print(f"  JSON: {json_file}")
    print(f"  TXT:  {txt_file}")
    print(f"  CSV:  {csv_file}")
    if ai_review_file:
        print(f"  AI Review: {ai_review_file}")

    return {
        'props': filtered_props,
        'all_props': all_props,
        'green_props': green_props,
        'yellow_props': yellow_props,
        'files': {
            'json': str(json_file),
            'txt': str(txt_file),
            'csv': str(csv_file),
            'ai_review': ai_review_file
        }
    }


def main():
    parser = argparse.ArgumentParser(description='Generate daily props projections')
    parser.add_argument('--date', type=str, help='Target date (YYYY-MM-DD). Defaults to today.')
    parser.add_argument('--output-dir', type=str, help='Output directory. Defaults to outputs/')
    parser.add_argument('--min-edge', type=float, default=5.0,
                        help='Minimum edge percentage (default: 5.0)')
    parser.add_argument('--stats', type=str, default='PTS,REB,AST,3PM',
                        help='Comma-separated stats to project (default: PTS,REB,AST,3PM)')
    parser.add_argument('--top-players', type=int, default=12,
                        help='Top N players per team to project (default: 12)')
    parser.add_argument('--include-combos', action='store_true',
                        help='Include combo props (PRA, PR, PA, RA)')

    args = parser.parse_args()

    # Parse stats
    stats = [s.strip().upper() for s in args.stats.split(',')]
    if args.include_combos:
        stats.extend(COMBO_STATS)

    try:
        result = generate_daily_props(
            target_date=args.date,
            output_dir=args.output_dir,
            min_edge=args.min_edge,
            stats=stats,
            top_players=args.top_players
        )

        if result and result['green_props']:
            print("\n" + "=" * 60)
            print("BEST PROPS (GREEN ZONE):")
            print("=" * 60)
            for i, prop in enumerate(result['green_props'][:10], 1):
                print(f"{i}. {prop['player_name']} {prop['prop_type']} "
                      f"{prop['pick']} {prop['line']} ({prop['edge_pct']:+.1f}%)")

        return 0 if result else 1

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
