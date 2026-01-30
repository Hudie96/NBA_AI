"""
AXIOM Daily Pipeline Backtest

Backtests the EXACT logic from daily_predictions.py against historical results.
This mimics what happens when you run the daily pipeline, then checks if picks won.

Usage:
    python scripts/backtest_daily_pipeline.py
    python scripts/backtest_daily_pipeline.py --start 2025-11-01 --end 2026-01-15
"""
import argparse
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config
from scripts.shared_utils import get_team_recent_games, calculate_team_stats
from scripts.rest_detection import get_team_rest_info, calculate_rest_adjustment

DB_PATH = config["database"]["path"]


def safe_print(text):
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))


def get_vegas_line(game_id, conn):
    """Get Vegas spread (home team perspective)."""
    result = conn.execute("""
        SELECT espn_current_spread, espn_closing_spread, covers_closing_spread
        FROM Betting WHERE game_id = ?
    """, (game_id,)).fetchone()

    if not result:
        return None
    return result[0] or result[1] or result[2]


def get_game_result(game_id, home_team, away_team, conn):
    """Get actual game result - who covered the spread."""
    # Get scores from TeamBox
    home_score = conn.execute("""
        SELECT tb.pts FROM TeamBox tb
        JOIN Teams t ON tb.team_id = t.team_id
        WHERE tb.game_id = ? AND t.abbreviation = ?
    """, (game_id, home_team)).fetchone()

    away_score = conn.execute("""
        SELECT tb.pts FROM TeamBox tb
        JOIN Teams t ON tb.team_id = t.team_id
        WHERE tb.game_id = ? AND t.abbreviation = ?
    """, (game_id, away_team)).fetchone()

    if not home_score or not away_score:
        return None

    return {
        'home_score': home_score[0],
        'away_score': away_score[0],
        'margin': home_score[0] - away_score[0]  # positive = home won by this much
    }


def generate_prediction(home_team, away_team, game_date, game_id, conn):
    """
    Generate prediction using EXACT same logic as daily_predictions.py
    """
    # Get recent games (data available BEFORE this game)
    home_games = get_team_recent_games(home_team, game_date, conn, limit=10)
    away_games = get_team_recent_games(away_team, game_date, conn, limit=10)

    if len(home_games) < 3 or len(away_games) < 3:
        return None

    # Calculate stats
    home_stats = calculate_team_stats(home_games, home_team)
    away_stats = calculate_team_stats(away_games, away_team)

    if not home_stats or not away_stats:
        return None

    # Predict scores with 2% home court advantage (same as daily_predictions.py)
    home_predicted = (home_stats['PPG'] * 1.02 + away_stats['OPP_PPG']) / 2
    away_predicted = (away_stats['PPG'] + home_stats['OPP_PPG']) / 2

    margin = home_predicted - away_predicted

    # Get rest/B2B info
    rest_adjustment = 0
    home_is_b2b = False
    away_is_b2b = False

    try:
        home_rest_info = get_team_rest_info(home_team, game_date, conn)
        away_rest_info = get_team_rest_info(away_team, game_date, conn)
        rest_adjustment, _ = calculate_rest_adjustment(home_rest_info, away_rest_info)
        home_is_b2b = home_rest_info['is_b2b']
        away_is_b2b = away_rest_info['is_b2b']
    except:
        pass

    # Adjusted margin (injury disabled, so just rest)
    adjusted_margin = margin + rest_adjustment

    # Determine favorite and spread
    favorite = home_team if adjusted_margin > 0 else away_team
    spread = abs(adjusted_margin)

    # Get Vegas line
    vegas_spread = get_vegas_line(game_id, conn)

    return {
        'home_team': home_team,
        'away_team': away_team,
        'game_date': game_date,
        'game_id': game_id,
        'favorite': favorite,
        'spread': spread,
        'home_is_b2b': home_is_b2b,
        'away_is_b2b': away_is_b2b,
        'vegas_spread': vegas_spread,
        'injury_adjustment': 0,  # Always 0 (disabled)
    }


def calculate_flag_score(pred):
    """Calculate flag score (same as flag_system.py)."""
    score = 0

    # injury_adj == 0 gives +5 (always true since disabled)
    if abs(pred.get('injury_adjustment', 0)) == 0:
        score += 5

    # Small spread < 3 gives +3
    if pred['spread'] < 3:
        score += 3

    # B2B gives +3
    if pred['home_is_b2b'] or pred['away_is_b2b']:
        score += 3

    return score


def categorize_game(pred):
    """Categorize into GREEN/YELLOW/RED (same as flag_system.py)."""
    flag_score = calculate_flag_score(pred)

    if flag_score >= 8:
        return "GREEN"
    elif flag_score >= 5:
        return "YELLOW"
    else:
        return "RED"


def check_if_covered(pred, result, conn):
    """
    Check if our pick covered the spread.

    Our pick is: bet on pred['favorite'] at -pred['spread']
    """
    if not result:
        return None

    actual_margin = result['margin']  # positive = home won

    # Determine if we're backing home or away
    if pred['favorite'] == pred['home_team']:
        # We're backing home at -spread
        # Home covers if: actual_margin > -spread (they win by more than spread, or lose by less)
        # Wait, spread is positive. If home is favorite by 5, spread = 5
        # We bet home -5. They need to win by >5 to cover.
        covered = actual_margin > pred['spread']
    else:
        # We're backing away at -spread
        # Away covers if: -actual_margin > spread (they win by more than spread)
        # If away is favorite by 5, spread = 5, they need to win by >5
        covered = -actual_margin > pred['spread']

    return covered


def check_vs_vegas(pred, result):
    """
    Check if betting the model's side vs Vegas would have won.

    If Vegas has home -3 and we predict home -5, we agree on home.
    If Vegas has home -3 and we predict away -1, we disagree.
    """
    if not result or pred['vegas_spread'] is None:
        return None, None

    vegas_spread = pred['vegas_spread']  # Home perspective (negative = home favored)
    actual_margin = result['margin']

    # Determine our pick vs Vegas
    model_favors_home = pred['favorite'] == pred['home_team']
    vegas_favors_home = vegas_spread < 0

    # Calculate if home covered Vegas spread
    # Home covers if: actual_margin + vegas_spread > 0
    # (e.g., Vegas -3, home wins by 5: 5 + (-3) = 2 > 0, covered)
    home_covered_vegas = actual_margin + vegas_spread > 0

    # Our pick result
    if model_favors_home:
        our_pick_covered = home_covered_vegas
    else:
        our_pick_covered = not home_covered_vegas

    # Edge (how much we disagreed with Vegas)
    if model_favors_home:
        model_spread_vs_vegas = pred['spread'] - abs(vegas_spread) if vegas_favors_home else pred['spread'] + abs(vegas_spread)
    else:
        model_spread_vs_vegas = pred['spread'] - abs(vegas_spread) if not vegas_favors_home else pred['spread'] + abs(vegas_spread)

    return our_pick_covered, model_spread_vs_vegas


def run_backtest(start_date, end_date, conn):
    """Run backtest over date range."""
    safe_print(f"\n{'='*80}")
    safe_print(f"AXIOM DAILY PIPELINE BACKTEST")
    safe_print(f"Date Range: {start_date} to {end_date}")
    safe_print(f"{'='*80}\n")

    # Get all games in range with results
    games_df = pd.read_sql("""
        SELECT g.game_id, g.home_team, g.away_team, DATE(g.date_time_utc) as game_date
        FROM Games g
        JOIN Betting b ON g.game_id = b.game_id
        WHERE DATE(g.date_time_utc) BETWEEN ? AND ?
          AND g.season = '2025-2026'
        ORDER BY g.date_time_utc
    """, conn, params=(start_date, end_date))

    safe_print(f"Games in range: {len(games_df)}")

    # Track results
    all_bets = []

    for _, game in games_df.iterrows():
        # Generate prediction
        pred = generate_prediction(
            game['home_team'],
            game['away_team'],
            game['game_date'],
            game['game_id'],
            conn
        )

        if not pred:
            continue

        # Get actual result
        result = get_game_result(
            game['game_id'],
            game['home_team'],
            game['away_team'],
            conn
        )

        if not result:
            continue

        # Categorize
        zone = categorize_game(pred)
        flag_score = calculate_flag_score(pred)

        # Check if covered
        covered = check_if_covered(pred, result, conn)
        covered_vs_vegas, edge = check_vs_vegas(pred, result)

        # Determine flags
        is_small_spread = pred['spread'] < 3
        is_b2b = pred['home_is_b2b'] or pred['away_is_b2b']

        # Which team on B2B
        b2b_team = None
        if pred['home_is_b2b']:
            b2b_team = pred['home_team']
        elif pred['away_is_b2b']:
            b2b_team = pred['away_team']

        # Is our pick fading the B2B team?
        fading_b2b = False
        if b2b_team:
            fading_b2b = pred['favorite'] != b2b_team

        all_bets.append({
            'game_date': game['game_date'],
            'game': f"{game['away_team']} @ {game['home_team']}",
            'pick': f"{pred['favorite']} -{pred['spread']:.1f}",
            'zone': zone,
            'flag_score': flag_score,
            'spread': pred['spread'],
            'is_small_spread': is_small_spread,
            'is_b2b': is_b2b,
            'fading_b2b': fading_b2b,
            'covered': covered,
            'covered_vs_vegas': covered_vs_vegas,
            'vegas_spread': pred['vegas_spread'],
            'actual_margin': result['margin'],
            'home_score': result['home_score'],
            'away_score': result['away_score'],
        })

    return pd.DataFrame(all_bets)


def generate_report(df, output_path=None):
    """Generate detailed backtest report."""
    if df.empty:
        safe_print("No bets to analyze")
        return

    safe_print(f"\nTotal bets analyzed: {len(df)}")
    safe_print(f"Date range: {df['game_date'].min()} to {df['game_date'].max()}")

    results = []

    # Overall
    overall_win = df['covered'].mean() * 100
    results.append({
        'Category': 'OVERALL',
        'Subcategory': 'All Picks',
        'Bets': len(df),
        'Wins': df['covered'].sum(),
        'Win%': round(overall_win, 1),
        'Edge': round(overall_win - 50, 1)
    })

    # By Zone
    safe_print(f"\n{'='*80}")
    safe_print("RESULTS BY ZONE")
    safe_print(f"{'='*80}")

    for zone in ['GREEN', 'YELLOW', 'RED']:
        zone_df = df[df['zone'] == zone]
        if len(zone_df) > 0:
            win_pct = zone_df['covered'].mean() * 100
            results.append({
                'Category': 'BY ZONE',
                'Subcategory': zone,
                'Bets': len(zone_df),
                'Wins': zone_df['covered'].sum(),
                'Win%': round(win_pct, 1),
                'Edge': round(win_pct - 50, 1)
            })
            safe_print(f"{zone}: {zone_df['covered'].sum()}-{len(zone_df) - zone_df['covered'].sum()} ({win_pct:.1f}%)")

    # By Flag Score
    safe_print(f"\n{'='*80}")
    safe_print("RESULTS BY FLAG SCORE")
    safe_print(f"{'='*80}")

    for score in sorted(df['flag_score'].unique()):
        score_df = df[df['flag_score'] == score]
        if len(score_df) >= 5:
            win_pct = score_df['covered'].mean() * 100
            results.append({
                'Category': 'BY FLAG SCORE',
                'Subcategory': f'Score {score}',
                'Bets': len(score_df),
                'Wins': score_df['covered'].sum(),
                'Win%': round(win_pct, 1),
                'Edge': round(win_pct - 50, 1)
            })
            safe_print(f"Flag {score}: {score_df['covered'].sum()}-{len(score_df) - score_df['covered'].sum()} ({win_pct:.1f}%)")

    # Small Spread Analysis
    safe_print(f"\n{'='*80}")
    safe_print("SMALL SPREAD ANALYSIS (< 3 points)")
    safe_print(f"{'='*80}")

    small_df = df[df['is_small_spread']]
    if len(small_df) > 0:
        win_pct = small_df['covered'].mean() * 100
        results.append({
            'Category': 'SPREAD SIZE',
            'Subcategory': 'Small Spread (<3)',
            'Bets': len(small_df),
            'Wins': small_df['covered'].sum(),
            'Win%': round(win_pct, 1),
            'Edge': round(win_pct - 50, 1)
        })
        safe_print(f"Small (<3): {small_df['covered'].sum()}-{len(small_df) - small_df['covered'].sum()} ({win_pct:.1f}%)")

    large_df = df[~df['is_small_spread']]
    if len(large_df) > 0:
        win_pct = large_df['covered'].mean() * 100
        results.append({
            'Category': 'SPREAD SIZE',
            'Subcategory': 'Large Spread (>=3)',
            'Bets': len(large_df),
            'Wins': large_df['covered'].sum(),
            'Win%': round(win_pct, 1),
            'Edge': round(win_pct - 50, 1)
        })
        safe_print(f"Large (>=3): {large_df['covered'].sum()}-{len(large_df) - large_df['covered'].sum()} ({win_pct:.1f}%)")

    # B2B Analysis
    safe_print(f"\n{'='*80}")
    safe_print("B2B FADE ANALYSIS")
    safe_print(f"{'='*80}")

    b2b_df = df[df['is_b2b']]
    if len(b2b_df) > 0:
        win_pct = b2b_df['covered'].mean() * 100
        results.append({
            'Category': 'B2B',
            'Subcategory': 'Any B2B Game',
            'Bets': len(b2b_df),
            'Wins': b2b_df['covered'].sum(),
            'Win%': round(win_pct, 1),
            'Edge': round(win_pct - 50, 1)
        })
        safe_print(f"B2B Games: {b2b_df['covered'].sum()}-{len(b2b_df) - b2b_df['covered'].sum()} ({win_pct:.1f}%)")

    fade_b2b_df = df[df['fading_b2b']]
    if len(fade_b2b_df) > 0:
        win_pct = fade_b2b_df['covered'].mean() * 100
        results.append({
            'Category': 'B2B',
            'Subcategory': 'Fading B2B Team',
            'Bets': len(fade_b2b_df),
            'Wins': fade_b2b_df['covered'].sum(),
            'Win%': round(win_pct, 1),
            'Edge': round(win_pct - 50, 1)
        })
        safe_print(f"Fading B2B: {fade_b2b_df['covered'].sum()}-{len(fade_b2b_df) - fade_b2b_df['covered'].sum()} ({win_pct:.1f}%)")

    # Combinations
    safe_print(f"\n{'='*80}")
    safe_print("COMBINATION ANALYSIS")
    safe_print(f"{'='*80}")

    # GREEN + Small Spread
    green_small = df[(df['zone'] == 'GREEN') & (df['is_small_spread'])]
    if len(green_small) >= 5:
        win_pct = green_small['covered'].mean() * 100
        results.append({
            'Category': 'COMBOS',
            'Subcategory': 'GREEN + Small Spread',
            'Bets': len(green_small),
            'Wins': green_small['covered'].sum(),
            'Win%': round(win_pct, 1),
            'Edge': round(win_pct - 50, 1)
        })
        safe_print(f"GREEN + Small: {green_small['covered'].sum()}-{len(green_small) - green_small['covered'].sum()} ({win_pct:.1f}%)")

    # GREEN + B2B Fade
    green_b2b = df[(df['zone'] == 'GREEN') & (df['fading_b2b'])]
    if len(green_b2b) >= 5:
        win_pct = green_b2b['covered'].mean() * 100
        results.append({
            'Category': 'COMBOS',
            'Subcategory': 'GREEN + B2B Fade',
            'Bets': len(green_b2b),
            'Wins': green_b2b['covered'].sum(),
            'Win%': round(win_pct, 1),
            'Edge': round(win_pct - 50, 1)
        })
        safe_print(f"GREEN + B2B Fade: {green_b2b['covered'].sum()}-{len(green_b2b) - green_b2b['covered'].sum()} ({win_pct:.1f}%)")

    # Small Spread + B2B Fade (best combo)
    best_combo = df[(df['is_small_spread']) & (df['fading_b2b'])]
    if len(best_combo) >= 3:
        win_pct = best_combo['covered'].mean() * 100
        results.append({
            'Category': 'COMBOS',
            'Subcategory': 'Small + B2B Fade (BEST)',
            'Bets': len(best_combo),
            'Wins': best_combo['covered'].sum(),
            'Win%': round(win_pct, 1),
            'Edge': round(win_pct - 50, 1)
        })
        safe_print(f"Small + B2B Fade: {best_combo['covered'].sum()}-{len(best_combo) - best_combo['covered'].sum()} ({win_pct:.1f}%)")

    # Vs Vegas (where we have lines)
    safe_print(f"\n{'='*80}")
    safe_print("VS VEGAS ANALYSIS")
    safe_print(f"{'='*80}")

    vegas_df = df[df['covered_vs_vegas'].notna()]
    if len(vegas_df) > 0:
        win_pct = vegas_df['covered_vs_vegas'].mean() * 100
        results.append({
            'Category': 'VS VEGAS',
            'Subcategory': 'All with Vegas Line',
            'Bets': len(vegas_df),
            'Wins': vegas_df['covered_vs_vegas'].sum(),
            'Win%': round(win_pct, 1),
            'Edge': round(win_pct - 50, 1)
        })
        safe_print(f"Vs Vegas: {vegas_df['covered_vs_vegas'].sum()}-{len(vegas_df) - vegas_df['covered_vs_vegas'].sum()} ({win_pct:.1f}%)")

    # Summary table
    results_df = pd.DataFrame(results)

    safe_print(f"\n{'='*80}")
    safe_print("FULL RESULTS TABLE")
    safe_print(f"{'='*80}")

    for category in results_df['Category'].unique():
        cat_df = results_df[results_df['Category'] == category]
        safe_print(f"\n### {category}")
        safe_print("-" * 70)
        safe_print(f"{'Subcategory':<30} {'Bets':>8} {'Wins':>8} {'Win%':>8} {'Edge':>8}")
        safe_print("-" * 70)

        for _, row in cat_df.iterrows():
            edge_str = f"+{row['Edge']}" if row['Edge'] > 0 else str(row['Edge'])
            safe_print(f"{row['Subcategory']:<30} {row['Bets']:>8} {row['Wins']:>8} {row['Win%']:>7.1f}% {edge_str:>7}%")

    # Save outputs
    if output_path:
        results_df.to_csv(output_path, index=False)
        safe_print(f"\nResults saved to: {output_path}")

        # Save detailed bets
        detail_path = output_path.replace('.csv', '_detail.csv')
        df.to_csv(detail_path, index=False)
        safe_print(f"Detailed bets saved to: {detail_path}")

    return results_df


def main():
    parser = argparse.ArgumentParser(description="AXIOM Daily Pipeline Backtest")
    parser.add_argument("--start", type=str, default="2025-10-22",
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, default="2026-01-09",
                        help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", "-o", type=str, default="outputs/pipeline_backtest.csv",
                        help="Output CSV path")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)

    # Run backtest
    df = run_backtest(args.start, args.end, conn)

    # Generate report
    generate_report(df, args.output)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
