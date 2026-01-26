"""
Backtest NBA Predictions Model

Backtests the prediction model on historical completed games,
comparing predictions vs actual results with and without injury adjustments.
"""
import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path
import csv

import numpy as np
import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config
from scripts.injury_impact import get_game_injury_adjustment
from scripts.shared_utils import get_team_recent_games, calculate_team_stats

DB_PATH = config["database"]["path"]
OUTPUT_DIR = PROJECT_ROOT / "outputs"


def generate_prediction_for_backtest(home_team, away_team, home_stats, away_stats, game_date, conn, apply_injuries=True):
    """Generate prediction for backtesting."""
    # Predict scores with 2% home court advantage
    home_predicted = (home_stats['PPG'] * 1.02 + away_stats['OPP_PPG']) / 2
    away_predicted = (away_stats['PPG'] + home_stats['OPP_PPG']) / 2

    margin = home_predicted - away_predicted
    base_spread = abs(margin)
    base_favorite = home_team if margin > 0 else away_team

    # Get injury adjustment
    injury_adjustment = 0
    if apply_injuries:
        try:
            injury_adjustment, _ = get_game_injury_adjustment(home_team, away_team, game_date, conn)
        except:
            pass  # Skip injuries if error

    # Adjust margin
    adjusted_margin = margin + injury_adjustment

    # Calculate win probability
    win_prob = 1 / (1 + np.exp(-adjusted_margin / 3.5))
    confidence = max(win_prob, 1 - win_prob)

    # Determine favorite and spread
    favorite = home_team if adjusted_margin > 0 else away_team
    spread = abs(adjusted_margin)

    return {
        'base_spread': base_spread,
        'base_favorite': base_favorite,
        'injury_adjustment': injury_adjustment,
        'spread': spread,
        'favorite': favorite,
        'confidence': confidence,
        'home_win_probability': win_prob
    }


def backtest(start_date, end_date):
    """Run backtest on completed games."""
    conn = sqlite3.connect(DB_PATH)

    # Get completed games
    query = '''
        SELECT DISTINCT g.game_id, g.home_team, g.away_team, g.date_time_utc,
               gs.home_score, gs.away_score
        FROM Games g
        JOIN GameStates gs ON g.game_id = gs.game_id
        WHERE DATE(g.date_time_utc) >= ?
          AND DATE(g.date_time_utc) <= ?
          AND gs.is_final_state = 1
          AND gs.home_score IS NOT NULL
          AND gs.away_score IS NOT NULL
        ORDER BY g.date_time_utc
    '''

    games_df = pd.read_sql(query, conn, params=(start_date, end_date))

    print(f"Found {len(games_df)} completed games from {start_date} to {end_date}")

    results = []

    for _, game in games_df.iterrows():
        game_id = game['game_id']
        home = game['home_team']
        away = game['away_team']
        game_date = game['date_time_utc'][:10]
        actual_home = game['home_score']
        actual_away = game['away_score']
        actual_margin = actual_home - actual_away  # Positive = home won

        # Get recent games for prediction
        home_games = get_team_recent_games(home, game_date, conn, limit=10)
        away_games = get_team_recent_games(away, game_date, conn, limit=10)

        if len(home_games) < 3 or len(away_games) < 3:
            continue  # Skip if insufficient data

        # Calculate stats
        home_stats = calculate_team_stats(home_games, home)
        away_stats = calculate_team_stats(away_games, away)

        if not home_stats or not away_stats:
            continue

        # Generate predictions with and without injuries
        pred_with_inj = generate_prediction_for_backtest(home, away, home_stats, away_stats, game_date, conn, apply_injuries=True)
        pred_no_inj = generate_prediction_for_backtest(home, away, home_stats, away_stats, game_date, conn, apply_injuries=False)

        # Determine outcomes
        # "Covered" means we picked the favorite and they won by more than the spread (or lost by less if underdog)
        # Simplified: Did the prediction get the winner right?
        actual_winner = home if actual_margin > 0 else away

        # With injuries
        correct_winner_inj = (pred_with_inj['favorite'] == actual_winner)
        # Check if pick covered the spread
        if pred_with_inj['favorite'] == home:
            # We picked home, did they cover?
            covered_inj = actual_margin > pred_with_inj['spread']
        else:
            # We picked away, did they cover?
            covered_inj = -actual_margin > pred_with_inj['spread']

        # Without injuries
        correct_winner_no_inj = (pred_no_inj['favorite'] == actual_winner)
        if pred_no_inj['favorite'] == home:
            covered_no_inj = actual_margin > pred_no_inj['spread']
        else:
            covered_no_inj = -actual_margin > pred_no_inj['spread']

        # Calculate error
        predicted_margin_inj = pred_with_inj['spread'] if pred_with_inj['favorite'] == home else -pred_with_inj['spread']
        error_inj = abs(predicted_margin_inj - actual_margin)

        predicted_margin_no_inj = pred_no_inj['spread'] if pred_no_inj['favorite'] == home else -pred_no_inj['spread']
        error_no_inj = abs(predicted_margin_no_inj - actual_margin)

        results.append({
            'game_id': game_id,
            'date': game_date,
            'game': f"{away} @ {home}",
            'actual_score': f"{actual_away}-{actual_home}",
            'actual_margin': actual_margin,
            'actual_winner': actual_winner,
            # With injuries
            'pred_favorite_inj': pred_with_inj['favorite'],
            'pred_spread_inj': pred_with_inj['spread'],
            'confidence_inj': pred_with_inj['confidence'],
            'injury_adj': pred_with_inj['injury_adjustment'],
            'covered_inj': covered_inj,
            'correct_winner_inj': correct_winner_inj,
            'error_inj': error_inj,
            # Without injuries
            'pred_favorite_no_inj': pred_no_inj['favorite'],
            'pred_spread_no_inj': pred_no_inj['spread'],
            'confidence_no_inj': pred_no_inj['confidence'],
            'covered_no_inj': covered_no_inj,
            'correct_winner_no_inj': correct_winner_no_inj,
            'error_no_inj': error_no_inj
        })

    conn.close()

    return pd.DataFrame(results)


def analyze_results(df):
    """Analyze backtest results."""
    total_games = len(df)

    if total_games == 0:
        print("No games to analyze")
        return

    # Overall accuracy with injuries
    covered_inj = df['covered_inj'].sum()
    correct_winner_inj = df['correct_winner_inj'].sum()

    # Overall accuracy without injuries
    covered_no_inj = df['covered_no_inj'].sum()
    correct_winner_no_inj = df['correct_winner_no_inj'].sum()

    # By confidence tier (with injuries)
    high_conf = df[df['confidence_inj'] >= 0.85]
    mid_conf = df[(df['confidence_inj'] >= 0.75) & (df['confidence_inj'] < 0.85)]
    low_conf = df[df['confidence_inj'] < 0.75]

    # Average errors
    avg_error_inj = df['error_inj'].mean()
    avg_error_no_inj = df['error_no_inj'].mean()

    # Top 5 biggest misses
    top_misses = df.nlargest(5, 'error_inj')[['date', 'game', 'pred_favorite_inj', 'pred_spread_inj', 'actual_margin', 'error_inj']]

    # Print summary
    print("\n" + "=" * 80)
    print("BACKTEST RESULTS SUMMARY")
    print("=" * 80)
    print(f"\nTotal Games: {total_games}")
    print(f"Date Range: {df['date'].min()} to {df['date'].max()}")

    print("\n--- WITH INJURY ADJUSTMENTS ---")
    print(f"Spread Coverage: {covered_inj}-{total_games - covered_inj} ({covered_inj/total_games:.1%})")
    print(f"Winner Accuracy: {correct_winner_inj}-{total_games - correct_winner_inj} ({correct_winner_inj/total_games:.1%})")
    print(f"Average Error: {avg_error_inj:.1f} points")

    print("\n--- WITHOUT INJURY ADJUSTMENTS ---")
    print(f"Spread Coverage: {covered_no_inj}-{total_games - covered_no_inj} ({covered_no_inj/total_games:.1%})")
    print(f"Winner Accuracy: {correct_winner_no_inj}-{total_games - correct_winner_no_inj} ({correct_winner_no_inj/total_games:.1%})")
    print(f"Average Error: {avg_error_no_inj:.1f} points")

    print("\n--- BY CONFIDENCE TIER (with injuries) ---")
    if len(high_conf) > 0:
        high_covered = high_conf['covered_inj'].sum()
        print(f"85%+ Confidence: {high_covered}-{len(high_conf) - high_covered} ({high_covered/len(high_conf):.1%}) - {len(high_conf)} games")

    if len(mid_conf) > 0:
        mid_covered = mid_conf['covered_inj'].sum()
        print(f"75-84% Confidence: {mid_covered}-{len(mid_conf) - mid_covered} ({mid_covered/len(mid_conf):.1%}) - {len(mid_conf)} games")

    if len(low_conf) > 0:
        low_covered = low_conf['covered_inj'].sum()
        print(f"Below 75% Confidence: {low_covered}-{len(low_conf) - low_covered} ({low_covered/len(low_conf):.1%}) - {len(low_conf)} games")

    print("\n--- TOP 5 BIGGEST MISSES ---")
    for idx, row in top_misses.iterrows():
        print(f"{row['date']} {row['game']}: Pred {row['pred_favorite_inj']} -{row['pred_spread_inj']:.1f}, Actual margin {row['actual_margin']:.0f}, Error {row['error_inj']:.1f}pts")

    print("\n--- INJURY ADJUSTMENT IMPACT ---")
    games_with_adj = df[df['injury_adj'] != 0]
    if len(games_with_adj) > 0:
        print(f"Games with injury adjustments: {len(games_with_adj)}/{total_games}")
        improved = (games_with_adj['error_inj'] < games_with_adj['error_no_inj']).sum()
        print(f"Injury adjustments improved accuracy: {improved}/{len(games_with_adj)} ({improved/len(games_with_adj):.1%})")
        avg_adj_size = games_with_adj['injury_adj'].abs().mean()
        print(f"Average adjustment size: {avg_adj_size:.1f} points")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    # Last 30 days: Dec 25, 2025 to Jan 24, 2026
    start = "2025-12-25"
    end = "2026-01-24"

    print(f"Running backtest from {start} to {end}...")

    results_df = backtest(start, end)

    # Save to CSV
    output_file = OUTPUT_DIR / "backtest_results.csv"
    results_df.to_csv(output_file, index=False)
    print(f"\nDetailed results saved to: {output_file}")

    # Analyze and print summary
    analyze_results(results_df)
