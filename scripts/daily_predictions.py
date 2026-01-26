"""
Daily NBA Predictions Generator

Generates predictions for today's NBA games using historical data.
Outputs to JSON, TXT, and CSV formats.
Optional Discord webhook integration for posting picks.

Usage:
    python scripts/daily_predictions.py
    python scripts/daily_predictions.py --date 2026-01-25
    python scripts/daily_predictions.py --min-confidence 0.75
    python scripts/daily_predictions.py --discord-webhook https://discord.com/api/webhooks/...
    python scripts/daily_predictions.py --output-dir custom/path
"""
import argparse
import csv
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config
from scripts.injury_impact import get_game_injury_adjustment, format_injury_summary
from scripts.rest_detection import get_team_rest_info, calculate_rest_adjustment, format_rest_summary
from scripts.flag_system import generate_ai_review_file
from scripts.shared_utils import get_team_recent_games, calculate_team_stats

DB_PATH = config["database"]["path"]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "outputs"


def get_vegas_line(game_id, conn):
    """Get Vegas line from Betting table.

    Prefers: espn_current_spread -> espn_closing_spread -> covers_closing_spread
    Returns spread from home team's perspective (positive = home favored)
    """
    query = '''
        SELECT espn_current_spread, espn_closing_spread, covers_closing_spread
        FROM Betting
        WHERE game_id = ?
    '''
    cursor = conn.cursor()
    cursor.execute(query, (game_id,))
    result = cursor.fetchone()

    if not result:
        return None

    espn_current, espn_closing, covers_closing = result

    # Prefer current, then closing, then covers
    vegas_spread = espn_current or espn_closing or covers_closing

    return vegas_spread


def calculate_edge(model_spread, vegas_spread, home_team, favorite):
    """Calculate edge between model and Vegas.

    Returns:
        dict with edge, direction, and recommendation
    """
    if vegas_spread is None:
        return {
            'vegas_spread': None,
            'edge': None,
            'direction': 'N/A',
            'recommendation': 'No Vegas line available'
        }

    # Convert spreads to home team perspective
    # Model spread: positive = home favored by that much
    model_margin = model_spread if favorite == home_team else -model_spread

    # Vegas spread is already from home perspective (positive = home favored)
    vegas_margin = vegas_spread

    # Edge = Model margin - Vegas margin
    # Positive edge = Model more confident in home
    # Negative edge = Model less confident in home
    edge = model_margin - vegas_margin

    # Determine recommendation
    if abs(edge) < 1.0:
        direction = "Even"
        recommendation = f"Model agrees with Vegas (~{abs(edge):.1f}pt difference)"
    elif abs(edge) >= 3.0:
        # Significant disagreement (3+ points)
        if edge > 0:
            # Model likes home more than Vegas
            direction = "Back Home"
            recommendation = f"MODEL DISAGREES: Back {home_team} (model {abs(edge):.1f}pts more confident)"
        else:
            # Model likes away more than Vegas
            direction = "Fade Home"
            recommendation = f"MODEL DISAGREES: Fade {home_team} (model {abs(edge):.1f}pts less confident)"
    else:
        # Minor disagreement (1-3 points)
        if edge > 0:
            direction = "Slight Back Home"
            recommendation = f"Model slightly favors {home_team} (+{edge:.1f}pts)"
        else:
            direction = "Slight Fade Home"
            recommendation = f"Model slightly fades {home_team} ({edge:.1f}pts)"

    return {
        'vegas_spread': round(vegas_spread, 1),
        'edge': round(edge, 1),
        'direction': direction,
        'recommendation': recommendation
    }


def generate_prediction(home_team, away_team, home_stats, away_stats, game_status, game_id=None, conn=None, game_date=None):
    """Generate prediction for a matchup with Vegas comparison and injury adjustment."""
    # Predict scores with 2% home court advantage
    home_predicted = (home_stats['PPG'] * 1.02 + away_stats['OPP_PPG']) / 2
    away_predicted = (away_stats['PPG'] + home_stats['OPP_PPG']) / 2

    margin = home_predicted - away_predicted
    base_spread = abs(margin)
    base_favorite = home_team if margin > 0 else away_team

    # Get injury adjustment - DISABLED (hurts performance, see backtest results)
    injury_adjustment = 0
    injury_details = {'home_injuries': [], 'away_injuries': [], 'home_impact': 0, 'away_impact': 0}
    # if game_date and conn:
    #     try:
    #         injury_adjustment, injury_details = get_game_injury_adjustment(home_team, away_team, game_date, conn)
    #     except Exception as e:
    #         print(f"Warning: Could not get injury data for {away_team} @ {home_team}: {e}")

    # Get rest/B2B adjustment
    rest_adjustment = 0
    rest_explanation = "No rest advantage"
    home_rest_info = {'is_b2b': False, 'days_rest': 1, 'last_game_date': None, 'games_in_last_3_days': 0}
    away_rest_info = {'is_b2b': False, 'days_rest': 1, 'last_game_date': None, 'games_in_last_3_days': 0}

    if game_date and conn:
        try:
            home_rest_info = get_team_rest_info(home_team, game_date, conn)
            away_rest_info = get_team_rest_info(away_team, game_date, conn)
            rest_adjustment, rest_explanation = calculate_rest_adjustment(home_rest_info, away_rest_info)
        except Exception as e:
            print(f"Warning: Could not get rest data for {away_team} @ {home_team}: {e}")

    # Adjust margin based on injuries and rest (positive adjustment = favor home)
    adjusted_margin = margin + injury_adjustment + rest_adjustment

    # Calculate win probability using adjusted margin
    # Each point ~2.8% win probability
    win_prob = 1 / (1 + np.exp(-adjusted_margin / 3.5))
    confidence = max(win_prob, 1 - win_prob)

    # Determine favorite and spread with injuries
    favorite = home_team if adjusted_margin > 0 else away_team
    spread = abs(adjusted_margin)

    # Get Vegas line and calculate edge
    vegas_comparison = {'vegas_spread': None, 'edge': None, 'direction': 'N/A', 'recommendation': 'No Vegas line'}
    if game_id and conn:
        vegas_spread = get_vegas_line(game_id, conn)
        vegas_comparison = calculate_edge(spread, vegas_spread, home_team, favorite)

    result = {
        'home_team': home_team,
        'away_team': away_team,
        'predicted_home_score': round(home_predicted, 1),
        'predicted_away_score': round(away_predicted, 1),
        'predicted_total': round(home_predicted + away_predicted, 1),
        'base_spread': round(base_spread, 1),
        'base_favorite': base_favorite,
        'injury_adjustment': round(injury_adjustment, 1),
        'spread': round(spread, 1),
        'favorite': favorite,
        'home_win_probability': round(win_prob, 3),
        'confidence': round(confidence, 3),
        'home_last10_ppg': round(home_stats['PPG'], 1),
        'home_last10_oppg': round(home_stats['OPP_PPG'], 1),
        'home_last10_record': home_stats['record'],
        'away_last10_ppg': round(away_stats['PPG'], 1),
        'away_last10_oppg': round(away_stats['OPP_PPG'], 1),
        'away_last10_record': away_stats['record'],
        'game_status': game_status,
        'pick': f"{favorite} -{spread}",
        'home_injuries_summary': format_injury_summary(injury_details['home_injuries']),
        'away_injuries_summary': format_injury_summary(injury_details['away_injuries']),
        'home_key_out': injury_details.get('home_key_out', []),
        'away_key_out': injury_details.get('away_key_out', []),
        'rest_adjustment': round(rest_adjustment, 1),
        'rest_explanation': rest_explanation,
        'home_rest_summary': format_rest_summary(home_rest_info),
        'away_rest_summary': format_rest_summary(away_rest_info),
        'home_is_b2b': home_rest_info['is_b2b'],
        'away_is_b2b': away_rest_info['is_b2b'],
        'home_rest_days': home_rest_info['days_rest'],
        'away_rest_days': away_rest_info['days_rest']
    }

    # Add Vegas comparison fields
    result.update(vegas_comparison)

    return result


def get_todays_games(target_date, conn):
    """Get all games for a specific date."""
    query = '''
        SELECT game_id, home_team, away_team, date_time_utc, status_text
        FROM Games
        WHERE DATE(date_time_utc) = ?
        ORDER BY date_time_utc
    '''
    return pd.read_sql(query, conn, params=(target_date,))


def send_discord_webhook(webhook_url, predictions, target_date, min_confidence=0.0):
    """Send predictions to Discord webhook."""
    if not webhook_url:
        return False

    # Filter by confidence
    filtered = [p for p in predictions if p['confidence'] >= min_confidence]

    if not filtered:
        print(f"No predictions meet confidence threshold ({min_confidence:.0%})")
        return False

    # Build Discord message
    embed = {
        "title": f"🏀 AXIOM NBA PICKS - {target_date}",
        "color": 0x1E90FF,  # Dodger blue
        "fields": [],
        "footer": {
            "text": f"Confidence threshold: {min_confidence:.0%} | {len(filtered)}/{len(predictions)} picks"
        },
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    # Add top picks
    for i, pred in enumerate(filtered[:10], 1):  # Limit to top 10
        game_str = f"{pred['away_team']} @ {pred['home_team']}"
        pick_str = pred['pick']
        score_str = f"{pred['predicted_away_score']:.0f}-{pred['predicted_home_score']:.0f}"

        field_value = (
            f"**Pick:** {pick_str}\n"
            f"**Score:** {score_str}\n"
            f"**Total:** {pred['predicted_total']:.0f}\n"
            f"**Confidence:** {pred['confidence']:.1%}"
        )

        embed["fields"].append({
            "name": f"{i}. {game_str}",
            "value": field_value,
            "inline": False
        })

    payload = {
        "username": "Axiom Sports",
        "embeds": [embed]
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        print(f"[SUCCESS] Posted {len(filtered)} picks to Discord")
        return True
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to post to Discord: {e}")
        return False


def generate_daily_predictions(target_date=None, output_dir=None, min_confidence=0.0, discord_webhook=None):
    """Generate predictions for all games on a specific date."""
    if target_date is None:
        target_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    if output_dir is None:
        output_dir = DEFAULT_OUTPUT_DIR
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)

    # Get today's games
    games_df = get_todays_games(target_date, conn)

    if len(games_df) == 0:
        print(f"No games found for {target_date}")
        conn.close()
        return None

    print(f"Found {len(games_df)} games for {target_date}")

    predictions = []
    skipped = []

    for _, game in games_df.iterrows():
        home = game['home_team']
        away = game['away_team']
        game_date = game['date_time_utc'][:10]
        status = game['status_text']

        # Get recent games
        home_games = get_team_recent_games(home, game_date, conn, limit=10)
        away_games = get_team_recent_games(away, game_date, conn, limit=10)

        # Need at least 3 games for prediction
        if len(home_games) < 3 or len(away_games) < 3:
            skipped.append({
                'game': f"{away} @ {home}",
                'reason': f"Insufficient data (Home: {len(home_games)}, Away: {len(away_games)} games)"
            })
            continue

        # Calculate stats
        home_stats = calculate_team_stats(home_games, home)
        away_stats = calculate_team_stats(away_games, away)

        if not home_stats or not away_stats:
            skipped.append({
                'game': f"{away} @ {home}",
                'reason': "Failed to calculate stats"
            })
            continue

        # Generate prediction with Vegas comparison and injury adjustment
        pred = generate_prediction(home, away, home_stats, away_stats, status, game['game_id'], conn, game_date)
        pred['game_id'] = game['game_id']
        pred['game_time'] = game['date_time_utc']
        predictions.append(pred)

    conn.close()

    # Sort by confidence (highest first)
    predictions.sort(key=lambda x: x['confidence'], reverse=True)

    # Save outputs
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

    # JSON output
    json_file = output_dir / f"predictions_{target_date}.json"
    with open(json_file, 'w') as f:
        json.dump({
            'date': target_date,
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'total_games': len(games_df),
            'predictions_count': len(predictions),
            'skipped_count': len(skipped),
            'predictions': predictions,
            'skipped': skipped
        }, f, indent=2)

    # Filter by confidence
    filtered_predictions = [p for p in predictions if p['confidence'] >= min_confidence]

    # TXT output (formatted for humans)
    txt_file = output_dir / f"predictions_{target_date}.txt"
    with open(txt_file, 'w') as f:
        f.write("=" * 100 + "\n")
        f.write(f"AXIOM NBA PREDICTIONS - {target_date}\n")
        f.write(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC\n")
        if min_confidence > 0:
            f.write(f"Confidence Filter: {min_confidence:.0%}+ ({len(filtered_predictions)}/{len(predictions)} picks)\n")
        f.write("=" * 100 + "\n\n")

        # Show filtered picks first if filter applied
        if min_confidence > 0 and filtered_predictions:
            f.write(f"TOP PICKS ({min_confidence:.0%}+ CONFIDENCE)\n")
            f.write("=" * 100 + "\n")
            f.write(f"{'GAME':<35} {'PICK':<15} {'CONFIDENCE':<12} {'PREDICTED SCORE':<20}\n")
            f.write("-" * 100 + "\n")

            for pred in filtered_predictions:
                game_str = f"{pred['away_team']} @ {pred['home_team']}"
                pick_str = f"{pred['favorite']} -{pred['spread']}"
                conf_str = f"{pred['confidence']:.1%}"
                score_str = f"{pred['predicted_away_score']}-{pred['predicted_home_score']}"

                f.write(f"{game_str:<35} {pick_str:<15} {conf_str:<12} {score_str:<20}\n")

            f.write("\n" + "=" * 100 + "\n\n")

        # Show all predictions
        if predictions:
            if min_confidence > 0:
                f.write("ALL PREDICTIONS\n")
                f.write("=" * 100 + "\n")

            f.write(f"{'GAME':<35} {'PICK':<15} {'CONFIDENCE':<12} {'PREDICTED SCORE':<20}\n")
            f.write("-" * 100 + "\n")

            for pred in predictions:
                game_str = f"{pred['away_team']} @ {pred['home_team']}"
                pick_str = f"{pred['favorite']} -{pred['spread']}"
                conf_str = f"{pred['confidence']:.1%}"
                score_str = f"{pred['predicted_away_score']}-{pred['predicted_home_score']}"

                f.write(f"{game_str:<35} {pick_str:<15} {conf_str:<12} {score_str:<20}\n")

            f.write("\n" + "=" * 100 + "\n\n")

            # Vegas comparison section
            games_with_vegas = [p for p in predictions if p['vegas_spread'] is not None]
            if games_with_vegas:
                f.write("VEGAS LINES COMPARISON\n")
                f.write("=" * 100 + "\n")
                f.write(f"{'GAME':<30} {'MODEL':<15} {'VEGAS':<15} {'EDGE':<10} {'DIRECTION':<20}\n")
                f.write("-" * 100 + "\n")

                for pred in predictions:
                    game_str = f"{pred['away_team']} @ {pred['home_team']}"
                    model_pick = f"{pred['favorite']} -{pred['spread']}"

                    if pred['vegas_spread'] is not None:
                        # Determine Vegas favorite
                        if pred['vegas_spread'] > 0:
                            vegas_favorite = pred['home_team']
                            vegas_spread = abs(pred['vegas_spread'])
                        else:
                            vegas_favorite = pred['away_team']
                            vegas_spread = abs(pred['vegas_spread'])

                        vegas_str = f"{vegas_favorite} -{vegas_spread:.1f}"
                        edge_str = f"{pred['edge']:+.1f}" if pred['edge'] else "N/A"
                        direction = pred['direction']

                        # Flag big disagreements
                        if abs(pred['edge']) >= 3.0:
                            direction = f"{direction} [!]"

                        f.write(f"{game_str:<30} {model_pick:<15} {vegas_str:<15} {edge_str:<10} {direction:<20}\n")
                    else:
                        f.write(f"{game_str:<30} {model_pick:<15} {'No line':<15} {'N/A':<10} {'N/A':<20}\n")

                f.write("\n[!] = Model disagrees by 3+ points\n")
                f.write("\n" + "=" * 100 + "\n\n")

            # Injury impact section
            games_with_injuries = [p for p in predictions if p['injury_adjustment'] != 0]
            if games_with_injuries:
                f.write("INJURY IMPACT\n")
                f.write("=" * 100 + "\n")
                f.write(f"{'GAME':<30} {'BASE':<15} {'INJ ADJ':<10} {'FINAL':<15} {'KEY INJURIES':<30}\n")
                f.write("-" * 100 + "\n")

                for pred in predictions:
                    game_str = f"{pred['away_team']} @ {pred['home_team']}"
                    base_pick = f"{pred['base_favorite']} -{pred['base_spread']}"
                    final_pick = f"{pred['favorite']} -{pred['spread']}"
                    adj_str = f"{pred['injury_adjustment']:+.1f}" if pred['injury_adjustment'] != 0 else "-"

                    # Build injury summary
                    injury_list = []
                    if pred['home_injuries_summary'] != 'None':
                        injury_list.append(f"{pred['home_team']}: {pred['home_injuries_summary']}")
                    if pred['away_injuries_summary'] != 'None':
                        injury_list.append(f"{pred['away_team']}: {pred['away_injuries_summary']}")

                    injury_str = "; ".join(injury_list) if injury_list else "None"

                    # Only show if there's an adjustment
                    if pred['injury_adjustment'] != 0:
                        f.write(f"{game_str:<30} {base_pick:<15} {adj_str:<10} {final_pick:<15} {injury_str[:30]:<30}\n")

                if not games_with_injuries:
                    f.write("  No significant injury impacts for today's games\n")

                f.write("\n" + "=" * 100 + "\n\n")

            f.write("DETAILED BREAKDOWN\n")
            f.write("=" * 100 + "\n\n")

            for pred in predictions:
                f.write(f"{pred['away_team']} @ {pred['home_team']} - {pred['game_status']}\n")
                f.write(f"  Pick: {pred['pick']}\n")
                f.write(f"  Predicted Score: {pred['away_team']} {pred['predicted_away_score']} @ {pred['home_team']} {pred['predicted_home_score']}\n")
                f.write(f"  Total: {pred['predicted_total']}\n")
                f.write(f"  Home Win Probability: {pred['home_win_probability']:.1%}\n")
                f.write(f"  Confidence: {pred['confidence']:.1%}\n")

                # Vegas comparison
                if pred['vegas_spread'] is not None:
                    f.write(f"  Vegas Line: {pred['vegas_spread']:+.1f} (home perspective)\n")
                    f.write(f"  Edge: {pred['edge']:+.1f} pts\n")
                    f.write(f"  Recommendation: {pred['recommendation']}\n")

                # Injury adjustment
                if pred['injury_adjustment'] != 0:
                    f.write(f"  Base Spread: {pred['base_favorite']} -{pred['base_spread']}\n")
                    f.write(f"  Injury Adjustment: {pred['injury_adjustment']:+.1f} pts\n")
                    f.write(f"  Adjusted Spread: {pred['favorite']} -{pred['spread']}\n")
                    if pred['home_injuries_summary'] != 'None':
                        f.write(f"    {pred['home_team']} OUT: {pred['home_injuries_summary']}\n")
                    if pred['away_injuries_summary'] != 'None':
                        f.write(f"    {pred['away_team']} OUT: {pred['away_injuries_summary']}\n")

                # Rest/B2B adjustment
                if pred['rest_adjustment'] != 0:
                    f.write(f"  Rest Adjustment: {pred['rest_adjustment']:+.1f} pts ({pred['rest_explanation']})\n")
                    f.write(f"    {pred['home_team']}: {pred['home_rest_summary']}\n")
                    f.write(f"    {pred['away_team']}: {pred['away_rest_summary']}\n")

                f.write(f"  {pred['home_team']} Last 10: {pred['home_last10_ppg']} PPG, {pred['home_last10_oppg']} OPPG ({pred['home_last10_record']})\n")
                f.write(f"  {pred['away_team']} Last 10: {pred['away_last10_ppg']} PPG, {pred['away_last10_oppg']} OPPG ({pred['away_last10_record']})\n")
                f.write("\n")

        if skipped:
            f.write("\n" + "=" * 100 + "\n")
            f.write(f"SKIPPED GAMES ({len(skipped)})\n")
            f.write("=" * 100 + "\n\n")
            for skip in skipped:
                f.write(f"{skip['game']}: {skip['reason']}\n")

    # CSV output (for analysis)
    csv_file = output_dir / f"predictions_{target_date}.csv"
    with open(csv_file, 'w', newline='') as f:
        if predictions:
            fieldnames = predictions[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(predictions)

    # AI Review file (flagged games organized by zones)
    ai_review_file = None
    if predictions:
        ai_review_file = generate_ai_review_file(predictions, target_date, output_dir)

    print(f"\n[SUCCESS] Predictions generated successfully!")
    print(f"  - {len(predictions)} total predictions")
    if min_confidence > 0:
        print(f"  - {len(filtered_predictions)} picks meet {min_confidence:.0%}+ confidence threshold")
    print(f"  - {len(skipped)} games skipped")
    print(f"\nOutput files:")
    print(f"  JSON: {json_file}")
    print(f"  TXT:  {txt_file}")
    print(f"  CSV:  {csv_file}")
    if ai_review_file:
        print(f"  AI Review: {ai_review_file}")

    # Send to Discord if webhook provided
    if discord_webhook:
        print(f"\nSending to Discord webhook...")
        send_discord_webhook(discord_webhook, predictions, target_date, min_confidence)

    return {
        'predictions': predictions,
        'filtered_predictions': filtered_predictions,
        'skipped': skipped,
        'files': {
            'json': str(json_file),
            'txt': str(txt_file),
            'csv': str(csv_file),
            'ai_review': str(ai_review_file) if ai_review_file else None
        }
    }


def main():
    parser = argparse.ArgumentParser(description='Generate daily NBA predictions')
    parser.add_argument('--date', type=str, help='Target date (YYYY-MM-DD). Defaults to today.')
    parser.add_argument('--output-dir', type=str, help='Output directory. Defaults to outputs/')
    parser.add_argument('--min-confidence', type=float, default=0.0,
                        help='Minimum confidence threshold (0.0-1.0). Default: 0.0 (no filter)')
    parser.add_argument('--discord-webhook', type=str,
                        help='Discord webhook URL to post picks')

    args = parser.parse_args()

    # Get Discord webhook from env if not provided as arg
    discord_webhook = args.discord_webhook or os.getenv('DISCORD_WEBHOOK_URL')

    try:
        result = generate_daily_predictions(
            target_date=args.date,
            output_dir=args.output_dir,
            min_confidence=args.min_confidence,
            discord_webhook=discord_webhook
        )

        if result and result['predictions']:
            print("\n" + "=" * 100)

            # Show filtered picks if threshold applied
            if args.min_confidence > 0 and result.get('filtered_predictions'):
                filtered = result['filtered_predictions']
                print(f"TOP PICKS ({args.min_confidence:.0%}+ CONFIDENCE):")
                print("=" * 100)
                for i, pred in enumerate(filtered[:10], 1):
                    print(f"{i}. {pred['away_team']} @ {pred['home_team']}: {pred['pick']} ({pred['confidence']:.1%} confidence)")

                if not filtered:
                    print(f"No picks meet the {args.min_confidence:.0%} confidence threshold")
            else:
                # Show all picks
                print("TOP PICKS (by confidence):")
                print("=" * 100)
                for i, pred in enumerate(result['predictions'][:5], 1):
                    print(f"{i}. {pred['away_team']} @ {pred['home_team']}: {pred['pick']} ({pred['confidence']:.1%} confidence)")

            return 0
        else:
            return 1

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
