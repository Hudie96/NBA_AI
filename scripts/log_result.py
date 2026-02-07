"""
Log a prediction result to results.csv (canonical format).

Note: find_edges.py and flag_system.py auto-log picks during the pipeline.
This script is for manually logging picks that weren't auto-logged.

Usage:
    python scripts/log_result.py "SAC @ BOS" "SPREAD" "BOS -17.7" 17.7 -11.5 "GOLD" "+6.2"
    python scripts/log_result.py "vs NOP" "PROP" "Adem Bona OVER 6.3 PA" 6.3 "" "S_TIER" "+73.0%"

Args:
    game: Matchup string (e.g., "BOS @ CHI" or "vs NOP")
    bet_type: SPREAD or PROP
    pick: Pick string
    line: Line value
    vegas_line: Vegas line (optional, use "" for none)
    tier: Tier classification
    edge: Edge value
"""
import argparse
import csv
from datetime import date
from pathlib import Path

CANONICAL_FIELDS = ['date', 'game', 'bet_type', 'pick', 'line', 'vegas_line', 'tier', 'edge', 'result', 'actual']


def main():
    parser = argparse.ArgumentParser(description='Log a prediction to results.csv')
    parser.add_argument('game', help='Matchup (e.g., "BOS @ CHI")')
    parser.add_argument('bet_type', choices=['SPREAD', 'PROP'], help='Bet type')
    parser.add_argument('pick', help='Pick string')
    parser.add_argument('line', help='Line value')
    parser.add_argument('vegas_line', help='Vegas line (use "" for none)')
    parser.add_argument('tier', help='Tier (PLATINUM/GOLD/SILVER/S_TIER)')
    parser.add_argument('edge', help='Edge value')
    parser.add_argument('--date', type=str, default=None, help='Date (YYYY-MM-DD), defaults to today')

    args = parser.parse_args()

    results_file = Path(__file__).parent.parent / 'data' / 'results.csv'

    row = {
        'date': args.date or date.today().isoformat(),
        'game': args.game,
        'bet_type': args.bet_type,
        'pick': args.pick,
        'line': args.line,
        'vegas_line': args.vegas_line,
        'tier': args.tier,
        'edge': args.edge,
        'result': '',
        'actual': ''
    }

    file_exists = results_file.exists()
    with open(results_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CANONICAL_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

    print(f"Logged: {row['date']} | {args.game} | {args.bet_type} | {args.pick}")


if __name__ == '__main__':
    main()
