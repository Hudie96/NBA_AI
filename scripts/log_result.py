"""
Log a prediction result to results.csv

Usage:
    python scripts/log_result.py "BOS @ CHI" "BOS -3.5" -3.5 8 1.6

Args:
    game: Matchup string (e.g., "BOS @ CHI")
    pick: Pick with spread (e.g., "BOS -3.5")
    spread: Numeric spread value
    flag_score: Flag score from flag_system
    model_edge: Model's calculated edge
"""
import argparse
import csv
from datetime import date
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description='Log a prediction to results.csv')
    parser.add_argument('game', help='Matchup (e.g., "BOS @ CHI")')
    parser.add_argument('pick', help='Pick with spread (e.g., "BOS -3.5")')
    parser.add_argument('spread', type=float, help='Spread value')
    parser.add_argument('flag_score', type=int, help='Flag score')
    parser.add_argument('model_edge', type=float, help='Model edge')

    args = parser.parse_args()

    results_file = Path(__file__).parent.parent / 'data' / 'results.csv'

    row = {
        'date': date.today().isoformat(),
        'game': args.game,
        'pick': args.pick,
        'spread': args.spread,
        'flag_score': args.flag_score,
        'model_edge': args.model_edge,
        'result': '',
        'margin': '',
        'clv': ''
    }

    with open(results_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        writer.writerow(row)

    print(f"Logged: {args.game} | {args.pick} | flag_score={args.flag_score} | edge={args.model_edge}")


if __name__ == '__main__':
    main()
