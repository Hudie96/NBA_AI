"""
Update a logged result with outcome data.

Usage:
    python scripts/update_result.py "2026-01-26" "BOS @ CHI" "W" 5 0.5

Args:
    date: Date of the game (YYYY-MM-DD)
    game: Matchup string (e.g., "BOS @ CHI")
    result: W or L
    margin: Margin of victory/defeat
    clv: Closing line value
"""
import argparse
import csv
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description='Update a result in results.csv')
    parser.add_argument('date', help='Game date (YYYY-MM-DD)')
    parser.add_argument('game', help='Matchup (e.g., "BOS @ CHI")')
    parser.add_argument('result', choices=['W', 'L', 'P'], help='Result (W/L/P for push)')
    parser.add_argument('margin', type=float, help='Margin of victory/defeat')
    parser.add_argument('clv', type=float, help='Closing line value')

    args = parser.parse_args()

    results_file = Path(__file__).parent.parent / 'data' / 'results.csv'

    rows = []
    updated = False
    fieldnames = ['date', 'game', 'pick', 'spread', 'flag_score', 'model_edge', 'result', 'margin', 'clv']

    with open(results_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['date'] == args.date and row['game'] == args.game:
                row['result'] = args.result
                row['margin'] = args.margin
                row['clv'] = args.clv
                updated = True
            rows.append(row)

    if not updated:
        print(f"Error: No matching row found for {args.date} | {args.game}")
        return 1

    with open(results_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Updated: {args.date} | {args.game} | {args.result} | margin={args.margin} | clv={args.clv}")
    return 0


if __name__ == '__main__':
    exit(main())
