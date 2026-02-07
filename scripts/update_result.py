"""
Update a logged result with outcome data.

Uses canonical CSV format: date,game,bet_type,pick,line,vegas_line,tier,edge,result,actual

Usage:
    python scripts/update_result.py "2026-01-30" "BOS -17.7" "W" 19
    python scripts/update_result.py "2026-01-30" "Tyler Kolek OVER 2.4 RA" "W" 6

Args:
    date: Date of the game (YYYY-MM-DD)
    pick: Pick string to match (e.g., "BOS -17.7" or "Tyler Kolek OVER 2.4 RA")
    result: W or L
    actual: Actual value (margin for spreads, stat value for props)
"""
import argparse
import csv
from pathlib import Path

CANONICAL_FIELDS = ['date', 'game', 'bet_type', 'pick', 'line', 'vegas_line', 'tier', 'edge', 'result', 'actual']


def main():
    parser = argparse.ArgumentParser(description='Update a result in results.csv')
    parser.add_argument('date', help='Game date (YYYY-MM-DD)')
    parser.add_argument('pick', help='Pick string to match (e.g., "BOS -17.7")')
    parser.add_argument('result', choices=['W', 'L', 'P'], help='Result (W/L/P for push)')
    parser.add_argument('actual', help='Actual value (margin for spreads, stat value for props)')

    args = parser.parse_args()

    results_file = Path(__file__).parent.parent / 'data' / 'results.csv'

    if not results_file.exists():
        print(f"Error: {results_file} not found")
        return 1

    rows = []
    updated = False

    with open(results_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['date'] == args.date and row['pick'] == args.pick and not row.get('result'):
                row['result'] = args.result
                row['actual'] = args.actual
                updated = True
            rows.append(row)

    if not updated:
        print(f"Error: No matching pending row found for {args.date} | {args.pick}")
        print("  (Row may already have a result, or pick string doesn't match)")
        return 1

    with open(results_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CANONICAL_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Updated: {args.date} | {args.pick} | {args.result} | actual={args.actual}")
    return 0


if __name__ == '__main__':
    exit(main())
