"""
AXIOM Pipeline Backtest

Backtests the full pipeline over recent games to measure actual performance.

Tests:
1. Spread edges (underdog ATS)
2. Total edges (unders)
3. Parlay performance
4. Props performance
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]


def get_game_results(conn, target_date):
    """Get actual game results for a date."""
    df = pd.read_sql(f"""
        SELECT
            g.game_id,
            g.home_team,
            g.away_team,
            home_tb.pts as home_score,
            away_tb.pts as away_score,
            home_tb.pts - away_tb.pts as margin,
            home_tb.pts + away_tb.pts as total_score,
            COALESCE(b.espn_closing_spread, b.espn_opening_spread) as spread,
            COALESCE(b.espn_closing_total, b.espn_opening_total) as total_line
        FROM Games g
        JOIN Teams home_t ON g.home_team = home_t.abbreviation
        JOIN Teams away_t ON g.away_team = away_t.abbreviation
        JOIN TeamBox home_tb ON g.game_id = home_tb.game_id AND home_t.team_id = home_tb.team_id
        JOIN TeamBox away_tb ON g.game_id = away_tb.game_id AND away_t.team_id = away_tb.team_id
        LEFT JOIN Betting b ON g.game_id = b.game_id
        WHERE DATE(g.date_time_utc) = '{target_date}'
        AND g.status_text = 'Final'
        AND home_tb.pts IS NOT NULL
    """, conn)
    return df


def backtest_spread_edges(conn, start_date, end_date):
    """Backtest underdog ATS picks."""
    results = []

    # Get all games in date range
    games = pd.read_sql(f"""
        SELECT
            g.game_id,
            g.home_team,
            g.away_team,
            DATE(g.date_time_utc) as game_date,
            home_tb.pts as home_score,
            away_tb.pts as away_score,
            home_tb.pts - away_tb.pts as margin,
            COALESCE(b.espn_closing_spread, b.espn_opening_spread) as spread
        FROM Games g
        JOIN Teams home_t ON g.home_team = home_t.abbreviation
        JOIN Teams away_t ON g.away_team = away_t.abbreviation
        JOIN TeamBox home_tb ON g.game_id = home_tb.game_id AND home_t.team_id = home_tb.team_id
        JOIN TeamBox away_tb ON g.game_id = away_tb.game_id AND away_t.team_id = away_tb.team_id
        LEFT JOIN Betting b ON g.game_id = b.game_id
        WHERE DATE(g.date_time_utc) BETWEEN '{start_date}' AND '{end_date}'
        AND g.status_text = 'Final'
        AND home_tb.pts IS NOT NULL
        AND b.espn_closing_spread IS NOT NULL
    """, conn)

    if games.empty:
        return pd.DataFrame()

    games['abs_spread'] = games['spread'].abs()

    # Big underdog ATS (+7 or more)
    big_dogs = games[games['abs_spread'] >= 7].copy()
    for _, game in big_dogs.iterrows():
        # Underdog is home when spread > 0, away when spread < 0
        if game['spread'] > 0:  # Home is underdog
            pick = f"{game['home_team']} +{game['spread']:.1f}"
            # Home covers if margin > -spread
            covered = game['margin'] > -game['spread']
        else:  # Away is underdog
            pick = f"{game['away_team']} +{abs(game['spread']):.1f}"
            # Away covers if margin < -spread (home wins by less than spread)
            covered = game['margin'] < -game['spread']

        results.append({
            'date': game['game_date'],
            'game': f"{game['away_team']} @ {game['home_team']}",
            'type': 'SPREAD',
            'edge': 'BIG_DOG_7',
            'pick': pick,
            'result': game['margin'],
            'spread': game['spread'],
            'hit': covered,
            'expected_rate': 0.644
        })

    # Double-digit underdog (+10 or more)
    huge_dogs = games[games['abs_spread'] >= 10].copy()
    for _, game in huge_dogs.iterrows():
        if game['spread'] > 0:
            pick = f"{game['home_team']} +{game['spread']:.1f}"
            covered = game['margin'] > -game['spread']
        else:
            pick = f"{game['away_team']} +{abs(game['spread']):.1f}"
            covered = game['margin'] < -game['spread']

        results.append({
            'date': game['game_date'],
            'game': f"{game['away_team']} @ {game['home_team']}",
            'type': 'SPREAD',
            'edge': 'DOUBLE_DIGIT_10',
            'pick': pick,
            'result': game['margin'],
            'spread': game['spread'],
            'hit': covered,
            'expected_rate': 0.608
        })

    return pd.DataFrame(results)


def backtest_total_edges(conn, start_date, end_date):
    """Backtest under picks."""
    results = []

    # Get team pace data
    try:
        pace_df = pd.read_sql("SELECT team_name, pace FROM team_advanced_stats", conn)
        team_name_map = {
            'Atlanta Hawks': 'ATL', 'Boston Celtics': 'BOS', 'Brooklyn Nets': 'BKN',
            'Charlotte Hornets': 'CHA', 'Chicago Bulls': 'CHI', 'Cleveland Cavaliers': 'CLE',
            'Dallas Mavericks': 'DAL', 'Denver Nuggets': 'DEN', 'Detroit Pistons': 'DET',
            'Golden State Warriors': 'GSW', 'Houston Rockets': 'HOU', 'Indiana Pacers': 'IND',
            'LA Clippers': 'LAC', 'Los Angeles Lakers': 'LAL', 'Memphis Grizzlies': 'MEM',
            'Miami Heat': 'MIA', 'Milwaukee Bucks': 'MIL', 'Minnesota Timberwolves': 'MIN',
            'New Orleans Pelicans': 'NOP', 'New York Knicks': 'NYK', 'Oklahoma City Thunder': 'OKC',
            'Orlando Magic': 'ORL', 'Philadelphia 76ers': 'PHI', 'Phoenix Suns': 'PHX',
            'Portland Trail Blazers': 'POR', 'Sacramento Kings': 'SAC', 'San Antonio Spurs': 'SAS',
            'Toronto Raptors': 'TOR', 'Utah Jazz': 'UTA', 'Washington Wizards': 'WAS'
        }
        pace_df['team_code'] = pace_df['team_name'].map(team_name_map)
        pace_map = pace_df.set_index('team_code')['pace'].to_dict()
    except:
        pace_map = {}

    # Get games
    games = pd.read_sql(f"""
        SELECT
            g.game_id,
            g.home_team,
            g.away_team,
            DATE(g.date_time_utc) as game_date,
            home_tb.pts + away_tb.pts as total_score,
            COALESCE(b.espn_closing_total, b.espn_opening_total) as total_line
        FROM Games g
        JOIN Teams home_t ON g.home_team = home_t.abbreviation
        JOIN Teams away_t ON g.away_team = away_t.abbreviation
        JOIN TeamBox home_tb ON g.game_id = home_tb.game_id AND home_t.team_id = home_tb.team_id
        JOIN TeamBox away_tb ON g.game_id = away_tb.game_id AND away_t.team_id = away_tb.team_id
        LEFT JOIN Betting b ON g.game_id = b.game_id
        WHERE DATE(g.date_time_utc) BETWEEN '{start_date}' AND '{end_date}'
        AND g.status_text = 'Final'
        AND home_tb.pts IS NOT NULL
        AND b.espn_closing_total IS NOT NULL
    """, conn)

    if games.empty:
        return pd.DataFrame()

    # Add pace data
    games['home_pace'] = games['home_team'].map(pace_map)
    games['away_pace'] = games['away_team'].map(pace_map)
    games['pace_sum'] = games['home_pace'].fillna(100) + games['away_pace'].fillna(100)

    # Low pace games (both teams < 100 pace)
    low_pace = games[(games['home_pace'] < 100) & (games['away_pace'] < 100)]
    for _, game in low_pace.iterrows():
        under_hit = game['total_score'] < game['total_line']
        results.append({
            'date': game['game_date'],
            'game': f"{game['away_team']} @ {game['home_team']}",
            'type': 'TOTAL',
            'edge': 'BOTH_LOW_PACE',
            'pick': f"UNDER {game['total_line']:.1f}",
            'result': game['total_score'],
            'line': game['total_line'],
            'hit': under_hit,
            'expected_rate': 0.773
        })

    # Pace sum low (< 200)
    pace_low = games[games['pace_sum'] < 200]
    for _, game in pace_low.iterrows():
        under_hit = game['total_score'] < game['total_line']
        results.append({
            'date': game['game_date'],
            'game': f"{game['away_team']} @ {game['home_team']}",
            'type': 'TOTAL',
            'edge': 'PACE_SUM_LOW',
            'pick': f"UNDER {game['total_line']:.1f}",
            'result': game['total_score'],
            'line': game['total_line'],
            'hit': under_hit,
            'expected_rate': 0.636
        })

    return pd.DataFrame(results)


def backtest_parlays(spread_results, total_results, start_date, end_date):
    """Simulate parlay performance based on daily picks."""
    parlay_results = []

    # Combine all picks
    all_picks = pd.concat([spread_results, total_results], ignore_index=True)
    if all_picks.empty:
        return pd.DataFrame()

    # Group by date
    for game_date in all_picks['date'].unique():
        day_picks = all_picks[all_picks['date'] == game_date]

        # Get unique picks (avoid duplicates from overlapping edges)
        unique_picks = day_picks.drop_duplicates(subset=['game', 'type'])

        # Select top picks by expected rate (simulating our selection logic)
        spread_picks = unique_picks[unique_picks['type'] == 'SPREAD'].nlargest(3, 'expected_rate')
        total_picks = unique_picks[unique_picks['type'] == 'TOTAL'].nlargest(2, 'expected_rate')

        parlay_legs = pd.concat([spread_picks, total_picks]).head(5)

        if len(parlay_legs) >= 3:
            all_hit = parlay_legs['hit'].all()
            num_legs = len(parlay_legs)
            payout = 1.91 ** num_legs

            parlay_results.append({
                'date': game_date,
                'legs': num_legs,
                'all_hit': all_hit,
                'payout': payout,
                'picks': ', '.join(parlay_legs['pick'].tolist()[:3]) + ('...' if num_legs > 3 else '')
            })

    return pd.DataFrame(parlay_results)


def calculate_roi(hits, total, payout_odds=-110):
    """Calculate ROI for a set of bets."""
    if total == 0:
        return 0

    # Standard -110 odds: win $100 on $110 bet
    win_amount = 100
    risk_amount = 110

    winnings = hits * win_amount
    losses = (total - hits) * risk_amount

    total_risked = total * risk_amount
    net = winnings - losses

    return (net / total_risked) * 100


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=10, help="Number of days to backtest")
    parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)

    # Get date range
    if args.start and args.end:
        start_date = args.start
        end_date = args.end
    else:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')

    print("=" * 70)
    print(f"AXIOM PIPELINE BACKTEST: {start_date} to {end_date}")
    print("=" * 70)

    # Run backtests
    print("\nBacktesting spread edges...")
    spread_results = backtest_spread_edges(conn, start_date, end_date)

    print("Backtesting total edges...")
    total_results = backtest_total_edges(conn, start_date, end_date)

    print("Simulating parlays...")
    parlay_results = backtest_parlays(spread_results, total_results, start_date, end_date)

    conn.close()

    # Print Results
    print("\n" + "=" * 70)
    print("SPREAD EDGE RESULTS (Underdog ATS)")
    print("=" * 70)

    if not spread_results.empty:
        # Deduplicate (same game might appear in multiple edge categories)
        spread_unique = spread_results.drop_duplicates(subset=['date', 'game'])

        for edge in spread_results['edge'].unique():
            edge_df = spread_results[spread_results['edge'] == edge]
            hits = edge_df['hit'].sum()
            total = len(edge_df)
            rate = hits / total * 100 if total > 0 else 0
            expected = edge_df['expected_rate'].iloc[0] * 100
            roi = calculate_roi(hits, total)

            print(f"\n{edge}:")
            print(f"  Record: {hits}W - {total - hits}L ({rate:.1f}%)")
            print(f"  Expected: {expected:.1f}%")
            print(f"  ROI: {roi:+.1f}%")

        # Overall spread
        total_hits = spread_unique['hit'].sum()
        total_bets = len(spread_unique)
        overall_rate = total_hits / total_bets * 100 if total_bets > 0 else 0
        overall_roi = calculate_roi(total_hits, total_bets)

        print(f"\nOVERALL SPREAD:")
        print(f"  Record: {total_hits}W - {total_bets - total_hits}L ({overall_rate:.1f}%)")
        print(f"  ROI: {overall_roi:+.1f}%")
    else:
        print("\nNo spread picks in this period")

    print("\n" + "=" * 70)
    print("TOTAL EDGE RESULTS (Unders)")
    print("=" * 70)

    if not total_results.empty:
        total_unique = total_results.drop_duplicates(subset=['date', 'game'])

        for edge in total_results['edge'].unique():
            edge_df = total_results[total_results['edge'] == edge]
            hits = edge_df['hit'].sum()
            total = len(edge_df)
            rate = hits / total * 100 if total > 0 else 0
            expected = edge_df['expected_rate'].iloc[0] * 100
            roi = calculate_roi(hits, total)

            print(f"\n{edge}:")
            print(f"  Record: {hits}W - {total - hits}L ({rate:.1f}%)")
            print(f"  Expected: {expected:.1f}%")
            print(f"  ROI: {roi:+.1f}%")

        # Overall totals
        total_hits = total_unique['hit'].sum()
        total_bets = len(total_unique)
        overall_rate = total_hits / total_bets * 100 if total_bets > 0 else 0
        overall_roi = calculate_roi(total_hits, total_bets)

        print(f"\nOVERALL TOTALS:")
        print(f"  Record: {total_hits}W - {total_bets - total_hits}L ({overall_rate:.1f}%)")
        print(f"  ROI: {overall_roi:+.1f}%")
    else:
        print("\nNo total picks in this period")

    print("\n" + "=" * 70)
    print("PARLAY RESULTS")
    print("=" * 70)

    if not parlay_results.empty:
        wins = parlay_results['all_hit'].sum()
        total = len(parlay_results)
        avg_payout = parlay_results['payout'].mean()

        # Calculate parlay ROI
        # Each parlay costs 1 unit
        # Winners pay avg_payout units
        total_risked = total
        total_won = wins * avg_payout
        parlay_roi = (total_won - total_risked) / total_risked * 100

        print(f"\nParlays: {wins}W - {total - wins}L ({wins/total*100:.1f}%)")
        print(f"Avg payout: {avg_payout:.1f}x")
        print(f"ROI: {parlay_roi:+.1f}%")

        print(f"\nDaily breakdown:")
        for _, row in parlay_results.iterrows():
            status = "WIN" if row['all_hit'] else "LOSS"
            print(f"  {row['date']}: {row['legs']}-leg {status} | {row['picks']}")
    else:
        print("\nNo parlays in this period")

    # Combined summary
    print("\n" + "=" * 70)
    print("COMBINED SUMMARY")
    print("=" * 70)

    all_bets = pd.concat([
        spread_results.drop_duplicates(subset=['date', 'game'])[['hit']],
        total_results.drop_duplicates(subset=['date', 'game'])[['hit']]
    ], ignore_index=True) if not spread_results.empty or not total_results.empty else pd.DataFrame()

    if not all_bets.empty:
        total_hits = all_bets['hit'].sum()
        total_bets = len(all_bets)
        overall_rate = total_hits / total_bets * 100
        overall_roi = calculate_roi(total_hits, total_bets)

        print(f"\nAll Straight Bets: {total_hits}W - {total_bets - total_hits}L ({overall_rate:.1f}%)")
        print(f"ROI: {overall_roi:+.1f}%")

        # Profitability check
        if overall_rate >= 52.4:
            print(f"\n[PROFITABLE] Beating the -110 breakeven of 52.4%")
        else:
            print(f"\n[UNPROFITABLE] Below -110 breakeven of 52.4%")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
