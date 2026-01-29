"""
AXIOM Edge Backtest System v3
Using verified spread data with calculated covers
"""

import sqlite3
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime, timedelta

DB_PATH = "data/NBA_AI_current.sqlite"

def get_conn():
    return sqlite3.connect(DB_PATH)

def get_master_dataset():
    """
    Build master dataset with:
    - Verified closing spreads
    - Calculated cover results
    - Rest/B2B flags
    - Team stats
    """
    conn = get_conn()

    sql = '''
    WITH all_team_games AS (
        SELECT home_team as team, game_id, DATE(date_time_utc) as game_date
        FROM Games WHERE status_text = 'Final'
        UNION ALL
        SELECT away_team as team, game_id, DATE(date_time_utc) as game_date
        FROM Games WHERE status_text = 'Final'
    ),
    team_schedule AS (
        SELECT
            team, game_id, game_date,
            JULIANDAY(game_date) - JULIANDAY(LAG(game_date) OVER (PARTITION BY team ORDER BY game_date)) as days_rest
        FROM all_team_games
    ),
    game_data AS (
        SELECT
            g.game_id,
            DATE(g.date_time_utc) as game_date,
            g.home_team,
            g.away_team,
            b.espn_closing_spread as spread,
            gs.home_margin,
            gs.home_score + gs.away_score as total_points,
            b.espn_closing_total as total_line,
            hs.days_rest as home_rest,
            aws.days_rest as away_rest,
            -- Calculate cover: margin + spread > 0 = home covered
            CASE
                WHEN (gs.home_margin + b.espn_closing_spread) > 0 THEN 1
                WHEN (gs.home_margin + b.espn_closing_spread) < 0 THEN 0
                ELSE NULL  -- Push
            END as home_covered,
            CASE
                WHEN (gs.home_margin + b.espn_closing_spread) < 0 THEN 1
                WHEN (gs.home_margin + b.espn_closing_spread) > 0 THEN 0
                ELSE NULL
            END as away_covered,
            -- Over/Under
            CASE
                WHEN (gs.home_score + gs.away_score) > b.espn_closing_total THEN 1
                WHEN (gs.home_score + gs.away_score) < b.espn_closing_total THEN 0
                ELSE NULL
            END as went_over
        FROM Games g
        JOIN Betting b ON g.game_id = b.game_id
        JOIN GameStates gs ON g.game_id = gs.game_id AND gs.is_final_state = 1
        LEFT JOIN team_schedule hs ON g.game_id = hs.game_id AND g.home_team = hs.team
        LEFT JOIN team_schedule aws ON g.game_id = aws.game_id AND g.away_team = aws.team
        WHERE b.espn_closing_spread IS NOT NULL
        AND g.status_text = 'Final'
    )
    SELECT * FROM game_data
    '''

    df = pd.read_sql_query(sql, conn)
    conn.close()

    # Add derived columns
    df['abs_spread'] = abs(df['spread'])
    df['home_b2b'] = (df['home_rest'] == 1).astype(int)
    df['away_b2b'] = (df['away_rest'] == 1).astype(int)
    df['rest_diff'] = df['home_rest'] - df['away_rest']
    df['home_is_dog'] = (df['spread'] > 0).astype(int)
    df['home_is_fav'] = (df['spread'] < 0).astype(int)

    return df


def test_edge(df, name, logic, condition_col=None, bet_col='home_covered'):
    """
    Test an edge hypothesis
    Returns: dict with results
    """
    if condition_col is not None:
        subset = df[df[condition_col] == 1].dropna(subset=[bet_col])
    else:
        subset = df.dropna(subset=[bet_col])

    n = len(subset)
    if n == 0:
        return {'name': name, 'n': 0, 'note': 'No data'}

    wins = int(subset[bet_col].sum())
    hit_rate = wins / n

    # Binomial test
    p_val = stats.binomtest(wins, n, 0.5, alternative='greater').pvalue

    # Wilson CI
    z = 1.96
    denom = 1 + z**2/n
    center = (hit_rate + z**2/(2*n)) / denom
    margin = z * np.sqrt((hit_rate*(1-hit_rate) + z**2/(4*n)) / n) / denom

    return {
        'name': name,
        'logic': logic,
        'n': n,
        'wins': wins,
        'hit_rate': round(hit_rate * 100, 1),
        'p_value': round(p_val, 4),
        'ci_low': round(max(0, center - margin) * 100, 1),
        'ci_high': round(min(1, center + margin) * 100, 1)
    }


def run_all_tests(df):
    """Run comprehensive edge tests"""
    results = []

    # ==== FATIGUE/REST EDGES ====
    print("Testing fatigue/rest edges...")

    # B2B Away (fade away, bet home)
    results.append(test_edge(
        df, '#31 B2B Road Fade',
        'Fade away team on B2B (bet home)',
        'away_b2b', 'home_covered'
    ))

    # B2B Home (fade home, bet away)
    results.append(test_edge(
        df, '#34 B2B Home Fade',
        'Fade home team on B2B (bet away)',
        'home_b2b', 'away_covered'
    ))

    # Rest advantage >= 2
    rest_adv = df[df['rest_diff'] >= 2].copy()
    if len(rest_adv) > 0:
        results.append(test_edge(
            rest_adv, '#41 Rest +2 Days (Home)',
            'Home team with 2+ more rest days covers',
            None, 'home_covered'
        ))

    # Rest disadvantage >= 2 (bet away)
    rest_disadv = df[df['rest_diff'] <= -2].copy()
    if len(rest_disadv) > 0:
        results.append(test_edge(
            rest_disadv, '#41b Rest +2 Days (Away)',
            'Away team with 2+ more rest days covers',
            None, 'away_covered'
        ))

    # ==== SPREAD SIZE EDGES ====
    print("Testing spread size edges...")

    # Large dogs (+8 or more)
    # Home dogs
    home_dogs = df[(df['spread'] >= 8)].copy()
    home_dogs['underdog_covered'] = home_dogs['home_covered']
    results.append(test_edge(
        home_dogs, 'Home Dogs +8',
        'Home underdogs of 8+ points cover',
        None, 'underdog_covered'
    ))

    # Away dogs
    away_dogs = df[(df['spread'] <= -8)].copy()
    away_dogs['underdog_covered'] = away_dogs['away_covered']
    results.append(test_edge(
        away_dogs, 'Away Dogs +8',
        'Away underdogs of 8+ points cover',
        None, 'underdog_covered'
    ))

    # Small favorites (1-3)
    small_home_fav = df[(df['spread'] >= -3) & (df['spread'] < 0)].copy()
    results.append(test_edge(
        small_home_fav, 'Small Home Favs (1-3)',
        'Home favorites of 1-3 points cover',
        None, 'home_covered'
    ))

    # Pick'em games (-1 to +1)
    pickem = df[(df['abs_spread'] <= 1)].copy()
    results.append(test_edge(
        pickem, 'Pick-em Games (Home)',
        'Home team in games with spread -1 to +1',
        None, 'home_covered'
    ))

    # ==== COMBINATION EDGES ====
    print("Testing combination edges...")

    # B2B + Large spread
    b2b_away_large = df[(df['away_b2b'] == 1) & (df['abs_spread'] >= 6)].copy()
    results.append(test_edge(
        b2b_away_large, 'B2B Road + Large Spread',
        'Fade B2B away team when spread is 6+',
        None, 'home_covered'
    ))

    # B2B + Small spread
    b2b_away_small = df[(df['away_b2b'] == 1) & (df['abs_spread'] <= 4)].copy()
    results.append(test_edge(
        b2b_away_small, 'B2B Road + Small Spread',
        'Fade B2B away team when spread is 4 or less',
        None, 'home_covered'
    ))

    # Rest advantage + Dog
    rest_dog = df[(df['rest_diff'] >= 2) & (df['spread'] > 0)].copy()
    results.append(test_edge(
        rest_dog, 'Rested Home Dog',
        'Home dog with 2+ more rest',
        None, 'home_covered'
    ))

    # ==== TOTALS EDGES ====
    print("Testing totals edges...")

    # High totals (230+)
    high_total = df[df['total_line'] >= 230].copy()
    results.append(test_edge(
        high_total, 'High Totals (230+) OVER',
        'Bet OVER when total is 230+',
        None, 'went_over'
    ))

    # Low totals (<215)
    low_total = df[df['total_line'] < 215].copy()
    low_total['went_under'] = 1 - low_total['went_over']
    results.append(test_edge(
        low_total, 'Low Totals (<215) UNDER',
        'Bet UNDER when total is <215',
        None, 'went_under'
    ))

    return results


def print_results(results):
    """Format and print results"""

    # Filter by quality
    validated = [r for r in results if r.get('n', 0) >= 30 and r.get('hit_rate', 0) > 54 and r.get('p_value', 1) < 0.10]
    promising = [r for r in results if r.get('n', 0) >= 20 and r.get('hit_rate', 0) > 50 and r not in validated]
    failed = [r for r in results if r not in validated and r not in promising]

    print("\n" + "="*80)
    print("VALIDATED EDGES (N>=30, Hit Rate>54%, p<0.10)")
    print("="*80)
    if validated:
        print(f"{'Edge':<35} {'N':>5} {'Wins':>5} {'Hit%':>7} {'p-val':>8} {'95% CI':>12}")
        print("-"*80)
        for r in sorted(validated, key=lambda x: x.get('p_value', 1)):
            ci = f"[{r['ci_low']}-{r['ci_high']}%]"
            print(f"{r['name']:<35} {r['n']:>5} {r['wins']:>5} {r['hit_rate']:>6.1f}% {r['p_value']:>8.4f} {ci:>12}")
            print(f"  Logic: {r.get('logic', 'N/A')}")
    else:
        print("None passed all filters.")

    print("\n" + "="*80)
    print("PROMISING (Needs More Data)")
    print("="*80)
    if promising:
        for r in promising:
            print(f"{r['name']}: N={r.get('n')}, {r.get('hit_rate')}%, p={r.get('p_value')}")
    else:
        print("None.")

    print("\n" + "="*80)
    print("NO EDGE FOUND")
    print("="*80)
    for r in failed:
        if r.get('n', 0) > 0:
            print(f"{r['name']}: N={r.get('n')}, {r.get('hit_rate')}%, p={r.get('p_value')}")


if __name__ == "__main__":
    print("="*80)
    print("AXIOM EDGE BACKTEST v3 - Verified Spread Data")
    print("="*80)

    df = get_master_dataset()
    print(f"\nLoaded {len(df)} games with verified spread data")
    print(f"Date range: {df['game_date'].min()} to {df['game_date'].max()}")
    print(f"Spreads available: {df['spread'].notna().sum()}")
    print(f"Totals available: {df['total_line'].notna().sum()}")
    print()

    results = run_all_tests(df)
    print_results(results)

    # Summary
    print("\n" + "="*80)
    print("QUICK SUMMARY")
    print("="*80)
    validated = [r for r in results if r.get('n', 0) >= 30 and r.get('hit_rate', 0) > 54 and r.get('p_value', 1) < 0.10]
    if validated:
        print(f"Found {len(validated)} validated edge(s)!")
        for r in validated:
            print(f"  - {r['name']}: {r['hit_rate']}% ({r['n']} games, p={r['p_value']})")
    else:
        print("No edges passed strict validation with current data.")
        print("Consider:")
        print("  1. Getting more betting data (currently 242 games)")
        print("  2. Expanding to player props")
        print("  3. Testing with historical seasons")
