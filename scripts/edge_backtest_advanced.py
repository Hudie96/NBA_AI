"""
AXIOM Edge Backtest - Advanced Stats Edition
Uses team ORTG, DRTG, Pace, TS%, eFG% for additional edges
"""

import sqlite3
import pandas as pd
import numpy as np
from scipy import stats

DB_PATH = "data/NBA_AI_current.sqlite"

# Team name to code mapping
TEAM_NAME_TO_CODE = {
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


def get_conn():
    return sqlite3.connect(DB_PATH)


def get_team_stats():
    """Get team advanced stats with team codes"""
    conn = get_conn()
    df = pd.read_sql_query('SELECT * FROM team_advanced_stats', conn)
    conn.close()

    df['team_code'] = df['team_name'].map(TEAM_NAME_TO_CODE)
    return df


def get_master_dataset():
    """Build master dataset with games, spreads, and team stats"""
    conn = get_conn()

    # Get team stats
    team_stats = get_team_stats()

    # Base game data with calculated covers
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
    )
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
        CASE
            WHEN (gs.home_margin + b.espn_closing_spread) > 0 THEN 1
            WHEN (gs.home_margin + b.espn_closing_spread) < 0 THEN 0
            ELSE NULL
        END as home_covered,
        CASE
            WHEN (gs.home_margin + b.espn_closing_spread) < 0 THEN 1
            WHEN (gs.home_margin + b.espn_closing_spread) > 0 THEN 0
            ELSE NULL
        END as away_covered,
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
    '''

    df = pd.read_sql_query(sql, conn)
    conn.close()

    # Join team stats
    home_stats = team_stats.add_prefix('home_')
    away_stats = team_stats.add_prefix('away_')

    df = df.merge(home_stats, left_on='home_team', right_on='home_team_code', how='left')
    df = df.merge(away_stats, left_on='away_team', right_on='away_team_code', how='left')

    # Derived columns
    df['abs_spread'] = abs(df['spread'])
    df['home_b2b'] = (df['home_rest'] == 1).astype(int)
    df['away_b2b'] = (df['away_rest'] == 1).astype(int)
    df['net_rating_diff'] = df['home_net_rating'] - df['away_net_rating']
    df['pace_diff'] = df['home_pace'] - df['away_pace']
    df['ortg_diff'] = df['home_off_rating'] - df['away_off_rating']
    df['drtg_diff'] = df['home_def_rating'] - df['away_def_rating']  # Lower is better

    return df


def test_edge(df, name, logic):
    """Test edge and return results"""
    df = df.dropna(subset=['covered'])
    n = len(df)

    if n < 10:
        return {'name': name, 'logic': logic, 'n': n, 'note': 'Sample too small'}

    wins = int(df['covered'].sum())
    hr = wins / n
    p_val = stats.binomtest(wins, n, 0.5, alternative='greater').pvalue

    return {
        'name': name,
        'logic': logic,
        'n': n,
        'wins': wins,
        'hit_rate': round(hr * 100, 1),
        'p_value': round(p_val, 4)
    }


def run_advanced_tests(df):
    """Run tests using advanced stats"""
    results = []

    print("="*70)
    print("ADVANCED STATS EDGE TESTS")
    print("="*70)

    # ====== OFFENSIVE/DEFENSIVE RATING EDGES ======
    print("\n--- ORTG/DRTG Tests ---")

    # Query #61: Top 5 DRTG vs Bottom 5 ORTG
    # Get rankings
    team_stats = get_team_stats()
    top5_drtg = team_stats.nsmallest(5, 'def_rating')['team_code'].tolist()  # Lower = better
    bottom5_ortg = team_stats.nsmallest(5, 'off_rating')['team_code'].tolist()  # Lower = worse

    # Home has top defense, away has bad offense
    test_df = df[(df['home_team'].isin(top5_drtg)) & (df['away_team'].isin(bottom5_ortg))].copy()
    test_df['covered'] = test_df['home_covered']
    results.append(test_edge(test_df, '#61 Top DRTG vs Bottom ORTG (Home)',
                            'Home has top 5 defense vs bottom 5 offense'))

    # Net Rating differential
    # Home has much better net rating
    test_df = df[df['net_rating_diff'] >= 5].copy()
    test_df['covered'] = test_df['home_covered']
    results.append(test_edge(test_df, 'Net Rating +5 Advantage (Home)',
                            'Home team has 5+ better net rating'))

    # Home has worse net rating but getting points
    test_df = df[(df['net_rating_diff'] <= -5) & (df['spread'] > 0)].copy()
    test_df['covered'] = test_df['home_covered']
    results.append(test_edge(test_df, 'Net Rating -5 Home Dog',
                            'Undervalued home dog (net rating doesnt match spread)'))

    # ====== PACE EDGES ======
    print("\n--- Pace Tests ---")

    # High pace teams (top 10)
    high_pace_teams = team_stats.nlargest(10, 'pace')['team_code'].tolist()
    low_pace_teams = team_stats.nsmallest(10, 'pace')['team_code'].tolist()

    # High pace home vs low pace away - expect OVER
    test_df = df[(df['home_team'].isin(high_pace_teams)) &
                 (df['away_team'].isin(low_pace_teams))].copy()
    test_df = test_df[test_df['went_over'].notna()]
    test_df['covered'] = test_df['went_over']
    results.append(test_edge(test_df, '#151 High vs Low Pace (OVER)',
                            'High pace home vs low pace away - bet OVER'))

    # Both high pace - OVER
    test_df = df[(df['home_team'].isin(high_pace_teams)) &
                 (df['away_team'].isin(high_pace_teams))].copy()
    test_df = test_df[test_df['went_over'].notna()]
    test_df['covered'] = test_df['went_over']
    results.append(test_edge(test_df, 'Both High Pace (OVER)',
                            'Both teams top 10 in pace - bet OVER'))

    # Both low pace - UNDER
    test_df = df[(df['home_team'].isin(low_pace_teams)) &
                 (df['away_team'].isin(low_pace_teams))].copy()
    test_df = test_df[test_df['went_over'].notna()]
    test_df['covered'] = 1 - test_df['went_over']  # UNDER
    results.append(test_edge(test_df, 'Both Low Pace (UNDER)',
                            'Both teams bottom 10 in pace - bet UNDER'))

    # ====== SHOOTING EFFICIENCY EDGES ======
    print("\n--- Shooting Efficiency Tests ---")

    # High TS% teams
    high_ts_teams = team_stats.nlargest(10, 'ts_pct')['team_code'].tolist()
    low_ts_teams = team_stats.nsmallest(10, 'ts_pct')['team_code'].tolist()

    # High TS% vs Low TS%
    test_df = df[(df['home_team'].isin(high_ts_teams)) &
                 (df['away_team'].isin(low_ts_teams))].copy()
    test_df['covered'] = test_df['home_covered']
    results.append(test_edge(test_df, '#1 High TS% vs Low TS% (Home)',
                            'High shooting efficiency team covers'))

    # High eFG% teams
    high_efg_teams = team_stats.nlargest(10, 'efg_pct')['team_code'].tolist()
    low_efg_teams = team_stats.nsmallest(10, 'efg_pct')['team_code'].tolist()

    test_df = df[(df['home_team'].isin(high_efg_teams)) &
                 (df['away_team'].isin(low_efg_teams))].copy()
    test_df['covered'] = test_df['home_covered']
    results.append(test_edge(test_df, '#10 High eFG% vs Low eFG% (Home)',
                            'High effective FG% team covers'))

    # ====== COMBINED RATING + SPREAD EDGES ======
    print("\n--- Rating + Spread Combination Tests ---")

    # Good team as dog (net rating > 0 but getting points)
    test_df = df[(df['home_net_rating'] > 2) & (df['spread'] > 3)].copy()
    test_df['covered'] = test_df['home_covered']
    results.append(test_edge(test_df, 'Good Team Home Dog (+3)',
                            'Positive net rating team as 3+ point dog'))

    # Bad team as favorite (net rating < 0 but laying points)
    test_df = df[(df['home_net_rating'] < -2) & (df['spread'] < -3)].copy()
    test_df['covered'] = test_df['away_covered']
    results.append(test_edge(test_df, 'Fade Bad Team Favorite',
                            'Negative net rating team as 3+ point favorite - fade'))

    # ====== DEFENSIVE RATING EDGES ======
    print("\n--- Defensive Rating Tests ---")

    # Top 5 defense
    top5_def = team_stats.nsmallest(5, 'def_rating')['team_code'].tolist()
    bottom5_def = team_stats.nlargest(5, 'def_rating')['team_code'].tolist()

    # Top defense as underdog
    test_df = df[(df['home_team'].isin(top5_def)) & (df['spread'] > 0)].copy()
    test_df['covered'] = test_df['home_covered']
    results.append(test_edge(test_df, '#136 Top Defense as Home Dog',
                            'Top 5 defense as home underdog covers'))

    test_df = df[(df['away_team'].isin(top5_def)) & (df['spread'] < 0)].copy()
    test_df['covered'] = test_df['away_covered']
    results.append(test_edge(test_df, '#136 Top Defense as Away Dog',
                            'Top 5 defense as away underdog covers'))

    # Bad defense as favorite - fade
    test_df = df[(df['home_team'].isin(bottom5_def)) & (df['spread'] < -5)].copy()
    test_df['covered'] = test_df['away_covered']
    results.append(test_edge(test_df, 'Fade Bad Defense Favorite',
                            'Bottom 5 defense as 5+ point favorite - bet away'))

    return results


def print_results(results):
    """Print formatted results"""

    # Sort by p-value
    valid = [r for r in results if 'hit_rate' in r and r['n'] >= 10]
    valid.sort(key=lambda x: x.get('p_value', 1))

    print("\n" + "="*80)
    print("RESULTS SUMMARY")
    print("="*80)
    print(f"{'Edge':<45} {'N':>5} {'Wins':>5} {'Hit%':>7} {'p-val':>8}")
    print("-"*80)

    for r in valid:
        hr = r.get('hit_rate', 0)
        pv = r.get('p_value', 1)

        # Mark significant results
        status = ''
        if hr > 54 and pv < 0.10:
            status = ' ** EDGE **'
        elif hr > 50 and pv < 0.20:
            status = ' (promising)'

        print(f"{r['name']:<45} {r['n']:>5} {r.get('wins', 'N/A'):>5} {hr:>6.1f}% {pv:>8.4f}{status}")

    # Highlight validated
    validated = [r for r in valid if r.get('hit_rate', 0) > 54 and r.get('p_value', 1) < 0.10]

    if validated:
        print("\n" + "="*80)
        print("VALIDATED EDGES (>54%, p<0.10)")
        print("="*80)
        for r in validated:
            print(f"\n{r['name']}")
            print(f"  N={r['n']}, Hit Rate={r['hit_rate']}%, p={r['p_value']}")
            print(f"  Logic: {r['logic']}")


if __name__ == "__main__":
    print("Loading data with advanced stats...")
    df = get_master_dataset()

    print(f"Loaded {len(df)} games")
    print(f"Games with team stats: {df['home_net_rating'].notna().sum()}")

    results = run_advanced_tests(df)
    print_results(results)
