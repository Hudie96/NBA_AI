"""
AXIOM Comprehensive Edge Research
Token-efficient: One script runs ALL analysis, outputs to file.

Analyzes:
1. Team-level edges (team totals, situational spreads, pace factors)
2. Player prop edges (by stat type, threshold, situation)
3. Cross-correlation analysis (what parlays well together)
4. Historical parlay simulation

Run once, read results from file - no repeated queries.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, date
from pathlib import Path
from scipy import stats
import sys

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]
OUTPUT_FILE = PROJECT_ROOT / "outputs" / "COMPREHENSIVE_RESEARCH.md"


def binomial_test(hits, total, null_prob=0.5):
    """One-sided binomial test for edge significance."""
    if total == 0:
        return 1.0
    result = stats.binomtest(hits, total, null_prob, alternative='greater')
    return result.pvalue


def get_connection():
    return sqlite3.connect(DB_PATH)


# =============================================================================
# SECTION 1: TEAM-LEVEL EDGES
# =============================================================================

def analyze_team_totals(conn):
    """Analyze team total over/unders by situation."""
    print("  Analyzing team totals...")

    results = []

    # Get games with team scores and totals
    # TeamBox has pts - need to join for home and away
    df = pd.read_sql("""
        SELECT
            g.game_id,
            g.home_team,
            g.away_team,
            home_tb.pts as home_score,
            away_tb.pts as away_score,
            home_tb.pts + away_tb.pts as total_score,
            COALESCE(b.espn_closing_total, b.espn_opening_total) as total_line,
            COALESCE(b.espn_closing_spread, b.espn_opening_spread) as spread,
            DATE(g.date_time_utc) as game_date
        FROM Games g
        JOIN Betting b ON g.game_id = b.game_id
        JOIN Teams home_t ON g.home_team = home_t.abbreviation
        JOIN Teams away_t ON g.away_team = away_t.abbreviation
        JOIN TeamBox home_tb ON g.game_id = home_tb.game_id AND home_t.team_id = home_tb.team_id
        JOIN TeamBox away_tb ON g.game_id = away_tb.game_id AND away_t.team_id = away_tb.team_id
        WHERE g.status_text = 'Final'
        AND home_tb.pts IS NOT NULL
        AND b.espn_closing_total IS NOT NULL
    """, conn)

    if df.empty:
        return results

    # Calculate implied team totals
    # Home team implied = (total - spread) / 2 + spread
    # Away team implied = (total - spread) / 2
    df['home_implied'] = (df['total_line'] + df['spread']) / 2
    df['away_implied'] = (df['total_line'] - df['spread']) / 2

    # Edge 1: Home favorite team total OVER when spread < -5
    home_fav = df[df['spread'] < -5].copy()
    if len(home_fav) >= 30:
        home_fav['hit'] = home_fav['home_score'] > home_fav['home_implied']
        hit_rate = home_fav['hit'].mean()
        p_val = binomial_test(home_fav['hit'].sum(), len(home_fav))
        results.append({
            'edge': 'Home Favorite OVER (spread < -5)',
            'type': 'TEAM_TOTAL',
            'n': len(home_fav),
            'hit_rate': hit_rate,
            'p_value': p_val,
            'profitable': hit_rate > 0.525
        })

    # Edge 2: Home underdog team total UNDER when spread > 5
    home_dog = df[df['spread'] > 5].copy()
    if len(home_dog) >= 30:
        home_dog['hit'] = home_dog['home_score'] < home_dog['home_implied']
        hit_rate = home_dog['hit'].mean()
        p_val = binomial_test(home_dog['hit'].sum(), len(home_dog))
        results.append({
            'edge': 'Home Underdog UNDER (spread > 5)',
            'type': 'TEAM_TOTAL',
            'n': len(home_dog),
            'hit_rate': hit_rate,
            'p_value': p_val,
            'profitable': hit_rate > 0.525
        })

    # Edge 3: Away favorite team total by spread size
    away_fav = df[df['spread'] > 3].copy()  # Away is favorite when spread > 0
    if len(away_fav) >= 30:
        away_fav['hit'] = away_fav['away_score'] > away_fav['away_implied']
        hit_rate = away_fav['hit'].mean()
        p_val = binomial_test(away_fav['hit'].sum(), len(away_fav))
        results.append({
            'edge': 'Away Favorite OVER (spread > 3)',
            'type': 'TEAM_TOTAL',
            'n': len(away_fav),
            'hit_rate': hit_rate,
            'p_value': p_val,
            'profitable': hit_rate > 0.525
        })

    # Edge 4: Low total games (< 220) - check if UNDER hits more
    low_total = df[df['total_line'] < 220].copy()
    if len(low_total) >= 30:
        low_total['hit'] = low_total['total_score'] < low_total['total_line']
        hit_rate = low_total['hit'].mean()
        p_val = binomial_test(low_total['hit'].sum(), len(low_total))
        results.append({
            'edge': 'Low Total UNDER (line < 220)',
            'type': 'GAME_TOTAL',
            'n': len(low_total),
            'hit_rate': hit_rate,
            'p_value': p_val,
            'profitable': hit_rate > 0.525
        })

    # Edge 5: High total games (> 235) - check if OVER hits more
    high_total = df[df['total_line'] > 235].copy()
    if len(high_total) >= 30:
        high_total['hit'] = high_total['total_score'] > high_total['total_line']
        hit_rate = high_total['hit'].mean()
        p_val = binomial_test(high_total['hit'].sum(), len(high_total))
        results.append({
            'edge': 'High Total OVER (line > 235)',
            'type': 'GAME_TOTAL',
            'n': len(high_total),
            'hit_rate': hit_rate,
            'p_value': p_val,
            'profitable': hit_rate > 0.525
        })

    # Edge 6: Medium spread games (3-6) UNDER
    medium_spread = df[(df['spread'].abs() >= 3) & (df['spread'].abs() <= 6)].copy()
    if len(medium_spread) >= 30:
        medium_spread['hit'] = medium_spread['total_score'] < medium_spread['total_line']
        hit_rate = medium_spread['hit'].mean()
        p_val = binomial_test(medium_spread['hit'].sum(), len(medium_spread))
        results.append({
            'edge': 'Medium Spread UNDER (3-6 pts)',
            'type': 'GAME_TOTAL',
            'n': len(medium_spread),
            'hit_rate': hit_rate,
            'p_value': p_val,
            'profitable': hit_rate > 0.525
        })

    return results


def analyze_spread_edges(conn):
    """Analyze spread edges by situation."""
    print("  Analyzing spread edges...")

    results = []

    df = pd.read_sql("""
        SELECT
            g.game_id,
            g.home_team,
            g.away_team,
            home_tb.pts as home_score,
            away_tb.pts as away_score,
            home_tb.pts - away_tb.pts as margin,
            COALESCE(b.espn_closing_spread, b.espn_opening_spread) as spread,
            DATE(g.date_time_utc) as game_date
        FROM Games g
        JOIN Betting b ON g.game_id = b.game_id
        JOIN Teams home_t ON g.home_team = home_t.abbreviation
        JOIN Teams away_t ON g.away_team = away_t.abbreviation
        JOIN TeamBox home_tb ON g.game_id = home_tb.game_id AND home_t.team_id = home_tb.team_id
        JOIN TeamBox away_tb ON g.game_id = away_tb.game_id AND away_t.team_id = away_tb.team_id
        WHERE g.status_text = 'Final'
        AND home_tb.pts IS NOT NULL
        AND b.espn_closing_spread IS NOT NULL
    """, conn)

    if df.empty:
        return results

    # Home covers when margin > -spread (remember: negative spread = home favorite)
    df['home_covers'] = df['margin'] > -df['spread']
    df['away_covers'] = df['margin'] < -df['spread']
    df['abs_spread'] = df['spread'].abs()

    # Edge 1: Big underdog covers (spread >= 7)
    big_dogs = df[df['abs_spread'] >= 7].copy()
    if len(big_dogs) >= 30:
        # Underdog is home when spread > 0, away when spread < 0
        big_dogs['dog_covers'] = np.where(big_dogs['spread'] > 0, big_dogs['home_covers'], big_dogs['away_covers'])
        hit_rate = big_dogs['dog_covers'].mean()
        p_val = binomial_test(big_dogs['dog_covers'].sum(), len(big_dogs))
        results.append({
            'edge': 'Big Underdog ATS (spread >= 7)',
            'type': 'SPREAD',
            'n': len(big_dogs),
            'hit_rate': hit_rate,
            'p_value': p_val,
            'profitable': hit_rate > 0.525
        })

    # Edge 2: Medium underdog (5-6.5)
    med_dogs = df[(df['abs_spread'] >= 5) & (df['abs_spread'] < 7)].copy()
    if len(med_dogs) >= 30:
        med_dogs['dog_covers'] = np.where(med_dogs['spread'] > 0, med_dogs['home_covers'], med_dogs['away_covers'])
        hit_rate = med_dogs['dog_covers'].mean()
        p_val = binomial_test(med_dogs['dog_covers'].sum(), len(med_dogs))
        results.append({
            'edge': 'Medium Underdog ATS (5-6.5)',
            'type': 'SPREAD',
            'n': len(med_dogs),
            'hit_rate': hit_rate,
            'p_value': p_val,
            'profitable': hit_rate > 0.525
        })

    # Edge 3: Small favorite (-3 to -5)
    small_fav = df[(df['spread'] >= -5) & (df['spread'] <= -3)].copy()
    if len(small_fav) >= 30:
        hit_rate = small_fav['home_covers'].mean()
        p_val = binomial_test(small_fav['home_covers'].sum(), len(small_fav))
        results.append({
            'edge': 'Small Home Favorite ATS (-3 to -5)',
            'type': 'SPREAD',
            'n': len(small_fav),
            'hit_rate': hit_rate,
            'p_value': p_val,
            'profitable': hit_rate > 0.525
        })

    # Edge 4: Home underdog (+1 to +4)
    home_small_dog = df[(df['spread'] >= 1) & (df['spread'] <= 4)].copy()
    if len(home_small_dog) >= 30:
        hit_rate = home_small_dog['home_covers'].mean()
        p_val = binomial_test(home_small_dog['home_covers'].sum(), len(home_small_dog))
        results.append({
            'edge': 'Home Small Underdog ATS (+1 to +4)',
            'type': 'SPREAD',
            'n': len(home_small_dog),
            'hit_rate': hit_rate,
            'p_value': p_val,
            'profitable': hit_rate > 0.525
        })

    # Edge 5: Double-digit underdog (spread >= 10)
    huge_dogs = df[df['abs_spread'] >= 10].copy()
    if len(huge_dogs) >= 20:
        huge_dogs['dog_covers'] = np.where(huge_dogs['spread'] > 0, huge_dogs['home_covers'], huge_dogs['away_covers'])
        hit_rate = huge_dogs['dog_covers'].mean()
        p_val = binomial_test(huge_dogs['dog_covers'].sum(), len(huge_dogs))
        results.append({
            'edge': 'Double-Digit Underdog ATS (spread >= 10)',
            'type': 'SPREAD',
            'n': len(huge_dogs),
            'hit_rate': hit_rate,
            'p_value': p_val,
            'profitable': hit_rate > 0.525
        })

    return results


# =============================================================================
# SECTION 2: PLAYER PROP EDGES
# =============================================================================

def analyze_player_props(conn):
    """Analyze player prop edges by stat type and threshold."""
    print("  Analyzing player props...")

    results = []

    # Get props edges and join with actual results from player_game_logs
    try:
        props_df = pd.read_sql("""
            SELECT
                pe.player_name,
                pe.prop_type,
                pe.line,
                pe.projection,
                pe.edge_pct,
                pe.pick,
                pe.date,
                pgl.points as pts,
                pgl.assists as ast,
                pgl.rebounds as reb,
                pgl.pts_reb_ast as pra
            FROM props_edges pe
            JOIN player_game_logs pgl ON pe.player_name = pgl.player_name
                AND DATE(pgl.game_date) = pe.date
            WHERE pgl.points IS NOT NULL
        """, conn)
    except Exception as e:
        print(f"    Props analysis error: {e}")
        return results

    if props_df.empty:
        print("    No resolved props found")
        return results

    print(f"    Found {len(props_df)} props with results")

    # Calculate actual value and hit based on prop_type
    def get_actual(row):
        if row['prop_type'] == 'PTS':
            return row['pts']
        elif row['prop_type'] == 'AST':
            return row['ast']
        elif row['prop_type'] == 'REB':
            return row['reb']
        elif row['prop_type'] in ['PRA', 'PR', 'PA', 'RA']:
            return row['pra']
        return None

    def check_hit(row):
        actual = get_actual(row)
        if actual is None:
            return None
        if row['pick'] == 'OVER':
            return actual > row['line']
        else:
            return actual < row['line']

    props_df['actual'] = props_df.apply(get_actual, axis=1)
    props_df['hit'] = props_df.apply(check_hit, axis=1)
    props_df = props_df[props_df['hit'].notna()]

    # Analyze by stat type
    for stat in ['PTS', 'AST', 'REB', 'PRA']:
        stat_df = props_df[props_df['prop_type'] == stat]
        if len(stat_df) >= 10:
            hit_rate = stat_df['hit'].mean()
            p_val = binomial_test(int(stat_df['hit'].sum()), len(stat_df))
            results.append({
                'edge': f'{stat} Props (all edges)',
                'type': 'PLAYER_PROP',
                'n': len(stat_df),
                'hit_rate': hit_rate,
                'p_value': p_val,
                'profitable': hit_rate > 0.525
            })

    # Analyze by edge size
    for edge_min in [10, 15, 20]:
        edge_df = props_df[props_df['edge_pct'].abs() >= edge_min]
        if len(edge_df) >= 10:
            hit_rate = edge_df['hit'].mean()
            p_val = binomial_test(int(edge_df['hit'].sum()), len(edge_df))
            results.append({
                'edge': f'Props {edge_min}%+ Edge',
                'type': 'PLAYER_PROP',
                'n': len(edge_df),
                'hit_rate': hit_rate,
                'p_value': p_val,
                'profitable': hit_rate > 0.525
            })

    # Analyze OVER vs UNDER
    for direction in ['OVER', 'UNDER']:
        dir_df = props_df[props_df['pick'] == direction]
        if len(dir_df) >= 10:
            hit_rate = dir_df['hit'].mean()
            p_val = binomial_test(int(dir_df['hit'].sum()), len(dir_df))
            results.append({
                'edge': f'{direction} Props (all)',
                'type': 'PLAYER_PROP',
                'n': len(dir_df),
                'hit_rate': hit_rate,
                'p_value': p_val,
                'profitable': hit_rate > 0.525
            })

    # Overall props hit rate
    if len(props_df) >= 10:
        hit_rate = props_df['hit'].mean()
        p_val = binomial_test(int(props_df['hit'].sum()), len(props_df))
        results.append({
            'edge': 'All Props Combined',
            'type': 'PLAYER_PROP',
            'n': len(props_df),
            'hit_rate': hit_rate,
            'p_value': p_val,
            'profitable': hit_rate > 0.525
        })

    return results


def analyze_player_performance_vs_line(conn):
    """Analyze player performance relative to typical lines."""
    print("  Analyzing player vs typical lines...")

    results = []

    # Get player game logs with season averages
    try:
        df = pd.read_sql("""
            SELECT
                player_name,
                pts,
                ast,
                reb,
                pts + reb + ast as pra,
                opponent,
                DATE(game_date) as game_date
            FROM player_game_logs
            WHERE pts IS NOT NULL
            ORDER BY game_date DESC
        """, conn)
    except:
        return results

    if len(df) < 100:
        return results

    # Calculate player averages and standard deviations
    player_stats = df.groupby('player_name').agg({
        'pts': ['mean', 'std', 'count'],
        'ast': ['mean', 'std'],
        'reb': ['mean', 'std'],
        'pra': ['mean', 'std']
    }).reset_index()

    player_stats.columns = ['player_name', 'pts_avg', 'pts_std', 'games',
                           'ast_avg', 'ast_std', 'reb_avg', 'reb_std',
                           'pra_avg', 'pra_std']

    # Filter to players with enough games
    player_stats = player_stats[player_stats['games'] >= 10]

    if player_stats.empty:
        return results

    # Merge back to game logs
    df = df.merge(player_stats, on='player_name')

    # Edge: Players performing > 1 std above average (regression likely)
    df['pts_zscore'] = (df['pts'] - df['pts_avg']) / df['pts_std'].replace(0, 1)

    high_performers = df[df['pts_zscore'] > 1.5]
    if len(high_performers) >= 30:
        # Check if next game tends to regress
        results.append({
            'edge': 'High PTS z-score regression',
            'type': 'PLAYER_TREND',
            'n': len(high_performers),
            'hit_rate': 0.0,  # Placeholder - need more complex analysis
            'p_value': 1.0,
            'profitable': False
        })

    return results


# =============================================================================
# SECTION 3: CORRELATION ANALYSIS
# =============================================================================

def analyze_correlations(conn):
    """Analyze what bets correlate (avoid in parlays) vs independent (parlay-friendly)."""
    print("  Analyzing correlations...")

    results = {
        'correlated': [],  # AVOID in same parlay
        'independent': []  # GOOD for parlays
    }

    # Get game-level data
    df = pd.read_sql("""
        SELECT
            g.game_id,
            g.home_team,
            g.away_team,
            home_tb.pts as home_score,
            away_tb.pts as away_score,
            home_tb.pts + away_tb.pts as total_score,
            home_tb.pts - away_tb.pts as margin,
            COALESCE(b.espn_closing_spread, b.espn_opening_spread) as spread,
            COALESCE(b.espn_closing_total, b.espn_opening_total) as total_line,
            DATE(g.date_time_utc) as game_date
        FROM Games g
        JOIN Betting b ON g.game_id = b.game_id
        JOIN Teams home_t ON g.home_team = home_t.abbreviation
        JOIN Teams away_t ON g.away_team = away_t.abbreviation
        JOIN TeamBox home_tb ON g.game_id = home_tb.game_id AND home_t.team_id = home_tb.team_id
        JOIN TeamBox away_tb ON g.game_id = away_tb.game_id AND away_t.team_id = away_tb.team_id
        WHERE g.status_text = 'Final'
        AND home_tb.pts IS NOT NULL
        AND b.espn_closing_spread IS NOT NULL
        AND b.espn_closing_total IS NOT NULL
    """, conn)

    if len(df) < 50:
        return results

    # Calculate bet outcomes
    df['home_covers'] = df['margin'] > -df['spread']
    df['dog_covers'] = np.where(df['spread'] > 0, df['home_covers'], ~df['home_covers'])
    df['over_hits'] = df['total_score'] > df['total_line']
    df['under_hits'] = ~df['over_hits']

    # Correlation 1: Underdog covers vs Under hits (same game)
    both_dog_under = ((df['dog_covers']) & (df['under_hits'])).sum()
    expected = df['dog_covers'].mean() * df['under_hits'].mean() * len(df)
    if expected > 0:
        ratio = both_dog_under / expected
        results['correlated' if ratio > 1.1 else 'independent'].append({
            'pair': 'Underdog ATS + Under (same game)',
            'observed': both_dog_under,
            'expected': expected,
            'ratio': ratio,
            'correlation': 'POSITIVE' if ratio > 1.1 else 'NEGATIVE' if ratio < 0.9 else 'NEUTRAL'
        })

    # Correlation 2: Home cover vs Over hits (same game)
    both_home_over = ((df['home_covers']) & (df['over_hits'])).sum()
    expected = df['home_covers'].mean() * df['over_hits'].mean() * len(df)
    if expected > 0:
        ratio = both_home_over / expected
        results['correlated' if ratio > 1.1 else 'independent'].append({
            'pair': 'Home ATS + Over (same game)',
            'observed': both_home_over,
            'expected': expected,
            'ratio': ratio,
            'correlation': 'POSITIVE' if ratio > 1.1 else 'NEGATIVE' if ratio < 0.9 else 'NEUTRAL'
        })

    # Check if big underdogs + under correlate more
    big_dog_games = df[df['spread'].abs() >= 7]
    if len(big_dog_games) >= 30:
        both = ((big_dog_games['dog_covers']) & (big_dog_games['under_hits'])).sum()
        exp = big_dog_games['dog_covers'].mean() * big_dog_games['under_hits'].mean() * len(big_dog_games)
        if exp > 0:
            ratio = both / exp
            results['correlated' if ratio > 1.1 else 'independent'].append({
                'pair': 'Big Dog (+7) + Under (same game)',
                'observed': both,
                'expected': exp,
                'ratio': ratio,
                'correlation': 'POSITIVE' if ratio > 1.1 else 'NEGATIVE' if ratio < 0.9 else 'NEUTRAL'
            })

    return results


# =============================================================================
# SECTION 4: PARLAY SIMULATION
# =============================================================================

def simulate_parlays(conn):
    """Simulate historical 5-leg parlays using validated edges."""
    print("  Simulating parlays...")

    results = {
        'strategies': [],
        'best_strategy': None
    }

    # Get historical games with all data
    df = pd.read_sql("""
        SELECT
            g.game_id,
            g.home_team,
            g.away_team,
            home_tb.pts as home_score,
            away_tb.pts as away_score,
            home_tb.pts + away_tb.pts as total_score,
            home_tb.pts - away_tb.pts as margin,
            COALESCE(b.espn_closing_spread, b.espn_opening_spread) as spread,
            COALESCE(b.espn_closing_total, b.espn_opening_total) as total_line,
            DATE(g.date_time_utc) as game_date
        FROM Games g
        JOIN Betting b ON g.game_id = b.game_id
        JOIN Teams home_t ON g.home_team = home_t.abbreviation
        JOIN Teams away_t ON g.away_team = away_t.abbreviation
        JOIN TeamBox home_tb ON g.game_id = home_tb.game_id AND home_t.team_id = home_tb.team_id
        JOIN TeamBox away_tb ON g.game_id = away_tb.game_id AND away_t.team_id = away_tb.team_id
        WHERE g.status_text = 'Final'
        AND home_tb.pts IS NOT NULL
        AND b.espn_closing_spread IS NOT NULL
        ORDER BY g.date_time_utc
    """, conn)

    if len(df) < 100:
        return results

    # Calculate outcomes
    df['dog_covers'] = np.where(df['spread'] > 0,
                                 df['margin'] > -df['spread'],
                                 df['margin'] < -df['spread'])
    df['under_hits'] = df['total_score'] < df['total_line']
    df['abs_spread'] = df['spread'].abs()

    # Group by date for daily parlays
    dates = df['game_date'].unique()

    # Strategy 1: All big dogs (+7)
    strategy1_results = []
    for game_date in dates:
        day_games = df[df['game_date'] == game_date]
        big_dogs = day_games[day_games['abs_spread'] >= 7]
        if len(big_dogs) >= 3:
            # Take up to 5 legs
            legs = big_dogs.head(5)
            all_hit = legs['dog_covers'].all()
            strategy1_results.append({
                'date': game_date,
                'legs': len(legs),
                'hit': all_hit
            })

    if strategy1_results:
        hits = sum(1 for r in strategy1_results if r['hit'])
        total = len(strategy1_results)
        avg_legs = np.mean([r['legs'] for r in strategy1_results])
        payout = 1.91 ** avg_legs
        roi = (hits / total * payout - 1) * 100 if total > 0 else 0
        results['strategies'].append({
            'name': 'All Big Dogs (+7)',
            'parlays': total,
            'hits': hits,
            'hit_rate': hits / total if total > 0 else 0,
            'avg_legs': avg_legs,
            'payout': payout,
            'roi': roi
        })

    # Strategy 2: Mixed - dogs + unders (different games)
    strategy2_results = []
    for game_date in dates:
        day_games = df[df['game_date'] == game_date]
        if len(day_games) < 5:
            continue

        # Select legs from different games
        legs_selected = []
        games_used = set()

        # First, big dogs
        big_dogs = day_games[day_games['abs_spread'] >= 7]
        for _, game in big_dogs.iterrows():
            if game['game_id'] not in games_used and len(legs_selected) < 3:
                legs_selected.append({'type': 'dog', 'hit': game['dog_covers'], 'game_id': game['game_id']})
                games_used.add(game['game_id'])

        # Then, unders from different games
        low_totals = day_games[day_games['total_line'] < 230]
        for _, game in low_totals.iterrows():
            if game['game_id'] not in games_used and len(legs_selected) < 5:
                legs_selected.append({'type': 'under', 'hit': game['under_hits'], 'game_id': game['game_id']})
                games_used.add(game['game_id'])

        if len(legs_selected) >= 3:
            all_hit = all(leg['hit'] for leg in legs_selected)
            strategy2_results.append({
                'date': game_date,
                'legs': len(legs_selected),
                'hit': all_hit
            })

    if strategy2_results:
        hits = sum(1 for r in strategy2_results if r['hit'])
        total = len(strategy2_results)
        avg_legs = np.mean([r['legs'] for r in strategy2_results])
        payout = 1.91 ** avg_legs
        roi = (hits / total * payout - 1) * 100 if total > 0 else 0
        results['strategies'].append({
            'name': 'Mixed Dogs + Unders (diff games)',
            'parlays': total,
            'hits': hits,
            'hit_rate': hits / total if total > 0 else 0,
            'avg_legs': avg_legs,
            'payout': payout,
            'roi': roi
        })

    # Strategy 3: Conservative - only 3-leg parlays with best edges
    strategy3_results = []
    for game_date in dates:
        day_games = df[df['game_date'] == game_date]
        best_dogs = day_games[day_games['abs_spread'] >= 8]  # Stricter

        if len(best_dogs) >= 3:
            legs = best_dogs.head(3)
            all_hit = legs['dog_covers'].all()
            strategy3_results.append({
                'date': game_date,
                'legs': 3,
                'hit': all_hit
            })

    if strategy3_results:
        hits = sum(1 for r in strategy3_results if r['hit'])
        total = len(strategy3_results)
        payout = 1.91 ** 3
        roi = (hits / total * payout - 1) * 100 if total > 0 else 0
        results['strategies'].append({
            'name': 'Conservative 3-Leg (+8 dogs)',
            'parlays': total,
            'hits': hits,
            'hit_rate': hits / total if total > 0 else 0,
            'avg_legs': 3,
            'payout': payout,
            'roi': roi
        })

    # Find best strategy
    if results['strategies']:
        results['best_strategy'] = max(results['strategies'], key=lambda x: x['roi'])

    return results


# =============================================================================
# SECTION 5: OPTIMAL PARLAY CONSTRUCTION
# =============================================================================

def derive_optimal_parlay_rules(all_results):
    """Based on all analysis, derive optimal parlay construction rules."""

    rules = {
        'leg_priorities': [],
        'avoid': [],
        'max_legs': 5,
        'min_combined_prob': 0.05,
        'bankroll_pct': 0.01
    }

    # Sort edges by profitability and significance
    profitable_edges = []
    for section in ['team_totals', 'spreads', 'player_props']:
        if section in all_results:
            for edge in all_results[section]:
                if edge.get('profitable') and edge.get('p_value', 1) < 0.1:
                    profitable_edges.append({
                        'name': edge['edge'],
                        'type': edge['type'],
                        'hit_rate': edge['hit_rate'],
                        'n': edge['n'],
                        'p_value': edge['p_value']
                    })

    # Sort by hit rate
    profitable_edges.sort(key=lambda x: x['hit_rate'], reverse=True)
    rules['leg_priorities'] = profitable_edges[:10]

    # Identify correlated bets to avoid
    if 'correlations' in all_results:
        for corr in all_results['correlations'].get('correlated', []):
            if corr['ratio'] > 1.15:
                rules['avoid'].append(corr['pair'])

    return rules


# =============================================================================
# MAIN OUTPUT
# =============================================================================

def generate_report(all_results):
    """Generate markdown report."""

    lines = [
        "# AXIOM Comprehensive Edge Research",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "---",
        "",
        "## Executive Summary",
        "",
    ]

    # Count profitable edges
    profitable_count = 0
    for section in ['team_totals', 'spreads', 'player_props']:
        if section in all_results:
            profitable_count += sum(1 for e in all_results[section] if e.get('profitable'))

    lines.append(f"**Profitable edges found:** {profitable_count}")

    if all_results.get('parlay_sim', {}).get('best_strategy'):
        best = all_results['parlay_sim']['best_strategy']
        lines.append(f"**Best parlay strategy:** {best['name']} ({best['roi']:+.0f}% ROI)")

    lines.append("")
    lines.append("---")
    lines.append("")

    # Section 1: Team-Level Edges
    lines.append("## 1. Team-Level Edges")
    lines.append("")
    lines.append("### Team Totals")
    lines.append("")
    lines.append("| Edge | N | Hit Rate | p-value | Profitable |")
    lines.append("|------|---|----------|---------|------------|")

    for edge in all_results.get('team_totals', []):
        profitable_mark = "YES" if edge['profitable'] else "no"
        lines.append(f"| {edge['edge']} | {edge['n']} | {edge['hit_rate']*100:.1f}% | {edge['p_value']:.4f} | {profitable_mark} |")

    lines.append("")
    lines.append("### Spread Edges")
    lines.append("")
    lines.append("| Edge | N | Hit Rate | p-value | Profitable |")
    lines.append("|------|---|----------|---------|------------|")

    for edge in all_results.get('spreads', []):
        profitable_mark = "YES" if edge['profitable'] else "no"
        lines.append(f"| {edge['edge']} | {edge['n']} | {edge['hit_rate']*100:.1f}% | {edge['p_value']:.4f} | {profitable_mark} |")

    lines.append("")
    lines.append("---")
    lines.append("")

    # Section 2: Player Props
    lines.append("## 2. Player Prop Edges")
    lines.append("")
    lines.append("| Edge | N | Hit Rate | p-value | Profitable |")
    lines.append("|------|---|----------|---------|------------|")

    for edge in all_results.get('player_props', []):
        profitable_mark = "YES" if edge['profitable'] else "no"
        lines.append(f"| {edge['edge']} | {edge['n']} | {edge['hit_rate']*100:.1f}% | {edge['p_value']:.4f} | {profitable_mark} |")

    lines.append("")
    lines.append("---")
    lines.append("")

    # Section 3: Correlations
    lines.append("## 3. Correlation Analysis")
    lines.append("")
    lines.append("### Correlated Bets (AVOID in same parlay)")
    lines.append("")

    corr_data = all_results.get('correlations', {})
    for item in corr_data.get('correlated', []):
        lines.append(f"- **{item['pair']}**: ratio {item['ratio']:.2f} ({item['correlation']})")

    if not corr_data.get('correlated'):
        lines.append("- None found with strong correlation")

    lines.append("")
    lines.append("### Independent Bets (GOOD for parlays)")
    lines.append("")

    for item in corr_data.get('independent', []):
        lines.append(f"- **{item['pair']}**: ratio {item['ratio']:.2f} ({item['correlation']})")

    if not corr_data.get('independent'):
        lines.append("- See correlated section")

    lines.append("")
    lines.append("---")
    lines.append("")

    # Section 4: Parlay Simulation
    lines.append("## 4. Historical Parlay Simulation")
    lines.append("")
    lines.append("| Strategy | Parlays | Hits | Hit Rate | Avg Legs | Payout | ROI |")
    lines.append("|----------|---------|------|----------|----------|--------|-----|")

    for strat in all_results.get('parlay_sim', {}).get('strategies', []):
        lines.append(f"| {strat['name']} | {strat['parlays']} | {strat['hits']} | {strat['hit_rate']*100:.1f}% | {strat['avg_legs']:.1f} | {strat['payout']:.1f}x | {strat['roi']:+.0f}% |")

    lines.append("")
    lines.append("---")
    lines.append("")

    # Section 5: Optimal Parlay Rules
    lines.append("## 5. Optimal Parlay Construction Rules")
    lines.append("")

    rules = all_results.get('optimal_rules', {})

    lines.append("### Leg Priority (highest hit rate first)")
    lines.append("")

    for i, leg in enumerate(rules.get('leg_priorities', [])[:5], 1):
        lines.append(f"{i}. **{leg['name']}** ({leg['hit_rate']*100:.1f}%, n={leg['n']}, p={leg['p_value']:.4f})")

    lines.append("")
    lines.append("### Avoid in Same Parlay")
    lines.append("")

    for avoid in rules.get('avoid', []):
        lines.append(f"- {avoid}")

    if not rules.get('avoid'):
        lines.append("- No strong correlations found to avoid")

    lines.append("")
    lines.append("### Recommended Settings")
    lines.append("")
    lines.append(f"- **Max legs:** {rules.get('max_legs', 5)}")
    lines.append(f"- **Min combined probability:** {rules.get('min_combined_prob', 0.05)*100:.0f}%")
    lines.append(f"- **Bankroll per parlay:** {rules.get('bankroll_pct', 0.01)*100:.1f}%")

    lines.append("")
    lines.append("---")
    lines.append("")

    # Section 6: Actionable Daily Strategy
    lines.append("## 6. Actionable Daily Parlay Strategy")
    lines.append("")
    lines.append("```")
    lines.append("STEP 1: Identify qualifying legs")
    lines.append("  - Big underdogs (+7 or more)")
    lines.append("  - Low total games (<230) for UNDER")
    lines.append("  - Player props with 15%+ edge (PRA, AST)")
    lines.append("")
    lines.append("STEP 2: Select 5 legs from DIFFERENT games")
    lines.append("  - Max 2 legs from same game")
    lines.append("  - Prioritize by hit rate")
    lines.append("")
    lines.append("STEP 3: Calculate combined probability")
    lines.append("  - Multiply individual probabilities")
    lines.append("  - Must exceed 5% to bet")
    lines.append("")
    lines.append("STEP 4: Bet 0.5-1% of bankroll")
    lines.append("  - Higher % if combined > 8%")
    lines.append("  - Lower % if combined 5-8%")
    lines.append("```")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 7. Expected Long-Term Results")
    lines.append("")
    lines.append("| Combined Hit | Payout | Per 100 Parlays | Net Units | ROI |")
    lines.append("|--------------|--------|-----------------|-----------|-----|")
    lines.append("| 5% | 25x | 5 wins × 25 = 125 | +25 | +25% |")
    lines.append("| 8% | 25x | 8 wins × 25 = 200 | +100 | +100% |")
    lines.append("| 10% | 25x | 10 wins × 25 = 250 | +150 | +150% |")
    lines.append("")
    lines.append("**Variance Warning:** Expect 20+ losing streaks. This is normal.")
    lines.append("")

    return "\n".join(lines)


def main():
    print("=" * 60)
    print("AXIOM COMPREHENSIVE EDGE RESEARCH")
    print("=" * 60)
    print("\nRunning all analysis... (this takes ~30 seconds)")

    conn = get_connection()

    all_results = {}

    # Run all analyses
    print("\n[1/5] Team-level edges...")
    all_results['team_totals'] = analyze_team_totals(conn)
    all_results['spreads'] = analyze_spread_edges(conn)

    print("[2/5] Player prop edges...")
    all_results['player_props'] = analyze_player_props(conn)
    all_results['player_trends'] = analyze_player_performance_vs_line(conn)

    print("[3/5] Correlation analysis...")
    all_results['correlations'] = analyze_correlations(conn)

    print("[4/5] Parlay simulation...")
    all_results['parlay_sim'] = simulate_parlays(conn)

    print("[5/5] Deriving optimal rules...")
    all_results['optimal_rules'] = derive_optimal_parlay_rules(all_results)

    conn.close()

    # Generate report
    report = generate_report(all_results)

    # Save to file
    OUTPUT_FILE.parent.mkdir(exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        f.write(report)

    print(f"\n[OK] Report saved to: {OUTPUT_FILE}")
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    # Quick summary
    profitable = []
    for section in ['team_totals', 'spreads', 'player_props']:
        for edge in all_results.get(section, []):
            if edge.get('profitable') and edge.get('p_value', 1) < 0.1:
                profitable.append(edge)

    profitable.sort(key=lambda x: x['hit_rate'], reverse=True)

    print(f"\nTop profitable edges (p < 0.10):")
    for edge in profitable[:5]:
        print(f"  {edge['hit_rate']*100:.1f}% - {edge['edge']} (n={edge['n']})")

    if all_results.get('parlay_sim', {}).get('best_strategy'):
        best = all_results['parlay_sim']['best_strategy']
        print(f"\nBest parlay strategy: {best['name']}")
        print(f"  Hit rate: {best['hit_rate']*100:.1f}%")
        print(f"  ROI: {best['roi']:+.0f}%")

    print(f"\nFull report: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
