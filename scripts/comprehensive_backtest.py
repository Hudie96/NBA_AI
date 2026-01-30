"""
AXIOM Comprehensive Backtest

Runs a full season backtest across all bet types and outputs detailed reports.

Usage:
    python scripts/comprehensive_backtest.py
    python scripts/comprehensive_backtest.py --output results.csv
"""
import argparse
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]


def safe_print(text):
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))


# =============================================================================
# SPREAD BACKTEST
# =============================================================================

def backtest_spreads(conn):
    """Backtest spread predictions against actual results."""
    safe_print("\n" + "=" * 80)
    safe_print("SPREAD BACKTEST")
    safe_print("=" * 80)

    # Get all games with betting lines and results
    df = pd.read_sql("""
        SELECT
            g.game_id,
            DATE(g.date_time_utc) as game_date,
            g.home_team,
            g.away_team,
            COALESCE(b.espn_closing_spread, b.espn_current_spread, b.covers_closing_spread) as spread_home,
            COALESCE(b.espn_closing_total, b.espn_current_total, b.covers_closing_total) as total,
            tb_home.pts as home_score,
            tb_away.pts as away_score,
            f_home.is_b2b as home_b2b,
            f_away.is_b2b as away_b2b,
            f_home.days_rest as home_rest,
            f_away.days_rest as away_rest
        FROM Games g
        JOIN Betting b ON g.game_id = b.game_id
        JOIN Teams t_home ON g.home_team = t_home.abbreviation
        JOIN Teams t_away ON g.away_team = t_away.abbreviation
        JOIN TeamBox tb_home ON g.game_id = tb_home.game_id AND t_home.team_id = tb_home.team_id
        JOIN TeamBox tb_away ON g.game_id = tb_away.game_id AND t_away.team_id = tb_away.team_id
        LEFT JOIN TeamFatiguePatterns f_home ON g.game_id = f_home.game_id AND g.home_team = f_home.team
        LEFT JOIN TeamFatiguePatterns f_away ON g.game_id = f_away.game_id AND g.away_team = f_away.team
        WHERE g.season = '2025-2026'
          AND (b.espn_closing_spread IS NOT NULL OR b.espn_current_spread IS NOT NULL)
        ORDER BY g.date_time_utc
    """, conn)

    if df.empty:
        safe_print("No spread data available")
        return None

    # Calculate actual margin and ATS result
    df['actual_margin'] = df['home_score'] - df['away_score']
    df['spread_away'] = -df['spread_home']  # Away spread is inverse of home
    df['home_covered'] = df['actual_margin'] + df['spread_home'] > 0
    df['away_covered'] = df['actual_margin'] + df['spread_away'] < 0
    df['total_points'] = df['home_score'] + df['away_score']
    df['over_hit'] = df['total_points'] > df['total']
    df['under_hit'] = df['total_points'] < df['total']

    # Categorize spreads
    df['spread_size'] = df['spread_home'].abs()
    df['spread_category'] = pd.cut(
        df['spread_size'],
        bins=[0, 3, 7, 10, 100],
        labels=['Small (0-3)', 'Medium (3-7)', 'Large (7-10)', 'Blowout (10+)']
    )

    # B2B analysis
    df['home_on_b2b'] = df['home_b2b'] == 1
    df['away_on_b2b'] = df['away_b2b'] == 1

    results = []

    # Overall ATS
    home_ats = df['home_covered'].mean() * 100
    away_ats = df['away_covered'].mean() * 100
    results.append({
        'Category': 'OVERALL',
        'Subcategory': 'Home Teams ATS',
        'Bets': len(df),
        'Wins': df['home_covered'].sum(),
        'Win%': round(home_ats, 1),
        'Edge': round(home_ats - 50, 1)
    })
    results.append({
        'Category': 'OVERALL',
        'Subcategory': 'Away Teams ATS',
        'Bets': len(df),
        'Wins': df['away_covered'].sum(),
        'Win%': round(away_ats, 1),
        'Edge': round(away_ats - 50, 1)
    })

    # By spread size
    for cat in df['spread_category'].dropna().unique():
        subset = df[df['spread_category'] == cat]
        if len(subset) >= 10:
            fav_covered = (subset['spread_home'] < 0).sum()
            fav_total = (subset['spread_home'] < 0).sum() + (subset['spread_away'] < 0).sum()

            # Dogs ATS (positive spread)
            dogs = subset[subset['spread_home'] > 0]
            if len(dogs) >= 5:
                dog_win = dogs['home_covered'].mean() * 100
                results.append({
                    'Category': 'SPREAD SIZE',
                    'Subcategory': f'{cat} - Dogs',
                    'Bets': len(dogs),
                    'Wins': dogs['home_covered'].sum(),
                    'Win%': round(dog_win, 1),
                    'Edge': round(dog_win - 50, 1)
                })

            # Favorites ATS
            favs = subset[subset['spread_home'] < 0]
            if len(favs) >= 5:
                fav_win = favs['home_covered'].mean() * 100
                results.append({
                    'Category': 'SPREAD SIZE',
                    'Subcategory': f'{cat} - Favs',
                    'Bets': len(favs),
                    'Wins': favs['home_covered'].sum(),
                    'Win%': round(fav_win, 1),
                    'Edge': round(fav_win - 50, 1)
                })

    # B2B Analysis - Fade B2B teams
    b2b_home = df[df['home_on_b2b']]
    if len(b2b_home) >= 10:
        # Fade home team on B2B = bet away
        fade_win = b2b_home['away_covered'].mean() * 100
        results.append({
            'Category': 'B2B FADE',
            'Subcategory': 'Fade Home on B2B',
            'Bets': len(b2b_home),
            'Wins': b2b_home['away_covered'].sum(),
            'Win%': round(fade_win, 1),
            'Edge': round(fade_win - 50, 1)
        })

    b2b_away = df[df['away_on_b2b']]
    if len(b2b_away) >= 10:
        # Fade away team on B2B = bet home
        fade_win = b2b_away['home_covered'].mean() * 100
        results.append({
            'Category': 'B2B FADE',
            'Subcategory': 'Fade Away on B2B',
            'Bets': len(b2b_away),
            'Wins': b2b_away['home_covered'].sum(),
            'Win%': round(fade_win, 1),
            'Edge': round(fade_win - 50, 1)
        })

    # Rest advantage
    df['rest_diff'] = df['home_rest'].fillna(1) - df['away_rest'].fillna(1)
    rest_adv = df[df['rest_diff'] >= 2]
    if len(rest_adv) >= 10:
        win_pct = rest_adv['home_covered'].mean() * 100
        results.append({
            'Category': 'REST ADVANTAGE',
            'Subcategory': 'Home +2 Rest Days',
            'Bets': len(rest_adv),
            'Wins': rest_adv['home_covered'].sum(),
            'Win%': round(win_pct, 1),
            'Edge': round(win_pct - 50, 1)
        })

    # Totals
    over_pct = df['over_hit'].mean() * 100
    under_pct = df['under_hit'].mean() * 100
    results.append({
        'Category': 'TOTALS',
        'Subcategory': 'Over',
        'Bets': len(df),
        'Wins': df['over_hit'].sum(),
        'Win%': round(over_pct, 1),
        'Edge': round(over_pct - 50, 1)
    })
    results.append({
        'Category': 'TOTALS',
        'Subcategory': 'Under',
        'Bets': len(df),
        'Wins': df['under_hit'].sum(),
        'Win%': round(under_pct, 1),
        'Edge': round(under_pct - 50, 1)
    })

    return pd.DataFrame(results), df


# =============================================================================
# PROPS BACKTEST
# =============================================================================

def backtest_props(conn):
    """Backtest player prop projections against actual results."""
    safe_print("\n" + "=" * 80)
    safe_print("PLAYER PROPS BACKTEST")
    safe_print("=" * 80)

    # Get player box scores with game info
    df = pd.read_sql("""
        SELECT
            pb.player_name,
            pb.game_id,
            DATE(g.date_time_utc) as game_date,
            pb.pts,
            pb.reb,
            pb.ast,
            pb.fg3m as threes_made,
            pb.stl,
            pb.blk,
            pb.min,
            t.abbreviation as team,
            CASE WHEN g.home_team = t.abbreviation THEN g.away_team ELSE g.home_team END as opponent
        FROM PlayerBox pb
        JOIN Games g ON pb.game_id = g.game_id
        JOIN Teams t ON pb.team_id = t.team_id
        WHERE g.season = '2025-2026'
          AND pb.min >= 15
        ORDER BY g.date_time_utc, pb.player_name
    """, conn)

    if df.empty:
        safe_print("No props data available")
        return None

    # Add combo stats
    df['pra'] = df['pts'] + df['reb'] + df['ast']
    df['pr'] = df['pts'] + df['reb']
    df['pa'] = df['pts'] + df['ast']
    df['ra'] = df['reb'] + df['ast']

    # Get player averages (rolling window simulation)
    results = []

    # Group by player and calculate running stats
    stat_configs = [
        ('PTS', 'pts'),
        ('REB', 'reb'),
        ('AST', 'ast'),
        ('3PM', 'threes_made'),
        ('PRA', 'pra'),
        ('PR', 'pr'),
        ('PA', 'pa'),
        ('RA', 'ra'),
    ]

    all_bets = []

    for player_name, player_df in df.groupby('player_name'):
        if len(player_df) < 15:  # Need enough games
            continue

        player_df = player_df.sort_values('game_date').reset_index(drop=True)

        for stat_name, stat_col in stat_configs:
            # Calculate rolling average (last 10 games) as "line"
            player_df[f'{stat_col}_avg'] = player_df[stat_col].rolling(10, min_periods=5).mean().shift(1)

            for i in range(10, len(player_df)):
                avg = player_df.iloc[i][f'{stat_col}_avg']
                actual = player_df.iloc[i][stat_col]

                if pd.isna(avg) or avg < 1:
                    continue

                # Use season avg as line
                line = avg

                # Calculate edge
                edge_pct = 0  # No edge when using own average as line

                # Simulate projection with slight variance
                # In real scenario, we'd have DVP adjustments etc.
                last_5_avg = player_df[stat_col].iloc[max(0,i-5):i].mean()
                projection = last_5_avg * 0.6 + avg * 0.4

                edge = projection - line
                edge_pct = (edge / line * 100) if line > 0 else 0

                pick = 'OVER' if edge > 0 else 'UNDER'
                hit = (actual > line) if pick == 'OVER' else (actual < line)

                # Categorize edge
                abs_edge = abs(edge_pct)
                if abs_edge >= 15:
                    edge_cat = 'HIGH (15%+)'
                elif abs_edge >= 10:
                    edge_cat = 'MEDIUM (10-15%)'
                elif abs_edge >= 5:
                    edge_cat = 'LOW (5-10%)'
                else:
                    edge_cat = 'NONE (<5%)'

                all_bets.append({
                    'player': player_name,
                    'game_date': player_df.iloc[i]['game_date'],
                    'stat': stat_name,
                    'line': round(line, 1),
                    'projection': round(projection, 1),
                    'actual': actual,
                    'pick': pick,
                    'edge_pct': round(edge_pct, 1),
                    'edge_cat': edge_cat,
                    'hit': hit
                })

    bets_df = pd.DataFrame(all_bets)

    if bets_df.empty:
        safe_print("No prop bets generated")
        return None

    # Aggregate results by stat
    for stat in bets_df['stat'].unique():
        stat_df = bets_df[bets_df['stat'] == stat]
        win_pct = stat_df['hit'].mean() * 100
        results.append({
            'Category': 'BY STAT',
            'Subcategory': stat,
            'Bets': len(stat_df),
            'Wins': stat_df['hit'].sum(),
            'Win%': round(win_pct, 1),
            'Edge': round(win_pct - 50, 1)
        })

    # By edge category
    for edge_cat in ['HIGH (15%+)', 'MEDIUM (10-15%)', 'LOW (5-10%)']:
        edge_df = bets_df[bets_df['edge_cat'] == edge_cat]
        if len(edge_df) >= 20:
            win_pct = edge_df['hit'].mean() * 100
            results.append({
                'Category': 'BY EDGE SIZE',
                'Subcategory': edge_cat,
                'Bets': len(edge_df),
                'Wins': edge_df['hit'].sum(),
                'Win%': round(win_pct, 1),
                'Edge': round(win_pct - 50, 1)
            })

    # Combo vs Individual
    combo_stats = ['PRA', 'PR', 'PA', 'RA']
    indiv_stats = ['PTS', 'REB', 'AST', '3PM']

    combo_df = bets_df[bets_df['stat'].isin(combo_stats)]
    indiv_df = bets_df[bets_df['stat'].isin(indiv_stats)]

    if len(combo_df) >= 50:
        win_pct = combo_df['hit'].mean() * 100
        results.append({
            'Category': 'STAT TYPE',
            'Subcategory': 'Combo Props (PRA/PR/PA/RA)',
            'Bets': len(combo_df),
            'Wins': combo_df['hit'].sum(),
            'Win%': round(win_pct, 1),
            'Edge': round(win_pct - 50, 1)
        })

    if len(indiv_df) >= 50:
        win_pct = indiv_df['hit'].mean() * 100
        results.append({
            'Category': 'STAT TYPE',
            'Subcategory': 'Individual Props (PTS/REB/AST/3PM)',
            'Bets': len(indiv_df),
            'Wins': indiv_df['hit'].sum(),
            'Win%': round(win_pct, 1),
            'Edge': round(win_pct - 50, 1)
        })

    # By pick direction
    for pick in ['OVER', 'UNDER']:
        pick_df = bets_df[bets_df['pick'] == pick]
        if len(pick_df) >= 50:
            win_pct = pick_df['hit'].mean() * 100
            results.append({
                'Category': 'PICK DIRECTION',
                'Subcategory': pick,
                'Bets': len(pick_df),
                'Wins': pick_df['hit'].sum(),
                'Win%': round(win_pct, 1),
                'Edge': round(win_pct - 50, 1)
            })

    # High edge + combo (best performers from prior backtest)
    best_df = bets_df[
        (bets_df['stat'].isin(combo_stats)) &
        (bets_df['edge_cat'] == 'HIGH (15%+)')
    ]
    if len(best_df) >= 20:
        win_pct = best_df['hit'].mean() * 100
        results.append({
            'Category': 'BEST STRATEGY',
            'Subcategory': 'Combo + 15%+ Edge',
            'Bets': len(best_df),
            'Wins': best_df['hit'].sum(),
            'Win%': round(win_pct, 1),
            'Edge': round(win_pct - 50, 1)
        })

    return pd.DataFrame(results), bets_df


# =============================================================================
# TEAM ATS BY TEAM
# =============================================================================

def backtest_by_team(conn):
    """Get ATS record by team."""
    safe_print("\n" + "=" * 80)
    safe_print("ATS BY TEAM")
    safe_print("=" * 80)

    df = pd.read_sql("""
        SELECT
            team_abbrev,
            ats_wins,
            ats_losses,
            ats_pushes,
            ats_win_pct,
            avg_margin,
            avg_spread
        FROM TeamATSStats
        ORDER BY ats_win_pct DESC
    """, conn)

    if df.empty:
        return None

    results = []
    for _, row in df.iterrows():
        total = row['ats_wins'] + row['ats_losses'] + row['ats_pushes']
        results.append({
            'Category': 'TEAM ATS',
            'Subcategory': row['team_abbrev'],
            'Bets': total,
            'Wins': row['ats_wins'],
            'Win%': round(row['ats_win_pct'] * 100, 1) if row['ats_win_pct'] else 0,
            'Edge': round((row['ats_win_pct'] - 0.5) * 100, 1) if row['ats_win_pct'] else 0
        })

    return pd.DataFrame(results)


# =============================================================================
# MAIN REPORT
# =============================================================================

def generate_report(conn, output_path=None):
    """Generate comprehensive backtest report."""
    safe_print("=" * 80)
    safe_print("AXIOM COMPREHENSIVE BACKTEST REPORT")
    safe_print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    safe_print("=" * 80)

    all_results = []

    # Spread backtest
    spread_results, spread_df = backtest_spreads(conn)
    if spread_results is not None:
        all_results.append(spread_results)
        safe_print(f"\nSpread games analyzed: {len(spread_df)}")

    # Props backtest
    props_results, props_df = backtest_props(conn)
    if props_results is not None:
        all_results.append(props_results)
        safe_print(f"Prop bets analyzed: {len(props_df)}")

    # Team ATS
    team_results = backtest_by_team(conn)
    if team_results is not None:
        all_results.append(team_results)

    # Combine all results
    if all_results:
        combined = pd.concat(all_results, ignore_index=True)

        # Print table
        safe_print("\n" + "=" * 80)
        safe_print("FULL RESULTS TABLE")
        safe_print("=" * 80)

        # Group by category for cleaner output
        for category in combined['Category'].unique():
            cat_df = combined[combined['Category'] == category]
            safe_print(f"\n### {category}")
            safe_print("-" * 70)
            safe_print(f"{'Subcategory':<35} {'Bets':>8} {'Wins':>8} {'Win%':>8} {'Edge':>8}")
            safe_print("-" * 70)

            for _, row in cat_df.iterrows():
                edge_str = f"+{row['Edge']}" if row['Edge'] > 0 else str(row['Edge'])
                safe_print(f"{row['Subcategory']:<35} {row['Bets']:>8} {row['Wins']:>8} {row['Win%']:>7.1f}% {edge_str:>7}%")

        # Summary stats
        safe_print("\n" + "=" * 80)
        safe_print("TOP EDGES (>5% Edge, 50+ Bets)")
        safe_print("=" * 80)

        top_edges = combined[(combined['Edge'] > 5) & (combined['Bets'] >= 50)].sort_values('Edge', ascending=False)
        if not top_edges.empty:
            safe_print(f"\n{'Category':<20} {'Subcategory':<30} {'Win%':>8} {'Edge':>8} {'Bets':>8}")
            safe_print("-" * 80)
            for _, row in top_edges.iterrows():
                safe_print(f"{row['Category']:<20} {row['Subcategory']:<30} {row['Win%']:>7.1f}% +{row['Edge']:>6.1f}% {row['Bets']:>7}")

        # Output to CSV if requested
        if output_path:
            combined.to_csv(output_path, index=False)
            safe_print(f"\nResults saved to: {output_path}")

            # Also save detailed data
            if spread_df is not None:
                spread_df.to_csv(output_path.replace('.csv', '_spreads_detail.csv'), index=False)
            if props_df is not None:
                props_df.to_csv(output_path.replace('.csv', '_props_detail.csv'), index=False)
                safe_print(f"Detailed spread data: {output_path.replace('.csv', '_spreads_detail.csv')}")
                safe_print(f"Detailed props data: {output_path.replace('.csv', '_props_detail.csv')}")

        return combined

    return None


def main():
    parser = argparse.ArgumentParser(description="AXIOM Comprehensive Backtest")
    parser.add_argument("--output", "-o", type=str, default="outputs/backtest_results.csv",
                        help="Output CSV path")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    generate_report(conn, args.output)
    conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
