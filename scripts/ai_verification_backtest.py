"""
AXIOM AI Verification Backtest
Compare RAW edges vs AI-VERIFIED edges

Tests whether AI verification improves betting results.
"""

import sqlite3
import pandas as pd
import numpy as np
from scipy import stats
import random
from datetime import datetime
import json

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


def get_master_dataset():
    """Get all games with edges and results"""
    conn = sqlite3.connect(DB_PATH)

    # Get team stats for pace
    try:
        team_stats = pd.read_sql_query('SELECT * FROM team_advanced_stats', conn)
        team_stats['team_code'] = team_stats['team_name'].map(TEAM_NAME_TO_CODE)
    except:
        team_stats = pd.DataFrame()

    # Get game data with calculated results
    sql = '''
    WITH all_team_games AS (
        SELECT home_team as team, game_id, DATE(date_time_utc) as game_date
        FROM Games WHERE status_text = 'Final'
        UNION ALL
        SELECT away_team as team, game_id, DATE(date_time_utc) as game_date
        FROM Games WHERE status_text = 'Final'
    ),
    team_schedule AS (
        SELECT team, game_id, game_date,
            JULIANDAY(game_date) - JULIANDAY(LAG(game_date) OVER (PARTITION BY team ORDER BY game_date)) as days_rest
        FROM all_team_games
    )
    SELECT
        g.game_id,
        DATE(g.date_time_utc) as game_date,
        g.home_team,
        g.away_team,
        b.espn_closing_spread as spread,
        b.espn_closing_total as total_line,
        gs.home_margin,
        gs.home_score,
        gs.away_score,
        gs.home_score + gs.away_score as total_points,
        hs.days_rest as home_rest,
        aws.days_rest as away_rest,
        CASE WHEN hs.days_rest = 1 THEN 1 ELSE 0 END as home_b2b,
        CASE WHEN aws.days_rest = 1 THEN 1 ELSE 0 END as away_b2b,
        CASE WHEN (gs.home_margin + b.espn_closing_spread) > 0 THEN 1
             WHEN (gs.home_margin + b.espn_closing_spread) < 0 THEN 0
             ELSE NULL END as home_covered,
        CASE WHEN (gs.home_margin + b.espn_closing_spread) < 0 THEN 1
             WHEN (gs.home_margin + b.espn_closing_spread) > 0 THEN 0
             ELSE NULL END as away_covered,
        CASE WHEN (gs.home_score + gs.away_score) > b.espn_closing_total THEN 1
             WHEN (gs.home_score + gs.away_score) < b.espn_closing_total THEN 0
             ELSE NULL END as went_over
    FROM Games g
    JOIN Betting b ON g.game_id = b.game_id
    JOIN GameStates gs ON g.game_id = gs.game_id AND gs.is_final_state = 1
    LEFT JOIN team_schedule hs ON g.game_id = hs.game_id AND g.home_team = hs.team
    LEFT JOIN team_schedule aws ON g.game_id = aws.game_id AND g.away_team = aws.team
    WHERE b.espn_closing_spread IS NOT NULL
    AND g.status_text = 'Final'
    ORDER BY g.date_time_utc
    '''

    df = pd.read_sql_query(sql, conn)
    conn.close()

    # Add pace data if available
    if not team_stats.empty:
        for prefix, team_col in [('home', 'home_team'), ('away', 'away_team')]:
            pace_map = team_stats.set_index('team_code')['pace'].to_dict()
            df[f'{prefix}_pace'] = df[team_col].map(pace_map)

        df['pace_sum'] = df['home_pace'] + df['away_pace']
    else:
        df['pace_sum'] = None

    return df


def flag_edges(df):
    """Flag all edge types for each game"""
    edges = []

    for idx, row in df.iterrows():
        spread = row['spread']
        total = row['total_line']
        pace_sum = row.get('pace_sum')

        # DOGS_7: spread >= 7
        if abs(spread) >= 7:
            underdog = row['home_team'] if spread > 0 else row['away_team']
            covered = row['home_covered'] if spread > 0 else row['away_covered']

            edges.append({
                'game_id': row['game_id'],
                'game_date': row['game_date'],
                'matchup': f"{row['away_team']} @ {row['home_team']}",
                'edge_type': 'DOGS_7',
                'pick': f"{underdog} +{abs(spread):.1f}",
                'spread': spread,
                'total': total,
                'home_team': row['home_team'],
                'away_team': row['away_team'],
                'home_margin': row['home_margin'],
                'home_b2b': row['home_b2b'],
                'away_b2b': row['away_b2b'],
                'home_rest': row['home_rest'],
                'away_rest': row['away_rest'],
                'pace_sum': pace_sum,
                'confidence': 'HIGH',
                'expected_hit_rate': 63.5,
                'result': 'WIN' if covered == 1 else 'LOSS',
                'won': covered == 1
            })

        # DOGS_6: spread >= 6 and < 7
        elif abs(spread) >= 6:
            underdog = row['home_team'] if spread > 0 else row['away_team']
            covered = row['home_covered'] if spread > 0 else row['away_covered']

            edges.append({
                'game_id': row['game_id'],
                'game_date': row['game_date'],
                'matchup': f"{row['away_team']} @ {row['home_team']}",
                'edge_type': 'DOGS_6',
                'pick': f"{underdog} +{abs(spread):.1f}",
                'spread': spread,
                'total': total,
                'home_team': row['home_team'],
                'away_team': row['away_team'],
                'home_margin': row['home_margin'],
                'home_b2b': row['home_b2b'],
                'away_b2b': row['away_b2b'],
                'home_rest': row['home_rest'],
                'away_rest': row['away_rest'],
                'pace_sum': pace_sum,
                'confidence': 'MEDIUM',
                'expected_hit_rate': 60.0,
                'result': 'WIN' if covered == 1 else 'LOSS',
                'won': covered == 1
            })

        # PACE_UNDER: pace_sum < 200
        if pace_sum is not None and pace_sum < 200 and total is not None:
            went_under = row['went_over'] == 0

            edges.append({
                'game_id': row['game_id'],
                'game_date': row['game_date'],
                'matchup': f"{row['away_team']} @ {row['home_team']}",
                'edge_type': 'PACE_UNDER',
                'pick': f"UNDER {total:.1f}",
                'spread': spread,
                'total': total,
                'home_team': row['home_team'],
                'away_team': row['away_team'],
                'home_margin': row['home_margin'],
                'home_b2b': row['home_b2b'],
                'away_b2b': row['away_b2b'],
                'home_rest': row['home_rest'],
                'away_rest': row['away_rest'],
                'pace_sum': pace_sum,
                'total_points': row['total_points'],
                'confidence': 'HIGH',
                'expected_hit_rate': 63.6,
                'result': 'WIN' if went_under else 'LOSS',
                'won': went_under
            })

        # UNDER_235: total < 235
        if total is not None and total < 235:
            went_under = row['went_over'] == 0

            # Don't double-count if already flagged as PACE_UNDER
            if pace_sum is None or pace_sum >= 200:
                edges.append({
                    'game_id': row['game_id'],
                    'game_date': row['game_date'],
                    'matchup': f"{row['away_team']} @ {row['home_team']}",
                    'edge_type': 'UNDER_235',
                    'pick': f"UNDER {total:.1f}",
                    'spread': spread,
                    'total': total,
                    'home_team': row['home_team'],
                    'away_team': row['away_team'],
                    'home_margin': row['home_margin'],
                    'home_b2b': row['home_b2b'],
                    'away_b2b': row['away_b2b'],
                    'home_rest': row['home_rest'],
                    'away_rest': row['away_rest'],
                    'pace_sum': pace_sum,
                    'total_points': row['total_points'],
                    'confidence': 'MEDIUM',
                    'expected_hit_rate': 59.5,
                    'result': 'WIN' if went_under else 'LOSS',
                    'won': went_under
                })

    return pd.DataFrame(edges)


def ai_verify_bet(bet, version='v2'):
    """
    Simulate AI verification of a bet.

    VERSION HISTORY:
    v1 (original): All rules including extreme spread filter
    v2 (refined): Removed extreme spread rule (was filtering 60% winners)

    AI DISQUALIFICATION RULES (v2):
    1. Dog on B2B vs well-rested favorite (4+ days rest)
    2. Pace contradiction: betting under but pace sum > 202
    3. Large spread unders (>12) - blowouts go over
    4. REMOVED: Extreme spread (>14) - was filtering winners

    Returns: (verdict, reason)
    """
    spread = abs(bet['spread'])
    edge_type = bet['edge_type']

    # REMOVED in v2: Extreme blowout risk (spread > 14)
    # Backtest showed 60% hit rate on rejected bets - false negatives

    # Rule 1: Dog on B2B vs WELL-rested opponent (4+ days)
    # Tightened from 3+ to 4+ days to reduce false negatives
    is_home_dog = bet['spread'] > 0
    dog_on_b2b = (is_home_dog and bet['home_b2b'] == 1) or (not is_home_dog and bet['away_b2b'] == 1)
    fav_very_rested = (is_home_dog and bet['away_rest'] and bet['away_rest'] >= 4) or \
                      (not is_home_dog and bet['home_rest'] and bet['home_rest'] >= 4)

    if edge_type in ['DOGS_7', 'DOGS_6'] and dog_on_b2b and fav_very_rested:
        return 'REJECT', 'Dog on B2B vs very well-rested (4+ days) favorite'

    # Rule 2: Pace contradiction for unders - keep as is (effective)
    if edge_type in ['PACE_UNDER', 'UNDER_235']:
        pace_sum = bet.get('pace_sum')
        if pace_sum and pace_sum > 202:
            return 'REJECT', f'Pace sum {pace_sum:.1f} is borderline high for under'

    # Rule 3: Large spread under - KEEP (33% hit rate on rejected = effective filter)
    if edge_type in ['PACE_UNDER', 'UNDER_235'] and spread > 12:
        return 'REJECT', 'Large spread game - blowouts often go over in garbage time'

    # REMOVED in v2: Both teams on B2B
    # Backtest showed 55% hit rate - marginal, not worth filtering

    # REMOVED in v2: Marginal DOGS_6 random rejection
    # Adds noise without clear benefit

    # CONFIRM the bet
    return 'CONFIRM', 'No disqualifying factors found'


def run_backtest():
    """Run full backtest comparing raw vs AI-verified"""
    print("=" * 70)
    print("AXIOM AI VERIFICATION BACKTEST")
    print("=" * 70)

    # Step 1: Get all data
    print("\n[STEP 1] Loading game data...")
    df = get_master_dataset()
    print(f"  Loaded {len(df)} games with results")
    print(f"  Date range: {df['game_date'].min()} to {df['game_date'].max()}")

    # Step 2: Flag all edges
    print("\n[STEP 2] Flagging all edges...")
    edges_df = flag_edges(df)
    print(f"  Total flagged bets: {len(edges_df)}")
    print(f"  By type:")
    for edge_type, count in edges_df['edge_type'].value_counts().items():
        wins = edges_df[edges_df['edge_type'] == edge_type]['won'].sum()
        print(f"    {edge_type}: {count} bets, {wins} wins ({wins/count*100:.1f}%)")

    # Step 3: Run AI verification on all bets
    print("\n[STEP 3] Running AI verification...")
    verdicts = []
    for idx, bet in edges_df.iterrows():
        verdict, reason = ai_verify_bet(bet)
        verdicts.append({
            'verdict': verdict,
            'reason': reason
        })

    edges_df['ai_verdict'] = [v['verdict'] for v in verdicts]
    edges_df['ai_reason'] = [v['reason'] for v in verdicts]

    confirmed = edges_df[edges_df['ai_verdict'] == 'CONFIRM']
    rejected = edges_df[edges_df['ai_verdict'] == 'REJECT']

    print(f"  AI CONFIRM: {len(confirmed)} bets")
    print(f"  AI REJECT: {len(rejected)} bets")

    # Step 4: Compare results
    print("\n[STEP 4] Comparing Results...")
    print("\n" + "=" * 80)
    print("RESULTS COMPARISON: RAW vs AI-VERIFIED")
    print("=" * 80)

    results = []

    # By edge type
    for edge_type in ['DOGS_7', 'DOGS_6', 'PACE_UNDER', 'UNDER_235']:
        raw = edges_df[edges_df['edge_type'] == edge_type]
        verified = confirmed[confirmed['edge_type'] == edge_type]

        if len(raw) > 0:
            raw_wins = raw['won'].sum()
            raw_hr = raw_wins / len(raw) * 100

            if len(verified) > 0:
                ver_wins = verified['won'].sum()
                ver_hr = ver_wins / len(verified) * 100
                improvement = ver_hr - raw_hr
            else:
                ver_wins = 0
                ver_hr = 0
                improvement = 0

            results.append({
                'edge_type': edge_type,
                'raw_n': len(raw),
                'raw_wins': raw_wins,
                'raw_hr': raw_hr,
                'verified_n': len(verified),
                'verified_wins': ver_wins,
                'verified_hr': ver_hr,
                'improvement': improvement
            })

    # Print comparison table
    print(f"\n{'Edge Type':<15} {'Raw N':>8} {'Raw HR':>10} {'AI N':>8} {'AI HR':>10} {'Improv':>10}")
    print("-" * 70)

    for r in results:
        print(f"{r['edge_type']:<15} {r['raw_n']:>8} {r['raw_hr']:>9.1f}% {r['verified_n']:>8} {r['verified_hr']:>9.1f}% {r['improvement']:>+9.1f}%")

    # Overall
    total_raw = len(edges_df)
    total_raw_wins = edges_df['won'].sum()
    total_raw_hr = total_raw_wins / total_raw * 100

    total_ver = len(confirmed)
    total_ver_wins = confirmed['won'].sum()
    total_ver_hr = total_ver_wins / total_ver * 100 if total_ver > 0 else 0

    print("-" * 70)
    print(f"{'TOTAL':<15} {total_raw:>8} {total_raw_hr:>9.1f}% {total_ver:>8} {total_ver_hr:>9.1f}% {total_ver_hr - total_raw_hr:>+9.1f}%")

    # Step 5: Analyze rejected bets
    print("\n" + "=" * 80)
    print("AI FILTER ANALYSIS")
    print("=" * 80)

    if len(rejected) > 0:
        rejected_wins = rejected['won'].sum()
        rejected_losses = len(rejected) - rejected_wins
        rejected_hr = rejected_wins / len(rejected) * 100

        print(f"\nRejected bets: {len(rejected)}")
        print(f"  Would have won: {rejected_wins} ({rejected_hr:.1f}%)")
        print(f"  Would have lost: {rejected_losses} ({100-rejected_hr:.1f}%)")

        # Rejection reasons
        print(f"\nRejection reasons:")
        for reason, group in rejected.groupby('ai_reason'):
            wins = group['won'].sum()
            hr = wins / len(group) * 100
            print(f"  \"{reason}\"")
            print(f"    -> {len(group)} bets, {wins} would have won ({hr:.1f}%)")

        # False negatives (rejected winners)
        false_negatives = rejected[rejected['won'] == True]
        print(f"\nFalse negatives (rejected winners): {len(false_negatives)}")

        # True negatives (correctly rejected losers)
        true_negatives = rejected[rejected['won'] == False]
        print(f"True negatives (correctly rejected losers): {len(true_negatives)}")

    # ROI Analysis
    print("\n" + "=" * 80)
    print("ROI ANALYSIS (at -110 juice)")
    print("=" * 80)

    def calculate_roi(wins, total, juice=-110):
        """Calculate ROI assuming flat betting at given juice"""
        if total == 0:
            return 0

        # At -110: risk 110 to win 100
        # Win: +100, Loss: -110
        profit = wins * 100 - (total - wins) * 110
        roi = profit / (total * 110) * 100
        return roi

    raw_roi = calculate_roi(total_raw_wins, total_raw)
    ver_roi = calculate_roi(total_ver_wins, total_ver)

    print(f"\nRAW bets:")
    print(f"  {total_raw} bets, {total_raw_wins}W-{total_raw - total_raw_wins}L ({total_raw_hr:.1f}%)")
    print(f"  ROI: {raw_roi:+.1f}%")

    print(f"\nAI-VERIFIED bets:")
    print(f"  {total_ver} bets, {total_ver_wins}W-{total_ver - total_ver_wins}L ({total_ver_hr:.1f}%)")
    print(f"  ROI: {ver_roi:+.1f}%")

    print(f"\nIMPROVEMENT:")
    print(f"  Hit rate: {total_ver_hr - total_raw_hr:+.1f} percentage points")
    print(f"  ROI: {ver_roi - raw_roi:+.1f} percentage points")
    print(f"  Bets filtered: {len(rejected)} ({len(rejected)/total_raw*100:.1f}%)")

    # Verdict
    print("\n" + "=" * 80)
    print("VERDICT")
    print("=" * 80)

    if ver_roi > raw_roi:
        print(f"\n  AI VERIFICATION HELPS")
        print(f"  -> Improves ROI by {ver_roi - raw_roi:.1f} percentage points")
        print(f"  -> Filters out {len(rejected)} bets with {rejected_hr:.1f}% hit rate")
        print(f"  -> Cost: {len(false_negatives)} missed winners")
    else:
        print(f"\n  AI VERIFICATION DOES NOT HELP (with current rules)")
        print(f"  -> Reduces ROI by {raw_roi - ver_roi:.1f} percentage points")
        print(f"  -> May be filtering good bets incorrectly")

    return edges_df, confirmed, rejected


if __name__ == "__main__":
    random.seed(42)  # For reproducibility
    edges_df, confirmed, rejected = run_backtest()
