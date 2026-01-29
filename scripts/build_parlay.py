"""
AXIOM Daily Parlay Builder

Builds optimal 5-leg parlays using validated edges:
- DOG +7: 63.5% (spread edge)
- DOG +6: 60.0% (spread edge)
- Low pace UNDER: 63.6% (total edge)
- PRA combos: 60.4% (prop edge)
- AST props: 55.6% (prop edge)

Rules:
1. Max 2 legs from same game
2. Prioritize highest individual hit rates
3. Mix spread/total/prop for diversification
"""

import sqlite3
import pandas as pd
from datetime import datetime, date
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]

# Validated edges with historical hit rates
EDGES = {
    'DOG_7': {'type': 'SPREAD', 'hit_rate': 0.635, 'description': 'Underdog +7 or more'},
    'DOG_6': {'type': 'SPREAD', 'hit_rate': 0.600, 'description': 'Underdog +6 to +6.9'},
    'PACE_UNDER': {'type': 'TOTAL', 'hit_rate': 0.636, 'description': 'Both teams pace < 100'},
    'LOW_TOTAL': {'type': 'TOTAL', 'hit_rate': 0.595, 'description': 'Total line < 235'},
    'PRA_COMBO': {'type': 'PROP', 'hit_rate': 0.604, 'description': 'PRA combo 15%+ edge'},
    'AST_PROP': {'type': 'PROP', 'hit_rate': 0.556, 'description': 'AST 15%+ edge'},
}

# Team pace data (from team_advanced_stats)
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


def get_todays_games(target_date=None):
    """Get games with betting lines for today."""
    conn = sqlite3.connect(DB_PATH)

    if target_date is None:
        target_date = date.today().isoformat()

    games = pd.read_sql(f"""
        SELECT
            g.game_id,
            g.home_team,
            g.away_team,
            DATE(g.date_time_utc) as game_date,
            COALESCE(b.espn_current_spread, b.espn_closing_spread, b.espn_opening_spread) as spread,
            COALESCE(b.espn_current_total, b.espn_closing_total, b.espn_opening_total) as total_line
        FROM Games g
        LEFT JOIN Betting b ON g.game_id = b.game_id
        WHERE DATE(g.date_time_utc) = '{target_date}'
        ORDER BY g.date_time_utc
    """, conn)

    # Get team pace
    try:
        pace_df = pd.read_sql("SELECT team_name, pace FROM team_advanced_stats", conn)
        pace_df['team_code'] = pace_df['team_name'].map(TEAM_NAME_TO_CODE)
        pace_map = pace_df.set_index('team_code')['pace'].to_dict()
    except:
        pace_map = {}

    conn.close()

    # Add pace data
    games['home_pace'] = games['home_team'].map(pace_map)
    games['away_pace'] = games['away_team'].map(pace_map)
    games['pace_sum'] = games['home_pace'] + games['away_pace']

    return games


def get_todays_props(target_date=None):
    """Get prop edges for today."""
    conn = sqlite3.connect(DB_PATH)

    if target_date is None:
        target_date = date.today().isoformat()

    try:
        props = pd.read_sql(f"""
            SELECT
                player_name,
                opponent,
                prop_type,
                line,
                projection,
                edge_pct,
                pick,
                confidence,
                stat_tier
            FROM props_edges
            WHERE date = '{target_date}'
            AND ABS(edge_pct) >= 15
            ORDER BY ABS(edge_pct) DESC
        """, conn)
    except:
        props = pd.DataFrame()

    conn.close()
    return props


def find_parlay_legs(games, props):
    """Find the best legs for a 5-leg parlay."""
    legs = []

    # 1. Find spread edges (DOG +7, DOG +6)
    for _, game in games.iterrows():
        spread = game['spread']
        if pd.isna(spread):
            continue

        abs_spread = abs(spread)
        underdog = game['home_team'] if spread > 0 else game['away_team']

        if abs_spread >= 7:
            legs.append({
                'game_id': game['game_id'],
                'game': f"{game['away_team']} @ {game['home_team']}",
                'type': 'SPREAD',
                'edge': 'DOG_7',
                'pick': f"{underdog} +{abs_spread:.1f}",
                'hit_rate': 0.635,
                'confidence': 'HIGH'
            })
        elif abs_spread >= 6:
            legs.append({
                'game_id': game['game_id'],
                'game': f"{game['away_team']} @ {game['home_team']}",
                'type': 'SPREAD',
                'edge': 'DOG_6',
                'pick': f"{underdog} +{abs_spread:.1f}",
                'hit_rate': 0.600,
                'confidence': 'MEDIUM'
            })

    # 2. Find total edges (PACE_UNDER, LOW_TOTAL)
    for _, game in games.iterrows():
        total = game['total_line']
        pace_sum = game['pace_sum']

        if pd.notna(pace_sum) and pace_sum < 200 and pd.notna(total):
            legs.append({
                'game_id': game['game_id'],
                'game': f"{game['away_team']} @ {game['home_team']}",
                'type': 'TOTAL',
                'edge': 'PACE_UNDER',
                'pick': f"UNDER {total:.1f}",
                'hit_rate': 0.636,
                'confidence': 'HIGH'
            })
        elif pd.notna(total) and total < 235:
            legs.append({
                'game_id': game['game_id'],
                'game': f"{game['away_team']} @ {game['home_team']}",
                'type': 'TOTAL',
                'edge': 'LOW_TOTAL',
                'pick': f"UNDER {total:.1f}",
                'hit_rate': 0.595,
                'confidence': 'MEDIUM'
            })

    # 3. Find prop edges (PRA, AST)
    if not props.empty:
        # PRA combos
        pra_props = props[props['prop_type'].isin(['PRA', 'PR', 'PA', 'RA'])]
        for _, prop in pra_props.head(3).iterrows():
            legs.append({
                'game_id': None,
                'game': f"vs {prop['opponent']}",
                'type': 'PROP',
                'edge': 'PRA_COMBO',
                'pick': f"{prop['player_name']} {prop['pick']} {prop['line']} {prop['prop_type']}",
                'hit_rate': 0.604,
                'confidence': prop['confidence']
            })

        # AST props
        ast_props = props[props['prop_type'] == 'AST']
        for _, prop in ast_props.head(2).iterrows():
            legs.append({
                'game_id': None,
                'game': f"vs {prop['opponent']}",
                'type': 'PROP',
                'edge': 'AST_PROP',
                'pick': f"{prop['player_name']} {prop['pick']} {prop['line']} AST",
                'hit_rate': 0.556,
                'confidence': prop['confidence']
            })

    return legs


def build_optimal_parlay(legs, num_legs=5):
    """Select the best 5 legs, avoiding too many from same game."""
    if len(legs) == 0:
        return []

    # Sort by hit rate
    legs_df = pd.DataFrame(legs)
    legs_df = legs_df.sort_values('hit_rate', ascending=False)

    selected = []
    game_counts = {}

    for _, leg in legs_df.iterrows():
        if len(selected) >= num_legs:
            break

        game_id = leg['game_id']

        # Max 2 legs per game
        if game_id and game_counts.get(game_id, 0) >= 2:
            continue

        selected.append(leg.to_dict())

        if game_id:
            game_counts[game_id] = game_counts.get(game_id, 0) + 1

    return selected


def calculate_parlay_odds(legs):
    """Calculate combined parlay probability and payout."""
    if not legs:
        return 0, 0, 0

    combined_prob = 1.0
    for leg in legs:
        combined_prob *= leg['hit_rate']

    # Standard -110 parlay payout
    payout = (1.91 ** len(legs))

    # ROI
    expected_roi = (combined_prob * payout - 1) * 100

    return combined_prob, payout, expected_roi


def main(target_date=None):
    if target_date is None:
        target_date = date.today().isoformat()

    print("=" * 60)
    print(f"AXIOM PARLAY BUILDER - {target_date}")
    print("=" * 60)

    # Get data
    games = get_todays_games(target_date)
    props = get_todays_props(target_date)

    print(f"\nFound {len(games)} games, {len(props)} prop edges")

    if len(games) == 0:
        print("\nNo games found for this date.")
        return

    # Find all potential legs
    legs = find_parlay_legs(games, props)
    print(f"Identified {len(legs)} potential parlay legs")

    # Build optimal 5-leg parlay
    parlay = build_optimal_parlay(legs, 5)

    if len(parlay) < 3:
        print("\nNot enough qualifying edges for a parlay today.")
        return

    # Calculate odds
    prob, payout, roi = calculate_parlay_odds(parlay)

    # Display
    print("\n" + "=" * 60)
    print(f"RECOMMENDED {len(parlay)}-LEG PARLAY")
    print("=" * 60)

    for i, leg in enumerate(parlay, 1):
        conf_marker = "***" if leg['confidence'] == 'HIGH' else "**"
        print(f"\nLEG {i} [{conf_marker}] ({leg['hit_rate']*100:.1f}%)")
        print(f"  {leg['pick']}")
        print(f"  {leg['game']} | Edge: {leg['edge']}")

    print("\n" + "-" * 60)
    print(f"Combined probability: {prob*100:.2f}%")
    print(f"Payout: {payout:.1f}x")
    print(f"Expected ROI: {roi:+.0f}%")

    # Risk assessment
    print("\n" + "=" * 60)
    print("RISK ASSESSMENT")
    print("=" * 60)

    if prob > 0.05 and roi > 25:
        print("\n  [STRONG] This parlay has positive expected value")
        print(f"  Recommended bet size: 0.5-1% of bankroll")
    elif prob > 0.04 and roi > 0:
        print("\n  [MARGINAL] This parlay is borderline +EV")
        print(f"  Recommended bet size: 0.25% of bankroll")
    else:
        print("\n  [WEAK] This parlay may not be +EV")
        print(f"  Consider fewer legs or skip today")

    return parlay


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="Target date YYYY-MM-DD")
    args = parser.parse_args()

    main(args.date)
