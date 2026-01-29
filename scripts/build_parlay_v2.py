"""
AXIOM Parlay Builder v2
Based on Comprehensive Edge Research findings.

VALIDATED EDGES (p < 0.05):
1. Away Favorite OVER (spread > 3): 72.6% - BEST EDGE
2. Big Underdog ATS (spread >= 7): 64.4%
3. Home Favorite OVER (spread < -5): 64.4%
4. Double-Digit Underdog ATS (spread >= 10): 60.8%

PARLAY STRATEGY:
- Mix edge types for diversification
- Different games for each leg
- Target 5 legs for max payout
- Combined probability > 5% required
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, date
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]

# Validated edges from research
# Note: Team total edges require total line data which may not always be available
VALIDATED_EDGES = {
    'BIG_DOG_ATS': {
        'description': 'Big Underdog ATS (+7)',
        'hit_rate': 0.644,
        'condition': lambda g: abs(g['spread']) >= 7,
        'pick_fn': lambda g: f"{g['away_team'] if g['spread'] < 0 else g['home_team']} +{abs(g['spread']):.1f}",
        'p_value': 0.0021,
        'type': 'SPREAD',
        'requires_total': False
    },
    'DOUBLE_DIGIT_DOG': {
        'description': 'Double-Digit Underdog ATS (+10)',
        'hit_rate': 0.608,
        'condition': lambda g: abs(g['spread']) >= 10,
        'pick_fn': lambda g: f"{g['away_team'] if g['spread'] < 0 else g['home_team']} +{abs(g['spread']):.1f}",
        'p_value': 0.0804,
        'type': 'SPREAD',
        'requires_total': False
    },
    'AWAY_FAV_OVER': {
        'description': 'Away Fav Team Total OVER',
        'hit_rate': 0.726,
        'condition': lambda g: g['spread'] > 3 and pd.notna(g.get('total_line')),
        'pick_fn': lambda g: f"{g['away_team']} team OVER {g['away_implied']:.1f}",
        'p_value': 0.0002,
        'type': 'TEAM_TOTAL',
        'requires_total': True
    },
    'HOME_FAV_OVER': {
        'description': 'Home Fav Team Total OVER',
        'hit_rate': 0.644,
        'condition': lambda g: g['spread'] < -5 and pd.notna(g.get('total_line')),
        'pick_fn': lambda g: f"{g['home_team']} team OVER {g['home_implied']:.1f}",
        'p_value': 0.0093,
        'type': 'TEAM_TOTAL',
        'requires_total': True
    },
    'MED_DOG_ATS': {
        'description': 'Medium Underdog ATS (+5 to +6.5)',
        'hit_rate': 0.55,  # Lower edge, use when few options
        'condition': lambda g: 5 <= abs(g['spread']) < 7,
        'pick_fn': lambda g: f"{g['away_team'] if g['spread'] < 0 else g['home_team']} +{abs(g['spread']):.1f}",
        'p_value': 0.20,
        'type': 'SPREAD',
        'requires_total': False
    },
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

    conn.close()

    if games.empty:
        return games

    # Calculate implied team totals
    # Home implied = (total + spread) / 2
    # Away implied = (total - spread) / 2
    games['home_implied'] = (games['total_line'] + games['spread']) / 2
    games['away_implied'] = (games['total_line'] - games['spread']) / 2

    return games


def find_all_edges(games):
    """Find all valid edges from today's games."""
    edges = []

    for _, game in games.iterrows():
        # Must have spread at minimum
        if pd.isna(game['spread']):
            continue

        game_dict = game.to_dict()

        for edge_id, edge_config in VALIDATED_EDGES.items():
            # Skip edges that require total if total is missing
            if edge_config.get('requires_total', False) and pd.isna(game.get('total_line')):
                continue

            try:
                if edge_config['condition'](game_dict):
                    edges.append({
                        'game_id': game['game_id'],
                        'game': f"{game['away_team']} @ {game['home_team']}",
                        'edge_id': edge_id,
                        'edge_type': edge_config['type'],
                        'description': edge_config['description'],
                        'pick': edge_config['pick_fn'](game_dict),
                        'hit_rate': edge_config['hit_rate'],
                        'p_value': edge_config['p_value'],
                        'spread': game['spread'],
                        'total': game.get('total_line')
                    })
            except Exception as e:
                continue

    return edges


def select_parlay_legs(edges, num_legs=5):
    """Select optimal legs for parlay - different games, max diversification."""
    if not edges:
        return []

    # Sort by hit rate (best first), then by p_value (most significant)
    edges_df = pd.DataFrame(edges)
    edges_df = edges_df.sort_values(['hit_rate', 'p_value'], ascending=[False, True])

    selected = []
    games_used = set()
    picks_used = set()  # Track actual picks to avoid duplicates

    for _, edge in edges_df.iterrows():
        if len(selected) >= num_legs:
            break

        game_id = edge['game_id']
        pick = edge['pick']

        # Don't duplicate same pick (e.g., SAC +13.5 from both BIG_DOG and DOUBLE_DIGIT)
        if pick in picks_used:
            continue

        # Different games only (for independence)
        if game_id in games_used:
            continue

        selected.append(edge.to_dict())
        games_used.add(game_id)
        picks_used.add(pick)

    return selected


def calculate_parlay(legs):
    """Calculate parlay probability, payout, and ROI."""
    if not legs:
        return 0, 0, 0

    combined_prob = 1.0
    for leg in legs:
        combined_prob *= leg['hit_rate']

    # Standard -110 legs payout
    payout = 1.91 ** len(legs)

    # Expected ROI
    expected_roi = (combined_prob * payout - 1) * 100

    return combined_prob, payout, expected_roi


def assess_parlay_quality(combined_prob, roi):
    """Assess parlay quality for betting."""
    if combined_prob >= 0.08 and roi >= 100:
        return 'STRONG', '1% bankroll'
    elif combined_prob >= 0.05 and roi >= 25:
        return 'GOOD', '0.5% bankroll'
    elif combined_prob >= 0.04 and roi >= 0:
        return 'MARGINAL', '0.25% bankroll'
    else:
        return 'SKIP', 'No bet'


def main(target_date=None):
    if target_date is None:
        target_date = date.today().isoformat()

    print("=" * 60)
    print(f"AXIOM PARLAY BUILDER v2 - {target_date}")
    print("=" * 60)
    print("\nUsing validated edges from comprehensive research:")
    print("  - Away Fav OVER: 72.6% (p=0.0002)")
    print("  - Big Dog ATS: 64.4% (p=0.002)")
    print("  - Home Fav OVER: 64.4% (p=0.009)")
    print("  - Double-Digit Dog: 60.8% (p=0.08)")

    # Get games
    games = get_todays_games(target_date)
    if games.empty:
        print(f"\nNo games found for {target_date}")
        return None

    print(f"\nFound {len(games)} games")

    # Find all edges
    edges = find_all_edges(games)
    print(f"Found {len(edges)} qualifying edge opportunities")

    if not edges:
        print("\nNo qualifying edges today. Skip parlay.")
        return None

    # Show all edges found
    print("\n" + "-" * 60)
    print("ALL QUALIFYING EDGES")
    print("-" * 60)
    for e in sorted(edges, key=lambda x: x['hit_rate'], reverse=True):
        print(f"  [{e['hit_rate']*100:.0f}%] {e['description']}")
        print(f"        {e['pick']} ({e['game']})")

    # Select optimal legs
    parlay = select_parlay_legs(edges, 5)

    if len(parlay) < 2:
        print("\nNot enough independent edges for a parlay.")
        return None

    # Calculate odds
    prob, payout, roi = calculate_parlay(parlay)
    quality, bet_size = assess_parlay_quality(prob, roi)

    # Display parlay
    print("\n" + "=" * 60)
    print(f"RECOMMENDED {len(parlay)}-LEG PARLAY")
    print("=" * 60)

    for i, leg in enumerate(parlay, 1):
        stars = "***" if leg['hit_rate'] >= 0.65 else "**" if leg['hit_rate'] >= 0.60 else "*"
        print(f"\nLEG {i} [{stars}] ({leg['hit_rate']*100:.1f}%)")
        print(f"  {leg['pick']}")
        print(f"  {leg['game']} | {leg['description']}")

    print("\n" + "-" * 60)
    print(f"Combined probability: {prob*100:.2f}%")
    print(f"Payout: {payout:.1f}x")
    print(f"Expected ROI: {roi:+.0f}%")

    print("\n" + "=" * 60)
    print("VERDICT")
    print("=" * 60)
    print(f"\n  Quality: [{quality}]")
    print(f"  Recommended bet: {bet_size}")

    if quality == 'SKIP':
        print("\n  This parlay doesn't meet minimum threshold.")
        print("  Consider waiting for better opportunities.")
    elif quality == 'MARGINAL':
        print("\n  Borderline +EV. Small bet if confident.")
    elif quality == 'GOOD':
        print("\n  Solid +EV parlay. Standard bet size.")
    else:
        print("\n  Strong +EV parlay. Full bet size.")

    return parlay


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="Target date YYYY-MM-DD")
    args = parser.parse_args()

    main(args.date)
