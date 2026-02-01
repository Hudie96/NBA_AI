"""
Daily Output Generator

Generates three outputs:
1. Predictions spreadsheet (outputs/predictions/)
2. Social posts (outputs/social/)
3. Performance tracker update (outputs/performance/)

Includes: Player Props, Spreads, ML, Totals
"""
import argparse
import csv
import json
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config
from scripts.find_edges import find_edges_for_today, get_stat_tier
from scripts.props_validator import get_todays_games

DB_PATH = config["database"]["path"]
OUTPUTS_DIR = PROJECT_ROOT / "outputs"
PREDICTIONS_DIR = OUTPUTS_DIR / "predictions"
SOCIAL_DIR = OUTPUTS_DIR / "social"
PERFORMANCE_DIR = OUTPUTS_DIR / "performance"

# Ensure directories exist
for d in [PREDICTIONS_DIR, SOCIAL_DIR, PERFORMANCE_DIR]:
    d.mkdir(parents=True, exist_ok=True)


def fetch_betting_lines(conn, target_date):
    """Fetch betting lines for today's games from ESPN."""
    from src.database_updater.betting import update_betting_data

    try:
        print("      Fetching betting lines from ESPN...")
        stats = update_betting_data(date_range=(target_date, target_date))
        print(f"      ESPN: {stats.get('espn_fetched', 0)} fetched, {stats.get('saved', 0)} saved")
        return True
    except Exception as e:
        print(f"      Warning: Could not fetch betting lines: {e}")
        return False


def get_spread_picks(conn, target_date):
    """Get spread/ML/total predictions for today's games."""
    games = get_todays_games(conn, target_date)
    picks = []

    for game in games:
        game_id = game.get('game_id')
        away_team = game['away_team']
        home_team = game['home_team']

        # Get betting lines
        line_data = conn.execute('''
            SELECT espn_current_spread, espn_current_total,
                   espn_current_ml_home, espn_current_ml_away
            FROM Betting
            WHERE game_id = ?
        ''', (game_id,)).fetchone()

        spread = line_data[0] if line_data and line_data[0] else None
        total = line_data[1] if line_data and line_data[1] else None
        ml_home = line_data[2] if line_data and line_data[2] else None
        ml_away = line_data[3] if line_data and line_data[3] else None

        # Get model prediction from daily_predictions logic
        # For now, use simple stats-based prediction
        model_spread, model_total = get_model_prediction(conn, away_team, home_team)

        game_info = {
            'game': f"{away_team} @ {home_team}",
            'away_team': away_team,
            'home_team': home_team,
            'vegas_spread': spread,
            'vegas_total': total,
            'ml_home': ml_home,
            'ml_away': ml_away,
            'model_spread': model_spread,
            'model_total': model_total,
        }

        # Calculate edges
        if spread is not None and model_spread is not None:
            spread_edge = model_spread - spread
            game_info['spread_edge'] = round(spread_edge, 1)

            # Determine pick
            if abs(spread_edge) >= 3:
                if spread_edge > 0:
                    game_info['spread_pick'] = f"{home_team} {spread:+.1f}"
                    game_info['spread_tier'] = 'GOLD' if spread_edge >= 5 else 'SILVER'
                else:
                    game_info['spread_pick'] = f"{away_team} {-spread:+.1f}"
                    game_info['spread_tier'] = 'GOLD' if abs(spread_edge) >= 5 else 'SILVER'

        if total is not None and model_total is not None:
            total_edge = model_total - total
            game_info['total_edge'] = round(total_edge, 1)

            if abs(total_edge) >= 5:
                direction = 'OVER' if total_edge > 0 else 'UNDER'
                game_info['total_pick'] = f"{direction} {total}"
                game_info['total_tier'] = 'SILVER'

        picks.append(game_info)

    return picks


def get_model_prediction(conn, away_team, home_team):
    """Get model spread and total prediction based on team stats."""
    # Get team offensive/defensive ratings
    home_stats = conn.execute('''
        SELECT off_rating, def_rating, pace
        FROM TeamAdvancedStats
        WHERE team_abbrev = ?
        ORDER BY updated_at DESC LIMIT 1
    ''', (home_team,)).fetchone()

    away_stats = conn.execute('''
        SELECT off_rating, def_rating, pace
        FROM TeamAdvancedStats
        WHERE team_abbrev = ?
        ORDER BY updated_at DESC LIMIT 1
    ''', (away_team,)).fetchone()

    if not home_stats or not away_stats:
        return None, None

    home_off, home_def, home_pace = home_stats
    away_off, away_def, away_pace = away_stats

    # Calculate expected scores using pace-adjusted ratings
    avg_pace = (home_pace + away_pace) / 2
    possessions = avg_pace  # Approximate possessions per game

    # Expected points = (Off Rating + Opp Def Rating) / 2 * possessions / 100
    home_expected = ((home_off + away_def) / 2) * possessions / 100
    away_expected = ((away_off + home_def) / 2) * possessions / 100

    # Add home court advantage (~3 points)
    home_expected += 3

    model_spread = round(home_expected - away_expected, 1)
    model_total = round(home_expected + away_expected, 1)

    return model_spread, model_total


def get_prop_picks(conn, target_date, min_games=20):
    """Get S_TIER HIGH confidence prop picks."""
    edges = find_edges_for_today(conn, target_date, min_games=min_games)

    # Filter to S_TIER HIGH only
    high_s_tier = [e for e in edges if e.get('stat_tier') == 'S_TIER' and e.get('confidence') == 'HIGH']

    # Parse factors JSON and add to edge dict
    for e in high_s_tier:
        factors = json.loads(e.get('factors', '{}'))
        e['l10_avg'] = factors.get('last_10_avg', 0) or 0
        e['season_avg'] = factors.get('season_avg', 0) or 0
        e['vs_opp_avg'] = factors.get('vs_opp_avg', 0) or 0
        e['stat'] = e.get('prop_type', '')

    # Group by player, keep best edge per player
    by_player = {}
    for e in high_s_tier:
        player = e['player_name']
        if player not in by_player or abs(e['edge_pct']) > abs(by_player[player]['edge_pct']):
            by_player[player] = e

    # Sort by edge
    picks = sorted(by_player.values(), key=lambda x: abs(x['edge_pct']), reverse=True)
    return picks


def generate_predictions_csv(conn, target_date, prop_picks, spread_picks):
    """Generate predictions spreadsheet with all bet types."""
    filepath = PREDICTIONS_DIR / f"picks_{target_date}.csv"

    rows = []

    # Add spread picks
    for g in spread_picks:
        if g.get('spread_pick'):
            rows.append({
                'date': target_date,
                'game': g['game'],
                'bet_type': 'SPREAD',
                'player': '',
                'pick': g['spread_pick'],
                'line': g['vegas_spread'],
                'projection': g['model_spread'],
                'edge': f"{g['spread_edge']:+.1f}" if g.get('spread_edge') else '',
                'l10_avg': '',
                'season_avg': '',
                'tier': g.get('spread_tier', ''),
                'confidence': 'HIGH' if abs(g.get('spread_edge', 0)) >= 5 else 'MEDIUM',
            })

        if g.get('total_pick'):
            rows.append({
                'date': target_date,
                'game': g['game'],
                'bet_type': 'TOTAL',
                'player': '',
                'pick': g['total_pick'],
                'line': g['vegas_total'],
                'projection': g['model_total'],
                'edge': f"{g['total_edge']:+.1f}" if g.get('total_edge') else '',
                'l10_avg': '',
                'season_avg': '',
                'tier': g.get('total_tier', ''),
                'confidence': 'MEDIUM',
            })

        # Add ML info (no pick, just info)
        if g.get('ml_home'):
            ml_pick = ''
            if g.get('spread_edge') and abs(g['spread_edge']) >= 7:
                # Strong edge - consider ML
                if g['spread_edge'] > 0:
                    ml_pick = f"{g['home_team']} ML ({g['ml_home']:+d})"
                else:
                    ml_pick = f"{g['away_team']} ML ({g['ml_away']:+d})"
            if ml_pick:
                rows.append({
                    'date': target_date,
                    'game': g['game'],
                    'bet_type': 'ML',
                    'player': '',
                    'pick': ml_pick,
                    'line': '',
                    'projection': '',
                    'edge': f"{g['spread_edge']:+.1f}" if g.get('spread_edge') else '',
                    'l10_avg': '',
                    'season_avg': '',
                    'tier': 'PLATINUM' if abs(g.get('spread_edge', 0)) >= 7 else '',
                    'confidence': 'HIGH',
                })

    # Add prop picks
    for p in prop_picks:
        direction = 'OVER' if p['edge'] > 0 else 'UNDER'
        team = p.get('team', '?')
        rows.append({
            'date': target_date,
            'game': f"{team} vs {p['opponent']}",
            'bet_type': 'PROP',
            'player': p['player_name'],
            'pick': f"{direction} {p['line']} {p['stat']}",
            'line': p['line'],
            'projection': round(p['projection'], 1),
            'edge': f"{p['edge_pct']:+.1f}%",
            'l10_avg': round(p['l10_avg'], 1),
            'season_avg': round(p['season_avg'], 1),
            'tier': 'S_TIER',
            'confidence': p['confidence'],
        })

    # Write CSV
    if rows:
        fieldnames = ['date', 'game', 'bet_type', 'player', 'pick', 'line', 'projection',
                      'edge', 'l10_avg', 'season_avg', 'tier', 'confidence']
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    return filepath, rows


def generate_social_posts(conn, target_date, prop_picks, spread_picks):
    """Generate social media content."""
    filepath = SOCIAL_DIR / f"posts_{target_date}.txt"

    games = get_todays_games(conn, target_date)

    content = []

    # Header
    content.append(f"AXIOM PICKS - {target_date}")
    content.append("=" * 50)
    content.append("")

    # Games today
    content.append(f"GAMES TODAY: {len(games)}")
    for g in games:
        content.append(f"  {g['away_team']} @ {g['home_team']}")
    content.append("")

    # === SPREAD PICKS ===
    spread_with_edge = [g for g in spread_picks if g.get('spread_pick')]
    if spread_with_edge:
        content.append("-" * 50)
        content.append("SPREAD PICKS")
        content.append("-" * 50)
        for g in spread_with_edge:
            tier = g.get('spread_tier', '')
            content.append(f"[{tier}] {g['spread_pick']} (edge: {g['spread_edge']:+.1f})")
        content.append("")

    # === TWITTER POST - SPREADS ===
    if spread_with_edge:
        content.append("-" * 50)
        content.append("TWITTER POST - SPREADS")
        content.append("-" * 50)
        content.append("")

        tweet = f"AXIOM Spreads {target_date[-5:]}\n\n"
        for g in spread_with_edge[:3]:
            tweet += f"{g['spread_pick']} ({g['spread_edge']:+.1f})\n"
        tweet += f"\n#NBA #NBABets #GamblingTwitter"
        content.append(tweet)
        content.append("")

    # === TWITTER POST - TOP PROPS ===
    content.append("-" * 50)
    content.append("TWITTER POST - TOP PROPS")
    content.append("-" * 50)
    content.append("")

    tweet = f"AXIOM Props {target_date[-5:]}\n\n"
    for p in prop_picks[:5]:
        direction = 'O' if p['edge'] > 0 else 'U'
        tweet += f"{p['player_name']} {direction}{p['line']} {p['stat']} ({p['edge_pct']:+.0f}%)\n"
    tweet += f"\nS-TIER combos | L10 trending\n#NBA #NBABets #PlayerProps"
    content.append(tweet)
    content.append("")

    # === INDIVIDUAL PLAYER POSTS ===
    content.append("-" * 50)
    content.append("TWITTER POSTS - TOP 3 PROPS")
    content.append("-" * 50)

    for p in prop_picks[:3]:
        content.append("")
        direction = 'OVER' if p['edge'] > 0 else 'UNDER'
        team = p.get('team', '?')
        player_tweet = f"{p['player_name']} {direction} {p['line']} {p['stat']}\n\n"
        player_tweet += f"L10: {p['l10_avg']:.1f}\n"
        player_tweet += f"Season: {p['season_avg']:.1f}\n"
        player_tweet += f"Projection: {p['projection']:.1f}\n"
        player_tweet += f"Edge: {p['edge_pct']:+.1f}%\n\n"
        player_tweet += f"{team} vs {p['opponent']} | S-TIER\n#NBA #PlayerProps"
        content.append(player_tweet)

    # Write file
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write('\n'.join(content))

    return filepath


def update_performance_tracker():
    """Update cumulative performance spreadsheet."""
    results_file = PROJECT_ROOT / "data" / "results.csv"
    performance_file = PERFORMANCE_DIR / "performance_tracker.csv"

    if not results_file.exists():
        return None

    # Read all results
    with open(results_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        results = list(reader)

    # Calculate stats by date and type
    from collections import defaultdict
    daily_stats = defaultdict(lambda: {'SPREAD': {'W': 0, 'L': 0}, 'PROP': {'W': 0, 'L': 0}, 'TOTAL': {'W': 0, 'L': 0}, 'ML': {'W': 0, 'L': 0}})

    for r in results:
        if r['result'] in ['W', 'L']:
            bet_type = r['bet_type'] if r['bet_type'] in ['SPREAD', 'PROP', 'TOTAL', 'ML'] else 'PROP'
            daily_stats[r['date']][bet_type][r['result']] += 1

    # Calculate cumulative
    rows = []
    cumulative = {'SPREAD': {'W': 0, 'L': 0}, 'PROP': {'W': 0, 'L': 0}, 'TOTAL': {'W': 0, 'L': 0}, 'ML': {'W': 0, 'L': 0}}

    for dt in sorted(daily_stats.keys()):
        stats = daily_stats[dt]

        for bet_type in ['SPREAD', 'PROP', 'TOTAL', 'ML']:
            cumulative[bet_type]['W'] += stats[bet_type]['W']
            cumulative[bet_type]['L'] += stats[bet_type]['L']

        spread_total = cumulative['SPREAD']['W'] + cumulative['SPREAD']['L']
        prop_total = cumulative['PROP']['W'] + cumulative['PROP']['L']
        total_all = sum(cumulative[t]['W'] + cumulative[t]['L'] for t in cumulative)
        total_w = sum(cumulative[t]['W'] for t in cumulative)

        rows.append({
            'date': dt,
            'spread_daily': f"{stats['SPREAD']['W']}-{stats['SPREAD']['L']}",
            'prop_daily': f"{stats['PROP']['W']}-{stats['PROP']['L']}",
            'spread_cumulative': f"{cumulative['SPREAD']['W']}-{cumulative['SPREAD']['L']}",
            'prop_cumulative': f"{cumulative['PROP']['W']}-{cumulative['PROP']['L']}",
            'spread_pct': f"{100*cumulative['SPREAD']['W']/spread_total:.1f}%" if spread_total else "N/A",
            'prop_pct': f"{100*cumulative['PROP']['W']/prop_total:.1f}%" if prop_total else "N/A",
            'total_cumulative': f"{total_w}-{total_all - total_w}",
            'total_pct': f"{100*total_w/total_all:.1f}%" if total_all else "N/A",
        })

    # Write performance tracker
    if rows:
        fieldnames = ['date', 'spread_daily', 'prop_daily', 'spread_cumulative',
                      'prop_cumulative', 'spread_pct', 'prop_pct', 'total_cumulative', 'total_pct']
        try:
            with open(performance_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
        except PermissionError:
            # Try alternate file
            alt_file = PERFORMANCE_DIR / f"performance_tracker_{datetime.now().strftime('%H%M%S')}.csv"
            with open(alt_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            return alt_file, rows

    return performance_file, rows


def main():
    parser = argparse.ArgumentParser(description='Generate daily outputs')
    parser.add_argument('--date', type=str, default=None,
                        help='Target date (YYYY-MM-DD), defaults to today')
    parser.add_argument('--skip-betting', action='store_true',
                        help='Skip fetching betting lines')
    args = parser.parse_args()

    target_date = args.date or date.today().isoformat()

    print("=" * 60)
    print(f"  AXIOM DAILY OUTPUT GENERATOR - {target_date}")
    print("=" * 60)
    print()

    conn = sqlite3.connect(DB_PATH)

    # Fetch betting lines if needed
    if not args.skip_betting:
        fetch_betting_lines(conn, target_date)

    # Get all picks
    print("[1/4] Getting spread/ML/total picks...")
    spread_picks = get_spread_picks(conn, target_date)
    spreads_with_edge = len([g for g in spread_picks if g.get('spread_pick')])
    print(f"      Games: {len(spread_picks)}, Spread picks: {spreads_with_edge}")
    print()

    print("[2/4] Getting prop picks...")
    prop_picks = get_prop_picks(conn, target_date)
    print(f"      S_TIER HIGH props: {len(prop_picks)}")
    print()

    # Generate predictions spreadsheet
    print("[3/4] Generating predictions spreadsheet...")
    pred_file, pred_rows = generate_predictions_csv(conn, target_date, prop_picks, spread_picks)
    print(f"      Saved: {pred_file}")
    print(f"      Total picks: {len(pred_rows)}")
    print()

    # Generate social posts
    print("[4/4] Generating social posts...")
    social_file = generate_social_posts(conn, target_date, prop_picks, spread_picks)
    print(f"      Saved: {social_file}")
    print()

    # Update performance tracker
    print("[5/5] Updating performance tracker...")
    perf_result = update_performance_tracker()
    if perf_result:
        perf_file, perf_rows = perf_result
        print(f"      Saved: {perf_file}")
        if perf_rows:
            latest = perf_rows[-1]
            print(f"      Latest: {latest['total_cumulative']} ({latest['total_pct']})")
    print()

    conn.close()

    print("=" * 60)
    print("  OUTPUT COMPLETE")
    print("=" * 60)
    print()
    print("Files generated:")
    print(f"  1. {PREDICTIONS_DIR}/picks_{target_date}.csv")
    print(f"  2. {SOCIAL_DIR}/posts_{target_date}.txt")
    print(f"  3. {PERFORMANCE_DIR}/performance_tracker.csv")


if __name__ == "__main__":
    main()
