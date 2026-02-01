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

# Minimum average minutes to be considered a "star" for social content
MIN_STAR_MINUTES = 25


def get_star_players(conn):
    """Get set of star player names (high minutes, starters)."""
    result = conn.execute('''
        SELECT player_name, AVG(min) as avg_min, COUNT(*) as games
        FROM PlayerBox
        WHERE min > 0
        GROUP BY player_name
        HAVING games >= 10 AND avg_min >= ?
        ORDER BY avg_min DESC
    ''', (MIN_STAR_MINUTES,)).fetchall()

    return {row[0] for row in result}


def is_star_player(player_name, star_set):
    """Check if player is a star (in top usage/minutes)."""
    return player_name in star_set


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

            # Determine pick - ONLY HOME TEAMS PASS (away teams = 43% win rate in backtest)
            if spread_edge >= 5:
                # Model favors home team by 5+ points = VALID pick
                game_info['spread_pick'] = f"{home_team} {spread:+.1f}"
                if spread_edge >= 7:
                    game_info['spread_tier'] = 'PLATINUM'
                elif spread_edge >= 5:
                    game_info['spread_tier'] = 'GOLD'
            elif spread_edge >= 3:
                # Home edge 3-5 = SILVER tier
                game_info['spread_pick'] = f"{home_team} {spread:+.1f}"
                game_info['spread_tier'] = 'SILVER'
            elif spread_edge < 0:
                # Model favors AWAY team = SKIP (43% win rate, losing strategy)
                game_info['spread_pick'] = f"{away_team} {-spread:+.1f}"
                game_info['spread_tier'] = 'SKIP'
            else:
                # Edge too small (0-3) = SKIP
                game_info['spread_pick'] = f"{home_team} {spread:+.1f}"
                game_info['spread_tier'] = 'SKIP'

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
    """Get HIGH confidence prop picks with PLATINUM/GOLD/SILVER tiers.

    Only includes STAR players (25+ min avg) - no bench players.

    Tier thresholds:
    - PLATINUM: Edge >= 25%
    - GOLD: Edge >= 20%
    - SILVER: Edge >= 15%
    """
    # Get star players first
    star_players = get_star_players(conn)

    edges = find_edges_for_today(conn, target_date, min_games=min_games)

    # Filter to HIGH confidence, 15%+ edge, STAR PLAYERS ONLY
    high_conf = [e for e in edges
                 if e.get('confidence') == 'HIGH'
                 and abs(e.get('edge_pct', 0)) >= 15
                 and e.get('player_name') in star_players]

    # Parse factors JSON and add to edge dict
    for e in high_conf:
        factors = json.loads(e.get('factors', '{}'))
        e['l10_avg'] = factors.get('last_10_avg', 0) or 0
        e['season_avg'] = factors.get('season_avg', 0) or 0
        e['vs_opp_avg'] = factors.get('vs_opp_avg', 0) or 0
        e['stat'] = e.get('prop_type', '')

        # Assign tier based on edge
        edge_abs = abs(e['edge_pct'])
        if edge_abs >= 25:
            e['prop_tier'] = 'PLATINUM'
        elif edge_abs >= 20:
            e['prop_tier'] = 'GOLD'
        else:
            e['prop_tier'] = 'SILVER'

    # Group by player, keep best edge per player
    by_player = {}
    for e in high_conf:
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
            tier = g.get('spread_tier', 'SKIP')
            # Confidence based on tier, not just edge size
            if tier == 'SKIP':
                confidence = 'SKIP'
            elif tier == 'PLATINUM':
                confidence = 'HIGH'
            elif tier == 'GOLD':
                confidence = 'HIGH'
            else:
                confidence = 'MEDIUM'

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
                'tier': tier,
                'confidence': confidence,
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

        # Add ML info - ONLY for home team plays with 7+ edge
        if g.get('ml_home') and g.get('spread_edge', 0) >= 7:
            # Only home team ML (away team picks are losing strategy)
            ml_pick = f"{g['home_team']} ML ({g['ml_home']:+d})"
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

    # Add prop picks with PLATINUM/GOLD/SILVER tiers based on edge
    for p in prop_picks:
        direction = 'OVER' if p['edge'] > 0 else 'UNDER'
        team = p.get('team', '?')
        edge_abs = abs(p['edge_pct'])

        # Prop tier based on edge percentage
        if edge_abs >= 25:
            prop_tier = 'PLATINUM'
        elif edge_abs >= 20:
            prop_tier = 'GOLD'
        elif edge_abs >= 15:
            prop_tier = 'SILVER'
        else:
            prop_tier = 'BRONZE'

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
            'tier': prop_tier,
            'confidence': p['confidence'],
        })

    # Write CSV
    if rows:
        fieldnames = ['date', 'game', 'bet_type', 'player', 'pick', 'line', 'projection',
                      'edge', 'l10_avg', 'season_avg', 'tier', 'confidence']
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
        except PermissionError:
            # Try alternate filename
            filepath = PREDICTIONS_DIR / f"picks_{target_date}_{datetime.now().strftime('%H%M%S')}.csv"
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

    return filepath, rows


def get_engagement_stats(conn, target_date, prop_picks, star_players=None):
    """Generate unique statistical insights for engagement posts.

    Only includes star players (25+ min avg) that people actually know.
    """
    stats_posts = []

    # Get star players if not provided
    if star_players is None:
        star_players = get_star_players(conn)

    # Filter to star players only
    star_props = [p for p in prop_picks if p['player_name'] in star_players]

    # If no star props, use top props but limit
    if not star_props:
        star_props = prop_picks[:5]

    for p in star_props[:10]:
        player_name = p['player_name']
        opponent = p['opponent']
        stat = p.get('stat', p.get('prop_type', ''))
        team = p.get('team', '')

        # Get opponent's pace ranking
        opp_pace = conn.execute('''
            SELECT pace,
                   (SELECT COUNT(*) + 1 FROM TeamAdvancedStats t2 WHERE t2.pace > t1.pace) as pace_rank
            FROM TeamAdvancedStats t1
            WHERE team_abbrev = ?
            ORDER BY updated_at DESC LIMIT 1
        ''', (opponent,)).fetchone()

        # Get opponent's defensive rating ranking
        opp_def = conn.execute('''
            SELECT def_rating,
                   (SELECT COUNT(*) + 1 FROM TeamAdvancedStats t2 WHERE t2.def_rating < t1.def_rating) as def_rank
            FROM TeamAdvancedStats t1
            WHERE team_abbrev = ?
            ORDER BY updated_at DESC LIMIT 1
        ''', (opponent,)).fetchone()

        if opp_pace:
            pace_rank = opp_pace[1]
            if pace_rank <= 5:
                stats_posts.append({
                    'player': player_name,
                    'insight': f"Matchup Alert",
                    'stat': f"{opponent} has #{pace_rank} fastest pace in NBA",
                    'opponent': opponent,
                    'matchup_note': f"More possessions = more opportunities"
                })
            elif pace_rank >= 26:
                stats_posts.append({
                    'player': player_name,
                    'insight': f"Pace Warning",
                    'stat': f"{opponent} has #{pace_rank} slowest pace in NBA",
                    'opponent': opponent,
                    'matchup_note': f"Fewer possessions tonight"
                })

        if opp_def:
            def_rank = opp_def[1]
            if def_rank >= 25:
                stats_posts.append({
                    'player': player_name,
                    'insight': f"Soft Defense Matchup",
                    'stat': f"{opponent} ranks #{def_rank} in defensive rating",
                    'opponent': opponent,
                    'matchup_note': f"Expect inflated stats tonight"
                })

        # Get player's hot/cold streak
        recent_games = conn.execute('''
            SELECT pb.pts, pb.reb, pb.ast, g.date_time_utc
            FROM PlayerBox pb
            JOIN Games g ON pb.game_id = g.game_id
            WHERE pb.player_name = ?
            ORDER BY g.date_time_utc DESC
            LIMIT 10
        ''', (player_name,)).fetchall()

        if len(recent_games) >= 5:
            last_5_pra = [g[0] + g[1] + g[2] for g in recent_games[:5]]
            prev_5_pra = [g[0] + g[1] + g[2] for g in recent_games[5:10]] if len(recent_games) >= 10 else []

            avg_last_5 = sum(last_5_pra) / len(last_5_pra)
            avg_prev_5 = sum(prev_5_pra) / len(prev_5_pra) if prev_5_pra else 0

            # Check for streak
            line = p.get('line', 0)
            overs_l5 = sum(1 for pra in last_5_pra if pra > line)
            if overs_l5 >= 4:
                stats_posts.append({
                    'player': player_name,
                    'insight': f"Hot Streak Alert",
                    'stat': f"{overs_l5}/5 OVER {line} {stat} in last 5 games",
                    'opponent': opponent,
                    'matchup_note': f"L5 avg: {avg_last_5:.1f}"
                })
            elif overs_l5 <= 1:
                stats_posts.append({
                    'player': player_name,
                    'insight': f"Cold Streak Alert",
                    'stat': f"Only {overs_l5}/5 OVER {line} {stat} in last 5",
                    'opponent': opponent,
                    'matchup_note': f"L5 avg: {avg_last_5:.1f}"
                })

            # Check for trend change
            if avg_prev_5 > 0:
                change_pct = ((avg_last_5 - avg_prev_5) / avg_prev_5) * 100
                if abs(change_pct) >= 25:
                    trend = "UP" if change_pct > 0 else "DOWN"
                    emoji = "trending up" if trend == "UP" else "trending down"
                    stats_posts.append({
                        'player': player_name,
                        'insight': f"Usage Trend",
                        'stat': f"PRA {trend} {abs(change_pct):.0f}% over last 5 games",
                        'opponent': opponent,
                        'matchup_note': f"{avg_last_5:.1f} vs {avg_prev_5:.1f} prior"
                    })

        # Get player's vs opponent history
        vs_opp = conn.execute('''
            SELECT pb.pts, pb.reb, pb.ast
            FROM PlayerBox pb
            JOIN Games g ON pb.game_id = g.game_id
            JOIN Teams t ON pb.team_id = t.team_id
            WHERE pb.player_name = ?
            AND (g.home_team = ? OR g.away_team = ?)
            AND t.abbreviation != ?
        ''', (player_name, opponent, opponent, opponent)).fetchall()

        if len(vs_opp) >= 3:
            avg_vs_opp = sum(g[0] + g[1] + g[2] for g in vs_opp) / len(vs_opp)
            line = p.get('line', 0)
            overs_vs_opp = sum(1 for g in vs_opp if g[0] + g[1] + g[2] > line)
            hit_rate = (overs_vs_opp / len(vs_opp)) * 100

            if hit_rate >= 75 or hit_rate <= 25:
                direction = "OVER" if hit_rate >= 75 else "UNDER"
                stats_posts.append({
                    'player': player_name,
                    'insight': f"vs {opponent} History",
                    'stat': f"{int(hit_rate)}% {direction} rate in {len(vs_opp)} career matchups",
                    'opponent': opponent,
                    'matchup_note': f"Avg: {avg_vs_opp:.1f} PRA vs {opponent}"
                })

    return stats_posts[:8]  # Return top 8 insights


def generate_social_posts(conn, target_date, prop_picks, spread_picks):
    """Generate social media content - SILVER tier free, premium elsewhere.

    Only includes STAR players (25+ min avg) in engagement posts.
    """
    filepath = SOCIAL_DIR / f"posts_{target_date}.txt"

    games = get_todays_games(conn, target_date)

    # Get star players for filtering
    star_players = get_star_players(conn)
    print(f"      Star players (25+ min avg): {len(star_players)}")

    content = []

    # Header
    content.append(f"AXIOM SOCIAL CONTENT - {target_date}")
    content.append("=" * 60)
    content.append("")

    # =========================================================
    # SECTION 1: FREE PICKS (SILVER TIER ONLY)
    # =========================================================
    content.append("=" * 60)
    content.append("FREE PICKS (SILVER TIER) - Post These")
    content.append("=" * 60)
    content.append("")

    # Filter to SILVER tier spreads only
    silver_spreads = [g for g in spread_picks if g.get('spread_tier') == 'SILVER']

    if silver_spreads:
        content.append("-" * 50)
        content.append("TWITTER - FREE SPREAD PICK")
        content.append("-" * 50)
        content.append("")

        for g in silver_spreads[:2]:
            tweet = f"Free pick of the day\n\n"
            tweet += f"{g['spread_pick']}\n\n"
            tweet += f"Model edge: {g['spread_edge']:+.1f} pts\n"
            tweet += f"Our model vs Vegas\n\n"
            tweet += f"Like + RT for more free picks\n"
            tweet += f"#NBA #FreePicks #NBABets"
            content.append(tweet)
            content.append("")

    # Filter to SILVER tier props (star players only) - 15-20% edge
    silver_props = [p for p in prop_picks
                    if p['player_name'] in star_players
                    and 15 <= abs(p['edge_pct']) < 20]

    if silver_props:
        content.append("-" * 50)
        content.append("TWITTER - FREE PROP PICK")
        content.append("-" * 50)
        content.append("")

        for p in silver_props[:2]:
            direction = 'OVER' if p['edge_pct'] > 0 else 'UNDER'
            tweet = f"Free prop pick\n\n"
            tweet += f"{p['player_name']}\n"
            tweet += f"{direction} {p['line']} {p['stat']}\n\n"
            tweet += f"L10 avg: {p['l10_avg']:.1f} | Line: {p['line']}\n"
            tweet += f"Edge: {abs(p['edge_pct']):.0f}%\n\n"
            tweet += f"Like + RT for more free picks\n"
            tweet += f"#NBA #PlayerProps #NBABets"
            content.append(tweet)
            content.append("")

    # =========================================================
    # SECTION 2: ENGAGEMENT POSTS (STATISTICAL INSIGHTS)
    # =========================================================
    content.append("=" * 60)
    content.append("ENGAGEMENT POSTS - Unique Stats That Get Clicks")
    content.append("=" * 60)
    content.append("")

    # Generate engagement stats (star players only)
    engagement_stats = get_engagement_stats(conn, target_date, prop_picks, star_players)

    for i, stat in enumerate(engagement_stats, 1):
        content.append("-" * 50)
        content.append(f"STAT POST #{i}")
        content.append("-" * 50)
        content.append("")

        tweet = f"{stat['player']} {stat['insight']}\n\n"
        tweet += f"{stat['stat']}\n\n"
        tweet += f"Tonight: vs {stat['opponent']}\n"
        tweet += f"{stat['matchup_note']}\n\n"
        tweet += f"#NBA #NBAStats #PlayerProps"
        content.append(tweet)
        content.append("")

    # =========================================================
    # SECTION 3: MATCHUP INSIGHTS
    # =========================================================
    content.append("=" * 60)
    content.append("MATCHUP INSIGHT POSTS")
    content.append("=" * 60)
    content.append("")

    for g in games[:3]:
        away = g['away_team']
        home = g['home_team']

        # Get team stats for comparison
        home_stats = conn.execute('''
            SELECT pace, off_rating, def_rating
            FROM TeamAdvancedStats WHERE team_abbrev = ?
            ORDER BY updated_at DESC LIMIT 1
        ''', (home,)).fetchone()

        away_stats = conn.execute('''
            SELECT pace, off_rating, def_rating
            FROM TeamAdvancedStats WHERE team_abbrev = ?
            ORDER BY updated_at DESC LIMIT 1
        ''', (away,)).fetchone()

        if home_stats and away_stats:
            pace_diff = abs(home_stats[0] - away_stats[0])
            combined_off = home_stats[1] + away_stats[1]

            content.append("-" * 50)
            content.append(f"MATCHUP: {away} @ {home}")
            content.append("-" * 50)
            content.append("")

            if pace_diff > 3:
                fast_team = home if home_stats[0] > away_stats[0] else away
                slow_team = away if fast_team == home else home
                tweet = f"{away} @ {home} Pace Mismatch\n\n"
                tweet += f"{fast_team}: {home_stats[0] if fast_team == home else away_stats[0]:.1f} pace\n"
                tweet += f"{slow_team}: {away_stats[0] if fast_team == home else home_stats[0]:.1f} pace\n\n"
                tweet += f"Pace gap: {pace_diff:.1f}\n"
                tweet += f"Expect: {'Faster' if fast_team == home else 'Slower'} game at home\n\n"
                tweet += f"#NBA #{home} #{away}"
                content.append(tweet)
                content.append("")

    # =========================================================
    # SECTION 4: PREMIUM PICKS TEASER (Don't give full pick)
    # =========================================================
    content.append("=" * 60)
    content.append("PREMIUM TEASER POSTS - Drive to Paid")
    content.append("=" * 60)
    content.append("")

    gold_platinum = [g for g in spread_picks if g.get('spread_tier') in ['GOLD', 'PLATINUM']]
    if gold_platinum:
        content.append("-" * 50)
        content.append("PREMIUM SPREAD TEASER")
        content.append("-" * 50)
        content.append("")

        tweet = f"Today's GOLD/PLATINUM picks\n\n"
        for g in gold_platinum[:2]:
            # Don't reveal the actual pick, just tease
            game = g['game']
            tweet += f"{game} - {g.get('spread_tier')} rated\n"
        tweet += f"\nModel edge: {abs(gold_platinum[0].get('spread_edge', 0)):.1f}+ pts\n\n"
        tweet += f"Get picks: [link]\n"
        tweet += f"#NBA #NBABets #PremiumPicks"
        content.append(tweet)
        content.append("")

    # Premium star player props teaser (GOLD/PLATINUM only)
    star_props = [p for p in prop_picks if p['player_name'] in star_players and abs(p['edge_pct']) >= 20][:5]
    if star_props:
        content.append("-" * 50)
        content.append("PREMIUM PROPS TEASER (Star Players)")
        content.append("-" * 50)
        content.append("")

        tweet = f"Today's GOLD/PLATINUM player props\n\n"
        for p in star_props[:3]:
            # Tease the player, not the full pick
            tier = 'PLATINUM' if abs(p['edge_pct']) >= 25 else 'GOLD'
            tweet += f"{p['player_name']} ({tier}) - Edge: {abs(p['edge_pct']):.0f}%\n"
        tweet += f"\nOur model vs Vegas lines\n"
        tweet += f"Get full picks: [link]\n\n"
        tweet += f"#NBA #PlayerProps #NBABets"
        content.append(tweet)
        content.append("")

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
    plat_count = len([p for p in prop_picks if p.get('prop_tier') == 'PLATINUM'])
    gold_count = len([p for p in prop_picks if p.get('prop_tier') == 'GOLD'])
    silver_count = len([p for p in prop_picks if p.get('prop_tier') == 'SILVER'])
    print(f"      Props: {len(prop_picks)} total (PLATINUM: {plat_count}, GOLD: {gold_count}, SILVER: {silver_count})")
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
