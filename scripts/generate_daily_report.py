"""
AXIOM Daily Report Generator

Generates a comprehensive markdown report with:
- Spread & Total edges
- Player props
- Parlay recommendations
- Historical performance

Usage:
    python scripts/generate_daily_report.py
    python scripts/generate_daily_report.py --date 2026-01-28
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
OUTPUT_DIR = PROJECT_ROOT / "outputs"


def get_spread_total_edges(target_date):
    """Get spread and total edge picks."""
    try:
        from scripts.spread_total_edges import SpreadTotalEdges
        analyzer = SpreadTotalEdges()

        target = datetime.strptime(target_date, '%Y-%m-%d').date()
        today = datetime.now().date()
        include_final = target < today

        games = analyzer.get_todays_games(target_date, include_final=include_final)
        picks = []

        for game in games:
            analysis = analyzer.analyze_game(game)

            if analysis['spread_edge']:
                picks.append({
                    'type': 'SPREAD',
                    'game': f"{analysis['away_team']} @ {analysis['home_team']}",
                    'pick': f"{analysis['underdog']} +{abs(analysis['spread']):.1f}",
                    'edge': analysis['spread_edge']['edge_type'],
                    'hit_rate': analysis['spread_edge']['historical']['hit_rate'],
                })

            if analysis['total_edge']:
                picks.append({
                    'type': 'TOTAL',
                    'game': f"{analysis['away_team']} @ {analysis['home_team']}",
                    'pick': f"UNDER {analysis['total_line']:.1f}" if analysis['total_line'] else "UNDER",
                    'edge': analysis['total_edge']['edge_type'],
                    'hit_rate': analysis['total_edge']['historical']['hit_rate'],
                    'warning': analysis['total_edge'].get('warning')
                })

        analyzer.close()
        return picks
    except Exception as e:
        print(f"  Spread/total error: {e}")
        return []


def get_props_edges(conn, target_date):
    """Get player prop edges."""
    try:
        df = pd.read_sql(f"""
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
            ORDER BY ABS(edge_pct) DESC
        """, conn)
        return df.to_dict('records')
    except:
        return []


def get_parlay(target_date):
    """Get optimal parlay."""
    try:
        from scripts.build_parlay_v2 import (
            get_todays_games, find_all_edges, select_parlay_legs,
            calculate_parlay, assess_parlay_quality
        )

        games = get_todays_games(target_date)
        edges = find_all_edges(games)
        parlay = select_parlay_legs(edges, 5)

        if parlay:
            prob, payout, roi = calculate_parlay(parlay)
            quality, bet_size = assess_parlay_quality(prob, roi)
            return {
                'legs': parlay,
                'combined_prob': prob,
                'payout': payout,
                'roi': roi,
                'quality': quality,
                'bet_size': bet_size
            }
        return None
    except Exception as e:
        print(f"  Parlay error: {e}")
        return None


def get_performance(conn):
    """Get historical performance."""
    try:
        # Props performance
        props_df = pd.read_sql("""
            SELECT
                confidence,
                COUNT(*) as total,
                SUM(CASE WHEN hit = 1 THEN 1 ELSE 0 END) as wins
            FROM props_edges
            WHERE hit IS NOT NULL
            GROUP BY confidence
        """, conn)

        return props_df.to_dict('records')
    except:
        return []


def get_todays_teams(target_date):
    """Get teams playing on the target date."""
    conn = sqlite3.connect(DB_PATH)
    try:
        games = pd.read_sql(f"""
            SELECT home_team, away_team
            FROM Games
            WHERE DATE(date_time_utc) = '{target_date}'
        """, conn)
        teams = set(games['home_team'].tolist() + games['away_team'].tolist())
        return teams
    except:
        return set()
    finally:
        conn.close()


def get_social_media_nuggets(conn, target_date):
    """Get stat nuggets relevant to today's games for social media posts."""
    nuggets = []
    teams_playing = get_todays_teams(target_date)

    if not teams_playing:
        return []

    # 1. Matchup dominance - players who crush opponents they face today
    try:
        matchups = pd.read_sql("""
            SELECT
                player_name,
                opponent,
                team,
                AVG(points) as avg_pts,
                AVG(rebounds) as avg_reb,
                AVG(assists) as avg_ast,
                COUNT(*) as games,
                MAX(points) as max_pts
            FROM player_game_logs
            GROUP BY player_name, opponent
            HAVING games >= 3
        """, conn)

        # Get season averages
        season_avgs = pd.read_sql("""
            SELECT player_name, team, AVG(points) as season_pts
            FROM player_game_logs
            GROUP BY player_name
        """, conn)
        avg_map = dict(zip(season_avgs['player_name'], season_avgs['season_pts']))
        team_map = dict(zip(season_avgs['player_name'], season_avgs['team']))

        for _, row in matchups.iterrows():
            # Check if this matchup is happening today
            player_team = team_map.get(row['player_name'])
            if row['opponent'] not in teams_playing or player_team not in teams_playing:
                continue

            season_avg = avg_map.get(row['player_name'], 0)
            if season_avg == 0:
                continue

            diff_pct = (row['avg_pts'] - season_avg) / season_avg * 100

            if diff_pct >= 20 and row['avg_pts'] >= 20:
                nuggets.append({
                    'type': 'matchup',
                    'priority': 1,
                    'hook': f"{row['player_name']} OWNS {row['opponent']}",
                    'stat': f"{row['avg_pts']:.1f} PPG vs {row['opponent']} ({int(row['games'])} games)",
                    'context': f"Season avg: {season_avg:.1f} | Career high vs them: {int(row['max_pts'])}",
                    'tweet': f"🎯 {row['player_name']} averages {row['avg_pts']:.1f} PTS vs {row['opponent']}\n\nThat's {diff_pct:.0f}% above his season avg of {season_avg:.1f}\n\nCareer high vs them: {int(row['max_pts'])} 🔥\n\n#NBA #NBABets #GamblingTwitter",
                    'score': diff_pct + row['avg_pts']
                })
    except Exception as e:
        print(f"  Matchup nuggets error: {e}")

    # 2. Player streaks
    try:
        for stat, label in [('points', 'PTS'), ('assists', 'AST'), ('rebounds', 'REB')]:
            players = pd.read_sql(f"""
                SELECT DISTINCT player_name, team, AVG({stat}) as season_avg
                FROM player_game_logs
                GROUP BY player_name
                HAVING COUNT(*) >= 20
            """, conn)

            for _, player in players.iterrows():
                if player['team'] not in teams_playing:
                    continue

                recent = pd.read_sql(f"""
                    SELECT game_date, {stat} as value
                    FROM player_game_logs
                    WHERE player_name = ?
                    ORDER BY game_date DESC
                    LIMIT 10
                """, conn, params=(player['player_name'],))

                if len(recent) < 5:
                    continue

                # Check over streak
                over_streak = 0
                for _, game in recent.iterrows():
                    if game['value'] > player['season_avg']:
                        over_streak += 1
                    else:
                        break

                if over_streak >= 5:
                    avg_streak = recent.head(over_streak)['value'].mean()
                    nuggets.append({
                        'type': 'streak',
                        'priority': 2,
                        'hook': f"{player['player_name']} is ON FIRE",
                        'stat': f"OVER {player['season_avg']:.1f} {label} in {over_streak} straight",
                        'context': f"Averaging {avg_streak:.1f} during streak",
                        'tweet': f"🔥 {player['player_name']} has gone OVER {player['season_avg']:.1f} {label} in {over_streak} STRAIGHT games\n\nAveraging {avg_streak:.1f} during the streak\n\nRide the hot hand? 🎰\n\n#NBA #PlayerProps #NBABets",
                        'score': over_streak * 10
                    })

                # Check under streak
                under_streak = 0
                for _, game in recent.iterrows():
                    if game['value'] < player['season_avg']:
                        under_streak += 1
                    else:
                        break

                if under_streak >= 5:
                    avg_streak = recent.head(under_streak)['value'].mean()
                    nuggets.append({
                        'type': 'streak',
                        'priority': 2,
                        'hook': f"{player['player_name']} in a SLUMP",
                        'stat': f"UNDER {player['season_avg']:.1f} {label} in {under_streak} straight",
                        'context': f"Averaging just {avg_streak:.1f} during streak",
                        'tweet': f"📉 {player['player_name']} has gone UNDER {player['season_avg']:.1f} {label} in {under_streak} STRAIGHT games\n\nAveraging just {avg_streak:.1f} during the slump\n\nFade the cold streak? ❄️\n\n#NBA #PlayerProps #NBABets",
                        'score': under_streak * 10
                    })
    except Exception as e:
        print(f"  Streak nuggets error: {e}")

    # 3. DVP matchups (team defense vs position)
    try:
        dvp = pd.read_sql("""
            SELECT team, position, stat, avg_allowed, league_avg, diff_from_avg, rank
            FROM defense_vs_position
            WHERE rank <= 3 OR rank >= 28
        """, conn)

        for _, row in dvp.iterrows():
            if row['team'] not in teams_playing:
                continue

            if row['rank'] <= 3:  # Worst defense
                nuggets.append({
                    'type': 'dvp',
                    'priority': 3,
                    'hook': f"{row['team']} gets TORCHED by {row['position']}s",
                    'stat': f"Allow {row['avg_allowed']:.1f} {row['stat']} (#{int(row['rank'])} worst)",
                    'context': f"League avg: {row['league_avg']:.1f}",
                    'tweet': f"🚨 {row['team']} allows {row['avg_allowed']:.1f} {row['stat']} to {row['position']}s\n\nThat's #{int(row['rank'])} WORST in the NBA\n\nLeague avg: {row['league_avg']:.1f}\n\nTarget {row['position']}s vs {row['team']} 🎯\n\n#NBA #NBABets #DVP",
                    'score': row['diff_from_avg']
                })
            else:  # Best defense
                nuggets.append({
                    'type': 'dvp',
                    'priority': 3,
                    'hook': f"{row['team']} LOCKS DOWN {row['position']}s",
                    'stat': f"Allow just {row['avg_allowed']:.1f} {row['stat']} (#{int(row['rank'])} best)",
                    'context': f"League avg: {row['league_avg']:.1f}",
                    'tweet': f"🔒 {row['team']} allows just {row['avg_allowed']:.1f} {row['stat']} to {row['position']}s\n\nThat's #{int(row['rank'])} BEST defense in the NBA\n\nFade {row['position']}s vs {row['team']} ❌\n\n#NBA #NBABets #DVP",
                    'score': abs(row['diff_from_avg'])
                })
    except Exception as e:
        print(f"  DVP nuggets error: {e}")

    # 4. B2B fade opportunities
    try:
        # Check if any team is on B2B today
        yesterday = (datetime.strptime(target_date, '%Y-%m-%d') - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        yesterday_teams = get_todays_teams(yesterday)
        b2b_teams = teams_playing.intersection(yesterday_teams)

        if b2b_teams:
            b2b_impact = pd.read_sql("""
                SELECT
                    player_name,
                    team,
                    AVG(CASE WHEN is_b2b = 0 THEN points END) as rest_pts,
                    AVG(CASE WHEN is_b2b = 1 THEN points END) as b2b_pts,
                    COUNT(CASE WHEN is_b2b = 1 THEN 1 END) as b2b_games
                FROM player_game_logs
                GROUP BY player_name
                HAVING b2b_games >= 5 AND rest_pts > 15
            """, conn)

            for _, row in b2b_impact.iterrows():
                if row['team'] not in b2b_teams:
                    continue
                if row['rest_pts'] is None or row['b2b_pts'] is None:
                    continue

                drop = row['rest_pts'] - row['b2b_pts']
                drop_pct = drop / row['rest_pts'] * 100

                if drop_pct >= 15:
                    nuggets.append({
                        'type': 'b2b',
                        'priority': 1,
                        'hook': f"{row['player_name']} STRUGGLES on B2Bs",
                        'stat': f"{row['rest_pts']:.1f} PPG normal → {row['b2b_pts']:.1f} on B2B",
                        'context': f"{row['team']} on back-to-back TODAY",
                        'tweet': f"😴 {row['player_name']} on a BACK-TO-BACK today\n\nNormal: {row['rest_pts']:.1f} PPG\nOn B2B: {row['b2b_pts']:.1f} PPG\n\nThat's a {drop_pct:.0f}% DROP 📉\n\nFade the tired legs? 🤔\n\n#NBA #PlayerProps #B2B",
                        'score': drop_pct
                    })
    except Exception as e:
        print(f"  B2B nuggets error: {e}")

    # Sort by priority then score
    nuggets.sort(key=lambda x: (x['priority'], -x['score']))
    return nuggets


def generate_markdown_report(target_date):
    """Generate comprehensive markdown report."""

    print(f"Generating report for {target_date}...")

    conn = sqlite3.connect(DB_PATH)

    # Gather all data
    print("  Fetching spread/total edges...")
    spread_edges = get_spread_total_edges(target_date)

    print("  Fetching props edges...")
    props = get_props_edges(conn, target_date)

    print("  Building parlay...")
    parlay = get_parlay(target_date)

    print("  Getting performance...")
    performance = get_performance(conn)

    print("  Generating social media nuggets...")
    nuggets = get_social_media_nuggets(conn, target_date)

    conn.close()

    # Build markdown
    lines = []

    # Header
    lines.append(f"# AXIOM Daily Report - {target_date}")
    lines.append(f"")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"")
    lines.append("---")
    lines.append("")

    # Summary
    lines.append("## Summary")
    lines.append("")
    spread_count = len([p for p in spread_edges if p['type'] == 'SPREAD'])
    total_count = len([p for p in spread_edges if p['type'] == 'TOTAL'])
    lines.append(f"- **Spread plays:** {spread_count}")
    lines.append(f"- **Total plays:** {total_count}")
    lines.append(f"- **Prop plays:** {len(props)}")
    if parlay:
        lines.append(f"- **Parlay:** {len(parlay['legs'])}-leg ({parlay['quality']})")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Spread Edges
    lines.append("## Spread Plays (Underdog ATS)")
    lines.append("")
    spread_plays = [p for p in spread_edges if p['type'] == 'SPREAD']
    if spread_plays:
        lines.append("| Game | Pick | Edge Type | Hit Rate |")
        lines.append("|------|------|-----------|----------|")
        for p in spread_plays:
            hr = p['hit_rate'] if p['hit_rate'] <= 1 else p['hit_rate'] / 100
            lines.append(f"| {p['game']} | **{p['pick']}** | {p['edge']} | {hr*100:.1f}% |")
    else:
        lines.append("*No qualifying spread plays today*")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Total Edges
    lines.append("## Total Plays (Under)")
    lines.append("")
    total_plays = [p for p in spread_edges if p['type'] == 'TOTAL']
    if total_plays:
        lines.append("| Game | Pick | Edge Type | Hit Rate |")
        lines.append("|------|------|-----------|----------|")
        for p in total_plays:
            warning = " [!]" if p.get('warning') else ""
            hr = p['hit_rate'] if p['hit_rate'] <= 1 else p['hit_rate'] / 100
            lines.append(f"| {p['game']} | **{p['pick']}** | {p['edge']} | {hr*100:.1f}%{warning} |")
    else:
        lines.append("*No qualifying total plays today*")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Props
    lines.append("## Player Props")
    lines.append("")
    if props:
        # High confidence
        high_props = [p for p in props if p['confidence'] == 'HIGH']
        med_props = [p for p in props if p['confidence'] == 'MEDIUM']
        low_props = [p for p in props if p['confidence'] == 'LOW']

        if high_props:
            lines.append("### HIGH Confidence")
            lines.append("")
            lines.append("| Player | Prop | Line | Pick | Edge | Tier |")
            lines.append("|--------|------|------|------|------|------|")
            for p in high_props:
                lines.append(f"| {p['player_name']} | {p['prop_type']} | {p['line']} | **{p['pick']}** | {p['edge_pct']:+.1f}% | {p['stat_tier']} |")
            lines.append("")

        if med_props:
            lines.append("### MEDIUM Confidence")
            lines.append("")
            lines.append("| Player | Prop | Line | Pick | Edge | Tier |")
            lines.append("|--------|------|------|------|------|------|")
            for p in med_props:
                lines.append(f"| {p['player_name']} | {p['prop_type']} | {p['line']} | **{p['pick']}** | {p['edge_pct']:+.1f}% | {p['stat_tier']} |")
            lines.append("")

        if low_props:
            lines.append("### LOW Confidence")
            lines.append("")
            lines.append("| Player | Prop | Line | Pick | Edge |")
            lines.append("|--------|------|------|------|------|")
            for p in low_props[:5]:  # Limit to top 5
                lines.append(f"| {p['player_name']} | {p['prop_type']} | {p['line']} | {p['pick']} | {p['edge_pct']:+.1f}% |")
            lines.append("")
    else:
        lines.append("*No prop edges found for today*")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Parlay
    lines.append("## Parlay of the Day")
    lines.append("")
    if parlay:
        lines.append(f"**Quality:** {parlay['quality']}")
        lines.append(f"**Recommended Bet:** {parlay['bet_size']}")
        lines.append("")
        lines.append("| Leg | Pick | Hit Rate |")
        lines.append("|-----|------|----------|")
        for i, leg in enumerate(parlay['legs'], 1):
            lines.append(f"| {i} | **{leg['pick']}** ({leg['game']}) | {leg['hit_rate']*100:.1f}% |")
        lines.append("")
        lines.append(f"**Combined Probability:** {parlay['combined_prob']*100:.1f}%")
        lines.append(f"**Payout:** {parlay['payout']:.1f}x")
        lines.append(f"**Expected ROI:** {parlay['roi']:+.0f}%")
    else:
        lines.append("*No qualifying parlay today*")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Performance
    lines.append("## Historical Performance")
    lines.append("")
    if performance:
        lines.append("| Confidence | Record | Win Rate |")
        lines.append("|------------|--------|----------|")
        for p in performance:
            wins = p['wins'] or 0
            total = p['total'] or 0
            rate = (wins/total*100) if total > 0 else 0
            lines.append(f"| {p['confidence']} | {wins}W-{total-wins}L | {rate:.1f}% |")
    else:
        lines.append("*No historical data yet*")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Social Media Section
    lines.append("## Social Media Posts")
    lines.append("")
    lines.append("Ready-to-post content based on today's matchups:")
    lines.append("")

    if nuggets:
        # Group by type
        nugget_types = {
            'matchup': ('Matchup Dominance', []),
            'streak': ('Hot/Cold Streaks', []),
            'dvp': ('Defense vs Position', []),
            'b2b': ('Back-to-Back Fades', [])
        }

        for n in nuggets:
            if n['type'] in nugget_types:
                nugget_types[n['type']][1].append(n)

        for ntype, (label, items) in nugget_types.items():
            if items:
                lines.append(f"### {label}")
                lines.append("")
                for item in items[:3]:  # Top 3 per category
                    lines.append(f"**{item['hook']}**")
                    lines.append(f"- {item['stat']}")
                    lines.append(f"- {item['context']}")
                    lines.append("")
                    lines.append("```")
                    lines.append(item['tweet'])
                    lines.append("```")
                    lines.append("")
    else:
        lines.append("*No notable nuggets for today's slate*")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Footer
    lines.append("## Bankroll Management")
    lines.append("")
    lines.append("| Play Type | Recommended Bet |")
    lines.append("|-----------|-----------------|")
    lines.append("| HIGH confidence prop | 2% bankroll |")
    lines.append("| MEDIUM confidence prop | 1% bankroll |")
    lines.append("| Spread/Total edge | 1-2% bankroll |")
    lines.append("| Parlay (STRONG) | 1% bankroll |")
    lines.append("| Parlay (GOOD) | 0.5% bankroll |")
    lines.append("| Parlay (MARGINAL) | 0.25% bankroll |")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("*Generated by AXIOM - Data-driven NBA betting*")

    return "\n".join(lines)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="Target date YYYY-MM-DD")
    args = parser.parse_args()

    target_date = args.date or date.today().isoformat()

    # Generate report
    report = generate_markdown_report(target_date)

    # Save to file
    OUTPUT_DIR.mkdir(exist_ok=True)
    filename = f"DAILY_REPORT_{target_date}.md"
    filepath = OUTPUT_DIR / filename

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(report)

    print(f"\nReport saved to: {filepath}")
    print("\n" + "=" * 60)
    # Print without emojis for console compatibility
    try:
        print(report)
    except UnicodeEncodeError:
        print(report.encode('ascii', 'ignore').decode('ascii'))


if __name__ == "__main__":
    main()
