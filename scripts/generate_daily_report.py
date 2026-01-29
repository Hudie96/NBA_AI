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

    with open(filepath, 'w') as f:
        f.write(report)

    print(f"\nReport saved to: {filepath}")
    print("\n" + "=" * 60)
    print(report)


if __name__ == "__main__":
    main()
