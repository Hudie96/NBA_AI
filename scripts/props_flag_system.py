"""
Props Flag System for AI Review

Confidence scoring system for player props, modeled after the spread flag system.

Edge thresholds based on props backtest methodology:
- 15%+ edge: HIGH confidence (historically profitable)
- 10-15% edge: MEDIUM confidence (selective value)
- 5-10% edge: LOW confidence (marginal)
- <5% edge: NO BET (insufficient edge)

Additional factors that improve hit rate:
- Player on rest (not B2B): +boost
- Favorable matchup (DVP weakness): +boost
- Consistent performer (low variance): +boost
- High sample size vs opponent: +boost
"""
import csv
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Results CSV path for props
PROPS_RESULTS_CSV = Path(__file__).parent.parent / 'data' / 'props_results.csv'


def _get_logged_props(target_date: str) -> set:
    """Get set of props already logged for a date (for deduplication)."""
    logged = set()
    if not PROPS_RESULTS_CSV.exists():
        return logged

    with open(PROPS_RESULTS_CSV, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['date'] == target_date:
                key = f"{row['player_name']}_{row['prop_type']}"
                logged.add(key)
    return logged


def log_flagged_prop(prop: Dict, target_date: str) -> bool:
    """
    Log a GREEN/YELLOW flagged prop pick to props_results.csv.

    Args:
        prop: Prop prediction dict
        target_date: Date string (YYYY-MM-DD)

    Returns:
        True if logged, False if duplicate or error
    """
    player = prop['player_name']
    prop_type = prop['prop_type']
    key = f"{player}_{prop_type}"

    # Check for duplicate
    logged_props = _get_logged_props(target_date)
    if key in logged_props:
        return False

    row = {
        'date': target_date,
        'player_name': player,
        'opponent': prop.get('opponent', ''),
        'prop_type': prop_type,
        'line': prop.get('line', 0),
        'projection': prop.get('projection', 0),
        'edge': prop.get('edge', 0),
        'edge_pct': prop.get('edge_pct', 0),
        'pick': prop.get('pick', ''),
        'confidence': prop.get('confidence', ''),
        'flag_score': prop.get('flag_score', 0),
        'actual': '',
        'result': '',
        'clv': ''
    }

    # Ensure CSV exists with headers
    if not PROPS_RESULTS_CSV.exists():
        PROPS_RESULTS_CSV.parent.mkdir(parents=True, exist_ok=True)
        with open(PROPS_RESULTS_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            writer.writeheader()

    # Append the row
    with open(PROPS_RESULTS_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        writer.writerow(row)

    return True


def calculate_props_flag_score(prop: Dict) -> int:
    """
    Calculate flag score for a prop prediction.

    Scoring factors:
    - Edge >= 15%: +5 (proven profitable threshold)
    - Edge >= 10%: +3
    - Edge >= 5%: +1
    - Player on rest (not B2B): +2
    - High sample vs opponent (3+ games): +2
    - Consistent performer (low variance): +1
    - Minutes stability (avg min > 25): +1

    Zone thresholds:
    - GREEN: flag_score >= 8 (Best props)
    - YELLOW: flag_score >= 5 (Signal props)
    - RED: flag_score < 5 (Skip)

    Args:
        prop: Prop prediction dict with keys:
            - edge_pct: Edge percentage
            - player_is_b2b: Boolean
            - vs_opp_games: Games vs this opponent
            - variance: Stat variance (std dev)
            - avg_minutes: Average minutes played

    Returns:
        Integer flag score
    """
    score = 0

    # Edge-based scoring (primary factor)
    edge_pct = abs(prop.get('edge_pct', 0))
    if edge_pct >= 15:
        score += 5  # Strong edge
    elif edge_pct >= 10:
        score += 3  # Good edge
    elif edge_pct >= 5:
        score += 1  # Marginal edge

    # Rest advantage
    player_is_b2b = prop.get('player_is_b2b', False)
    if not player_is_b2b:
        score += 2  # Rested player

    # Sample size vs opponent
    vs_opp_games = prop.get('vs_opp_games', 0)
    if vs_opp_games >= 3:
        score += 2  # Good sample vs this opponent

    # Consistency (low variance is good for props)
    variance = prop.get('variance', 0)
    avg_stat = prop.get('projection', 1)
    if avg_stat > 0 and variance > 0:
        cv = variance / avg_stat  # Coefficient of variation
        if cv < 0.3:  # Low variance
            score += 1

    # Minutes stability
    avg_minutes = prop.get('avg_minutes', 0)
    if avg_minutes >= 28:
        score += 1  # High usage player

    return score


def categorize_prop(prop: Dict) -> str:
    """
    Categorize a prop prediction into Green/Yellow/Red zone.

    GREEN (Best Props - target 60%+ hit rate):
        - flag_score >= 8
        - Strong edge + multiple supporting factors

    YELLOW (Signal Props - target 55%+ hit rate):
        - flag_score >= 5
        - Moderate edge with some support

    RED (Skip - insufficient edge):
        - flag_score < 5
        - Low edge or unfavorable factors

    Args:
        prop: Prop prediction dict

    Returns:
        "GREEN", "YELLOW", or "RED"
    """
    flag_score = calculate_props_flag_score(prop)

    if flag_score >= 8:
        return "GREEN"
    elif flag_score >= 5:
        return "YELLOW"
    else:
        return "RED"


def get_confidence_level(edge_pct: float, flag_score: int) -> str:
    """
    Determine confidence level based on edge and flag score.

    Args:
        edge_pct: Edge percentage (absolute value)
        flag_score: Calculated flag score

    Returns:
        "HIGH", "MEDIUM", "LOW", or "NONE"
    """
    edge_pct = abs(edge_pct)

    if edge_pct >= 15 and flag_score >= 8:
        return "HIGH"
    elif edge_pct >= 10 and flag_score >= 5:
        return "MEDIUM"
    elif edge_pct >= 5:
        return "LOW"
    else:
        return "NONE"


def get_props_zone_stats() -> Dict[str, str]:
    """Return target win rates for each zone."""
    return {
        'GREEN': 'Target 60%+ (15%+ edge with support)',
        'YELLOW': 'Target 55%+ (10-15% edge)',
        'RED': 'Skip (<10% edge or unfavorable)'
    }


def format_props_ai_review_item(prop: Dict, rank: int, zone: str) -> List[str]:
    """
    Format a single prop for AI review output.

    Args:
        prop: Prop prediction dict
        rank: Rank/number in the zone
        zone: "GREEN", "YELLOW", or "RED"

    Returns:
        List of formatted lines
    """
    lines = []

    player = prop['player_name']
    opponent = prop.get('opponent', 'UNK')
    prop_type = prop['prop_type']
    line = prop.get('line', 0)
    projection = prop.get('projection', 0)
    edge = prop.get('edge', 0)
    edge_pct = prop.get('edge_pct', 0)
    pick = prop.get('pick', '')
    confidence = prop.get('confidence', '')

    # Build reason
    reasons = []
    if abs(edge_pct) >= 15:
        reasons.append(f"Strong Edge ({edge_pct:+.1f}%)")
    elif abs(edge_pct) >= 10:
        reasons.append(f"Good Edge ({edge_pct:+.1f}%)")

    if not prop.get('player_is_b2b', False):
        reasons.append("Rested")

    if prop.get('vs_opp_games', 0) >= 3:
        reasons.append(f"vs {opponent}: {prop.get('vs_opp_games')}g sample")

    # Main line
    main_line = f"{rank}. {player} {prop_type} {pick} {line}"
    if zone == "GREEN":
        main_line = f"**BEST PROP** {main_line}"
    elif zone == "YELLOW":
        main_line = f"SIGNAL {main_line}"

    lines.append(main_line)

    # Details
    lines.append(f"   vs {opponent} | Proj: {projection:.1f} | Line: {line} | Edge: {edge:+.1f} ({edge_pct:+.1f}%)")

    # Reason
    if reasons:
        lines.append(f"   Reason: {' + '.join(reasons)}")

    # Stats context
    last_10 = prop.get('last_10_avg')
    season = prop.get('season_avg')
    vs_opp = prop.get('vs_opp_avg')

    if last_10 is not None:
        stats_line = f"   L10: {last_10:.1f}"
        if season is not None:
            stats_line += f" | Season: {season:.1f}"
        if vs_opp is not None:
            stats_line += f" | vs{opponent}: {vs_opp:.1f}"
        lines.append(stats_line)

    lines.append("")  # Blank line

    return lines


def generate_props_ai_review_file(props: List[Dict], target_date: str, output_dir) -> str:
    """
    Generate AI review file for props with games categorized into zones.

    Args:
        props: List of prop prediction dicts
        target_date: Date string (YYYY-MM-DD)
        output_dir: Output directory path

    Returns:
        Path to generated file
    """
    from pathlib import Path

    # Categorize all props
    green_props = []
    yellow_props = []
    red_props = []
    logged_count = 0

    for prop in props:
        zone = categorize_prop(prop)
        flag_score = calculate_props_flag_score(prop)
        prop['flag_score'] = flag_score
        prop['zone'] = zone

        if zone == "GREEN":
            green_props.append(prop)
            if log_flagged_prop(prop, target_date):
                logged_count += 1
        elif zone == "YELLOW":
            yellow_props.append(prop)
            if log_flagged_prop(prop, target_date):
                logged_count += 1
        else:
            red_props.append(prop)

    if logged_count > 0:
        print(f"[AUTO-LOG] Logged {logged_count} flagged props to {PROPS_RESULTS_CSV}")

    # Sort by edge (highest first)
    green_props.sort(key=lambda x: abs(x.get('edge_pct', 0)), reverse=True)
    yellow_props.sort(key=lambda x: abs(x.get('edge_pct', 0)), reverse=True)

    # Generate output
    output_file = Path(output_dir) / f"props_ai_review_{target_date}.txt"
    zone_stats = get_props_zone_stats()

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write(f"PROPS AI REVIEW - {target_date}\n")
        f.write("=" * 80 + "\n\n")

        f.write(f"Total Props Analyzed: {len(props)}\n")
        f.write(f"GREEN (Best): {len(green_props)} | YELLOW (Signal): {len(yellow_props)} | RED (Skip): {len(red_props)}\n")
        f.write("\n" + "=" * 80 + "\n\n")

        # GREEN Zone
        if green_props:
            f.write(f"=== **BEST PROP** BEST PROPS (GREEN ZONE - {zone_stats['GREEN']}) ===\n\n")
            for i, prop in enumerate(green_props, 1):
                lines = format_props_ai_review_item(prop, i, "GREEN")
                for line in lines:
                    f.write(line + "\n")
        else:
            f.write(f"=== **BEST PROP** BEST PROPS (GREEN ZONE - {zone_stats['GREEN']}) ===\n\n")
            f.write("No GREEN zone props today.\n\n")

        # YELLOW Zone
        f.write("=" * 80 + "\n")
        if yellow_props:
            f.write(f"=== SIGNAL SIGNAL PROPS (YELLOW ZONE - {zone_stats['YELLOW']}) ===\n\n")
            for i, prop in enumerate(yellow_props, 1):
                lines = format_props_ai_review_item(prop, i, "YELLOW")
                for line in lines:
                    f.write(line + "\n")
        else:
            f.write(f"=== SIGNAL SIGNAL PROPS (YELLOW ZONE - {zone_stats['YELLOW']}) ===\n\n")
            f.write("No YELLOW zone props today.\n\n")

        # RED Zone summary
        f.write("=" * 80 + "\n")
        f.write(f"=== SKIP ({len(red_props)} props below threshold) ===\n\n")
        if red_props:
            # Group by stat type
            by_stat = {}
            for prop in red_props:
                stat = prop['prop_type']
                if stat not in by_stat:
                    by_stat[stat] = 0
                by_stat[stat] += 1
            for stat, count in sorted(by_stat.items()):
                f.write(f"  {stat}: {count} props skipped\n")
        else:
            f.write("  None\n")

        # Legend
        f.write("\n" + "=" * 80 + "\n")
        f.write("LEGEND:\n")
        f.write("-" * 80 + "\n")
        f.write("Edge = (Projection - Line) / Line * 100\n")
        f.write("GREEN = 15%+ edge with supporting factors\n")
        f.write("YELLOW = 10-15% edge\n")
        f.write("RED = <10% edge or insufficient data\n")
        f.write("\n")
        f.write("Props require minimum 10 prior games for projection\n")
        f.write("=" * 80 + "\n")

    return str(output_file)


def get_props_results_summary() -> Dict:
    """
    Get summary of props results by confidence level.

    Returns:
        Dict with summary stats
    """
    if not PROPS_RESULTS_CSV.exists():
        return {'total': 0, 'pending': 0, 'by_confidence': {}}

    results = {'total': 0, 'pending': 0, 'completed': 0, 'by_confidence': {}}

    with open(PROPS_RESULTS_CSV, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            results['total'] += 1

            if not row.get('result'):
                results['pending'] += 1
            else:
                results['completed'] += 1

                conf = row.get('confidence', 'UNKNOWN')
                if conf not in results['by_confidence']:
                    results['by_confidence'][conf] = {'wins': 0, 'losses': 0, 'pushes': 0}

                if row['result'] == 'WIN':
                    results['by_confidence'][conf]['wins'] += 1
                elif row['result'] == 'LOSS':
                    results['by_confidence'][conf]['losses'] += 1
                else:
                    results['by_confidence'][conf]['pushes'] += 1

    return results
