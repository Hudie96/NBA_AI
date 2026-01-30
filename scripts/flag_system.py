"""
Flag System for AI Review

BACKTEST-VALIDATED SYSTEM (518 games, Oct 2025 - Jan 2026):

The model excels at finding undervalued HOME teams, but fails at away teams.
Only bet when model likes HOME more than Vegas by 5+ points.

TIERS:
- PLATINUM (84.4%): GREEN zone + Model +7 vs Vegas on HOME (38-7, +61% ROI)
- GOLD (78.9%): GREEN zone + Model +5 vs Vegas on HOME (56-15, +51% ROI)
- SILVER (74.4%): Model +5 vs Vegas on HOME, any zone (99-34, +42% ROI)
- SKIP: Model favors away, or edge < 5 (losing strategy)

GREEN zone = Small spread (<3) OR B2B situation
"""
import csv
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Results CSV path
RESULTS_CSV = Path(__file__).parent.parent / 'data' / 'results.csv'


def _get_logged_games(target_date: str) -> set:
    """Get set of games already logged for a date (for deduplication)."""
    logged = set()
    if not RESULTS_CSV.exists():
        return logged

    with open(RESULTS_CSV, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['date'] == target_date:
                logged.add(row['game'])
    return logged


def calculate_home_edge_vs_vegas(prediction: Dict) -> Optional[float]:
    """
    Calculate how much more the model likes HOME than Vegas does.

    Returns:
        Positive = model more bullish on home than Vegas
        Negative = model more bullish on away than Vegas
        None = no Vegas line available
    """
    vegas_spread = prediction.get('vegas_spread')
    if vegas_spread is None:
        return None

    # Model's view: who does it favor and by how much?
    model_spread = prediction['spread']
    model_favorite = prediction['favorite']
    home_team = prediction['home_team']

    # Convert to home margin (positive = home favored)
    if model_favorite == home_team:
        model_home_margin = model_spread
    else:
        model_home_margin = -model_spread

    # Vegas spread is from home perspective (negative = home favored by that amount)
    # e.g., vegas_spread = -5 means home is 5pt favorite
    vegas_home_margin = -vegas_spread

    # Edge = how much more we like home than Vegas
    edge = model_home_margin - vegas_home_margin

    return edge


def is_green_zone(prediction: Dict) -> bool:
    """Check if game qualifies for GREEN zone (small spread or B2B)."""
    spread = prediction['spread']
    home_b2b = prediction.get('home_is_b2b', False)
    away_b2b = prediction.get('away_is_b2b', False)

    return spread < 3 or home_b2b or away_b2b


def categorize_game(prediction: Dict) -> Tuple[str, Optional[float]]:
    """
    Categorize a game into PLATINUM/GOLD/SILVER/SKIP.

    Based on backtest (518 games, Oct 2025 - Jan 2026):
    - PLATINUM: GREEN + Model +7 vs Vegas on HOME = 84.4% (38-7)
    - GOLD: GREEN + Model +5 vs Vegas on HOME = 78.9% (56-15)
    - SILVER: Model +5 vs Vegas on HOME = 74.4% (99-34)
    - SKIP: Everything else (model likes away, or edge < 5)

    Returns:
        Tuple of (zone, edge_vs_vegas)
    """
    edge = calculate_home_edge_vs_vegas(prediction)

    # No Vegas line = can't calculate edge = skip
    if edge is None:
        return "SKIP", None

    # Model must favor HOME (edge > 0) with significant margin
    green = is_green_zone(prediction)

    # Check if model actually favors home team
    model_favors_home = prediction['favorite'] == prediction['home_team']

    if not model_favors_home:
        # Model favors away = SKIP (43.4% win rate historically)
        return "SKIP", edge

    if edge >= 7 and green:
        return "PLATINUM", edge
    elif edge >= 5 and green:
        return "GOLD", edge
    elif edge >= 5:
        return "SILVER", edge
    else:
        return "SKIP", edge


def get_zone_stats() -> Dict[str, str]:
    """Return historical win rates for each zone."""
    return {
        'PLATINUM': '84.4% (38-7 in backtest, +61% ROI)',
        'GOLD': '78.9% (56-15 in backtest, +51% ROI)',
        'SILVER': '74.4% (99-34 in backtest, +42% ROI)',
        'SKIP': 'No edge or negative edge - do not bet'
    }


def log_flagged_pick(prediction: Dict, target_date: str, zone: str, edge: float) -> bool:
    """
    Log a PLATINUM/GOLD/SILVER pick to results.csv.
    """
    game_str = f"{prediction['away_team']} @ {prediction['home_team']}"

    # Check for duplicate
    logged_games = _get_logged_games(target_date)
    if game_str in logged_games:
        return False

    # Build pick string
    pick_str = f"{prediction['favorite']} -{prediction['spread']:.1f}"

    row = {
        'date': target_date,
        'game': game_str,
        'pick': pick_str,
        'spread': prediction['spread'],
        'zone': zone,
        'edge_vs_vegas': round(edge, 1) if edge else 0,
        'vegas_spread': prediction.get('vegas_spread'),
        'result': '',
        'margin': '',
        'clv': ''
    }

    # Ensure CSV exists with headers
    if not RESULTS_CSV.exists():
        RESULTS_CSV.parent.mkdir(parents=True, exist_ok=True)
        with open(RESULTS_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            writer.writeheader()

    # Append the row
    with open(RESULTS_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        writer.writerow(row)

    return True


def format_ai_review_game(prediction: Dict, rank: int, zone: str, edge: float) -> List[str]:
    """Format a single game for AI review output."""
    lines = []

    spread = prediction['spread']
    home_b2b = prediction.get('home_is_b2b', False)
    away_b2b = prediction.get('away_is_b2b', False)

    # Build reason
    reasons = []
    reasons.append(f"Model +{edge:.1f} vs Vegas")

    if spread < 3:
        reasons.append(f"Small spread ({spread:.1f})")
    if home_b2b or away_b2b:
        b2b_team = prediction['home_team'] if home_b2b else prediction['away_team']
        reasons.append(f"B2B ({b2b_team})")

    # Zone marker
    zone_markers = {
        'PLATINUM': '💎 PLATINUM',
        'GOLD': '🥇 GOLD',
        'SILVER': '🥈 SILVER'
    }
    marker = zone_markers.get(zone, zone)

    # Main line
    game_str = f"{prediction['away_team']} @ {prediction['home_team']}"
    main_line = f"{marker} {rank}. {game_str} | {prediction['favorite']} -{spread:.1f}"
    lines.append(main_line)

    # Reason
    lines.append(f"   Reason: {' + '.join(reasons)}")

    # Stats
    lines.append(f"   {prediction['home_team']}: {prediction['home_last10_record']}, {prediction['home_last10_ppg']:.0f}/{prediction['home_last10_oppg']:.0f} PPG/OPPG")
    lines.append(f"   {prediction['away_team']}: {prediction['away_last10_record']}, {prediction['away_last10_ppg']:.0f}/{prediction['away_last10_oppg']:.0f} PPG/OPPG")

    # Vegas comparison
    if prediction.get('vegas_spread') is not None:
        vegas_spread = prediction['vegas_spread']
        vegas_fav = prediction['home_team'] if vegas_spread < 0 else prediction['away_team']
        vegas_line = f"{vegas_fav} -{abs(vegas_spread):.1f}"
        lines.append(f"   Vegas: {vegas_line} | Our Edge: +{edge:.1f}")

    lines.append("")
    return lines


def generate_ai_review_file(predictions: List[Dict], target_date: str, output_dir) -> str:
    """Generate AI review file with games categorized into tiers."""
    from pathlib import Path

    # Categorize all predictions
    platinum_games = []
    gold_games = []
    silver_games = []
    skip_games = []
    logged_count = 0

    for pred in predictions:
        zone, edge = categorize_game(pred)
        pred['_zone'] = zone
        pred['_edge'] = edge

        if zone == "PLATINUM":
            platinum_games.append(pred)
            if log_flagged_pick(pred, target_date, zone, edge):
                logged_count += 1
        elif zone == "GOLD":
            gold_games.append(pred)
            if log_flagged_pick(pred, target_date, zone, edge):
                logged_count += 1
        elif zone == "SILVER":
            silver_games.append(pred)
            if log_flagged_pick(pred, target_date, zone, edge):
                logged_count += 1
        else:
            skip_games.append(pred)

    if logged_count > 0:
        print(f"[AUTO-LOG] Logged {logged_count} picks to {RESULTS_CSV}")

    # Sort by edge (highest first)
    platinum_games.sort(key=lambda x: x['_edge'] or 0, reverse=True)
    gold_games.sort(key=lambda x: x['_edge'] or 0, reverse=True)
    silver_games.sort(key=lambda x: x['_edge'] or 0, reverse=True)

    # Generate output
    output_file = Path(output_dir) / f"ai_review_{target_date}.txt"
    zone_stats = get_zone_stats()

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write(f"AXIOM BETTING PICKS - {target_date}\n")
        f.write("=" * 80 + "\n\n")

        total_plays = len(platinum_games) + len(gold_games) + len(silver_games)
        f.write(f"Total Games: {len(predictions)} | Plays: {total_plays}\n")
        f.write(f"PLATINUM: {len(platinum_games)} | GOLD: {len(gold_games)} | SILVER: {len(silver_games)} | SKIP: {len(skip_games)}\n")
        f.write("\n")

        # PLATINUM
        f.write("=" * 80 + "\n")
        f.write(f"💎 PLATINUM TIER - {zone_stats['PLATINUM']}\n")
        f.write("=" * 80 + "\n\n")
        if platinum_games:
            for i, pred in enumerate(platinum_games, 1):
                lines = format_ai_review_game(pred, i, "PLATINUM", pred['_edge'])
                f.write("\n".join(lines) + "\n")
        else:
            f.write("No PLATINUM plays today.\n\n")

        # GOLD
        f.write("=" * 80 + "\n")
        f.write(f"🥇 GOLD TIER - {zone_stats['GOLD']}\n")
        f.write("=" * 80 + "\n\n")
        if gold_games:
            for i, pred in enumerate(gold_games, 1):
                lines = format_ai_review_game(pred, i, "GOLD", pred['_edge'])
                f.write("\n".join(lines) + "\n")
        else:
            f.write("No GOLD plays today.\n\n")

        # SILVER
        f.write("=" * 80 + "\n")
        f.write(f"🥈 SILVER TIER - {zone_stats['SILVER']}\n")
        f.write("=" * 80 + "\n\n")
        if silver_games:
            for i, pred in enumerate(silver_games, 1):
                lines = format_ai_review_game(pred, i, "SILVER", pred['_edge'])
                f.write("\n".join(lines) + "\n")
        else:
            f.write("No SILVER plays today.\n\n")

        # SKIP summary
        f.write("=" * 80 + "\n")
        f.write(f"SKIP ({len(skip_games)} games) - No edge or model favors away\n")
        f.write("=" * 80 + "\n\n")
        for pred in skip_games:
            game_str = f"{pred['away_team']} @ {pred['home_team']}"
            edge = pred.get('_edge')
            reason = "No Vegas line" if edge is None else f"Edge {edge:+.1f}" if edge else "Model favors away"
            f.write(f"  - {game_str}: {reason}\n")

        # Legend
        f.write("\n" + "=" * 80 + "\n")
        f.write("STRATEGY (Backtest: 518 games, Oct 2025 - Jan 2026)\n")
        f.write("-" * 80 + "\n")
        f.write("Only bet HOME teams where model is 5+ points more bullish than Vegas.\n")
        f.write("Model excels at finding undervalued home teams (74-84% win rate).\n")
        f.write("Model FAILS at finding undervalued away teams (43% - losing).\n")
        f.write("=" * 80 + "\n")

    return str(output_file)


# Legacy function for backwards compatibility
def calculate_flag_score(prediction: Dict) -> int:
    """Legacy function - returns edge as int for compatibility."""
    edge = calculate_home_edge_vs_vegas(prediction)
    return int(edge) if edge else 0
