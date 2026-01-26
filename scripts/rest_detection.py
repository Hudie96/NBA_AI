"""
Rest and Back-to-Back Detection

Analyzes team rest patterns and flags potential fatigue situations.
"""
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Tuple


def get_team_rest_info(team: str, game_date: str, conn: sqlite3.Connection) -> Dict:
    """
    Get rest information for a team before a specific game.

    Args:
        team: Team abbreviation (e.g., "LAL", "BOS")
        game_date: Game date in YYYY-MM-DD format
        conn: Database connection

    Returns:
        Dict with rest information:
        - last_game_date: Date of last game
        - days_rest: Days since last game (0 = back-to-back)
        - is_b2b: True if playing on consecutive nights
        - games_in_last_3_days: Number of games in last 3 days
    """
    cursor = conn.cursor()

    # Get most recent game before this date
    query = """
        SELECT MAX(date_time_utc) as last_game
        FROM Games
        WHERE (home_team = ? OR away_team = ?)
          AND DATE(date_time_utc) < ?
          AND date_time_utc IS NOT NULL
    """

    cursor.execute(query, (team, team, game_date))
    result = cursor.fetchone()

    if not result or not result[0]:
        return {
            'last_game_date': None,
            'days_rest': 999,  # No recent game found
            'is_b2b': False,
            'games_in_last_3_days': 0
        }

    last_game_dt_str = result[0][:10]  # Extract date from datetime
    last_game_dt = datetime.strptime(last_game_dt_str, '%Y-%m-%d')
    game_dt = datetime.strptime(game_date, '%Y-%m-%d')

    days_rest = (game_dt - last_game_dt).days
    is_b2b = (days_rest == 1)

    # Count games in last 3 days
    three_days_ago = (game_dt - timedelta(days=3)).strftime('%Y-%m-%d')

    query_recent = """
        SELECT COUNT(*) as game_count
        FROM Games
        WHERE (home_team = ? OR away_team = ?)
          AND DATE(date_time_utc) >= ?
          AND DATE(date_time_utc) < ?
    """

    cursor.execute(query_recent, (team, team, three_days_ago, game_date))
    games_in_3_days = cursor.fetchone()[0]

    return {
        'last_game_date': last_game_dt_str,
        'days_rest': days_rest,
        'is_b2b': is_b2b,
        'games_in_last_3_days': games_in_3_days
    }


def calculate_rest_adjustment(
    home_rest: Dict,
    away_rest: Dict
) -> Tuple[float, str]:
    """
    Calculate spread adjustment based on rest advantage.

    Rest impact on performance:
    - B2B (0 days rest): Team typically -3 points
    - 1 day rest: Neutral
    - 2+ days rest vs B2B opponent: +3 points (fresh vs tired)

    Args:
        home_rest: Home team rest info
        away_rest: Away team rest info

    Returns:
        Tuple of (adjustment, explanation)
        - adjustment: Points to adjust spread (positive = favor home)
        - explanation: Human-readable explanation
    """
    home_b2b = home_rest['is_b2b']
    away_b2b = away_rest['is_b2b']
    home_days = home_rest['days_rest']
    away_days = away_rest['days_rest']

    explanations = []
    adjustment = 0.0

    # Both teams B2B - neutral
    if home_b2b and away_b2b:
        return 0.0, "Both teams on B2B (neutral)"

    # Home team B2B penalty
    if home_b2b and not away_b2b:
        adjustment -= 3.0
        explanations.append("Home on B2B (-3pts)")

    # Away team B2B penalty
    if away_b2b and not home_b2b:
        adjustment += 3.0
        explanations.append("Away on B2B (+3pts)")

    # Rest advantage (2+ days vs 0-1 days)
    if home_days >= 2 and away_days <= 1 and not away_b2b:
        adjustment += 2.0
        explanations.append(f"Home rested {home_days}d (+2pts)")
    elif away_days >= 2 and home_days <= 1 and not home_b2b:
        adjustment -= 2.0
        explanations.append(f"Away rested {away_days}d (-2pts)")

    if not explanations:
        return 0.0, "No rest advantage"

    return adjustment, ", ".join(explanations)


def format_rest_summary(rest_info: Dict) -> str:
    """
    Format rest info for display.

    Args:
        rest_info: Rest information dict

    Returns:
        Formatted string like "B2B: YES [!]" or "REST: 2 days"
    """
    if rest_info['is_b2b']:
        return "B2B: YES [!]"
    elif rest_info['days_rest'] >= 3:
        return f"REST: {rest_info['days_rest']} days"
    elif rest_info['days_rest'] == 2:
        return "REST: 2 days"
    else:
        return f"REST: {rest_info['days_rest']}d"
