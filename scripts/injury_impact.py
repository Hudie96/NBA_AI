"""
Injury Impact Calculator

Calculates the impact of injuries on game predictions by analyzing
player stats and adjusting spreads accordingly.
"""
import sqlite3
from typing import Dict, List, Tuple, Optional


def convert_name_to_playerbox_format(injury_name: str) -> str:
    """
    Convert injury report name (Last, First) to PlayerBox format (First Last).

    Examples:
        "Tatum, Jayson" -> "Jayson Tatum"
        "Davis, Anthony" -> "Anthony Davis"
    """
    if ',' not in injury_name:
        return injury_name

    parts = injury_name.split(',', 1)
    if len(parts) != 2:
        return injury_name

    last_name = parts[0].strip()
    first_name = parts[1].strip()

    return f"{first_name} {last_name}"


def get_team_injuries(team: str, report_date: str, conn: sqlite3.Connection) -> List[Dict]:
    """
    Get injuries for a specific team on a specific date with player stats.

    Args:
        team: Team abbreviation (e.g., "BOS", "LAL")
        report_date: Date of injury report (YYYY-MM-DD)
        conn: Database connection

    Returns:
        List of dicts with player, status, stats
    """
    cursor = conn.cursor()

    # Get injuries with stats, converting names for PlayerBox join
    query = """
        SELECT
            ir.player_name,
            ir.status,
            ir.injury_type,
            ir.body_part,
            AVG(pb.pts) as avg_pts,
            AVG(pb.min) as avg_min,
            AVG(pb.reb) as avg_reb,
            AVG(pb.ast) as avg_ast,
            COUNT(DISTINCT pb.game_id) as games_played
        FROM InjuryReports ir
        LEFT JOIN PlayerBox pb ON (
            -- Convert "Last, First" to "First Last" for join
            CASE
                WHEN ir.player_name LIKE '%,%'
                THEN SUBSTR(ir.player_name, INSTR(ir.player_name, ',') + 2) || ' ' || SUBSTR(ir.player_name, 1, INSTR(ir.player_name, ',') - 1)
                ELSE ir.player_name
            END = pb.player_name
        )
        WHERE ir.team = ?
          AND DATE(ir.report_timestamp) = ?
          AND ir.status IN ('Out', 'Doubtful')
        GROUP BY ir.player_name, ir.status, ir.injury_type, ir.body_part
        ORDER BY avg_pts DESC
    """

    cursor.execute(query, (team, report_date))
    results = cursor.fetchall()

    injuries = []
    for row in results:
        player_name, status, injury_type, body_part, pts, mins, reb, ast, games_played = row

        injury_dict = {
            'player': player_name,
            'status': status,
            'injury': f"{injury_type or ''} {body_part or ''}".strip() or "Unknown",
            'ppg': pts if pts else 0.0,
            'mpg': mins if mins else 0.0,
            'rpg': reb if reb else 0.0,
            'apg': ast if ast else 0.0,
            'games_played': games_played if games_played else 0
        }

        injuries.append(injury_dict)

    return injuries


def calculate_injury_impact(injuries: List[Dict]) -> float:
    """
    Calculate total injury impact for a team.

    Formula:
    - OUT players with 25+ PPG: -8 points per player
    - OUT players with 20-24 PPG: -6 points
    - OUT players with 15-19 PPG: -4 points
    - OUT players with 10-14 PPG: -2 points
    - OUT players with 5-9 PPG: -1 point
    - Doubtful players: 50% of OUT impact

    Args:
        injuries: List of injury dicts from get_team_injuries()

    Returns:
        Total impact in points (negative = hurt team)
    """
    total_impact = 0.0

    for injury in injuries:
        ppg = injury['ppg']
        status = injury['status']
        games_played = injury['games_played']

        # Skip players with no stats (< 5 games played)
        if games_played < 5:
            continue

        # Calculate base impact based on PPG
        if ppg >= 25:
            base_impact = -8.0
        elif ppg >= 20:
            base_impact = -6.0
        elif ppg >= 15:
            base_impact = -4.0
        elif ppg >= 10:
            base_impact = -2.0
        elif ppg >= 5:
            base_impact = -1.0
        else:
            base_impact = 0.0

        # Adjust for status (Doubtful = 50% of Out)
        if status == 'Doubtful':
            base_impact *= 0.5

        total_impact += base_impact

    return total_impact


def get_game_injury_adjustment(
    home_team: str,
    away_team: str,
    game_date: str,
    conn: sqlite3.Connection
) -> Tuple[float, Dict]:
    """
    Calculate injury adjustment for a game.

    Args:
        home_team: Home team abbreviation
        away_team: Away team abbreviation
        game_date: Game date (YYYY-MM-DD)
        conn: Database connection

    Returns:
        Tuple of (adjustment, details_dict)
        - adjustment: Points to adjust spread (positive = favor home, negative = favor away)
        - details_dict: Injury details for both teams
    """
    # Use previous day's injury report (reports are typically from day before game)
    from datetime import datetime, timedelta
    game_dt = datetime.strptime(game_date, '%Y-%m-%d')
    report_date = (game_dt - timedelta(days=1)).strftime('%Y-%m-%d')

    # Get injuries for both teams
    home_injuries = get_team_injuries(home_team, report_date, conn)
    away_injuries = get_team_injuries(away_team, report_date, conn)

    # Calculate impact
    home_impact = calculate_injury_impact(home_injuries)
    away_impact = calculate_injury_impact(away_injuries)

    # Net adjustment (positive = home favored, negative = away favored)
    # If home has worse injuries (more negative), away is favored (negative adjustment)
    net_adjustment = home_impact - away_impact

    details = {
        'home_injuries': home_injuries,
        'away_injuries': away_injuries,
        'home_impact': home_impact,
        'away_impact': away_impact,
        'net_adjustment': net_adjustment,
        'home_key_out': [inj for inj in home_injuries if inj['status'] == 'Out' and inj['ppg'] >= 15],
        'away_key_out': [inj for inj in away_injuries if inj['status'] == 'Out' and inj['ppg'] >= 15]
    }

    return net_adjustment, details


def format_injury_summary(injuries: List[Dict], max_players: int = 3) -> str:
    """
    Format injury list for display.

    Args:
        injuries: List of injury dicts
        max_players: Max number of players to show

    Returns:
        Formatted string like "Tatum (Out, 28ppg), Brown (Doubtful, 23ppg)"
    """
    # Filter to key injuries (10+ PPG or Out status)
    key_injuries = [
        inj for inj in injuries
        if inj['status'] == 'Out' and (inj['ppg'] >= 10 or inj['games_played'] >= 5)
    ]

    if not key_injuries:
        return "None"

    # Sort by PPG descending
    key_injuries.sort(key=lambda x: x['ppg'], reverse=True)

    # Take top N
    top_injuries = key_injuries[:max_players]

    parts = []
    for inj in top_injuries:
        # Convert name format for display
        name = convert_name_to_playerbox_format(inj['player'])
        # Use just first name for brevity
        first_name = name.split()[0]
        parts.append(f"{first_name} ({inj['ppg']:.0f}ppg)")

    result = ", ".join(parts)

    # Add "and X more" if there are additional injuries
    if len(key_injuries) > max_players:
        more_count = len(key_injuries) - max_players
        result += f", +{more_count} more"

    return result
