"""
Props Validator - Ensures props are only generated for players in today's games.

This module provides validation to prevent stale or incorrect props from being
included in daily reports.
"""
import sqlite3
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]


def get_todays_games(conn, target_date=None):
    """
    Get all games scheduled for the target date.

    Returns:
        List of dicts with game_id, home_team, away_team
    """
    if target_date is None:
        target_date = date.today().isoformat()

    games = conn.execute("""
        SELECT game_id, home_team, away_team
        FROM Games
        WHERE DATE(date_time_utc) = ?
    """, (target_date,)).fetchall()

    return [
        {"game_id": g[0], "home_team": g[1], "away_team": g[2]}
        for g in games
    ]


def get_teams_playing_today(conn, target_date=None):
    """
    Get set of team abbreviations playing on target date.

    Returns:
        Set of team abbreviations (e.g., {'LAL', 'BOS', 'MIA'})
    """
    games = get_todays_games(conn, target_date)
    teams = set()
    for g in games:
        teams.add(g["home_team"])
        teams.add(g["away_team"])
    return teams


def get_player_team(player_name, conn):
    """
    Get the team a player is currently on.

    Checks multiple sources:
    1. player_positions table (from roster fetch)
    2. Most recent game in player_game_logs
    3. PlayerBox table

    Returns:
        Team abbreviation or None if not found
    """
    # Try player_game_logs first (most recent)
    result = conn.execute("""
        SELECT team
        FROM player_game_logs
        WHERE player_name = ?
        ORDER BY game_date DESC
        LIMIT 1
    """, (player_name,)).fetchone()

    if result:
        return result[0]

    # Try PlayerBox (has team_id, need to join with Teams)
    result = conn.execute("""
        SELECT t.abbreviation
        FROM PlayerBox pb
        JOIN Teams t ON pb.team_id = t.team_id
        WHERE pb.player_name = ?
        ORDER BY pb.game_id DESC
        LIMIT 1
    """, (player_name,)).fetchone()

    if result:
        return result[0]

    return None


def is_player_playing_today(player_name, conn, target_date=None):
    """
    Check if a player's team is playing on the target date.

    Returns:
        Tuple of (is_playing: bool, team: str or None, opponent: str or None)
    """
    team = get_player_team(player_name, conn)
    if not team:
        return False, None, None

    games = get_todays_games(conn, target_date)

    for game in games:
        if game["home_team"] == team:
            return True, team, game["away_team"]
        elif game["away_team"] == team:
            return True, team, game["home_team"]

    return False, team, None


def get_valid_players_for_props(conn, target_date=None, min_games=20):
    """
    Get all players who:
    1. Are on a team playing today
    2. Have enough game history for projections

    Returns:
        List of dicts with player_name, team, opponent
    """
    if target_date is None:
        target_date = date.today().isoformat()

    teams_playing = get_teams_playing_today(conn, target_date)

    if not teams_playing:
        return []

    # Get players with enough games on teams playing today
    placeholders = ",".join(["?" for _ in teams_playing])

    players = conn.execute(f"""
        SELECT player_name, team, COUNT(*) as games
        FROM player_game_logs
        WHERE team IN ({placeholders})
        GROUP BY player_name, team
        HAVING games >= ?
        ORDER BY games DESC
    """, (*teams_playing, min_games)).fetchall()

    # Match each player to their opponent
    games = get_todays_games(conn, target_date)
    game_lookup = {}
    for g in games:
        game_lookup[g["home_team"]] = g["away_team"]
        game_lookup[g["away_team"]] = g["home_team"]

    valid_players = []
    for player_name, team, games_count in players:
        opponent = game_lookup.get(team)
        if opponent:
            valid_players.append({
                "player_name": player_name,
                "team": team,
                "opponent": opponent,
                "games": games_count
            })

    return valid_players


def validate_prop(player_name, opponent, conn, target_date=None):
    """
    Validate that a prop is valid for the target date.

    Checks:
    1. Player's team is playing today
    2. Player's team is playing against the specified opponent

    Returns:
        Tuple of (is_valid: bool, error_message: str or None)
    """
    is_playing, team, actual_opponent = is_player_playing_today(
        player_name, conn, target_date
    )

    if not team:
        return False, f"Player '{player_name}' not found in database"

    if not is_playing:
        return False, f"Player's team ({team}) is not playing on {target_date or 'today'}"

    if actual_opponent != opponent:
        return False, f"Player's team ({team}) is playing {actual_opponent}, not {opponent}"

    return True, None


def filter_valid_props(props_list, conn, target_date=None):
    """
    Filter a list of props to only include valid ones.

    Args:
        props_list: List of dicts with at least 'player_name' and 'opponent' keys
        conn: Database connection
        target_date: Target date (defaults to today)

    Returns:
        Tuple of (valid_props, invalid_props_with_reasons)
    """
    valid = []
    invalid = []

    for prop in props_list:
        is_valid, error = validate_prop(
            prop["player_name"],
            prop["opponent"],
            conn,
            target_date
        )

        if is_valid:
            valid.append(prop)
        else:
            invalid.append({**prop, "error": error})

    return valid, invalid
