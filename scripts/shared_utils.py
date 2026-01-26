"""
Shared utility functions for AXIOM scripts.

Consolidates common functions used across daily_predictions.py, backtest.py,
and other analytics scripts to avoid duplication.
"""
import numpy as np
import pandas as pd


def get_team_recent_games(team, before_date, conn, limit=10):
    """
    Get recent games for a team before a specific date.

    Args:
        team: Team abbreviation (e.g., 'BOS')
        before_date: Date string (YYYY-MM-DD) - get games before this date
        conn: SQLite connection
        limit: Max number of games to return (default 10)

    Returns:
        DataFrame with columns: game_date, home, away, home_score, away_score
    """
    query = '''
        SELECT game_date, home, away, home_score, away_score
        FROM GameStates
        WHERE (home = ? OR away = ?)
          AND is_final_state = 1
          AND game_date < ?
        ORDER BY game_date DESC
        LIMIT ?
    '''
    return pd.read_sql(query, conn, params=(team, team, before_date, limit))


def calculate_team_stats(games, team):
    """
    Calculate stats for a team from their recent games.

    Args:
        games: DataFrame from get_team_recent_games()
        team: Team abbreviation to calculate stats for

    Returns:
        dict with keys: Win_Pct, PPG, OPP_PPG, Net_PPG, games_count, record
        Returns None if no games provided
    """
    if len(games) == 0:
        return None

    scores = []
    opp_scores = []
    wins = 0

    for _, game in games.iterrows():
        if game['home'] == team:
            score = game['home_score']
            opp_score = game['away_score']
        else:
            score = game['away_score']
            opp_score = game['home_score']

        scores.append(score)
        opp_scores.append(opp_score)
        if score > opp_score:
            wins += 1

    return {
        'Win_Pct': wins / len(games),
        'PPG': np.mean(scores),
        'OPP_PPG': np.mean(opp_scores),
        'Net_PPG': np.mean(scores) - np.mean(opp_scores),
        'games_count': len(games),
        'record': f"{wins}-{len(games) - wins}"
    }
