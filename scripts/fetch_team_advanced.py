"""
AXIOM Data Fetcher - Team Advanced Stats
Fetches: Pace, ORTG, DRTG, TS%, eFG%, Net Rating
Source: NBA.com via nba_api
"""

import sqlite3
import pandas as pd
import time
from datetime import datetime

try:
    from nba_api.stats.endpoints import leaguedashteamstats
    HAS_NBA_API = True
except ImportError:
    HAS_NBA_API = False
    print("WARNING: nba_api not installed. Run: pip install nba_api")

DB_PATH = "data/NBA_AI_current.sqlite"


def fetch_team_advanced_stats(season='2025-26'):
    """
    Fetch team advanced stats from NBA.com

    Returns DataFrame with:
    - TEAM_ID, TEAM_NAME
    - OFF_RATING, DEF_RATING, NET_RATING
    - PACE, TS_PCT, EFG_PCT
    - AST_TO, AST_RATIO
    """
    if not HAS_NBA_API:
        raise ImportError("nba_api required. Install with: pip install nba_api")

    print(f"Fetching team advanced stats for {season}...")

    # Fetch advanced stats
    stats = leaguedashteamstats.LeagueDashTeamStats(
        season=season,
        measure_type_detailed_defense='Advanced',
        per_mode_detailed='PerGame',
        season_type_all_star='Regular Season'
    )
    time.sleep(1)  # Rate limit

    df = stats.get_data_frames()[0]

    # Select and rename columns
    cols = {
        'TEAM_ID': 'team_id',
        'TEAM_NAME': 'team_name',
        'GP': 'games_played',
        'W': 'wins',
        'L': 'losses',
        'OFF_RATING': 'off_rating',
        'DEF_RATING': 'def_rating',
        'NET_RATING': 'net_rating',
        'PACE': 'pace',
        'TS_PCT': 'ts_pct',
        'EFG_PCT': 'efg_pct',
        'AST_TO': 'ast_to_ratio',
        'AST_RATIO': 'ast_ratio',
        'OREB_PCT': 'oreb_pct',
        'DREB_PCT': 'dreb_pct',
        'REB_PCT': 'reb_pct',
        'TM_TOV_PCT': 'tov_pct'
    }

    # Filter to available columns
    available_cols = [c for c in cols.keys() if c in df.columns]
    result = df[available_cols].rename(columns=cols)

    # Add metadata
    result['season'] = season
    result['fetched_at'] = datetime.now().isoformat()

    print(f"Fetched stats for {len(result)} teams")
    return result


def fetch_team_traditional_stats(season='2025-26'):
    """
    Fetch team traditional stats (PPG, RPG, APG, etc.)
    """
    if not HAS_NBA_API:
        raise ImportError("nba_api required")

    print(f"Fetching team traditional stats for {season}...")

    stats = leaguedashteamstats.LeagueDashTeamStats(
        season=season,
        measure_type_detailed_defense='Base',
        per_mode_detailed='PerGame',
        season_type_all_star='Regular Season'
    )
    time.sleep(1)

    df = stats.get_data_frames()[0]

    cols = {
        'TEAM_ID': 'team_id',
        'TEAM_NAME': 'team_name',
        'PTS': 'ppg',
        'REB': 'rpg',
        'AST': 'apg',
        'STL': 'spg',
        'BLK': 'bpg',
        'TOV': 'topg',
        'FG_PCT': 'fg_pct',
        'FG3_PCT': 'fg3_pct',
        'FT_PCT': 'ft_pct',
        'PLUS_MINUS': 'plus_minus'
    }

    available_cols = [c for c in cols.keys() if c in df.columns]
    result = df[available_cols].rename(columns=cols)

    result['season'] = season
    result['fetched_at'] = datetime.now().isoformat()

    return result


def save_to_database(df, table_name):
    """Save DataFrame to SQLite database"""
    conn = sqlite3.connect(DB_PATH)

    # Check if table exists
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    exists = cursor.fetchone() is not None

    # Save data
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

    action = "Updated" if exists else "Created"
    print(f"{action} table '{table_name}' with {len(df)} rows")


def create_team_mapping():
    """Create team_id to team_abbrev mapping"""
    mapping = {
        1610612737: 'ATL', 1610612738: 'BOS', 1610612739: 'CLE',
        1610612740: 'NOP', 1610612741: 'CHI', 1610612742: 'DAL',
        1610612743: 'DEN', 1610612744: 'GSW', 1610612745: 'HOU',
        1610612746: 'LAC', 1610612747: 'LAL', 1610612748: 'MIA',
        1610612749: 'MIL', 1610612750: 'MIN', 1610612751: 'BKN',
        1610612752: 'NYK', 1610612753: 'ORL', 1610612754: 'IND',
        1610612755: 'PHI', 1610612756: 'PHX', 1610612757: 'POR',
        1610612758: 'SAC', 1610612759: 'SAS', 1610612760: 'OKC',
        1610612761: 'TOR', 1610612762: 'UTA', 1610612763: 'MEM',
        1610612764: 'WAS', 1610612765: 'DET', 1610612766: 'CHA'
    }
    return mapping


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Fetch team advanced stats')
    parser.add_argument('--season', default='2025-26', help='Season (e.g., 2025-26)')
    args = parser.parse_args()

    try:
        # Fetch advanced stats
        adv_df = fetch_team_advanced_stats(args.season)
        save_to_database(adv_df, 'team_advanced_stats')

        # Fetch traditional stats
        trad_df = fetch_team_traditional_stats(args.season)
        save_to_database(trad_df, 'team_traditional_stats')

        print("\nSample advanced stats:")
        print(adv_df[['team_name', 'off_rating', 'def_rating', 'net_rating', 'pace']].head(10))

    except ImportError as e:
        print(f"Error: {e}")
        print("Install nba_api: pip install nba_api")
    except Exception as e:
        print(f"Error fetching data: {e}")
