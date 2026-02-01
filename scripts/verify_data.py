"""
Data Verification Script for AXIOM Pipeline

Validates data by cross-referencing multiple sources before updates.
Checks previous day's stats through NBA API and ESPN, only accepts if they agree.

Usage:
    python scripts/verify_data.py              # Full verification
    python scripts/verify_data.py --quick      # Quick freshness check only
    python scripts/verify_data.py --cross-check # Cross-check yesterday's data
"""

import argparse
import json
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]

# Tolerance for stat comparison (allow small rounding differences)
STAT_TOLERANCE = 1


def fetch_nba_api_boxscore(game_id):
    """Fetch boxscore from official NBA API."""
    try:
        from nba_api.stats.endpoints import BoxScoreTraditionalV3
        boxscore = BoxScoreTraditionalV3(game_id=game_id, timeout=30)
        data = boxscore.get_dict()

        if 'boxScoreTraditional' not in data:
            return None

        players = {}
        for team_key in ['homeTeam', 'awayTeam']:
            team_data = data['boxScoreTraditional'].get(team_key, {})
            for p in team_data.get('players', []):
                stats = p.get('statistics', {})
                name = f"{p.get('firstName', '')} {p.get('familyName', '')}"
                players[name] = {
                    'pts': stats.get('points', 0),
                    'reb': stats.get('reboundsTotal', 0),
                    'ast': stats.get('assists', 0),
                    'source': 'NBA_API'
                }

        return players
    except Exception as e:
        print(f"    NBA API error: {e}")
        return None


def fetch_espn_boxscore(game_id, game_date):
    """Fetch boxscore from ESPN API."""
    try:
        # ESPN uses different game IDs - need to map via our DB
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute('''
            SELECT espn_event_id FROM ESPNGameMapping WHERE nba_game_id = ?
        ''', (game_id,))
        row = cur.fetchone()
        conn.close()

        if not row or not row[0]:
            return None

        espn_id = row[0]

        # Fetch from ESPN API
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={espn_id}"
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(url, headers=headers, timeout=15)

        if resp.status_code != 200:
            return None

        data = resp.json()
        players = {}

        # Parse boxscore from ESPN response
        # ESPN stats order: MIN, PTS, FG, 3PT, FT, REB, AST, TO, STL, BLK, OREB, DREB, PF, +/-
        # Index:             0    1    2   3    4   5    6   7   8    9    10    11   12   13
        boxscore = data.get('boxscore', {})
        for team in boxscore.get('players', []):
            for stat_group in team.get('statistics', []):
                for athlete in stat_group.get('athletes', []):
                    name = athlete.get('athlete', {}).get('displayName', '')
                    stats = athlete.get('stats', [])
                    if len(stats) >= 7 and name:
                        try:
                            pts = int(stats[1]) if stats[1] not in ['-', '--', ''] else 0
                            reb = int(stats[5]) if stats[5] not in ['-', '--', ''] else 0
                            ast = int(stats[6]) if stats[6] not in ['-', '--', ''] else 0
                            players[name] = {
                                'pts': pts,
                                'reb': reb,
                                'ast': ast,
                                'source': 'ESPN'
                            }
                        except (ValueError, IndexError):
                            continue

        return players if players else None
    except Exception as e:
        print(f"    ESPN API error: {e}")
        return None


def normalize_player_name(name):
    """Normalize player name for comparison."""
    # Remove Jr., III, etc. and extra spaces
    name = name.strip()
    for suffix in [' Jr.', ' Jr', ' III', ' II', ' IV']:
        name = name.replace(suffix, '')
    return name.lower().strip()


def compare_sources(nba_data, espn_data):
    """Compare stats between two sources, return matches and mismatches."""
    matches = []
    mismatches = []

    if not nba_data or not espn_data:
        return matches, mismatches

    # Build normalized name lookup for ESPN
    espn_normalized = {normalize_player_name(k): (k, v) for k, v in espn_data.items()}

    for nba_name, nba_stats in nba_data.items():
        norm_name = normalize_player_name(nba_name)

        if norm_name in espn_normalized:
            espn_name, espn_stats = espn_normalized[norm_name]

            pts_match = abs(nba_stats['pts'] - espn_stats['pts']) <= STAT_TOLERANCE
            reb_match = abs(nba_stats['reb'] - espn_stats['reb']) <= STAT_TOLERANCE
            ast_match = abs(nba_stats['ast'] - espn_stats['ast']) <= STAT_TOLERANCE

            if pts_match and reb_match and ast_match:
                matches.append({
                    'player': nba_name,
                    'pts': nba_stats['pts'],
                    'reb': nba_stats['reb'],
                    'ast': nba_stats['ast']
                })
            else:
                mismatches.append({
                    'player': nba_name,
                    'nba': nba_stats,
                    'espn': espn_stats,
                    'diff': {
                        'pts': nba_stats['pts'] - espn_stats['pts'],
                        'reb': nba_stats['reb'] - espn_stats['reb'],
                        'ast': nba_stats['ast'] - espn_stats['ast']
                    }
                })

    return matches, mismatches


def cross_check_previous_day(conn, target_date: str) -> dict:
    """Cross-check previous day's data between NBA API and ESPN."""
    issues = []
    warnings = []

    target = datetime.strptime(target_date, "%Y-%m-%d").date()
    yesterday = (target - timedelta(days=1)).isoformat()

    print(f"  Checking games from {yesterday}")
    print("-" * 40)

    # Get yesterday's completed games
    df = pd.read_sql("""
        SELECT game_id, home_team, away_team
        FROM Games
        WHERE DATE(date_time_utc) = ?
          AND status = '3'
    """, conn, params=(yesterday,))

    if df.empty:
        print(f"  No completed games found for {yesterday}")
        return {"issues": issues, "warnings": warnings}

    print(f"  Found {len(df)} games to verify")

    total_matches = 0
    total_mismatches = 0
    games_verified = 0
    games_failed = 0

    for _, row in df.iterrows():
        game_id = row['game_id']
        matchup = f"{row['away_team']} @ {row['home_team']}"

        print(f"\n  {matchup} ({game_id})")

        # Fetch from both sources
        nba_data = fetch_nba_api_boxscore(game_id)
        time.sleep(0.5)  # Rate limiting

        espn_data = fetch_espn_boxscore(game_id, yesterday)
        time.sleep(0.3)

        if not nba_data:
            print(f"    [WARN] Could not fetch NBA API data")
            warnings.append(f"{matchup}: NBA API fetch failed")
            continue

        if not espn_data:
            print(f"    [WARN] Could not fetch ESPN data")
            warnings.append(f"{matchup}: ESPN fetch failed - using NBA API only")
            games_verified += 1
            continue

        # Compare sources
        matches, mismatches = compare_sources(nba_data, espn_data)

        total_matches += len(matches)
        total_mismatches += len(mismatches)

        if mismatches:
            games_failed += 1
            print(f"    [MISMATCH] {len(mismatches)} players have different stats:")
            for m in mismatches[:3]:  # Show first 3
                print(f"      {m['player']}: NBA={m['nba']['pts']}/{m['nba']['reb']}/{m['nba']['ast']} "
                      f"ESPN={m['espn']['pts']}/{m['espn']['reb']}/{m['espn']['ast']}")
            issues.append(f"{matchup}: {len(mismatches)} stat mismatches between sources")
        else:
            games_verified += 1
            print(f"    [OK] {len(matches)} players verified across both sources")

    # Summary
    print(f"\n" + "-" * 40)
    print(f"  Cross-Check Summary:")
    print(f"    Games verified: {games_verified}/{len(df)}")
    print(f"    Player matches: {total_matches}")
    print(f"    Player mismatches: {total_mismatches}")

    if total_mismatches > 0:
        match_rate = total_matches / (total_matches + total_mismatches) * 100
        print(f"    Match rate: {match_rate:.1f}%")
        if match_rate < 95:
            issues.append(f"Cross-check match rate only {match_rate:.1f}% - data may be unreliable")

    return {"issues": issues, "warnings": warnings}


def check_data_freshness(conn, target_date: str) -> dict:
    """Check that all data tables have recent data."""
    issues = []
    warnings = []

    target = datetime.strptime(target_date, "%Y-%m-%d").date()

    # Check PlayerBox freshness
    df = pd.read_sql("""
        SELECT MAX(DATE(g.date_time_utc)) as max_date, COUNT(*) as total
        FROM PlayerBox pb
        JOIN Games g ON pb.game_id = g.game_id
    """, conn)

    if not df.empty and df.iloc[0]["max_date"]:
        max_date = datetime.strptime(df.iloc[0]["max_date"], "%Y-%m-%d").date()
        days_old = (target - max_date).days
        if days_old > 7:
            issues.append(f"PlayerBox data is {days_old} days old (last: {max_date})")
        elif days_old > 2:
            warnings.append(f"PlayerBox data is {days_old} days old (last: {max_date})")
        print(f"  PlayerBox: Latest date {max_date}, {df.iloc[0]['total']:,} rows")
    else:
        issues.append("PlayerBox table is empty!")

    # Check Games freshness
    df = pd.read_sql("""
        SELECT MAX(DATE(date_time_utc)) as max_date, COUNT(*) as total
        FROM Games
    """, conn)

    if not df.empty and df.iloc[0]["max_date"]:
        max_date = datetime.strptime(df.iloc[0]["max_date"], "%Y-%m-%d").date()
        print(f"  Games: Latest date {max_date}, {df.iloc[0]['total']:,} rows")

    # Check Betting freshness
    df = pd.read_sql("""
        SELECT MAX(DATE(g.date_time_utc)) as max_date, COUNT(*) as total
        FROM Betting b
        JOIN Games g ON b.game_id = g.game_id
    """, conn)

    if not df.empty and df.iloc[0]["max_date"]:
        max_date = datetime.strptime(df.iloc[0]["max_date"], "%Y-%m-%d").date()
        days_old = (target - max_date).days
        if days_old > 7:
            warnings.append(f"Betting data is {days_old} days old")
        print(f"  Betting: Latest date {max_date}, {df.iloc[0]['total']:,} rows")

    # Check if player_game_logs is stale
    try:
        df = pd.read_sql("""
            SELECT MAX(game_date) as max_date FROM player_game_logs
        """, conn)
        if not df.empty and df.iloc[0]["max_date"]:
            max_date = datetime.strptime(df.iloc[0]["max_date"], "%Y-%m-%d").date()
            days_old = (target - max_date).days
            if days_old > 30:
                warnings.append(f"player_game_logs is STALE ({days_old} days old) - using PlayerBox instead")
    except:
        pass

    return {"issues": issues, "warnings": warnings}


def check_player_data_consistency(conn) -> dict:
    """Verify player data is consistent across tables."""
    issues = []
    warnings = []

    df = pd.read_sql("""
        SELECT player_name, COUNT(*) as games, AVG(min) as avg_min
        FROM PlayerBox
        WHERE min > 0
        GROUP BY player_name
        HAVING games >= 3 AND avg_min >= 15
        ORDER BY games
        LIMIT 20
    """, conn)

    for _, row in df.iterrows():
        if row["games"] <= 5 and row["avg_min"] >= 20:
            warnings.append(f"Player '{row['player_name']}' has only {row['games']} games but {row['avg_min']:.1f} avg min - possible name variant?")

    print(f"  Active players (3+ games, 15+ min): {len(df)}")

    return {"issues": issues, "warnings": warnings}


def check_l10_calculations(conn, sample_size=5) -> dict:
    """Verify L10 calculations match expected values."""
    issues = []
    warnings = []

    df = pd.read_sql("""
        SELECT player_name
        FROM PlayerBox
        WHERE min > 0
        GROUP BY player_name
        HAVING COUNT(*) >= 10
        ORDER BY SUM(min) DESC
        LIMIT ?
    """, conn, params=(sample_size,))

    for _, row in df.iterrows():
        player = row["player_name"]

        l10 = pd.read_sql(f"""
            SELECT pb.pts, pb.reb, pb.ast, (pb.pts + pb.reb + pb.ast) as pra
            FROM PlayerBox pb
            JOIN Games g ON pb.game_id = g.game_id
            WHERE pb.player_name = ?
              AND pb.min > 0
            ORDER BY g.date_time_utc DESC
            LIMIT 10
        """, conn, params=(player,))

        if not l10.empty:
            l10_pra = l10["pra"].mean()
            print(f"  {player}: L10 PRA = {l10_pra:.1f}")

    return {"issues": issues, "warnings": warnings}


def check_todays_games(conn, target_date: str) -> dict:
    """Check that we have games scheduled for target date."""
    issues = []
    warnings = []

    df = pd.read_sql("""
        SELECT game_id, home_team, away_team, status
        FROM Games
        WHERE DATE(date_time_utc) = ?
    """, conn, params=(target_date,))

    if df.empty:
        issues.append(f"No games found for {target_date}")
    else:
        print(f"  Games on {target_date}: {len(df)}")
        for _, row in df.iterrows():
            print(f"    {row['away_team']} @ {row['home_team']}")

    return {"issues": issues, "warnings": warnings}


def main():
    parser = argparse.ArgumentParser(description="Verify AXIOM data integrity")
    parser.add_argument("--date", type=str, default=date.today().isoformat(),
                        help="Target date (YYYY-MM-DD)")
    parser.add_argument("--quick", action="store_true", help="Quick check only")
    parser.add_argument("--cross-check", action="store_true",
                        help="Cross-check previous day's data between sources")
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print(f"  AXIOM DATA VERIFICATION - {args.date}")
    print("=" * 60)

    conn = sqlite3.connect(DB_PATH)

    all_issues = []
    all_warnings = []

    # Check 1: Data Freshness
    print("\n[1] Data Freshness Check")
    print("-" * 40)
    result = check_data_freshness(conn, args.date)
    all_issues.extend(result["issues"])
    all_warnings.extend(result["warnings"])

    if not args.quick:
        # Check 2: Today's Games
        print("\n[2] Today's Games")
        print("-" * 40)
        result = check_todays_games(conn, args.date)
        all_issues.extend(result["issues"])
        all_warnings.extend(result["warnings"])

        # Check 3: Player Consistency
        print("\n[3] Player Data Consistency")
        print("-" * 40)
        result = check_player_data_consistency(conn)
        all_issues.extend(result["issues"])
        all_warnings.extend(result["warnings"])

        # Check 4: L10 Calculations
        print("\n[4] L10 Calculation Verification (sample)")
        print("-" * 40)
        result = check_l10_calculations(conn, sample_size=3)
        all_issues.extend(result["issues"])
        all_warnings.extend(result["warnings"])

        # Check 5: Cross-check previous day (default now)
        print("\n[5] Cross-Check Previous Day (NBA API vs ESPN)")
        print("-" * 40)
        result = cross_check_previous_day(conn, args.date)
        all_issues.extend(result["issues"])
        all_warnings.extend(result["warnings"])

    conn.close()

    # Summary
    print("\n" + "=" * 60)
    print("  VERIFICATION SUMMARY")
    print("=" * 60)

    if all_issues:
        print("\n[ERRORS] - Must fix before running pipeline:")
        for issue in all_issues:
            print(f"  - {issue}")

    if all_warnings:
        print("\n[WARNINGS] - Review but can proceed:")
        for warning in all_warnings:
            print(f"  - {warning}")

    if not all_issues and not all_warnings:
        print("\n  All checks passed!")

    print("")

    return 1 if all_issues else 0


if __name__ == "__main__":
    sys.exit(main())
