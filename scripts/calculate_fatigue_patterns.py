"""
AXIOM: Fatigue Pattern Calculator
Calculates schedule-based fatigue (B2B, 3-in-4, 4-in-6, 5-in-7, road trips).

Usage:
    python scripts/calculate_fatigue_patterns.py
    python scripts/calculate_fatigue_patterns.py --date 2026-01-29
"""

import sqlite3
import sys
from datetime import datetime, timedelta, date
from pathlib import Path
from collections import defaultdict

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]


def safe_print(text):
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))


def create_tables(conn):
    """Create fatigue pattern table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS TeamFatiguePatterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            game_date TEXT NOT NULL,
            team TEXT NOT NULL,
            is_home INTEGER,
            -- Fatigue flags
            is_b2b INTEGER DEFAULT 0,
            is_b2b_second INTEGER DEFAULT 0,
            is_3_in_4 INTEGER DEFAULT 0,
            is_4_in_6 INTEGER DEFAULT 0,
            is_5_in_7 INTEGER DEFAULT 0,
            -- Rest days
            days_rest INTEGER,
            -- Road trip info
            consecutive_road_games INTEGER DEFAULT 0,
            consecutive_home_games INTEGER DEFAULT 0,
            -- Opponent fatigue
            opp_is_b2b INTEGER DEFAULT 0,
            opp_days_rest INTEGER,
            rest_advantage INTEGER,
            -- Timestamps
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(game_id, team)
        )
    """)
    conn.commit()
    safe_print("Created TeamFatiguePatterns table")


def get_team_schedule(conn, team, season="2025-2026"):
    """Get all games for a team."""
    games = conn.execute("""
        SELECT game_id, DATE(date_time_utc) as game_date,
               home_team, away_team,
               CASE WHEN home_team = ? THEN 1 ELSE 0 END as is_home
        FROM Games
        WHERE (home_team = ? OR away_team = ?)
          AND season = ?
        ORDER BY game_date
    """, (team, team, team, season)).fetchall()
    return games


def calculate_team_fatigue(conn, team, season="2025-2026"):
    """Calculate fatigue patterns for a team's games."""
    games = get_team_schedule(conn, team, season)
    if not games:
        return 0

    count = 0
    game_dates = [(g[0], datetime.strptime(g[1], '%Y-%m-%d').date(), g[2], g[3], g[4]) for g in games]

    for i, (game_id, game_date, home, away, is_home) in enumerate(game_dates):
        # Calculate days rest (look back)
        days_rest = None
        if i > 0:
            prev_date = game_dates[i-1][1]
            days_rest = (game_date - prev_date).days - 1  # -1 because same day would be 0 rest

        # B2B flags
        is_b2b = 1 if days_rest == 0 else 0
        is_b2b_second = is_b2b  # Second of B2B

        # Check if first of B2B (next game is tomorrow)
        if i < len(game_dates) - 1:
            next_date = game_dates[i+1][1]
            if (next_date - game_date).days == 1:
                is_b2b = 1

        # 3-in-4: 3 games in 4 days (check window)
        is_3_in_4 = 0
        window_start = game_date - timedelta(days=3)
        games_in_window = sum(1 for _, d, _, _, _ in game_dates if window_start <= d <= game_date)
        if games_in_window >= 3:
            is_3_in_4 = 1

        # 4-in-6: 4 games in 6 days
        is_4_in_6 = 0
        window_start = game_date - timedelta(days=5)
        games_in_window = sum(1 for _, d, _, _, _ in game_dates if window_start <= d <= game_date)
        if games_in_window >= 4:
            is_4_in_6 = 1

        # 5-in-7: 5 games in 7 days
        is_5_in_7 = 0
        window_start = game_date - timedelta(days=6)
        games_in_window = sum(1 for _, d, _, _, _ in game_dates if window_start <= d <= game_date)
        if games_in_window >= 5:
            is_5_in_7 = 1

        # Consecutive road/home games
        consecutive_road = 0
        consecutive_home = 0
        if not is_home:
            # Count consecutive road games ending with this one
            for j in range(i, -1, -1):
                if game_dates[j][4] == 0:  # is_home = 0
                    consecutive_road += 1
                else:
                    break
        else:
            # Count consecutive home games
            for j in range(i, -1, -1):
                if game_dates[j][4] == 1:
                    consecutive_home += 1
                else:
                    break

        # Get opponent
        opponent = away if is_home else home

        # Calculate opponent's rest (simplified - just check their previous game)
        opp_prev = conn.execute("""
            SELECT MAX(DATE(date_time_utc))
            FROM Games
            WHERE (home_team = ? OR away_team = ?)
              AND DATE(date_time_utc) < ?
        """, (opponent, opponent, game_date.isoformat())).fetchone()[0]

        opp_days_rest = None
        opp_is_b2b = 0
        if opp_prev:
            opp_prev_date = datetime.strptime(opp_prev, '%Y-%m-%d').date()
            opp_days_rest = (game_date - opp_prev_date).days - 1
            opp_is_b2b = 1 if opp_days_rest == 0 else 0

        # Rest advantage
        rest_advantage = None
        if days_rest is not None and opp_days_rest is not None:
            rest_advantage = days_rest - opp_days_rest

        conn.execute("""
            INSERT OR REPLACE INTO TeamFatiguePatterns
            (game_id, game_date, team, is_home, is_b2b, is_b2b_second,
             is_3_in_4, is_4_in_6, is_5_in_7, days_rest,
             consecutive_road_games, consecutive_home_games,
             opp_is_b2b, opp_days_rest, rest_advantage, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            game_id, game_date.isoformat(), team, is_home,
            is_b2b, is_b2b_second, is_3_in_4, is_4_in_6, is_5_in_7,
            days_rest, consecutive_road, consecutive_home,
            opp_is_b2b, opp_days_rest, rest_advantage,
            datetime.now().isoformat()
        ))
        count += 1

    conn.commit()
    return count


def calculate_all_fatigue(conn, season="2025-2026"):
    """Calculate fatigue for all teams."""
    teams = conn.execute("SELECT DISTINCT abbreviation FROM Teams").fetchall()

    total = 0
    for (team,) in teams:
        safe_print(f"  Processing {team}...")
        count = calculate_team_fatigue(conn, team, season)
        total += count

    return total


def get_todays_fatigue(conn, target_date=None):
    """Get fatigue info for today's games."""
    if target_date is None:
        target_date = date.today().isoformat()

    games = conn.execute("""
        SELECT f.team, f.is_home, f.is_b2b, f.is_3_in_4, f.is_4_in_6,
               f.days_rest, f.consecutive_road_games, f.opp_is_b2b, f.rest_advantage,
               g.home_team, g.away_team
        FROM TeamFatiguePatterns f
        JOIN Games g ON f.game_id = g.game_id
        WHERE f.game_date = ?
        ORDER BY f.game_id, f.is_home DESC
    """, (target_date,)).fetchall()

    return games


def display_insights(conn, target_date=None):
    """Display fatigue insights."""
    if target_date is None:
        target_date = date.today().isoformat()

    safe_print("\n" + "=" * 60)
    safe_print(f"FATIGUE ANALYSIS - {target_date}")
    safe_print("=" * 60)

    games = get_todays_fatigue(conn, target_date)

    if not games:
        safe_print(f"\nNo fatigue data for {target_date}")
        safe_print("Run with all games to populate data first.")
        return

    # Group by matchup
    matchups = defaultdict(list)
    for row in games:
        team, is_home, b2b, in4, in6, rest, road, opp_b2b, adv, home, away = row
        key = f"{away} @ {home}"
        matchups[key].append(row)

    safe_print("\nTODAY'S FATIGUE SITUATIONS:")
    safe_print("-" * 60)

    for matchup, teams_data in matchups.items():
        flags = []
        for row in teams_data:
            team, is_home, b2b, in4, in6, rest, road, opp_b2b, adv, _, _ = row
            loc = "H" if is_home else "A"

            team_flags = []
            if b2b:
                team_flags.append("B2B")
            if in4:
                team_flags.append("3-in-4")
            if in6:
                team_flags.append("4-in-6")
            if road and road >= 4:
                team_flags.append(f"{road}th road")
            if adv and adv >= 2:
                team_flags.append(f"+{adv} rest adv")
            elif adv and adv <= -2:
                team_flags.append(f"{adv} rest")

            if team_flags:
                flags.append(f"{team} ({loc}): {', '.join(team_flags)}")

        if flags:
            safe_print(f"\n{matchup}")
            for f in flags:
                safe_print(f"  {f}")

    # Summary stats
    safe_print("\n" + "=" * 60)
    safe_print("FATIGUE EDGES")
    safe_print("=" * 60)

    # Teams on B2B
    b2b_teams = [row for row in games if row[2]]  # is_b2b
    if b2b_teams:
        safe_print("\nTEAMS ON B2B (Fade):")
        for row in b2b_teams:
            safe_print(f"  {row[0]} ({row[9]} @ {row[10] if row[1] else row[9]})")

    # Big rest advantages
    rest_adv = [(row[0], row[8], row[9], row[10], row[1]) for row in games if row[8] and row[8] >= 2]
    if rest_adv:
        safe_print("\nREST ADVANTAGE (+2 days):")
        for team, adv, home, away, is_home in rest_adv:
            matchup = f"{away} @ {home}"
            safe_print(f"  {team} +{adv} days rest ({matchup})")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="Target date YYYY-MM-DD")
    parser.add_argument("--season", default="2025-2026", help="Season to calculate")
    parser.add_argument("--rebuild", action="store_true", help="Rebuild all fatigue data")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)

    if args.rebuild:
        safe_print(f"Rebuilding fatigue patterns for {args.season}...")
        count = calculate_all_fatigue(conn, args.season)
        safe_print(f"Calculated {count} fatigue records")

    target_date = args.date or date.today().isoformat()
    display_insights(conn, target_date)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
