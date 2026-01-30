"""
AXIOM Master Data Refresh Script
Fetches and updates ALL data sources in one command.

Usage:
    python scripts/refresh_all_data.py                    # Full refresh
    python scripts/refresh_all_data.py --quick            # Skip slow fetches
    python scripts/refresh_all_data.py --advanced-only    # Only advanced stats
    python scripts/refresh_all_data.py --report           # Generate summary report

Data Sources:
    1. Team Advanced Stats (Pace, ORTG, DRTG, Net Rating)
    2. Player Advanced Stats (TS%, eFG%, USG%, 150 players)
    3. Team Clutch Stats (last 5 min, within 5 pts)
    4. Player Clutch Stats (100 players)
    5. Historical ATS (calculated from games + betting)
    6. Player Game Logs (season stats)
    7. Defense vs Position (DVP rankings)
    8. Player vs Team History
"""

import argparse
import sqlite3
import sys
import time
from datetime import datetime, date
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]

# Check for nba_api
try:
    from nba_api.stats.endpoints import (
        leaguedashteamstats,
        leaguedashplayerstats,
        leaguedashteamclutch,
        leaguedashplayerclutch
    )
    from nba_api.stats.static import teams, players
    NBA_API_AVAILABLE = True
except ImportError:
    NBA_API_AVAILABLE = False


def safe_print(text):
    """Print with encoding safety for Windows console."""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))


def print_header(text):
    """Print formatted header."""
    safe_print("\n" + "=" * 60)
    safe_print(f"  {text}")
    safe_print("=" * 60)


def print_step(num, text):
    """Print step indicator."""
    safe_print(f"\n[Step {num}] {text}")
    safe_print("-" * 40)


# =============================================================================
# TABLE CREATION
# =============================================================================

def create_all_tables(conn):
    """Create all required tables if they don't exist."""

    # Team Advanced Stats
    conn.execute("""
        CREATE TABLE IF NOT EXISTS TeamAdvancedStats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER NOT NULL,
            team_abbrev TEXT NOT NULL,
            team_name TEXT NOT NULL,
            season TEXT NOT NULL,
            games_played INTEGER,
            pace REAL,
            off_rating REAL,
            def_rating REAL,
            net_rating REAL,
            ast_pct REAL,
            ast_to_ratio REAL,
            oreb_pct REAL,
            dreb_pct REAL,
            reb_pct REAL,
            ts_pct REAL,
            efg_pct REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(team_id, season)
        )
    """)

    # Player Advanced Stats
    conn.execute("""
        CREATE TABLE IF NOT EXISTS PlayerAdvancedStats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            player_name TEXT NOT NULL,
            team_abbrev TEXT,
            season TEXT NOT NULL,
            games_played INTEGER,
            minutes REAL,
            ts_pct REAL,
            efg_pct REAL,
            usg_pct REAL,
            ast_pct REAL,
            reb_pct REAL,
            oreb_pct REAL,
            dreb_pct REAL,
            tov_pct REAL,
            off_rating REAL,
            def_rating REAL,
            net_rating REAL,
            pie REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(player_id, season)
        )
    """)

    # Team Clutch Stats
    conn.execute("""
        CREATE TABLE IF NOT EXISTS TeamClutchStats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER NOT NULL,
            team_abbrev TEXT NOT NULL,
            team_name TEXT NOT NULL,
            season TEXT NOT NULL,
            clutch_gp INTEGER,
            clutch_wins INTEGER,
            clutch_losses INTEGER,
            clutch_win_pct REAL,
            clutch_pts REAL,
            clutch_fgm REAL,
            clutch_fga REAL,
            clutch_fg_pct REAL,
            clutch_fg3m REAL,
            clutch_fg3a REAL,
            clutch_fg3_pct REAL,
            clutch_ftm REAL,
            clutch_fta REAL,
            clutch_ft_pct REAL,
            clutch_plus_minus REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(team_id, season)
        )
    """)

    # Player Clutch Stats
    conn.execute("""
        CREATE TABLE IF NOT EXISTS PlayerClutchStats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            player_name TEXT NOT NULL,
            team_abbrev TEXT,
            season TEXT NOT NULL,
            clutch_gp INTEGER,
            clutch_mins REAL,
            clutch_pts REAL,
            clutch_fgm REAL,
            clutch_fga REAL,
            clutch_fg_pct REAL,
            clutch_fg3m REAL,
            clutch_fg3a REAL,
            clutch_fg3_pct REAL,
            clutch_ftm REAL,
            clutch_fta REAL,
            clutch_ft_pct REAL,
            clutch_plus_minus REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(player_id, season)
        )
    """)

    # Team ATS Stats
    conn.execute("""
        CREATE TABLE IF NOT EXISTS TeamATSStats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_abbrev TEXT NOT NULL,
            season TEXT NOT NULL,
            total_games INTEGER DEFAULT 0,
            ats_wins INTEGER DEFAULT 0,
            ats_losses INTEGER DEFAULT 0,
            ats_pushes INTEGER DEFAULT 0,
            ats_win_pct REAL,
            home_ats_wins INTEGER DEFAULT 0,
            home_ats_losses INTEGER DEFAULT 0,
            away_ats_wins INTEGER DEFAULT 0,
            away_ats_losses INTEGER DEFAULT 0,
            as_favorite_wins INTEGER DEFAULT 0,
            as_favorite_losses INTEGER DEFAULT 0,
            as_underdog_wins INTEGER DEFAULT 0,
            as_underdog_losses INTEGER DEFAULT 0,
            avg_spread REAL,
            avg_margin REAL,
            cover_margin REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(team_abbrev, season)
        )
    """)

    # Game ATS Results
    conn.execute("""
        CREATE TABLE IF NOT EXISTS GameATSResults (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            game_date TEXT NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            spread REAL,
            total_line REAL,
            home_score INTEGER,
            away_score INTEGER,
            margin INTEGER,
            home_covered INTEGER,
            away_covered INTEGER,
            push INTEGER DEFAULT 0,
            total_points INTEGER,
            over_hit INTEGER,
            under_hit INTEGER,
            season TEXT,
            UNIQUE(game_id)
        )
    """)

    conn.commit()
    safe_print("All tables created/verified")


# =============================================================================
# DATA FETCHERS
# =============================================================================

def get_team_abbrev_map():
    """Get mapping of team ID to abbreviation."""
    if not NBA_API_AVAILABLE:
        return {}
    team_list = teams.get_teams()
    return {t['id']: t['abbreviation'] for t in team_list}


def fetch_team_advanced_stats(conn, season="2024-25"):
    """Fetch team advanced stats from NBA API."""
    if not NBA_API_AVAILABLE:
        safe_print("  SKIPPED: nba_api not installed")
        return 0

    safe_print(f"  Fetching from NBA API...")

    try:
        stats = leaguedashteamstats.LeagueDashTeamStats(
            season=season,
            measure_type_detailed_defense='Advanced',
            per_mode_detailed='PerGame'
        )
        time.sleep(1)

        df = stats.get_data_frames()[0]
        abbrev_map = get_team_abbrev_map()
        df['TEAM_ABBREV'] = df['TEAM_ID'].map(abbrev_map)

        count = 0
        for _, row in df.iterrows():
            conn.execute("""
                INSERT OR REPLACE INTO TeamAdvancedStats
                (team_id, team_abbrev, team_name, season, games_played,
                 pace, off_rating, def_rating, net_rating,
                 ast_pct, ast_to_ratio, oreb_pct, dreb_pct, reb_pct,
                 ts_pct, efg_pct, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get('TEAM_ID'),
                row.get('TEAM_ABBREV', ''),
                row.get('TEAM_NAME', ''),
                season,
                row.get('GP', 0),
                row.get('E_PACE') or row.get('PACE'),
                row.get('OFF_RATING') or row.get('E_OFF_RATING'),
                row.get('DEF_RATING') or row.get('E_DEF_RATING'),
                row.get('NET_RATING') or row.get('E_NET_RATING'),
                row.get('AST_PCT'),
                row.get('AST_TO') or row.get('AST_TOV'),
                row.get('OREB_PCT'),
                row.get('DREB_PCT'),
                row.get('REB_PCT'),
                row.get('TS_PCT'),
                row.get('EFG_PCT'),
                datetime.now().isoformat()
            ))
            count += 1

        conn.commit()
        safe_print(f"  Saved {count} team records")
        return count

    except Exception as e:
        safe_print(f"  ERROR: {e}")
        return 0


def fetch_player_advanced_stats(conn, season="2024-25", top_n=150):
    """Fetch player advanced stats from NBA API."""
    if not NBA_API_AVAILABLE:
        safe_print("  SKIPPED: nba_api not installed")
        return 0

    safe_print(f"  Fetching top {top_n} players from NBA API...")

    try:
        stats = leaguedashplayerstats.LeagueDashPlayerStats(
            season=season,
            measure_type_detailed_defense='Advanced',
            per_mode_detailed='PerGame'
        )
        time.sleep(1)

        df = stats.get_data_frames()[0]
        df = df.sort_values('MIN', ascending=False).head(top_n)

        count = 0
        for _, row in df.iterrows():
            conn.execute("""
                INSERT OR REPLACE INTO PlayerAdvancedStats
                (player_id, player_name, team_abbrev, season, games_played,
                 minutes, ts_pct, efg_pct, usg_pct, ast_pct, reb_pct,
                 oreb_pct, dreb_pct, tov_pct, off_rating, def_rating,
                 net_rating, pie, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get('PLAYER_ID'),
                row.get('PLAYER_NAME', ''),
                row.get('TEAM_ABBREVIATION', ''),
                season,
                row.get('GP', 0),
                row.get('MIN', 0),
                row.get('TS_PCT'),
                row.get('EFG_PCT'),
                row.get('USG_PCT'),
                row.get('AST_PCT'),
                row.get('REB_PCT'),
                row.get('OREB_PCT'),
                row.get('DREB_PCT'),
                row.get('TM_TOV_PCT'),
                row.get('OFF_RATING') or row.get('E_OFF_RATING'),
                row.get('DEF_RATING') or row.get('E_DEF_RATING'),
                row.get('NET_RATING') or row.get('E_NET_RATING'),
                row.get('PIE'),
                datetime.now().isoformat()
            ))
            count += 1

        conn.commit()
        safe_print(f"  Saved {count} player records")
        return count

    except Exception as e:
        safe_print(f"  ERROR: {e}")
        return 0


def fetch_team_clutch_stats(conn, season="2024-25"):
    """Fetch team clutch stats from NBA API."""
    if not NBA_API_AVAILABLE:
        safe_print("  SKIPPED: nba_api not installed")
        return 0

    safe_print(f"  Fetching team clutch stats...")

    try:
        stats = leaguedashteamclutch.LeagueDashTeamClutch(
            season=season,
            clutch_time='Last 5 Minutes',
            ahead_behind='Ahead or Behind',
            point_diff=5,
            per_mode_detailed='PerGame'
        )
        time.sleep(1)

        df = stats.get_data_frames()[0]
        abbrev_map = get_team_abbrev_map()
        df['TEAM_ABBREV'] = df['TEAM_ID'].map(abbrev_map)

        count = 0
        for _, row in df.iterrows():
            conn.execute("""
                INSERT OR REPLACE INTO TeamClutchStats
                (team_id, team_abbrev, team_name, season, clutch_gp,
                 clutch_wins, clutch_losses, clutch_win_pct, clutch_pts,
                 clutch_fgm, clutch_fga, clutch_fg_pct, clutch_fg3m, clutch_fg3a,
                 clutch_fg3_pct, clutch_ftm, clutch_fta, clutch_ft_pct,
                 clutch_plus_minus, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get('TEAM_ID'),
                row.get('TEAM_ABBREV', ''),
                row.get('TEAM_NAME', ''),
                season,
                row.get('GP', 0),
                row.get('W', 0),
                row.get('L', 0),
                row.get('W_PCT'),
                row.get('PTS'),
                row.get('FGM'),
                row.get('FGA'),
                row.get('FG_PCT'),
                row.get('FG3M'),
                row.get('FG3A'),
                row.get('FG3_PCT'),
                row.get('FTM'),
                row.get('FTA'),
                row.get('FT_PCT'),
                row.get('PLUS_MINUS'),
                datetime.now().isoformat()
            ))
            count += 1

        conn.commit()
        safe_print(f"  Saved {count} team clutch records")
        return count

    except Exception as e:
        safe_print(f"  ERROR: {e}")
        return 0


def fetch_player_clutch_stats(conn, season="2024-25", top_n=100):
    """Fetch player clutch stats from NBA API."""
    if not NBA_API_AVAILABLE:
        safe_print("  SKIPPED: nba_api not installed")
        return 0

    safe_print(f"  Fetching top {top_n} player clutch stats...")

    try:
        stats = leaguedashplayerclutch.LeagueDashPlayerClutch(
            season=season,
            clutch_time='Last 5 Minutes',
            ahead_behind='Ahead or Behind',
            point_diff=5,
            per_mode_detailed='PerGame'
        )
        time.sleep(1)

        df = stats.get_data_frames()[0]
        df = df.sort_values('MIN', ascending=False).head(top_n)

        count = 0
        for _, row in df.iterrows():
            conn.execute("""
                INSERT OR REPLACE INTO PlayerClutchStats
                (player_id, player_name, team_abbrev, season, clutch_gp,
                 clutch_mins, clutch_pts, clutch_fgm, clutch_fga, clutch_fg_pct,
                 clutch_fg3m, clutch_fg3a, clutch_fg3_pct, clutch_ftm, clutch_fta,
                 clutch_ft_pct, clutch_plus_minus, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get('PLAYER_ID'),
                row.get('PLAYER_NAME', ''),
                row.get('TEAM_ABBREVIATION', ''),
                season,
                row.get('GP', 0),
                row.get('MIN'),
                row.get('PTS'),
                row.get('FGM'),
                row.get('FGA'),
                row.get('FG_PCT'),
                row.get('FG3M'),
                row.get('FG3A'),
                row.get('FG3_PCT'),
                row.get('FTM'),
                row.get('FTA'),
                row.get('FT_PCT'),
                row.get('PLUS_MINUS'),
                datetime.now().isoformat()
            ))
            count += 1

        conn.commit()
        safe_print(f"  Saved {count} player clutch records")
        return count

    except Exception as e:
        safe_print(f"  ERROR: {e}")
        return 0


def calculate_ats_stats(conn, season="2025-26"):
    """Calculate ATS stats from game results."""
    safe_print(f"  Calculating ATS from game data...")

    # Normalize season format
    if len(season) == 7:
        normalized_season = f"{season[:4]}-20{season[5:]}"
    else:
        normalized_season = season

    # Get games with scores and spreads
    games = conn.execute("""
        SELECT g.game_id, DATE(g.date_time_utc) as game_date,
               g.home_team, g.away_team, g.season,
               COALESCE(b.espn_closing_spread, b.espn_current_spread, b.espn_opening_spread) as spread,
               COALESCE(b.espn_closing_total, b.espn_current_total, b.espn_opening_total) as total_line,
               tb_home.pts as home_score, tb_away.pts as away_score
        FROM Games g
        JOIN Betting b ON g.game_id = b.game_id
        JOIN Teams t_home ON t_home.abbreviation = g.home_team
        JOIN Teams t_away ON t_away.abbreviation = g.away_team
        JOIN TeamBox tb_home ON g.game_id = tb_home.game_id AND tb_home.team_id = t_home.team_id
        JOIN TeamBox tb_away ON g.game_id = tb_away.game_id AND tb_away.team_id = t_away.team_id
        WHERE (b.espn_closing_spread IS NOT NULL OR b.espn_current_spread IS NOT NULL)
        ORDER BY game_date DESC
    """).fetchall()

    game_count = 0
    for game in games:
        game_id, game_date, home, away, game_season, spread, total_line, home_score, away_score = game

        if spread is None or home_score is None or away_score is None:
            continue

        margin = home_score - away_score
        total_points = home_score + away_score
        ats_margin = margin + spread

        if abs(ats_margin) < 0.5:
            home_covered, away_covered, push = 0, 0, 1
        elif ats_margin > 0:
            home_covered, away_covered, push = 1, 0, 0
        else:
            home_covered, away_covered, push = 0, 1, 0

        over_hit = 1 if total_line and total_points > total_line else 0
        under_hit = 1 if total_line and total_points < total_line else 0

        conn.execute("""
            INSERT OR REPLACE INTO GameATSResults
            (game_id, game_date, home_team, away_team, spread, total_line,
             home_score, away_score, margin, home_covered, away_covered, push,
             total_points, over_hit, under_hit, season)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            game_id, game_date, home, away, spread, total_line,
            home_score, away_score, margin, home_covered, away_covered, push,
            total_points, over_hit, under_hit, game_season
        ))
        game_count += 1

    conn.commit()
    safe_print(f"  Saved {game_count} game ATS results")

    # Aggregate team stats
    all_teams = conn.execute("""
        SELECT DISTINCT home_team FROM GameATSResults
        UNION
        SELECT DISTINCT away_team FROM GameATSResults
    """).fetchall()

    for (team,) in all_teams:
        home_stats = conn.execute("""
            SELECT COUNT(*), SUM(home_covered), SUM(away_covered), SUM(push),
                   AVG(spread), AVG(margin), AVG(margin + spread)
            FROM GameATSResults WHERE home_team = ?
        """, (team,)).fetchone()

        away_stats = conn.execute("""
            SELECT COUNT(*), SUM(away_covered), SUM(home_covered), SUM(push)
            FROM GameATSResults WHERE away_team = ?
        """, (team,)).fetchone()

        fav_stats = conn.execute("""
            SELECT
                SUM(CASE WHEN home_team = ? AND spread < 0 THEN home_covered
                         WHEN away_team = ? AND spread > 0 THEN away_covered ELSE 0 END),
                SUM(CASE WHEN home_team = ? AND spread < 0 THEN away_covered
                         WHEN away_team = ? AND spread > 0 THEN home_covered ELSE 0 END)
            FROM GameATSResults WHERE home_team = ? OR away_team = ?
        """, (team, team, team, team, team, team)).fetchone()

        dog_stats = conn.execute("""
            SELECT
                SUM(CASE WHEN home_team = ? AND spread > 0 THEN home_covered
                         WHEN away_team = ? AND spread < 0 THEN away_covered ELSE 0 END),
                SUM(CASE WHEN home_team = ? AND spread > 0 THEN away_covered
                         WHEN away_team = ? AND spread < 0 THEN home_covered ELSE 0 END)
            FROM GameATSResults WHERE home_team = ? OR away_team = ?
        """, (team, team, team, team, team, team)).fetchone()

        total_games = (home_stats[0] or 0) + (away_stats[0] or 0)
        total_wins = (home_stats[1] or 0) + (away_stats[1] or 0)
        total_losses = (home_stats[2] or 0) + (away_stats[2] or 0)
        total_pushes = (home_stats[3] or 0) + (away_stats[3] or 0)
        win_pct = total_wins / (total_wins + total_losses) if (total_wins + total_losses) > 0 else 0

        conn.execute("""
            INSERT OR REPLACE INTO TeamATSStats
            (team_abbrev, season, total_games, ats_wins, ats_losses, ats_pushes, ats_win_pct,
             home_ats_wins, home_ats_losses, away_ats_wins, away_ats_losses,
             as_favorite_wins, as_favorite_losses, as_underdog_wins, as_underdog_losses,
             avg_spread, avg_margin, cover_margin, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            team, season, total_games, total_wins, total_losses, total_pushes, win_pct,
            home_stats[1] or 0, home_stats[2] or 0,
            away_stats[1] or 0, away_stats[2] or 0,
            fav_stats[0] or 0, fav_stats[1] or 0,
            dog_stats[0] or 0, dog_stats[1] or 0,
            home_stats[4], home_stats[5], home_stats[6],
            datetime.now().isoformat()
        ))

    conn.commit()
    safe_print(f"  Aggregated ATS for {len(all_teams)} teams")
    return game_count


def run_external_script(script_name, args=None):
    """Run an external Python script."""
    import subprocess
    cmd = [sys.executable, f"scripts/{script_name}"]
    if args:
        cmd.extend(args)

    try:
        result = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True)
        if result.returncode == 0:
            safe_print(f"  SUCCESS")
            return True
        else:
            safe_print(f"  FAILED: {result.stderr[:200]}")
            return False
    except Exception as e:
        safe_print(f"  ERROR: {e}")
        return False


# =============================================================================
# REPORT GENERATION
# =============================================================================

def generate_data_report(conn):
    """Generate a summary report of all data."""
    report = []
    report.append("# AXIOM Data Inventory Report")
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report.append("")

    # Table counts
    report.append("## Data Summary")
    report.append("")
    report.append("| Table | Records | Last Updated |")
    report.append("|-------|---------|--------------|")

    tables = [
        ("TeamAdvancedStats", "updated_at"),
        ("PlayerAdvancedStats", "updated_at"),
        ("TeamClutchStats", "updated_at"),
        ("PlayerClutchStats", "updated_at"),
        ("TeamATSStats", "updated_at"),
        ("GameATSResults", "game_date"),
        ("Games", "date_time_utc"),
        ("Betting", None),
        ("TeamBox", None),
        ("PlayerBox", None),
    ]

    for table, date_col in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            if date_col:
                last = conn.execute(f"SELECT MAX({date_col}) FROM {table}").fetchone()[0]
                last = last[:10] if last else "N/A"
            else:
                last = "N/A"
            report.append(f"| {table} | {count:,} | {last} |")
        except:
            report.append(f"| {table} | N/A | N/A |")

    # Key insights
    report.append("")
    report.append("## Key Betting Insights")
    report.append("")

    # Best/Worst ATS
    report.append("### ATS Leaders")
    ats = conn.execute("""
        SELECT team_abbrev, ats_wins, ats_losses, ats_win_pct
        FROM TeamATSStats ORDER BY ats_win_pct DESC LIMIT 5
    """).fetchall()
    report.append("**Best ATS:**")
    for team, w, l, pct in ats:
        report.append(f"- {team}: {w}-{l} ({pct*100:.1f}%)")

    ats_worst = conn.execute("""
        SELECT team_abbrev, ats_wins, ats_losses, ats_win_pct
        FROM TeamATSStats ORDER BY ats_win_pct ASC LIMIT 5
    """).fetchall()
    report.append("")
    report.append("**Worst ATS:**")
    for team, w, l, pct in ats_worst:
        report.append(f"- {team}: {w}-{l} ({pct*100:.1f}%)")

    # Pace extremes
    report.append("")
    report.append("### Pace Extremes")
    pace = conn.execute("""
        SELECT team_abbrev, pace FROM TeamAdvancedStats
        WHERE pace IS NOT NULL ORDER BY pace DESC LIMIT 3
    """).fetchall()
    report.append("**Fastest (OVER candidates):**")
    for team, p in pace:
        report.append(f"- {team}: {p:.1f}")

    pace_slow = conn.execute("""
        SELECT team_abbrev, pace FROM TeamAdvancedStats
        WHERE pace IS NOT NULL ORDER BY pace ASC LIMIT 3
    """).fetchall()
    report.append("")
    report.append("**Slowest (UNDER candidates):**")
    for team, p in pace_slow:
        report.append(f"- {team}: {p:.1f}")

    # Clutch
    report.append("")
    report.append("### Clutch Performance")
    clutch = conn.execute("""
        SELECT team_abbrev, clutch_win_pct FROM TeamClutchStats
        ORDER BY clutch_win_pct DESC LIMIT 5
    """).fetchall()
    report.append("**Best Clutch:**")
    for team, pct in clutch:
        if pct:
            report.append(f"- {team}: {pct*100:.1f}%")

    # Player efficiency
    report.append("")
    report.append("### Player Efficiency")
    eff = conn.execute("""
        SELECT player_name, team_abbrev, ts_pct, usg_pct FROM PlayerAdvancedStats
        WHERE ts_pct >= 0.62 AND usg_pct >= 0.20
        ORDER BY ts_pct DESC LIMIT 5
    """).fetchall()
    report.append("**Elite Efficiency (TS% > 62%, USG > 20%):**")
    for name, team, ts, usg in eff:
        clean_name = name.encode('ascii', 'replace').decode('ascii')
        report.append(f"- {clean_name} ({team}): {ts*100:.1f}% TS, {usg*100:.1f}% USG")

    fade = conn.execute("""
        SELECT player_name, team_abbrev, ts_pct, usg_pct FROM PlayerAdvancedStats
        WHERE ts_pct < 0.55 AND usg_pct >= 0.25
        ORDER BY usg_pct DESC LIMIT 5
    """).fetchall()
    report.append("")
    report.append("**Fade Candidates (TS% < 55%, USG > 25%):**")
    for name, team, ts, usg in fade:
        clean_name = name.encode('ascii', 'replace').decode('ascii')
        report.append(f"- {clean_name} ({team}): {ts*100:.1f}% TS, {usg*100:.1f}% USG")

    return "\n".join(report)


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="AXIOM Master Data Refresh")
    parser.add_argument("--season", default="2024-25", help="Season (e.g., 2024-25)")
    parser.add_argument("--quick", action="store_true", help="Skip slow fetches (player logs, DVP)")
    parser.add_argument("--advanced-only", action="store_true", help="Only fetch advanced stats")
    parser.add_argument("--report", action="store_true", help="Generate summary report")
    parser.add_argument("--output", type=str, help="Output report to file")
    args = parser.parse_args()

    print_header(f"AXIOM DATA REFRESH - {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    conn = sqlite3.connect(DB_PATH)

    # Create tables
    print_step(0, "Creating/Verifying Tables")
    create_all_tables(conn)

    # Step 1: Team Advanced Stats
    print_step(1, "Team Advanced Stats (Pace/ORTG/DRTG)")
    fetch_team_advanced_stats(conn, args.season)

    # Step 2: Player Advanced Stats
    print_step(2, "Player Advanced Stats (TS%/eFG%/USG%)")
    fetch_player_advanced_stats(conn, args.season, top_n=150)

    # Step 3: Team Clutch Stats
    print_step(3, "Team Clutch Stats")
    fetch_team_clutch_stats(conn, args.season)

    # Step 4: Player Clutch Stats
    print_step(4, "Player Clutch Stats")
    fetch_player_clutch_stats(conn, args.season, top_n=100)

    # Step 5: ATS Stats
    print_step(5, "Historical ATS Calculation")
    calculate_ats_stats(conn, "2025-26")

    if not args.advanced_only:
        # Step 6: Play Types (ISO, PnR, Transition, etc.)
        print_step(6, "Play Type Data")
        if run_external_script("fetch_play_types.py", ["--season", args.season]):
            safe_print("  Play types updated")

        # Step 7: Hustle Stats
        print_step(7, "Hustle Stats (Deflections, Contested Shots)")
        if run_external_script("fetch_hustle_stats.py", ["--season", args.season]):
            safe_print("  Hustle stats updated")

        # Step 8: Shooting Zones
        print_step(8, "Shooting Zones (Paint, Corner 3, etc.)")
        if run_external_script("fetch_shooting_zones.py", ["--season", args.season]):
            safe_print("  Shooting zones updated")

        # Step 9: Fatigue Patterns
        print_step(9, "Fatigue Patterns (B2B, 3-in-4, etc.)")
        if run_external_script("calculate_fatigue_patterns.py", ["--rebuild", "--season", "2025-2026"]):
            safe_print("  Fatigue patterns updated")

    if not args.advanced_only and not args.quick:
        # Step 10: Player Game Logs
        print_step(10, "Player Game Logs")
        if run_external_script("fetch_player_logs.py", ["--season", args.season]):
            safe_print("  Player logs updated")

        # Step 11: Defense vs Position
        print_step(11, "Defense vs Position (DVP)")
        if run_external_script("fetch_dvp.py", ["--season", args.season]):
            safe_print("  DVP updated")

        # Step 12: Player vs Team History
        print_step(12, "Player vs Team History")
        if run_external_script("build_player_vs_team.py", ["--season", args.season]):
            safe_print("  Player vs Team updated")

    # Generate report
    if args.report or args.output:
        print_step("R", "Generating Data Report")
        report = generate_data_report(conn)

        if args.output:
            output_path = Path(args.output)
            output_path.write_text(report, encoding='utf-8')
            safe_print(f"  Report saved to: {args.output}")
        else:
            safe_print(report)

    conn.close()

    print_header("DATA REFRESH COMPLETE")

    # Summary
    conn = sqlite3.connect(DB_PATH)
    safe_print("\nData Summary:")
    tables = [
        "TeamAdvancedStats", "PlayerAdvancedStats",
        "TeamClutchStats", "PlayerClutchStats",
        "TeamATSStats", "GameATSResults",
        "TeamPlayTypes", "PlayerPlayTypes",
        "TeamHustleStats", "PlayerHustleStats",
        "TeamShootingZones", "TeamFatiguePatterns"
    ]
    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            safe_print(f"  {table}: {count:,} records")
        except:
            pass
    conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
