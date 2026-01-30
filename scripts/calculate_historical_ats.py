"""
AXIOM Phase 4: Historical ATS Calculator
Calculates team ATS records from existing game data.

Usage:
    python scripts/calculate_historical_ats.py
    python scripts/calculate_historical_ats.py --season 2024-25
"""

import sqlite3
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config

DB_PATH = config["database"]["path"]


def safe_print(text):
    """Print with encoding safety for Windows console."""
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', 'replace').decode('ascii'))


def create_ats_tables(conn):
    """Create tables for ATS stats."""
    # Team ATS summary
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

    # Game-by-game ATS results
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
    safe_print("Created/verified ATS tables")


def normalize_season(season):
    """Convert season format (2024-25 -> 2024-2025 or vice versa)."""
    if len(season) == 7:  # 2024-25 format
        year1 = season[:4]
        year2 = "20" + season[5:]
        return f"{year1}-{year2}"
    return season


def calculate_game_ats(conn, season="2024-25"):
    """Calculate ATS results for each game."""
    normalized_season = normalize_season(season)
    safe_print(f"\nCalculating game-by-game ATS results for {normalized_season}...")

    # Get all games with scores and spreads
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
          AND (g.season = ? OR g.season LIKE ?)
        ORDER BY game_date DESC
    """, (normalized_season, f"%{season.split('-')[0]}%")).fetchall()

    count = 0
    for game in games:
        game_id, game_date, home, away, game_season, spread, total_line, home_score, away_score = game

        if spread is None or home_score is None or away_score is None:
            continue

        margin = home_score - away_score
        total_points = home_score + away_score

        # ATS calculations (spread is from home perspective: negative = home favored)
        # Home covers if: margin > -spread (i.e., home wins by more than spread)
        ats_margin = margin + spread

        if abs(ats_margin) < 0.5:  # Push
            home_covered = 0
            away_covered = 0
            push = 1
        elif ats_margin > 0:  # Home covered
            home_covered = 1
            away_covered = 0
            push = 0
        else:  # Away covered
            home_covered = 0
            away_covered = 1
            push = 0

        # Total (over/under)
        if total_line:
            if total_points > total_line:
                over_hit = 1
                under_hit = 0
            elif total_points < total_line:
                over_hit = 0
                under_hit = 1
            else:
                over_hit = 0
                under_hit = 0
        else:
            over_hit = None
            under_hit = None

        try:
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
            count += 1
        except Exception as e:
            safe_print(f"Error saving game {game_id}: {e}")

    conn.commit()
    safe_print(f"Saved {count} game ATS results")
    return count


def calculate_team_ats(conn, season="2024-25"):
    """Aggregate ATS stats by team."""
    safe_print(f"\nAggregating team ATS records...")

    # Get all teams
    teams = conn.execute("""
        SELECT DISTINCT home_team FROM GameATSResults
        UNION
        SELECT DISTINCT away_team FROM GameATSResults
    """).fetchall()

    for (team,) in teams:
        # Home games
        home_stats = conn.execute("""
            SELECT
                COUNT(*) as games,
                SUM(home_covered) as wins,
                SUM(away_covered) as losses,
                SUM(push) as pushes,
                AVG(spread) as avg_spread,
                AVG(margin) as avg_margin,
                AVG(margin + spread) as cover_margin
            FROM GameATSResults
            WHERE home_team = ?
        """, (team,)).fetchone()

        # Away games
        away_stats = conn.execute("""
            SELECT
                COUNT(*) as games,
                SUM(away_covered) as wins,
                SUM(home_covered) as losses,
                SUM(push) as pushes
            FROM GameATSResults
            WHERE away_team = ?
        """, (team,)).fetchone()

        # As favorite (spread < 0 when home, spread > 0 when away)
        fav_stats = conn.execute("""
            SELECT
                SUM(CASE WHEN home_team = ? AND spread < 0 THEN home_covered
                         WHEN away_team = ? AND spread > 0 THEN away_covered
                         ELSE 0 END) as wins,
                SUM(CASE WHEN home_team = ? AND spread < 0 THEN away_covered
                         WHEN away_team = ? AND spread > 0 THEN home_covered
                         ELSE 0 END) as losses
            FROM GameATSResults
            WHERE home_team = ? OR away_team = ?
        """, (team, team, team, team, team, team)).fetchone()

        # As underdog (spread > 0 when home, spread < 0 when away)
        dog_stats = conn.execute("""
            SELECT
                SUM(CASE WHEN home_team = ? AND spread > 0 THEN home_covered
                         WHEN away_team = ? AND spread < 0 THEN away_covered
                         ELSE 0 END) as wins,
                SUM(CASE WHEN home_team = ? AND spread > 0 THEN away_covered
                         WHEN away_team = ? AND spread < 0 THEN home_covered
                         ELSE 0 END) as losses
            FROM GameATSResults
            WHERE home_team = ? OR away_team = ?
        """, (team, team, team, team, team, team)).fetchone()

        # Calculate totals
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
    safe_print(f"Saved ATS stats for {len(teams)} teams")


def display_ats_stats(conn, season="2025-26"):
    """Display ATS statistics."""
    # Use season as-is (the data is stored in the format passed to calculate_team_ats)
    safe_print("\n" + "=" * 70)
    safe_print(f"HISTORICAL ATS RECORDS - {season}")
    safe_print("=" * 70)

    # Overall ATS rankings
    safe_print("\nOVERALL ATS RANKINGS:")
    safe_print("-" * 60)
    safe_print(f"{'Rank':<5} {'Team':<6} {'Record':<12} {'WIN%':<8} {'Home':<10} {'Away':<10}")
    safe_print("-" * 60)

    rankings = conn.execute("""
        SELECT team_abbrev, ats_wins, ats_losses, ats_pushes, ats_win_pct,
               home_ats_wins, home_ats_losses, away_ats_wins, away_ats_losses
        FROM TeamATSStats
        WHERE season = ?
        ORDER BY ats_win_pct DESC
    """, (season,)).fetchall()

    for i, (team, w, l, p, pct, hw, hl, aw, al) in enumerate(rankings, 1):
        record = f"{w}-{l}" + (f"-{p}" if p else "")
        pct_str = f"{pct*100:.1f}%" if pct else "N/A"
        home_rec = f"{hw}-{hl}"
        away_rec = f"{aw}-{al}"
        safe_print(f"{i:<5} {team:<6} {record:<12} {pct_str:<8} {home_rec:<10} {away_rec:<10}")

    # Favorite/Underdog splits
    safe_print("\n" + "-" * 60)
    safe_print("AS UNDERDOG (Best to Worst):")
    safe_print("-" * 60)

    dog_rankings = conn.execute("""
        SELECT team_abbrev, as_underdog_wins, as_underdog_losses,
               CAST(as_underdog_wins AS FLOAT) / NULLIF(as_underdog_wins + as_underdog_losses, 0) as dog_pct
        FROM TeamATSStats
        WHERE season = ? AND (as_underdog_wins + as_underdog_losses) >= 3
        ORDER BY dog_pct DESC
    """, (season,)).fetchall()

    for team, w, l, pct in dog_rankings[:10]:
        pct_str = f"{pct*100:.1f}%" if pct else "N/A"
        safe_print(f"  {team}: {w}-{l} ({pct_str})")

    # Betting insights
    safe_print("\n" + "=" * 70)
    safe_print("BETTING INSIGHTS")
    safe_print("=" * 70)

    # Best ATS teams
    safe_print("\nBEST ATS TEAMS (55%+ cover rate):")
    best = [r for r in rankings if r[4] and r[4] >= 0.55]
    for team, w, l, p, pct, _, _, _, _ in best:
        safe_print(f"  {team}: {w}-{l} ({pct*100:.1f}%)")

    # Worst ATS teams
    safe_print("\nWORST ATS TEAMS (<45% cover rate):")
    worst = [r for r in rankings if r[4] and r[4] < 0.45]
    for team, w, l, p, pct, _, _, _, _ in worst:
        safe_print(f"  {team}: {w}-{l} ({pct*100:.1f}%)")

    # Home vs Away disparity
    safe_print("\nHOME/AWAY DISPARITY:")
    for team, w, l, p, pct, hw, hl, aw, al in rankings:
        home_pct = hw / (hw + hl) if (hw + hl) > 0 else 0
        away_pct = aw / (aw + al) if (aw + al) > 0 else 0
        diff = abs(home_pct - away_pct)
        if diff >= 0.20:  # 20%+ difference
            better = "HOME" if home_pct > away_pct else "AWAY"
            safe_print(f"  {team}: {better} much better ({home_pct*100:.0f}% home vs {away_pct*100:.0f}% away)")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", default="2025-26", help="Season (e.g., 2025-26)")
    parser.add_argument("--all", action="store_true", help="Process all seasons")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)

    # Create tables
    create_ats_tables(conn)

    if args.all:
        # Process all seasons
        seasons = conn.execute("SELECT DISTINCT season FROM Games").fetchall()
        for (season,) in seasons:
            safe_print(f"\n{'='*70}")
            safe_print(f"Processing season: {season}")
            calculate_game_ats(conn, season)
            calculate_team_ats(conn, season)
        display_ats_stats(conn, seasons[-1][0] if seasons else "2025-2026")
    else:
        # Process single season
        calculate_game_ats(conn, args.season)
        calculate_team_ats(conn, args.season)
        display_ats_stats(conn, args.season)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
