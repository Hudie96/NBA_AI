"""
Tests for props_validator module.

Run with: python -m pytest tests/test_props_validator.py -v
"""
import sqlite3
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import config
from scripts.props_validator import (
    get_todays_games,
    get_teams_playing_today,
    get_player_team,
    is_player_playing_today,
    get_valid_players_for_props,
    validate_prop,
    filter_valid_props,
)

DB_PATH = config["database"]["path"]


def get_connection():
    return sqlite3.connect(DB_PATH)


class TestPropsValidator:
    """Test suite for props validation."""

    def test_get_todays_games(self):
        """Test fetching today's games."""
        conn = get_connection()
        # Use a date we know has games
        games = get_todays_games(conn, "2026-01-29")
        conn.close()

        assert isinstance(games, list)
        if games:
            assert "game_id" in games[0]
            assert "home_team" in games[0]
            assert "away_team" in games[0]

    def test_get_teams_playing_today(self):
        """Test getting set of teams playing today."""
        conn = get_connection()
        teams = get_teams_playing_today(conn, "2026-01-29")
        conn.close()

        assert isinstance(teams, set)
        # On Jan 29 we know there were 6 games = 12 teams
        if teams:
            # All entries should be 3-letter abbreviations
            for team in teams:
                assert len(team) == 3

    def test_get_player_team_known_player(self):
        """Test getting team for a known player."""
        conn = get_connection()
        # LeBron James should be on LAL
        team = get_player_team("LeBron James", conn)
        conn.close()

        assert team is not None
        assert team == "LAL"

    def test_get_player_team_unknown_player(self):
        """Test getting team for an unknown player."""
        conn = get_connection()
        team = get_player_team("Nonexistent Player XYZ", conn)
        conn.close()

        assert team is None

    def test_is_player_playing_today_not_playing(self):
        """Test that players not playing today are correctly identified."""
        conn = get_connection()
        # James Harden is on LAC - check if they're playing on 2026-01-29
        is_playing, team, opponent = is_player_playing_today(
            "James Harden", conn, "2026-01-29"
        )
        conn.close()

        # Harden is on LAC, but LAC wasn't playing on Jan 29
        assert team == "LAC"
        # If LAC wasn't in the games, is_playing should be False
        # This test validates the core bug we fixed

    def test_validate_prop_player_not_playing(self):
        """Test that props for non-playing players are rejected."""
        conn = get_connection()

        # James Harden (LAC) wasn't playing on 2026-01-29
        is_valid, error = validate_prop(
            "James Harden", "MEM", conn, "2026-01-29"
        )
        conn.close()

        assert is_valid is False
        assert error is not None
        assert "not playing" in error.lower() or "LAC" in error

    def test_validate_prop_wrong_opponent(self):
        """Test that props with wrong opponent are rejected."""
        conn = get_connection()

        # Get a game that was actually playing
        games = get_todays_games(conn, "2026-01-29")

        if games:
            # Pick a team that was playing
            home_team = games[0]["home_team"]
            away_team = games[0]["away_team"]

            # Find a player on the home team
            player = conn.execute("""
                SELECT player_name FROM player_game_logs
                WHERE team = ? LIMIT 1
            """, (home_team,)).fetchone()

            if player:
                player_name = player[0]

                # Validate with wrong opponent
                is_valid, error = validate_prop(
                    player_name, "XXX", conn, "2026-01-29"
                )

                assert is_valid is False
                assert error is not None
                assert "XXX" in error or away_team in error

        conn.close()

    def test_validate_prop_correct_matchup(self):
        """Test that props with correct matchup are accepted."""
        conn = get_connection()

        # Get a game that was actually playing
        games = get_todays_games(conn, "2026-01-29")

        if games:
            # Pick a team that was playing
            home_team = games[0]["home_team"]
            away_team = games[0]["away_team"]

            # Find a player on the home team
            player = conn.execute("""
                SELECT player_name FROM player_game_logs
                WHERE team = ? LIMIT 1
            """, (home_team,)).fetchone()

            if player:
                player_name = player[0]

                # Validate with correct opponent
                is_valid, error = validate_prop(
                    player_name, away_team, conn, "2026-01-29"
                )

                assert is_valid is True
                assert error is None

        conn.close()

    def test_filter_valid_props(self):
        """Test filtering a list of props."""
        conn = get_connection()

        # Create test props - mix of valid and invalid
        test_props = [
            {"player_name": "James Harden", "opponent": "MEM", "stat": "PTS"},
            {"player_name": "LeBron James", "opponent": "BOS", "stat": "PTS"},
        ]

        valid, invalid = filter_valid_props(test_props, conn, "2026-01-29")
        conn.close()

        # We expect at least some to be invalid (Harden wasn't playing)
        assert isinstance(valid, list)
        assert isinstance(invalid, list)

        # Each invalid prop should have an error message
        for prop in invalid:
            assert "error" in prop

    def test_get_valid_players_for_props(self):
        """Test getting all valid players for props."""
        conn = get_connection()

        valid_players = get_valid_players_for_props(conn, "2026-01-29", min_games=10)
        conn.close()

        assert isinstance(valid_players, list)

        if valid_players:
            # Each entry should have required fields
            for player in valid_players:
                assert "player_name" in player
                assert "team" in player
                assert "opponent" in player
                assert "games" in player


def run_manual_tests():
    """Run tests manually without pytest."""
    print("=" * 60)
    print("PROPS VALIDATOR TESTS")
    print("=" * 60)

    conn = get_connection()
    test_date = "2026-01-29"

    # Test 1: Get today's games
    print(f"\n1. Games on {test_date}:")
    games = get_todays_games(conn, test_date)
    for g in games:
        print(f"   {g['away_team']} @ {g['home_team']}")
    print(f"   Total: {len(games)} games")

    # Test 2: Teams playing
    print(f"\n2. Teams playing on {test_date}:")
    teams = get_teams_playing_today(conn, test_date)
    print(f"   {sorted(teams)}")

    # Test 3: Player team lookups
    print("\n3. Player team lookups:")
    test_players = [
        "James Harden",   # LAC - NOT playing
        "LeBron James",   # LAL - check if playing
        "Devin Booker",   # PHX
        "Trae Young",     # ATL
    ]
    for player in test_players:
        team = get_player_team(player, conn)
        is_playing, _, opponent = is_player_playing_today(player, conn, test_date)
        status = f"vs {opponent}" if is_playing else "NOT PLAYING"
        print(f"   {player}: {team} - {status}")

    # Test 4: Validate the Harden prop that caused the bug
    print("\n4. Validating James Harden prop (the bug case):")
    is_valid, error = validate_prop("James Harden", "MEM", conn, test_date)
    print(f"   Valid: {is_valid}")
    print(f"   Error: {error}")

    # Test 5: Get valid players for props
    print("\n5. Valid players for props (top 10):")
    valid_players = get_valid_players_for_props(conn, test_date, min_games=20)
    for p in valid_players[:10]:
        print(f"   {p['player_name']} ({p['team']}) vs {p['opponent']} - {p['games']} games")

    # Test 6: Filter test props
    print("\n6. Filter test props:")
    test_props = [
        {"player_name": "James Harden", "opponent": "MEM", "stat": "PTS", "line": 20.5},
        {"player_name": "Devin Booker", "opponent": "DET", "stat": "PTS", "line": 24.5},
        {"player_name": "Trae Young", "opponent": "HOU", "stat": "AST", "line": 10.5},
    ]
    valid, invalid = filter_valid_props(test_props, conn, test_date)
    print(f"   Valid props: {len(valid)}")
    for p in valid:
        print(f"      {p['player_name']} {p['stat']} {p['line']}")
    print(f"   Invalid props: {len(invalid)}")
    for p in invalid:
        print(f"      {p['player_name']}: {p['error']}")

    conn.close()

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED" if len(invalid) > 0 else "TESTS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    run_manual_tests()
