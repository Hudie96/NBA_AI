# AXIOM NHL - Project Setup Script

## Overview
Build an NHL betting prediction system modeled after AXIOM NBA. Data-driven picks with tiered confidence ratings (PLATINUM/GOLD/SILVER) for spreads, moneylines, totals, and player props.

## Core Architecture

### Database Schema (SQLite)
```sql
-- Games table
CREATE TABLE Games (
    game_id TEXT PRIMARY KEY,
    date_time_utc TEXT,
    home_team TEXT,
    away_team TEXT,
    home_score INTEGER,
    away_score INTEGER,
    status TEXT,
    season TEXT
);

-- Team Stats
CREATE TABLE TeamAdvancedStats (
    team_abbrev TEXT PRIMARY KEY,
    goals_per_game REAL,
    goals_against_per_game REAL,
    power_play_pct REAL,
    penalty_kill_pct REAL,
    shots_per_game REAL,
    shots_against_per_game REAL,
    faceoff_win_pct REAL,
    corsi_for_pct REAL,  -- possession metric
    fenwick_for_pct REAL,
    expected_goals_for REAL,
    expected_goals_against REAL,
    updated_at TEXT
);

-- Player Stats (for props)
CREATE TABLE PlayerBox (
    id INTEGER PRIMARY KEY,
    game_id TEXT,
    player_id TEXT,
    player_name TEXT,
    team TEXT,
    position TEXT,
    goals INTEGER,
    assists INTEGER,
    points INTEGER,
    shots INTEGER,
    hits INTEGER,
    blocked_shots INTEGER,
    time_on_ice REAL,  -- in minutes
    power_play_points INTEGER,
    plus_minus INTEGER
);

-- Betting Lines
CREATE TABLE Betting (
    game_id TEXT PRIMARY KEY,
    puckline_home REAL,      -- typically -1.5 or +1.5
    puckline_away REAL,
    total REAL,
    ml_home INTEGER,
    ml_away INTEGER,
    updated_at TEXT
);

-- Goalie Stats (critical for NHL)
CREATE TABLE GoalieStats (
    player_id TEXT,
    player_name TEXT,
    team TEXT,
    games_played INTEGER,
    wins INTEGER,
    losses INTEGER,
    save_pct REAL,
    goals_against_avg REAL,
    shutouts INTEGER,
    quality_starts INTEGER,
    updated_at TEXT
);
```

### Data Sources
1. **NHL API** (free): https://api-web.nhle.com/
   - Schedule: `/v1/schedule/{date}`
   - Boxscores: `/v1/gamecenter/{game_id}/boxscore`
   - Standings: `/v1/standings/{date}`
   - Player stats: `/v1/player/{player_id}/landing`

2. **ESPN API** (free): For betting lines
   - `https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard`

3. **Natural Stat Trick** (scrape): Advanced stats (Corsi, Fenwick, xG)

### Key Metrics for NHL Predictions

**Team Level:**
- Goals For/Against per game
- Expected Goals (xG) differential
- Corsi For % (shot attempt differential)
- Power Play % / Penalty Kill %
- Home/Away splits
- Back-to-back fatigue (huge in NHL)
- Goalie starter info

**Player Props:**
- Shots on Goal (SOG)
- Points (Goals + Assists)
- Assists
- Blocked Shots
- Hits
- Time on Ice
- Power Play Points

### Tier System

**Spreads (Puckline):**
- PLATINUM: Model edge >= 0.8 goals + goalie advantage
- GOLD: Model edge >= 0.5 goals
- SILVER: Model edge >= 0.3 goals

**Moneyline:**
- PLATINUM: Strong favorite with goalie edge
- GOLD: Model disagrees with line by 15%+
- SILVER: Model disagrees by 10%+

**Player Props:**
- PLATINUM: Edge >= 25% (star players only)
- GOLD: Edge >= 20%
- SILVER: Edge >= 15%

### Critical NHL-Specific Factors

1. **Goalie Starters** - Most important factor
   - Check daily for confirmed starters
   - Backup goalies = fade that team
   - Save % and GAA trends

2. **Back-to-Back Games**
   - NHL teams struggle badly on B2B
   - Especially road B2B
   - Often start backup goalie

3. **Rest Advantage**
   - 2+ days rest vs B2B = significant edge
   - Track travel distance too

4. **Home Ice**
   - Last change advantage (matchups)
   - Worth ~0.1-0.2 goals

5. **Special Teams**
   - Power play % matters a lot
   - Penalty kill % in matchups

### Daily Pipeline

```python
# scripts/run_daily.py

1. Verify data freshness
2. Fetch today's schedule + betting lines
3. Check goalie starters (critical!)
4. Calculate team projections
5. Generate spread/ML/total picks
6. Find player prop edges
7. Generate output files
```

### Star Players for Props (examples)
Connor McDavid, Leon Draisaitl, Nathan MacKinnon, Auston Matthews,
Nikita Kucherov, Cale Makar, David Pastrnak, Kirill Kaprizov,
Jack Eichel, Mika Zibanejad, Matthew Tkachuk, Jason Robertson

Filter by: 15+ minutes TOI average, top 150 scorers

### Output Files
- `picks_YYYY-MM-DD.csv` - All picks with tiers
- `posts_YYYY-MM-DD.txt` - Social media content
- `performance_tracker.csv` - Running record

### Sample Prediction Logic

```python
def predict_game(home_team, away_team, home_goalie, away_goalie):
    # Base projection from team stats
    home_xg = home_team.expected_goals_for
    away_xg = away_team.expected_goals_for

    # Adjust for opponent defense
    home_xg *= (away_team.goals_against_per_game / league_avg_goals)
    away_xg *= (home_team.goals_against_per_game / league_avg_goals)

    # Goalie adjustment (huge factor)
    home_xg *= (1 - home_goalie.save_pct) / (1 - league_avg_save_pct)
    away_xg *= (1 - away_goalie.save_pct) / (1 - league_avg_save_pct)

    # Home ice advantage
    home_xg *= 1.03
    away_xg *= 0.97

    # Back-to-back adjustment
    if home_team.is_b2b:
        home_xg *= 0.92
    if away_team.is_b2b:
        away_xg *= 0.92

    return home_xg, away_xg
```

### Getting Started

1. Set up SQLite database with schema above
2. Build NHL API fetchers for schedule/boxscores
3. Build goalie starter scraper (Daily Faceoff)
4. Implement team projection model
5. Add player prop projections
6. Create tier classification
7. Build daily pipeline
8. Track results

### Key Differences from NBA
- Goalie starters matter MORE than any NBA equivalent
- Puckline (-1.5) instead of spread
- Lower scoring = more variance
- Back-to-backs even more impactful
- Fewer games = smaller sample sizes
