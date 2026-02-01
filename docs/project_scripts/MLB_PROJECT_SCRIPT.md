# AXIOM MLB - Project Setup Script

## Overview
Build an MLB betting prediction system modeled after AXIOM NBA. Data-driven picks with tiered confidence ratings (PLATINUM/GOLD/SILVER) for moneylines, run lines, totals, and player props.

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
    season TEXT,
    doubleheader INTEGER,  -- 0, 1, or 2
    day_night TEXT,
    venue TEXT
);

-- Team Stats
CREATE TABLE TeamAdvancedStats (
    team_abbrev TEXT PRIMARY KEY,
    runs_per_game REAL,
    runs_against_per_game REAL,
    ops REAL,              -- on-base plus slugging
    era REAL,              -- team ERA
    whip REAL,             -- walks + hits per inning
    babip REAL,            -- batting avg on balls in play
    k_rate REAL,           -- strikeout rate
    bb_rate REAL,          -- walk rate
    home_record TEXT,
    away_record TEXT,
    last_10 TEXT,
    vs_left REAL,          -- OPS vs lefties
    vs_right REAL,         -- OPS vs righties
    bullpen_era REAL,
    updated_at TEXT
);

-- Starting Pitchers (CRITICAL)
CREATE TABLE PitcherStats (
    player_id TEXT PRIMARY KEY,
    player_name TEXT,
    team TEXT,
    hand TEXT,             -- L or R
    wins INTEGER,
    losses INTEGER,
    era REAL,
    whip REAL,
    k_per_9 REAL,
    bb_per_9 REAL,
    hr_per_9 REAL,
    fip REAL,              -- fielding independent pitching
    xfip REAL,             -- expected FIP
    war REAL,
    avg_innings REAL,
    home_era REAL,
    away_era REAL,
    vs_left_ops REAL,
    vs_right_ops REAL,
    last_5_era REAL,
    updated_at TEXT
);

-- Batter Stats (for props)
CREATE TABLE BatterStats (
    player_id TEXT PRIMARY KEY,
    player_name TEXT,
    team TEXT,
    position TEXT,
    batting_avg REAL,
    obp REAL,
    slg REAL,
    ops REAL,
    hr INTEGER,
    rbi INTEGER,
    runs INTEGER,
    stolen_bases INTEGER,
    k_rate REAL,
    bb_rate REAL,
    vs_left_ops REAL,
    vs_right_ops REAL,
    home_ops REAL,
    away_ops REAL,
    last_14_ops REAL,
    updated_at TEXT
);

-- Player Game Logs
CREATE TABLE PlayerBox (
    id INTEGER PRIMARY KEY,
    game_id TEXT,
    player_id TEXT,
    player_name TEXT,
    team TEXT,
    position TEXT,
    at_bats INTEGER,
    hits INTEGER,
    runs INTEGER,
    rbi INTEGER,
    hr INTEGER,
    walks INTEGER,
    strikeouts INTEGER,
    stolen_bases INTEGER,
    total_bases INTEGER,
    -- Pitcher stats
    innings_pitched REAL,
    earned_runs INTEGER,
    hits_allowed INTEGER,
    walks_allowed INTEGER,
    strikeouts_pitched INTEGER
);

-- Betting Lines
CREATE TABLE Betting (
    game_id TEXT PRIMARY KEY,
    runline_home REAL,     -- typically -1.5 or +1.5
    runline_away REAL,
    runline_home_odds INTEGER,
    runline_away_odds INTEGER,
    total REAL,            -- typically 7-10 runs
    ml_home INTEGER,
    ml_away INTEGER,
    f5_ml_home INTEGER,    -- first 5 innings ML
    f5_ml_away INTEGER,
    f5_total REAL,
    updated_at TEXT
);

-- Probable Pitchers
CREATE TABLE ProbablePitchers (
    game_id TEXT PRIMARY KEY,
    home_pitcher_id TEXT,
    home_pitcher_name TEXT,
    away_pitcher_id TEXT,
    away_pitcher_name TEXT,
    confirmed INTEGER,     -- 0 or 1
    updated_at TEXT
);

-- Ballpark Factors
CREATE TABLE BallparkFactors (
    venue TEXT PRIMARY KEY,
    team TEXT,
    runs_factor REAL,      -- 1.0 = neutral
    hr_factor REAL,
    hits_factor REAL,
    doubles_factor REAL,
    triples_factor REAL,
    roof TEXT              -- Open, Closed, Retractable, None
);

-- Weather (important for outdoor parks)
CREATE TABLE GameWeather (
    game_id TEXT PRIMARY KEY,
    temperature INTEGER,
    wind_speed INTEGER,
    wind_direction TEXT,
    precipitation_pct INTEGER,
    humidity INTEGER
);
```

### Data Sources
1. **MLB Stats API** (free): https://statsapi.mlb.com/
   - Schedule: `/api/v1/schedule?sportId=1&date={date}`
   - Boxscores: `/api/v1.1/game/{game_pk}/feed/live`
   - Player stats: `/api/v1/people/{player_id}/stats`
   - Probable pitchers: `/api/v1/schedule?sportId=1&date={date}&hydrate=probablePitcher`

2. **ESPN API** (free): Betting lines
   - `https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard`

3. **FanGraphs**: Advanced stats (FIP, xFIP, WAR, wRC+)

4. **Baseball Savant**: Statcast data (exit velo, barrel %, xBA, xSLG)

5. **Ballpark Pal / Weather**: Park factors and weather

### Key Metrics for MLB Predictions

**Pitching (Most Important):**
- ERA, FIP, xFIP
- K/9, BB/9, HR/9
- WHIP
- Home/Away splits
- vs Left/Right splits
- Recent form (last 3-5 starts)
- Pitch count / workload

**Team Offense:**
- wRC+ (weighted runs created)
- OPS vs LHP / RHP
- K rate, BB rate
- BABIP (luck indicator)
- Recent form

**Bullpen:**
- Bullpen ERA, FIP
- High leverage stats
- Recent usage (tired arms)

**Ballpark & Weather:**
- Park factor (Coors = high runs, Oakland = low)
- Wind direction (Wrigley especially)
- Temperature (ball carries in heat)
- Humidity

### Tier System

**Moneyline:**
- PLATINUM: Ace pitcher + lineup edge + park factor
- GOLD: Good pitcher matchup + model edge 8%+
- SILVER: Model edge 5%+ on implied probability

**Run Line (-1.5):**
- PLATINUM: Ace vs weak team + bullpen advantage
- GOLD: Strong favorite + recent blowout form
- SILVER: Favorite with 60%+ ML probability

**Totals:**
- PLATINUM: Weather + park + both pitchers align
- GOLD: Two factors strongly align
- SILVER: Model edge 0.8+ runs from line

**Player Props:**
- PLATINUM: Edge >= 25% (star players only)
- GOLD: Edge >= 20%
- SILVER: Edge >= 15%

### Critical MLB-Specific Factors

1. **Starting Pitcher**
   - THE most important factor
   - Ace vs #5 starter = massive edge
   - Always check probables are confirmed
   - Monitor for scratches up to game time

2. **Handedness Matchups**
   - LHP vs heavy right-handed lineup = advantage offense
   - Platoon advantages are significant
   - Check lineup cards for platoon players

3. **Ballpark Factors**
   - Coors Field: +30% run scoring
   - Petco Park: -15% run scoring
   - Wind at Wrigley can add 2+ runs
   - Temperature matters (hot = more HRs)

4. **Bullpen**
   - Check usage last 3 days
   - Back-to-back games = tired arms
   - Closer availability matters for run line

5. **Day Games After Night**
   - Road teams struggle
   - Especially with travel
   - Fade road teams in getaway days

6. **Divisional Games**
   - More familiarity = tighter games
   - Aces often lined up for rivals

7. **First 5 Innings (F5)**
   - Removes bullpen variance
   - Pure pitching matchup bet
   - Good for backing aces

### Daily Pipeline

```python
# scripts/run_daily.py

1. Verify data freshness
2. Fetch today's schedule + betting lines
3. Check probable pitchers (CRITICAL - must be confirmed)
4. Fetch weather for outdoor parks
5. Calculate pitcher matchup edges
6. Generate ML/runline/total picks
7. Find player prop edges
8. Generate F5 picks
9. Generate output files
```

### Star Players for Props (examples)

**Batters:**
Shohei Ohtani, Mookie Betts, Ronald Acuna Jr, Corey Seager,
Freddie Freeman, Juan Soto, Aaron Judge, Yordan Alvarez,
Trea Turner, Marcus Semien, Bobby Witt Jr, Gunnar Henderson

**Pitchers:**
Shohei Ohtani, Gerrit Cole, Spencer Strider, Zack Wheeler,
Corbin Burnes, Dylan Cease, Tyler Glasnow, Logan Webb

Filter by: Regular starters, top 100 in PA or IP

### MLB-Specific Bet Types

1. **Moneyline** - Most popular MLB bet
2. **Run Line** - Spread, usually -1.5
3. **Total (Over/Under)** - Runs scored
4. **First 5 Innings (F5)** - ML and Total
5. **Team Total** - One team's runs O/U
6. **Player Props:**
   - Hits (O/U 0.5, 1.5)
   - Total Bases
   - RBI
   - Runs Scored
   - Strikeouts (pitchers)
   - Hits Allowed
   - Outs Recorded
7. **First Inning** - Yes/No run scored
8. **Grand Salami** - Total runs all games

### Output Files
- `picks_YYYY-MM-DD.csv` - All picks with tiers
- `posts_YYYY-MM-DD.txt` - Social media content
- `performance_tracker.csv` - Running record

### Sample Prediction Logic

```python
def predict_game(home_team, away_team, home_pitcher, away_pitcher, venue, weather):
    # Base run expectation from team stats
    home_runs = home_team.runs_per_game
    away_runs = away_team.runs_per_game

    # Pitcher adjustment (biggest factor)
    league_era = 4.20  # example
    home_runs *= (away_pitcher.era / league_era)
    away_runs *= (home_pitcher.era / league_era)

    # Handedness matchup
    if away_pitcher.hand == 'L':
        home_runs *= (home_team.vs_left_ops / home_team.ops)
    else:
        home_runs *= (home_team.vs_right_ops / home_team.ops)

    # Ballpark factor
    park_factor = BALLPARK_FACTORS[venue]['runs_factor']
    home_runs *= park_factor
    away_runs *= park_factor

    # Weather adjustment
    if weather:
        temp_adj = (weather.temperature - 70) * 0.01  # ~1% per 10 degrees
        wind_adj = calculate_wind_impact(weather, venue)
        home_runs *= (1 + temp_adj + wind_adj)
        away_runs *= (1 + temp_adj + wind_adj)

    # Home field (small in MLB, ~53% win rate)
    home_runs *= 1.02
    away_runs *= 0.98

    return home_runs, away_runs

def calculate_ml_probability(home_runs, away_runs):
    """Convert run projections to win probability."""
    # Using Pythagorean expectation
    home_win_prob = home_runs**1.83 / (home_runs**1.83 + away_runs**1.83)
    return home_win_prob

def project_strikeouts(pitcher, opponent_team):
    """Project pitcher strikeouts for props."""
    base_k = pitcher.k_per_9 * (pitcher.avg_innings / 9)

    # Adjust for opponent K rate
    opponent_k_rate = opponent_team.k_rate
    league_k_rate = 0.22  # example
    base_k *= (opponent_k_rate / league_k_rate)

    return base_k
```

### Ballpark Factors Reference
```python
BALLPARK_FACTORS = {
    'Coors Field': {'runs': 1.30, 'hr': 1.40},
    'Great American': {'runs': 1.12, 'hr': 1.20},
    'Fenway Park': {'runs': 1.08, 'hr': 0.95},
    'Wrigley Field': {'runs': 1.05, 'hr': 1.10},  # wind dependent
    'Yankee Stadium': {'runs': 1.05, 'hr': 1.15},
    'Oracle Park': {'runs': 0.88, 'hr': 0.80},
    'Petco Park': {'runs': 0.92, 'hr': 0.85},
    'Tropicana Field': {'runs': 0.95, 'hr': 0.90},
    # Add all 30 parks...
}
```

### Getting Started

1. Set up SQLite database with schema above
2. Build MLB Stats API fetchers
3. Create probable pitcher confirmation system
4. Build ballpark factors table
5. Add weather fetching for outdoor parks
6. Implement pitcher matchup model
7. Add player prop projections
8. Create tier classification
9. Build daily pipeline
10. Track results

### Key Differences from NBA
- Starting pitcher is EVERYTHING
- Must confirm pitchers before betting
- Run line (-1.5) instead of spread
- Lower margin = more variance on ML
- 162 games = massive sample size
- Weather and ballpark matter a lot
- F5 bets remove bullpen variance
- Day games after night games = fade road teams
