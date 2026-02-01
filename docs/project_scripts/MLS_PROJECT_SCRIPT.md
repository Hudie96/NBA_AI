# AXIOM MLS - Project Setup Script

## Overview
Build an MLS betting prediction system modeled after AXIOM NBA. Data-driven picks with tiered confidence ratings (PLATINUM/GOLD/SILVER) for spreads, moneylines, totals, and player props.

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
    venue TEXT
);

-- Team Stats
CREATE TABLE TeamAdvancedStats (
    team_abbrev TEXT PRIMARY KEY,
    goals_per_game REAL,
    goals_against_per_game REAL,
    xg_per_game REAL,           -- expected goals
    xga_per_game REAL,          -- expected goals against
    shots_per_game REAL,
    shots_on_target_pct REAL,
    possession_pct REAL,
    pass_completion_pct REAL,
    ppda REAL,                  -- passes allowed per defensive action (pressing)
    home_record TEXT,
    away_record TEXT,
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
    minutes INTEGER,
    goals INTEGER,
    assists INTEGER,
    shots INTEGER,
    shots_on_target INTEGER,
    key_passes INTEGER,
    tackles INTEGER,
    interceptions INTEGER,
    fouls_committed INTEGER,
    fouls_drawn INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER
);

-- Betting Lines
CREATE TABLE Betting (
    game_id TEXT PRIMARY KEY,
    spread_home REAL,        -- typically -0.5, -1, -1.5
    spread_away REAL,
    total REAL,              -- typically 2.5 or 3
    ml_home INTEGER,
    ml_away INTEGER,
    ml_draw INTEGER,         -- three-way moneyline
    both_teams_score TEXT,   -- YES/NO odds
    updated_at TEXT
);

-- Injuries/Suspensions
CREATE TABLE InjuryReports (
    player_id TEXT,
    player_name TEXT,
    team TEXT,
    status TEXT,             -- OUT, DOUBTFUL, QUESTIONABLE
    reason TEXT,
    report_date TEXT
);
```

### Data Sources
1. **ESPN API** (free):
   - `https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1/scoreboard`
   - `https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1/teams`

2. **FotMob API** (free): Advanced stats
   - Team xG, possession, etc.

3. **American Soccer Analysis** (free): MLS-specific advanced metrics
   - Goals Added (g+), xG, xA

4. **Odds API** or ESPN: Betting lines

### Key Metrics for MLS Predictions

**Team Level:**
- Goals For/Against per game
- Expected Goals (xG) differential
- xG overperformance (regression candidate)
- Home/Away form (MLS has BIG home advantage)
- Possession %
- PPDA (pressing intensity)
- Days rest
- Travel distance (cross-country trips matter)
- Altitude (Colorado, Salt Lake)

**Player Props:**
- Shots
- Shots on Target
- Goals + Assists (anytime scorer)
- Tackles + Interceptions
- Fouls Committed/Drawn
- Cards (yellows especially)

### Tier System

**Spreads:**
- PLATINUM: Model edge >= 0.6 goals + strong form
- GOLD: Model edge >= 0.4 goals
- SILVER: Model edge >= 0.25 goals

**Moneyline (3-way):**
- PLATINUM: Strong home favorite, model says 55%+ win prob
- GOLD: Model disagrees with implied prob by 12%+
- SILVER: Model disagrees by 8%+

**Player Props:**
- PLATINUM: Edge >= 25% (star players only)
- GOLD: Edge >= 20%
- SILVER: Edge >= 15%

### Critical MLS-Specific Factors

1. **Home Field Advantage**
   - MLS has one of the biggest HFAs in world soccer
   - Worth ~0.4-0.5 goals
   - Altitude teams (COL, RSL) even more
   - Travel fatigue for away teams

2. **Fixture Congestion**
   - US Open Cup, Leagues Cup, CCL games
   - Teams rotate squads
   - Check who played midweek

3. **Designated Players (DPs)**
   - Star players make huge difference
   - Check if available (injury, international duty)

4. **Turf vs Grass**
   - Some teams play on turf (SEA, ATL, NE, POR)
   - Away teams sometimes struggle on turf

5. **Playoff Race Context**
   - Late season motivation matters
   - Teams locked into spots may rest players

6. **Weather**
   - Heat in summer (Texas, Florida, LA)
   - Altitude (Denver is 5,280 ft)

### Daily Pipeline

```python
# scripts/run_daily.py

1. Verify data freshness
2. Fetch today's schedule + betting lines
3. Check injuries and lineup news
4. Check for midweek fixtures (rotation)
5. Calculate team projections
6. Generate spread/ML/total picks
7. Find player prop edges
8. Generate output files
```

### Star Players for Props (examples)
Lionel Messi, Lorenzo Insigne, Xherdan Shaqiri, Riqui Puig,
Chicho Arango, Hany Mukhtar, Lucho Acosta, Cucho Hernandez,
Christian Benteke, Denis Bouanga, Carles Gil, Thiago Almada

Filter by: 60+ minutes average, top scorers/assisters

### MLS-Specific Bet Types

1. **3-Way Moneyline** - Home/Draw/Away
2. **Spread** - Usually -0.5, -1, -1.5
3. **Total Goals** - Over/Under 2.5 or 3
4. **Both Teams to Score (BTTS)** - Yes/No
5. **Anytime Goalscorer** - Player to score
6. **Team Totals** - Over/Under goals for one team
7. **Corners** - Over/Under total corners
8. **Cards** - Over/Under cards, player to be carded

### Output Files
- `picks_YYYY-MM-DD.csv` - All picks with tiers
- `posts_YYYY-MM-DD.txt` - Social media content
- `performance_tracker.csv` - Running record

### Sample Prediction Logic

```python
def predict_game(home_team, away_team):
    # Base projection from xG
    home_xg = home_team.xg_per_game
    away_xg = away_team.xg_per_game

    # Adjust for opponent defense
    home_xg *= (away_team.xga_per_game / league_avg_xga)
    away_xg *= (home_team.xga_per_game / league_avg_xga)

    # Home field advantage (huge in MLS)
    home_xg *= 1.15
    away_xg *= 0.88

    # Altitude adjustment
    if home_team in ['COL', 'RSL']:
        home_xg *= 1.08
        away_xg *= 0.94

    # Rest/congestion adjustment
    if home_team.days_rest < 4:
        home_xg *= 0.95
    if away_team.days_rest < 4:
        away_xg *= 0.95

    # Travel adjustment
    if away_team.travel_distance > 1500:  # miles
        away_xg *= 0.96

    return home_xg, away_xg

def predict_total(home_xg, away_xg):
    expected_total = home_xg + away_xg
    # MLS averages ~3.0 goals per game
    return expected_total

def predict_btts(home_team, away_team, home_xg, away_xg):
    # Probability both teams score
    home_scores_prob = 1 - poisson.pmf(0, home_xg)
    away_scores_prob = 1 - poisson.pmf(0, away_xg)
    return home_scores_prob * away_scores_prob
```

### Getting Started

1. Set up SQLite database with schema above
2. Build ESPN/FotMob API fetchers
3. Build team xG projection model
4. Add home field and travel adjustments
5. Implement player prop projections
6. Add BTTS and team total predictions
7. Create tier classification
8. Build daily pipeline
9. Track results

### Key Differences from NBA
- Draws are common (bet the draw market)
- Lower scoring = more variance
- Home field advantage is MASSIVE
- Fewer games per season = less data
- International breaks affect schedules
- Altitude and travel matter more
- BTTS is a unique popular market
