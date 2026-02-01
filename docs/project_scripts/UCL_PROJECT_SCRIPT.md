# AXIOM UCL - UEFA Champions League Project Setup Script

## Overview
Build a Champions League betting prediction system modeled after AXIOM NBA. Data-driven picks with tiered confidence ratings (PLATINUM/GOLD/SILVER) for spreads, moneylines, totals, and player props.

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
    round TEXT,           -- Group Stage, R16, QF, SF, Final
    leg TEXT,             -- 1st Leg, 2nd Leg, Single
    aggregate_home INTEGER,
    aggregate_away INTEGER,
    venue TEXT,
    neutral_venue INTEGER  -- 1 for finals
);

-- Team Stats (domestic + UCL combined)
CREATE TABLE TeamAdvancedStats (
    team_name TEXT PRIMARY KEY,
    league TEXT,           -- Premier League, La Liga, etc.
    domestic_position INTEGER,
    ucl_coefficient REAL,  -- UEFA coefficient ranking
    goals_per_game REAL,
    goals_against_per_game REAL,
    xg_per_game REAL,
    xga_per_game REAL,
    possession_pct REAL,
    pass_completion_pct REAL,
    ppda REAL,
    clean_sheet_pct REAL,
    home_record TEXT,
    away_record TEXT,
    ucl_form TEXT,         -- Last 5 UCL results
    updated_at TEXT
);

-- Player Stats
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
    dribbles_completed INTEGER,
    fouls_committed INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER
);

-- Betting Lines
CREATE TABLE Betting (
    game_id TEXT PRIMARY KEY,
    spread_home REAL,
    spread_away REAL,
    total REAL,
    ml_home INTEGER,
    ml_away INTEGER,
    ml_draw INTEGER,
    both_teams_score TEXT,
    to_qualify_home INTEGER,  -- For knockout rounds
    to_qualify_away INTEGER,
    updated_at TEXT
);

-- Head to Head History
CREATE TABLE HeadToHead (
    team1 TEXT,
    team2 TEXT,
    competition TEXT,
    total_matches INTEGER,
    team1_wins INTEGER,
    team2_wins INTEGER,
    draws INTEGER,
    team1_goals INTEGER,
    team2_goals INTEGER,
    last_meeting TEXT
);

-- League Strength Rankings
CREATE TABLE LeagueStrength (
    league TEXT PRIMARY KEY,
    uefa_coefficient REAL,
    avg_xg REAL,
    avg_xga REAL,
    ucl_win_rate REAL,
    strength_multiplier REAL  -- for cross-league comparisons
);
```

### Data Sources
1. **ESPN API** (free):
   - `https://site.api.espn.com/apis/site/v2/sports/soccer/uefa.champions/scoreboard`

2. **FotMob API** (free): Advanced stats
   - Team xG, player stats, lineups

3. **Transfermarkt**: Squad values, injuries

4. **UEFA Official**: Coefficient rankings, historical data

5. **FBref/StatsBomb**: Advanced metrics (xG, xA, progressive passes)

### Key Metrics for UCL Predictions

**Team Level:**
- UEFA Coefficient (club ranking)
- Domestic league form
- UCL-specific form
- xG differential
- Home/Away European form
- Squad depth (rotation capacity)
- Manager UCL experience
- Historical knockout performance

**Cross-League Adjustments:**
- Premier League teams vs Bundesliga
- La Liga teams vs Ligue 1
- Normalize xG across leagues

**Player Props:**
- Shots / Shots on Target
- Goals + Assists
- Key Passes / Chances Created
- Tackles + Interceptions
- Cards (yellows common in UCL)

### Tier System

**Spreads:**
- PLATINUM: Elite club (top 8 coefficient) + model edge >= 0.6
- GOLD: Model edge >= 0.4 goals
- SILVER: Model edge >= 0.25 goals

**Moneyline (3-way):**
- PLATINUM: Coefficient mismatch + model 55%+ win prob
- GOLD: Model disagrees with implied prob by 12%+
- SILVER: Model disagrees by 8%+

**To Qualify (Knockouts):**
- PLATINUM: Strong home leg result + coefficient edge
- GOLD: Coefficient + form advantage
- SILVER: Model edge on aggregate

**Player Props:**
- PLATINUM: Edge >= 25% (star players only)
- GOLD: Edge >= 20%
- SILVER: Edge >= 15%

### Critical UCL-Specific Factors

1. **UEFA Coefficient**
   - Best predictor of UCL success
   - Real Madrid, Bayern, Man City top tier
   - Matters more than domestic form

2. **Knockout Stage Psychology**
   - Away goals rule removed (as of 2021)
   - First leg strategy differs
   - Big clubs have experience edge

3. **Domestic Fixture Congestion**
   - Check weekend matches before/after
   - Top clubs manage minutes
   - Rotation in group stage vs knockouts

4. **Travel & Time Zones**
   - Eastern European away trips
   - UK teams to Russia/Turkey historically tough

5. **Group Stage vs Knockouts**
   - Group stage: More goals, less pressure
   - Knockouts: Tighter, more tactical

6. **Manager Experience**
   - Ancelotti, Guardiola, Klopp = edge
   - First-time UCL managers struggle

7. **Historical H2H**
   - Some clubs have mental edge
   - Barcelona vs PSG, etc.

### League Strength Multipliers
```python
LEAGUE_MULTIPLIERS = {
    'Premier League': 1.10,
    'La Liga': 1.05,
    'Bundesliga': 1.00,
    'Serie A': 0.98,
    'Ligue 1': 0.95,
    'Primeira Liga': 0.88,
    'Eredivisie': 0.85,
    'Other': 0.80
}
```

### Daily Pipeline

```python
# scripts/run_daily.py

1. Verify data freshness
2. Fetch matchday schedule + betting lines
3. Check injuries and lineup news
4. Check domestic weekend fixtures (rotation)
5. Calculate team projections (cross-league adjusted)
6. Generate spread/ML/total picks
7. Generate to-qualify picks (knockouts)
8. Find player prop edges
9. Generate output files
```

### Star Players for Props (examples)
Erling Haaland, Kylian Mbappe, Vinicius Jr, Jude Bellingham,
Mohamed Salah, Harry Kane, Robert Lewandowski, Bukayo Saka,
Jamal Musiala, Florian Wirtz, Lamine Yamal, Cole Palmer,
Rodri, Phil Foden, Antoine Griezmann, Raphinha

Filter by: 70+ minutes average, top scorers/assisters, star clubs

### UCL-Specific Bet Types

1. **3-Way Moneyline** - Home/Draw/Away
2. **Asian Handicap** - More precise than spread
3. **Total Goals** - Over/Under 2.5/3/3.5
4. **Both Teams to Score** - Yes/No
5. **To Qualify** - Who advances (knockouts)
6. **Anytime Goalscorer**
7. **First Goalscorer**
8. **Correct Score** - High variance, high odds
9. **Half-Time/Full-Time**
10. **Corners** - Over/Under

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

    # Cross-league adjustment
    home_multiplier = LEAGUE_MULTIPLIERS[home_team.league]
    away_multiplier = LEAGUE_MULTIPLIERS[away_team.league]

    home_xg *= home_multiplier / away_multiplier
    away_xg *= away_multiplier / home_multiplier

    # Coefficient adjustment (experience matters)
    coef_diff = home_team.ucl_coefficient - away_team.ucl_coefficient
    if coef_diff > 20:
        home_xg *= 1.08
        away_xg *= 0.94
    elif coef_diff < -20:
        home_xg *= 0.94
        away_xg *= 1.08

    # Home advantage (smaller in UCL than domestic)
    home_xg *= 1.08
    away_xg *= 0.94

    # Knockout adjustment (tighter games)
    if game.round in ['R16', 'QF', 'SF', 'Final']:
        home_xg *= 0.92
        away_xg *= 0.92

    return home_xg, away_xg

def predict_qualifier(home_team, away_team, first_leg_score=None):
    """Predict who advances in knockout round."""
    if first_leg_score:
        # Second leg - factor in aggregate
        home_agg = first_leg_score['away'] + predicted_home_goals
        away_agg = first_leg_score['home'] + predicted_away_goals
    else:
        # First leg - use two-leg simulation
        pass

    # Historical knockout performance matters
    home_ko_factor = home_team.knockout_win_rate
    away_ko_factor = away_team.knockout_win_rate

    return home_qualify_prob, away_qualify_prob
```

### Handling Different Stages

**Group Stage:**
- More open, higher scoring
- Dead rubbers in matchday 6
- Watch for rotation

**Round of 16:**
- First leg often cagey
- Big clubs usually advance
- Upsets happen with 1st leg away win

**Quarter/Semi Finals:**
- Highest quality matches
- Very tactical, lower scoring
- Coefficient matters most here

**Final:**
- Neutral venue
- One-off = more variance
- Experience crucial

### Getting Started

1. Set up SQLite database with schema above
2. Build ESPN/FotMob API fetchers
3. Create league strength multipliers
4. Build cross-league xG adjustment model
5. Add coefficient-based adjustments
6. Implement knockout qualification model
7. Add player prop projections
8. Create tier classification
9. Build matchday pipeline
10. Track results

### Key Differences from NBA
- Draws are common and valuable
- Knockout rounds have two-leg format
- Cross-league comparisons needed
- UEFA coefficient is key predictor
- Lower scoring = more variance
- Fewer matches = less data per team
- BTTS and to-qualify are unique markets
