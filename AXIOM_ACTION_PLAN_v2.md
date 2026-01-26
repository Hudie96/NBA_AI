# AXIOM Action Plan v2 — Stacked Approach

> Decision: Run team spreads (proven 62.5% edge) + build player props (primary focus)

---

## Current State

### Team Spreads System (KEEP RUNNING)
- ✅ injury_adj logic fixed (ABSENCE = edge)
- ✅ Flag system working (GREEN/YELLOW/RED)
- ✅ Results tracking set up (results.csv)
- ⏳ Needs: Live tracking for 2+ weeks to validate

### Player Props System (BUILD NOW)
- ❌ No player data pipeline
- ❌ No defense vs position data
- ❌ No props projection model
- ❌ No props flagging system

---

## Week 1: Player Data Foundation

### Task 1.1: Player Game Logs Pipeline
**Goal:** Fetch and store historical player stats

**Data Source:** NBA.com Stats API (free, no key needed)
```
https://stats.nba.com/stats/playergamelogs?Season=2024-25&SeasonType=Regular%20Season
```

**Schema:**
```sql
CREATE TABLE player_game_logs (
    player_id TEXT,
    player_name TEXT,
    game_id TEXT,
    game_date DATE,
    team TEXT,
    opponent TEXT,
    home_away TEXT,        -- 'HOME' or 'AWAY'
    minutes REAL,
    points INTEGER,
    rebounds INTEGER,
    assists INTEGER,
    steals INTEGER,
    blocks INTEGER,
    turnovers INTEGER,
    threes_made INTEGER,
    fg_pct REAL,
    usage_rate REAL,
    is_b2b INTEGER,        -- 1 if back-to-back
    days_rest INTEGER,     -- days since last game
    PRIMARY KEY (player_id, game_id)
);
```

**Script:** `scripts/fetch_player_logs.py`
```python
# Scaffold
def fetch_player_game_logs(season='2024-25'):
    """Fetch all player game logs for season from NBA.com"""
    pass

def calculate_rest_days(df):
    """Add is_b2b and days_rest columns"""
    pass

def save_to_db(df, db_path='data/axiom.db'):
    """Insert/update player_game_logs table"""
    pass

if __name__ == '__main__':
    df = fetch_player_game_logs()
    df = calculate_rest_days(df)
    save_to_db(df)
    print(f"Loaded {len(df)} player game logs")
```

**Success Criteria:**
- [ ] 50,000+ rows loaded (full season)
- [ ] B2B and rest days calculated correctly
- [ ] Can query "LeBron's last 10 games"

---

### Task 1.2: Defense vs Position Rankings
**Goal:** Know which defenses are weak vs which positions

**Data Source:** NBA.com or Basketball Reference
```
https://stats.nba.com/stats/leaguedashptdefend
```

**Schema:**
```sql
CREATE TABLE defense_vs_position (
    team TEXT,
    position TEXT,          -- 'PG', 'SG', 'SF', 'PF', 'C'
    stat TEXT,              -- 'PTS', 'REB', 'AST', '3PM'
    avg_allowed REAL,       -- average allowed to that position
    league_avg REAL,        -- league average for comparison
    diff_from_avg REAL,     -- positive = allows more (bad D)
    rank INTEGER,           -- 1 = worst defense (allows most)
    updated_date DATE,
    PRIMARY KEY (team, position, stat)
);
```

**Script:** `scripts/fetch_dvp.py`

**Success Criteria:**
- [ ] All 30 teams × 5 positions × 4 stats = 600 rows
- [ ] Rankings 1-30 for each position/stat combo
- [ ] Can query "worst defenses vs PG scoring"

---

### Task 1.3: Player vs Team History
**Goal:** Aggregate how each player performs vs specific opponents

**Schema:**
```sql
CREATE TABLE player_vs_team (
    player_id TEXT,
    player_name TEXT,
    opponent TEXT,
    games INTEGER,
    avg_pts REAL,
    avg_reb REAL,
    avg_ast REAL,
    avg_3pm REAL,
    avg_min REAL,
    last_game_date DATE,
    PRIMARY KEY (player_id, opponent)
);
```

**Script:** `scripts/build_player_vs_team.py`
```python
# Aggregates from player_game_logs
def build_player_vs_team():
    """
    SELECT player_id, player_name, opponent,
           COUNT(*) as games,
           AVG(points) as avg_pts,
           AVG(rebounds) as avg_reb,
           AVG(assists) as avg_ast,
           AVG(threes_made) as avg_3pm,
           AVG(minutes) as avg_min,
           MAX(game_date) as last_game_date
    FROM player_game_logs
    GROUP BY player_id, player_name, opponent
    HAVING COUNT(*) >= 3  -- minimum sample
    """
    pass
```

**Success Criteria:**
- [ ] Table populated from game logs
- [ ] Can query "Tatum's averages vs Miami"
- [ ] Minimum 3 games required for inclusion

---

## Week 2: Props Projection & Flagging

### Task 2.1: Baseline Props Projections
**Goal:** Project tonight's player lines based on recent form + matchup

**Logic:**
```python
def project_player_prop(player_id, opponent, stat='PTS'):
    """
    Projection = weighted average of:
    - Last 10 games average (40%)
    - Season average (30%)
    - vs This Opponent average (20%)
    - DvP adjustment (10%)
    """
    last_10 = get_last_n_games_avg(player_id, stat, n=10)
    season = get_season_avg(player_id, stat)
    vs_opp = get_vs_opponent_avg(player_id, opponent, stat)
    dvp_adj = get_dvp_adjustment(opponent, player_position, stat)
    
    projection = (
        last_10 * 0.40 +
        season * 0.30 +
        vs_opp * 0.20 +
        (season + dvp_adj) * 0.10
    )
    return projection
```

**Script:** `scripts/project_props.py`

**Success Criteria:**
- [ ] Generates projections for top 50 usage players
- [ ] Projection within 15% of actual (backtest check)

---

### Task 2.2: Props Edge Finder
**Goal:** Flag props where our projection differs from the line

**Schema:**
```sql
CREATE TABLE props_edges (
    date DATE,
    player_name TEXT,
    opponent TEXT,
    prop_type TEXT,         -- 'PTS', 'REB', 'AST', '3PM', 'PRA'
    line REAL,              -- book's line (e.g., 24.5)
    projection REAL,        -- our projection (e.g., 27.2)
    edge REAL,              -- projection - line (positive = over)
    edge_pct REAL,          -- edge as percentage
    confidence TEXT,        -- 'HIGH', 'MEDIUM', 'LOW'
    factors TEXT,           -- JSON of supporting factors
    PRIMARY KEY (date, player_name, prop_type)
);
```

**Confidence Criteria:**
```python
def calculate_confidence(edge_pct, sample_size, dvp_rank):
    """
    HIGH: edge >= 10% AND sample >= 10 AND favorable DvP
    MEDIUM: edge >= 5% AND sample >= 5
    LOW: edge >= 3%
    """
    pass
```

**Success Criteria:**
- [ ] Surfaces 5-15 props per night
- [ ] Includes reasoning (factors JSON)
- [ ] Ranks by confidence

---

### Task 2.3: Props Results Tracking
**Goal:** Log predictions and outcomes for validation

**Schema:**
```sql
CREATE TABLE props_results (
    date DATE,
    player_name TEXT,
    prop_type TEXT,
    line REAL,
    projection REAL,
    edge REAL,
    pick TEXT,              -- 'OVER' or 'UNDER'
    confidence TEXT,
    actual REAL,            -- actual stat line
    result TEXT,            -- 'WIN', 'LOSS', 'PUSH'
    PRIMARY KEY (date, player_name, prop_type)
);
```

**Script:** `scripts/log_prop_result.py`

---

## Week 3: Content Engine

### Task 3.1: Stat Nugget Generator
**Goal:** Auto-generate tweetable stats from data

**Examples to Generate:**
```python
def find_stat_nuggets():
    """
    Patterns to look for:
    - Player streaks (hit over 5 straight)
    - Extreme splits (home vs away > 20% diff)
    - Historical matchup dominance (avg 30+ vs team)
    - DvP extremes (team allows most to position)
    - B2B impact outliers (player drops 30% on B2B)
    """
    pass
```

**Output Format:**
```python
{
    "hook": "LeBron has hit the points over in 8 straight games vs Eastern teams",
    "stat": "8-0 on overs, avg 29.4 pts",
    "tonight": "Tonight: vs Celtics, line 25.5",
    "tweet": "LeBron is 8-0 on points overs vs the East this season (avg 29.4).\n\nTonight vs Boston, the line is 25.5.\n\nThe books are begging you to take the under. 🤔"
}
```

---

### Task 3.2: Daily Pick Card Generator
**Goal:** Create visual-ready pick summaries

**Output:**
```
=== AXIOM DAILY CARD — Jan 27 ===

🏀 SPREAD PICK (Team System)
Knicks -4.5 ⭐⭐⭐⭐
• injury_adj = 0 ✓
• Home fav sweet spot ✓
• Travel edge ✓

🎯 PROP PICKS (Player System)
1. Brunson OVER 7.5 ast ⭐⭐⭐⭐
   • 14-3 vs bottom-10 ast D
   • Projection: 8.9

2. Randle UNDER 22.5 pts ⭐⭐⭐
   • Pacers allow fewest paint pts
   • Projection: 19.8

📊 STAT OF THE DAY
"Teams on 4+ game west road trips: 12-31 ATS"
Lakers in Phoenix tonight (game 5 of trip)

Season: 67-52 (56.3%) | Props: 23-15 (60.5%)
=====================================
```

---

## Week 4: Integration & Validation

### Task 4.1: Unified Daily Pipeline
**Goal:** One command runs everything

```bash
python scripts/daily_pipeline.py

# Does:
# 1. Update player game logs (yesterday's games)
# 2. Refresh DvP rankings
# 3. Generate team spread picks (existing system)
# 4. Generate props projections + edges
# 5. Find stat nuggets
# 6. Output daily card
```

---

### Task 4.2: 2-Week Backtest (Props)
**Goal:** Validate props model before going live

```python
def backtest_props(start_date, end_date):
    """
    For each day in range:
    1. Generate projections using only data available that morning
    2. Flag edges
    3. Compare to actual results
    4. Calculate hit rate by confidence level
    """
    pass
```

**Success Criteria:**
- [ ] HIGH confidence: > 55% hit rate
- [ ] MEDIUM confidence: > 52% hit rate
- [ ] Sample size: 200+ props

---

## Parallel Track: Team Spreads (Passive)

While building player props, keep team spreads running:

| Day | Action |
|-----|--------|
| Daily | Run existing `daily_predictions.py` |
| Daily | Log picks to `results.csv` |
| After games | Update results with `update_result.py` |
| Weekly | Review hit rate, adjust if needed |

**Goal:** 2 weeks of live tracking to validate 62.5% edge holds.

---

## Success Metrics (End of Week 4)

| Metric | Target |
|--------|--------|
| Player game logs loaded | 50,000+ rows |
| DvP table populated | 600 rows |
| Props projections running | 50+ players/night |
| Props backtest hit rate | > 53% |
| Team spreads live results | 20+ games tracked |
| Content pieces generated | 5+ per day |
| Twitter followers | First 100 |

---

## File Structure After Week 4

```
axiom/
├── CLAUDE.md
├── data/
│   ├── axiom.db              # SQLite with all tables
│   └── results.csv           # Team spread tracking
├── scripts/
│   ├── shared_utils.py       # Common functions
│   ├── daily_predictions.py  # Team spreads (existing)
│   ├── fetch_player_logs.py  # NEW
│   ├── fetch_dvp.py          # NEW
│   ├── build_player_vs_team.py  # NEW
│   ├── project_props.py      # NEW
│   ├── find_edges.py         # NEW
│   ├── generate_content.py   # NEW
│   ├── daily_pipeline.py     # NEW (unified)
│   ├── log_result.py
│   └── update_result.py
├── .claude/
│   ├── decisions.md
│   └── agents/
└── docs/
    └── ARCHITECTURE.md
```

---

## Next Immediate Step

**Task 1.1: Player Game Logs Pipeline**

Tell Claude Code:
```
Read AXIOM_ACTION_PLAN_v2.md Task 1.1.

Create scripts/fetch_player_logs.py that:
1. Fetches player game logs from NBA.com stats API for 2024-25 season
2. Calculates is_b2b and days_rest for each row
3. Saves to player_game_logs table in data/axiom.db

Use proper headers to avoid NBA.com blocking.
Test with one player first, then full fetch.
```
