# AXIOM - NBA Betting & Content System

## Project Vision
Data-driven NBA betting system + content engine for social media marketing.
Two outputs: profitable picks AND engaging content that builds an audience.

## Current State (as of 2026-01-26)

### What's Built
- Database infrastructure (SQLite, 750K+ rows)
- Schedule, boxscore, betting line, injury data pipelines
- Daily prediction generator with Vegas comparison
- Flag scoring system based on backtested signals
- Results tracking (auto-log picks, manual result updates)
- Backtest framework validating signal efficacy
- REST/B2B detection for fatigue factors

### What's In Progress
- Live result tracking (results.csv created, needs daily updates)
- CLV (closing line value) measurement
- Player props validation (backtest in progress)

### What's Planned
- Content generation for social media
- Bankroll/Kelly sizing calculator
- Social media distribution automation
- Performance dashboard

## Tech Stack
- Database: SQLite (data/NBA_AI_current.sqlite)
- Language: Python 3.x
- Key libs: pandas, numpy, sqlite3, requests

## Key Files

### Scripts (scripts/)
| File | Purpose |
|------|---------|
| `daily_predictions.py` | Main pipeline - generates daily spread picks |
| `daily_props.py` | Player props projections (PTS, REB, AST, 3PM) |
| `flag_system.py` | Calculates flag_score, categorizes GREEN/YELLOW/RED |
| `props_flag_system.py` | Props confidence scoring and zone categorization |
| `build_player_game_logs.py` | ETL: builds player_game_logs from PlayerBox |
| `project_props.py` | Core projection logic for player stats |
| `shared_utils.py` | Shared functions (get_team_recent_games, calculate_team_stats) |
| `injury_impact.py` | Injury adjustment calculations (currently disabled) |
| `rest_detection.py` | B2B and rest day detection |
| `backtest.py` | Historical validation of signals |
| `backtest_props.py` | Props model validation |
| `log_result.py` | Log new picks to results.csv |
| `update_result.py` | Update pick outcomes |

### Database Updaters (src/database_updater/)
| File | Purpose |
|------|---------|
| `schedule.py` | Fetch NBA schedule from API |
| `boxscores.py` | Player and team box scores |
| `betting.py` | Vegas lines from ESPN/Covers |
| `nba_official_injuries.py` | Injury reports |

## The Proven Signal

Backtest (123 games, 2025-12-25 to 2026-01-24):

```
injury_adj = 0  →  62.5% cover rate (p=0.018) ← THE EDGE
injury_adj > 0  →  36.3% cover rate (skip these)

Signal + spread < 3  →  72.7%
Signal + B2B fade    →  71.4%
```

**Flag Score Calculation:**
- +5 if injury_adj == 0 (proven signal)
- +3 if spread < 3 (small spread bonus)
- +3 if opponent on B2B (fatigue factor)

**Zone Thresholds:**
- GREEN: flag_score >= 8 (Best bets)
- YELLOW: flag_score >= 5 (Signal only)
- RED: flag_score < 5 (Skip)

## Daily Workflow

```bash
# 1. Update schedule (if needed)
python -m src.database_updater.schedule --season=2024-2025

# 2. Generate spread predictions
python scripts/daily_predictions.py

# 3. Generate props projections
python scripts/build_player_game_logs.py  # First time only, or --rebuild
python scripts/daily_props.py --min-edge 10

# 4. Review ai_review_DATE.txt for GREEN/YELLOW spread picks
# 5. Review props_ai_review_DATE.txt for props picks
# 6. Picks auto-logged to data/results.csv and data/props_results.csv

# 7. After games, update results
python scripts/update_result.py "2026-01-26" "BOS @ CHI" "W" 5 0.5
```

## Database Schema

| Table | Rows | Purpose |
|-------|------|---------|
| Games | 2,149 | Schedule metadata |
| GameStates | 331,193 | Game state snapshots |
| PlayerBox | 14,975 | Player stats |
| player_game_logs | derived | Player stats with combos (built from PlayerBox) |
| player_vs_team | derived | Player averages vs each opponent |
| Betting | 677 | Vegas lines |
| InjuryReports | 13,034 | Injury data |

Date range: 2024-10-04 to 2026-01-24

## Commands

| Command | Action |
|---------|--------|
| "Run predictions" | `python scripts/daily_predictions.py` |
| "Run props" | `python scripts/daily_props.py` |
| "Run props (10%+ edge)" | `python scripts/daily_props.py --min-edge 10` |
| "Build player logs" | `python scripts/build_player_game_logs.py` |
| "Log pick" | `python scripts/log_result.py "GAME" "PICK" spread flag_score edge` |
| "Update result" | `python scripts/update_result.py "DATE" "GAME" "W/L" margin clv` |
| "Backtest" | `python scripts/backtest.py` |
| "Backtest props" | `python scripts/backtest_props.py` |
| "Update schedule" | `python -m src.database_updater.schedule --season=2024-2025` |

## Project Goals

1. **BETTING**: Validate edge with live tracking, achieve >55% hit rate on GREEN zone
2. **CONTENT**: Generate daily insights and player stats for social media
3. **MONETIZATION**: Build audience → paid picks service + affiliate revenue

## On-Demand Context

- `@docs/AXIOM_ARCHITECTURE.md` - Visual system diagram
- `@DATA_MODEL.md` - Database schema details
- `@decisions.md` - Architectural decisions log

## Critical Rules

- **Data freshness**: Predictions require data within 7 days
- **Anti-hallucination**: Only cite stats from database queries
- **PA-legal markets only**: DraftKings, FanDuel, BetMGM, ESPN BET
- **Injury signal**: injury_adj = 0 IS the edge (inverted from intuition)
