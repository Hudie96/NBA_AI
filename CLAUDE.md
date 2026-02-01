# AXIOM - NBA Betting & Content System

## Project Vision
Data-driven NBA betting system + content engine for social media marketing.
Two outputs: profitable picks AND engaging content that builds an audience.

## Current State (as of 2026-01-31)

### Live Performance
**Record: 10-3 (76.9%)**
- Spreads: 2-1 (66.7%)
- Props: 8-2 (80.0%)

### Tier System

**Spreads:**
| Tier | Win Rate | Criteria |
|------|----------|----------|
| PLATINUM | 84.4% | Model edge +7 pts vs Vegas |
| GOLD | 78.9% | Model edge +5 pts vs Vegas |
| SILVER | 74.4% | Model edge +3 pts vs Vegas |

**Props (Star Players Only):**
| Tier | Edge | Description |
|------|------|-------------|
| PLATINUM | 25%+ | Highest confidence |
| GOLD | 20-25% | High confidence |
| SILVER | 15-20% | Medium confidence |

### Key Lessons Learned

1. **Star players only for props** - Bench player lines are soft (easy edges) but nobody cares. Only show players with 25+ min avg that people recognize.

2. **Two-source data verification** - Cross-check stats between NBA API and ESPN before updating. Catches bad data.

3. **Keep thresholds high** - Lowering edge thresholds to get more picks hurts performance. Better to have 3 good picks than 30 mediocre ones.

4. **Separate experimental branch** - Test changes and backtest on `experimental` branch, keep `main` clean for production.

5. **Social content tiers** - Give away SILVER picks for free (builds audience), keep GOLD/PLATINUM premium (monetization).

6. **PlayerBox > player_game_logs** - The player_game_logs table is stale. Always use PlayerBox for current season stats.

## Tech Stack
- Database: SQLite (data/NBA_AI_current.sqlite, 800K+ data points)
- Language: Python 3.x
- Key libs: pandas, numpy, sqlite3, nba_api
- Branches: `main` (production), `experimental` (testing)

## Scripts (17 active)

### Core Pipeline
| Script | Purpose |
|--------|---------|
| `run_daily.py` | **Master script** - orchestrates full pipeline |
| `verify_data.py` | Cross-check data between NBA API & ESPN |
| `refresh_all_data.py` | Update all data sources |
| `update_boxscores.py` | Fetch missing PlayerBox data |
| `daily_predictions.py` | Generate spread/ML predictions |
| `find_edges.py` | Find player prop edges |
| `project_props.py` | Project player stats |
| `generate_daily_output.py` | **Unified output** - picks CSV + social posts |
| `ai_verify_picks.py` | AI verification of picks |

### Supporting Scripts
| Script | Purpose |
|--------|---------|
| `flag_system.py` | Tier classification logic |
| `props_validator.py` | Validate prop projections |
| `shared_utils.py` | Team stats calculation |
| `rest_detection.py` | B2B and rest detection |
| `injury_impact.py` | Injury adjustments (disabled) |
| `backtest_daily_pipeline.py` | Historical validation |
| `log_result.py` | Log new picks |
| `update_result.py` | Update pick outcomes |

## Daily Workflow

```bash
# Full pipeline (recommended)
python scripts/run_daily.py

# Quick mode (skip slow fetches)
python scripts/run_daily.py --quick

# After games, update results
python scripts/update_result.py "2026-01-31" "MIN @ MEM" "W" 8
```

## Output Files

| Location | Contents |
|----------|----------|
| `outputs/predictions/picks_DATE.csv` | All picks with tiers |
| `outputs/social/posts_DATE.txt` | Social media content |
| `outputs/performance/performance_tracker.csv` | Running record |
| `outputs/ai_review_DATE.txt` | AI-verified picks |
| `data/results.csv` | Historical results |

## Social Content Structure

```
FREE (post these):
- SILVER tier spreads
- SILVER tier props (star players)

PREMIUM (teaser only):
- GOLD/PLATINUM spreads
- GOLD/PLATINUM props
```

## Git Workflow

```bash
# Production
git checkout main

# Experimental/testing
git checkout experimental

# After testing, merge to main
git checkout main
git merge experimental
git push
```

## Commands

| Say | Runs |
|-----|------|
| "Run daily" | `python scripts/run_daily.py` |
| "Quick run" | `python scripts/run_daily.py --quick` |
| "Switch to experimental" | `git checkout experimental` |
| "Update result" | `python scripts/update_result.py DATE GAME W/L margin` |

## Database Tables (Key)

| Table | Rows | Purpose |
|-------|------|---------|
| PlayerBox | 18,800+ | Player game stats (CURRENT) |
| Games | 2,200+ | Schedule |
| Betting | 700+ | Vegas lines |
| TeamAdvancedStats | 30 | Pace, ORTG, DRTG |

## Critical Rules

1. **Star players only** - Props filter to 25+ min avg players
2. **High thresholds** - 15%+ edge for props, 3+ pts for spreads
3. **Verify data** - Cross-check sources before trusting
4. **Track everything** - Log all picks to results.csv
5. **PA-legal only** - DraftKings, FanDuel, BetMGM, ESPN BET

## Other Sports Templates

Project scripts for expanding to other sports in `docs/project_scripts/`:
- NHL (goalie starters critical)
- MLS (home advantage huge)
- UCL (UEFA coefficient matters)
- MLB (starting pitcher is everything)
