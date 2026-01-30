# AXIOM - NBA Betting System

## Project Vision
Data-driven NBA betting system focused on spread predictions with backtested edge.

## Current State (as of 2026-01-30)

### The Edge (518-game backtest, Oct 2025 - Jan 2026)
Model excels at finding undervalued HOME teams, fails at away teams.

| Tier | Win Rate | Record | ROI | Criteria |
|------|----------|--------|-----|----------|
| PLATINUM | 84.4% | 38-7 | +61% | GREEN zone + Model +7 vs Vegas on HOME |
| GOLD | 78.9% | 56-15 | +51% | GREEN zone + Model +5 vs Vegas on HOME |
| SILVER | 74.4% | 99-34 | +42% | Model +5 vs Vegas on HOME (any zone) |
| SKIP | — | — | — | Model favors away OR edge < 5 |

GREEN zone = Small spread (<3) OR B2B situation

## Tech Stack
- Database: SQLite (data/NBA_AI_current.sqlite)
- Language: Python 3.x
- Key libs: pandas, numpy, sqlite3, nba_api

## Scripts (17 total)

### Core Pipeline
| Script | Purpose |
|--------|---------|
| `run_daily.py` | **Master script** - runs everything in order |
| `daily_predictions.py` | Generate daily picks with tier classification |
| `flag_system.py` | PLATINUM/GOLD/SILVER/SKIP categorization |
| `refresh_all_data.py` | Update all data sources |

### Utilities
| Script | Purpose |
|--------|---------|
| `shared_utils.py` | Team stats calculation |
| `rest_detection.py` | B2B and rest pattern detection |
| `injury_impact.py` | Injury adjustments (currently disabled) |
| `props_validator.py` | Validate player props |

### Results Tracking
| Script | Purpose |
|--------|---------|
| `log_result.py` | Log new picks to results.csv |
| `update_result.py` | Update pick outcomes |

### Analysis & Content
| Script | Purpose |
|--------|---------|
| `backtest_daily_pipeline.py` | Validate strategies historically |
| `find_edges.py` | Props edge finder |
| `project_props.py` | Player prop projections |
| `generate_daily_report.py` | Daily report generation |
| `generate_card.py` | Betting card images |
| `generate_nuggets.py` | Social content nuggets |
| `ai_verify_picks.py` | AI verification |

## Daily Workflow

```bash
# Full pipeline (data refresh + predictions)
python scripts/run_daily.py

# Quick mode (skip slow fetches)
python scripts/run_daily.py --quick

# Predictions only (use existing data)
python scripts/run_daily.py --predictions

# With content generation
python scripts/run_daily.py --content

# Specific date
python scripts/run_daily.py --date 2026-01-30

# After games complete, update results
python scripts/update_result.py "2026-01-30" "BOS @ CHI" "W" 5 0.5
```

## Output Files

| File | Contents |
|------|----------|
| `outputs/ai_review_DATE.txt` | PLATINUM/GOLD/SILVER picks |
| `outputs/predictions_DATE.json` | Full prediction data |
| `data/results.csv` | Historical pick tracking |

## Commands

| Say | Runs |
|-----|------|
| "Run daily" | `python scripts/run_daily.py` |
| "Quick predictions" | `python scripts/run_daily.py --quick --predictions` |
| "Update result" | `python scripts/update_result.py DATE GAME W/L margin` |
| "Backtest" | `python scripts/backtest_daily_pipeline.py` |

## Critical Rules

- **Only bet HOME teams** where model is 5+ points more bullish than Vegas
- **Model fails at away teams** (43.4% - losing strategy)
- **Data freshness**: Predictions require data within 7 days
- **PA-legal markets**: DraftKings, FanDuel, BetMGM, ESPN BET
