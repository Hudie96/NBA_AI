# AXIOM System Architecture

> Last updated: 2026-01-26

## System Overview

```mermaid
flowchart TD
    subgraph DataSources["Data Sources"]
        NBA["NBA API"]
        ESPN["ESPN API"]
        COVERS["Covers.com"]
    end

    subgraph Database["SQLite Database (axiom.db)"]
        Games["Games\n2,149 rows"]
        GameStates["GameStates\n331K rows"]
        PlayerBox["PlayerBox\n15K rows"]
        Betting["Betting\n677 rows"]
        Injuries["InjuryReports\n13K rows"]
        PbP["PbP_Logs\n393K rows"]
    end

    subgraph Updaters["Database Updaters (src/database_updater/)"]
        SchedUp["schedule.py"]
        BoxUp["boxscores.py"]
        PbpUp["pbp.py"]
        BetUp["betting.py"]
        InjUp["nba_official_injuries.py"]
        GSUp["game_states.py"]
    end

    subgraph Scripts["Daily Scripts (scripts/)"]
        DP["daily_predictions.py"]
        FS["flag_system.py"]
        SU["shared_utils.py"]
        II["injury_impact.py"]
        RD["rest_detection.py"]
        BT["backtest.py"]
    end

    subgraph ResultsTracking["Results Tracking"]
        LR["log_result.py"]
        UR["update_result.py"]
        CSV["data/results.csv"]
    end

    subgraph Outputs["Outputs (outputs/)"]
        JSON["predictions_DATE.json"]
        TXT["predictions_DATE.txt"]
        PCSV["predictions_DATE.csv"]
        AIR["ai_review_DATE.txt"]
    end

    %% Data flow
    NBA --> SchedUp & BoxUp & PbpUp & InjUp
    ESPN --> BetUp
    COVERS --> BetUp

    SchedUp --> Games
    BoxUp --> PlayerBox & TeamBox
    PbpUp --> PbP
    BetUp --> Betting
    InjUp --> Injuries
    GSUp --> GameStates

    Games --> DP
    GameStates --> DP
    Betting --> DP
    Injuries --> II

    SU --> DP & BT
    II --> DP
    RD --> DP
    DP --> FS
    FS --> AIR
    FS --> CSV

    DP --> JSON & TXT & PCSV

    LR --> CSV
    UR --> CSV

    %% Styling
    classDef built fill:#90EE90,stroke:#228B22
    classDef inprogress fill:#FFD700,stroke:#DAA520
    classDef planned fill:#D3D3D3,stroke:#808080

    class Games,GameStates,PlayerBox,Betting,Injuries,PbP built
    class SchedUp,BoxUp,PbpUp,BetUp,InjUp,GSUp built
    class DP,FS,SU,II,RD,BT built
    class LR,UR,CSV built
    class JSON,TXT,PCSV,AIR built
```

## Legend

| Symbol | Status |
|--------|--------|
| Green boxes | Built and working |
| Yellow boxes | In progress |
| Gray boxes | Planned |

## Daily Workflow

```mermaid
flowchart LR
    subgraph Morning["Morning Update"]
        A1["1. Update schedule"] --> A2["2. Update boxscores"]
        A2 --> A3["3. Update betting lines"]
    end

    subgraph Predictions["Generate Predictions"]
        B1["4. Run daily_predictions.py"] --> B2["5. Calculate flag_score"]
        B2 --> B3["6. Categorize GREEN/YELLOW/RED"]
        B3 --> B4["7. Auto-log to results.csv"]
    end

    subgraph PostGame["Post-Game"]
        C1["8. Update results"] --> C2["9. Track CLV"]
    end

    Morning --> Predictions --> PostGame
```

## Component Details

### Data Updaters (src/database_updater/)

| File | Purpose | Frequency |
|------|---------|-----------|
| `schedule.py` | Fetch NBA schedule | Daily |
| `boxscores.py` | Player/team box scores | After games |
| `pbp.py` | Play-by-play logs | After games |
| `betting.py` | Vegas lines from ESPN/Covers | Before games |
| `nba_official_injuries.py` | Injury reports | Daily |
| `game_states.py` | Reconstruct game states | After games |

### Scripts (scripts/)

| File | Purpose | Status |
|------|---------|--------|
| `daily_predictions.py` | Main prediction pipeline | Active |
| `flag_system.py` | Calculate flag_score, categorize zones | Active |
| `shared_utils.py` | Shared functions (team stats) | Active |
| `injury_impact.py` | Calculate injury adjustments | Active (disabled) |
| `rest_detection.py` | Detect B2B situations | Active |
| `backtest.py` | Historical validation | Active |
| `log_result.py` | Log new picks | Active |
| `update_result.py` | Update pick results | Active |
| `generate_performance_chart.py` | Visualization | Unused |

## Planned Components

```mermaid
flowchart TD
    subgraph Planned["Planned Features"]
        PP["Player Props Engine"]
        CG["Content Generation"]
        SM["Social Media Distribution"]
        BK["Bankroll/Kelly Sizing"]
        CLV["CLV Tracking Dashboard"]
    end

    classDef planned fill:#D3D3D3,stroke:#808080
    class PP,CG,SM,BK,CLV planned
```

## Database Schema Summary

| Table | Rows | Purpose |
|-------|------|---------|
| Games | 2,149 | Schedule and game metadata |
| GameStates | 331,193 | Point-in-time game snapshots |
| PlayerBox | 14,975 | Player box scores |
| TeamBox | 1,132 | Team box scores |
| Betting | 677 | Vegas lines and odds |
| InjuryReports | 13,034 | Player injury status |
| PbP_Logs | 393,325 | Play-by-play data |
| Players | 5,119 | Player metadata |
| Teams | 30 | Team metadata |

## The Proven Signal

```
injury_adj = 0  →  62.5% cover rate (p=0.018)
  + spread < 3  →  72.7% cover rate
  + B2B fade    →  71.4% cover rate

Flag Score Calculation:
  +5 if injury_adj == 0 (the signal)
  +3 if spread < 3
  +3 if opponent on B2B

Zone Thresholds:
  GREEN:  flag_score >= 8
  YELLOW: flag_score >= 5
  RED:    flag_score < 5
```
