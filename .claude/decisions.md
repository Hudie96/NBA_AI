# AXIOM Decision Log — Stacked Approach

> Add this to your existing decisions.md

---

## Decision: Stacked Architecture (Team Spreads + Player Props)

**Date:** 2025-01-26

**Context:**
- Team spread model has proven 62.5% edge (injury_adj = 0 signal, p=0.018)
- Player props are better for content (people follow players, not spreads)
- Props market is less efficient (books can't price 200 props as well as 10 spreads)
- Full pivot would abandon proven edge; pure team focus limits content

**Options Considered:**
1. **Full Pivot** — Abandon team spreads, go all-in on player props
2. **Stack (CHOSEN)** — Run both systems, props as primary dev focus
3. **Validate First** — Run team spreads 2 weeks before building props

**Decision:** Option B — Stacked Approach

**Rationale:**
- Don't abandon working edge (team spreads already built + validated)
- Player props become primary development focus
- Both systems feed content engine differently:
  - Team spreads: "Boring but profitable" background picks
  - Player props: "Viral content + potential edges" main show
- Diversified edge sources reduce variance
- Content calendar can pull from both (daily spread pick + 2-3 props + stat nuggets)

**Trade-offs:**
- (+) Keep proven 62.5% edge running
- (+) Player props enable better content
- (+) Diversified betting approach
- (-) More systems to maintain
- (-) Attention split during build phase

**Implementation:**
- Team spreads: Run daily, log results, passive maintenance
- Player props: Active development (4-week plan)
- Content: Pull from both systems

**Reversal Conditions:**
- If team spreads underperform live (<50% over 30 games), deprioritize
- If player props model fails backtest (<52%), pivot strategy
- If maintenance burden too high, consolidate to one system

---

## Database Architecture Decision

**Date:** 2025-01-26

**Decision:** Add player tables to existing axiom.db

**New Tables:**
- `player_game_logs` — Individual player game stats
- `defense_vs_position` — DvP rankings by team/position/stat
- `player_vs_team` — Aggregated matchup history
- `props_edges` — Daily edge flags
- `props_results` — Prediction tracking

**Rationale:**
- Single database simpler than multiple files
- SQLite handles this scale easily
- Joins between team and player data possible
- Consistent with existing architecture

---

## Data Source Decision

**Date:** 2025-01-26

**Decision:** Use NBA.com Stats API (free, no key)

**Alternatives Considered:**
- Basketball Reference (scraping required, slower)
- Paid APIs (unnecessary cost at this stage)
- ESPN API (less granular)

**Rationale:**
- NBA.com has official, granular data
- Free with proper headers
- JSON responses easy to parse
- Same source used by most analytics tools
