"""
AI Pick Verification System

Runs each HIGH/MEDIUM confidence pick through Claude for verification.
Checks for red flags, sample size issues, and whether the edge is explainable.

Output:
- CONFIRM: Pick passes verification, show on daily card
- FLAG: Pick has concerns, show as "risky play"
- REJECT: Pick fails verification, drop entirely

Usage:
    python scripts/ai_verify_picks.py
    python scripts/ai_verify_picks.py --date 2025-01-26
    python scripts/ai_verify_picks.py --dry-run  # Don't update database
"""
import argparse
import json
import os
import sqlite3
import sys
from datetime import date
from pathlib import Path

import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

from src.config import config

DB_PATH = config["database"]["path"]

# Try to import Anthropic SDK
try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False
    print("Warning: anthropic package not installed. Install with: pip install anthropic")


VERIFICATION_PROMPT = """You are a sports betting analyst verifying a prop bet pick.

PICK: {player} {pick} {line} {stat}
OPPONENT: {opponent}
MODEL EDGE: {edge_pct:+.1f}%
PROJECTION: {projection}

SUPPORTING DATA:
- Last 10 games avg: {last_10_avg}
- Season avg: {season_avg}
- vs {opponent} history: {vs_opp_avg} ({vs_opp_games} games)
- DvP rank: {dvp_rank} (1=worst D allows most, 30=best D)
- Position: {position}
- Games played: {season_games}

VERIFY THIS PICK. Check for:
1. Does the data actually support this edge?
2. Any red flags? (inconsistent recent form, tiny sample vs opponent)
3. Sample size concerns? (vs opponent history <3 games is weak)
4. Is the edge explainable or just noise?

IMPORTANT RULES:
- If edge > 20% and all data points align: likely CONFIRM
- If vs opponent sample < 3 games: lower confidence
- If projection differs significantly from L10 AND season avg: FLAG
- If edge seems to come from one outlier factor: FLAG or REJECT

OUTPUT EXACTLY THREE LINES:
VERDICT: CONFIRM or FLAG or REJECT
CONFIDENCE: HIGH or MEDIUM or LOW
REASON: [one clear sentence explaining your decision]"""


def get_picks_to_verify(conn, target_date=None):
    """Get HIGH and MEDIUM confidence picks that need verification."""
    if target_date is None:
        target_date = date.today().isoformat()

    picks = pd.read_sql("""
        SELECT player_name, opponent, prop_type, line, projection,
               edge, edge_pct, pick, confidence, stat_tier, factors
        FROM props_edges
        WHERE date = ?
          AND confidence IN ('HIGH', 'MEDIUM')
        ORDER BY
            CASE confidence WHEN 'HIGH' THEN 1 ELSE 2 END,
            ABS(edge_pct) DESC
    """, conn, params=(target_date,))

    return picks


def get_additional_context(player_name, opponent, stat, conn):
    """Get additional context for verification."""
    # Get last 5 games for recent form
    stat_col_map = {
        "PTS": "points", "REB": "rebounds", "AST": "assists",
        "3PM": "threes_made", "STL": "steals", "BLK": "blocks",
        "PRA": "pts_reb_ast", "PR": "pts_reb", "PA": "pts_ast", "RA": "reb_ast"
    }
    col = stat_col_map.get(stat, "points")

    last_5 = pd.read_sql(f"""
        SELECT {col} as val, opponent, home_away, days_rest, is_b2b
        FROM player_game_logs
        WHERE player_name = ?
        ORDER BY game_date DESC
        LIMIT 5
    """, conn, params=(player_name,))

    context = {
        "last_5_values": last_5["val"].tolist() if not last_5.empty else [],
        "last_5_avg": round(last_5["val"].mean(), 1) if not last_5.empty else None,
        "recent_opponents": last_5["opponent"].tolist() if not last_5.empty else [],
    }

    # Check if next game is B2B (would need schedule data)
    # For now, use recent B2B rate as proxy
    if not last_5.empty:
        context["recent_b2b_rate"] = last_5["is_b2b"].mean()

    return context


def build_verification_prompt(pick, factors, additional_context):
    """Build the verification prompt for a pick."""
    # Parse factors JSON
    if isinstance(factors, str):
        factors = json.loads(factors)

    prompt = VERIFICATION_PROMPT.format(
        player=pick["player_name"],
        pick=pick["pick"],
        line=pick["line"],
        stat=pick["prop_type"],
        opponent=pick["opponent"],
        edge_pct=pick["edge_pct"],
        projection=pick["projection"],
        last_10_avg=factors.get("last_10_avg", "N/A"),
        season_avg=factors.get("season_avg", "N/A"),
        vs_opp_avg=factors.get("vs_opp_avg", "N/A"),
        vs_opp_games=factors.get("vs_opp_games", 0),
        dvp_rank=factors.get("dvp_rank", "N/A"),
        position=factors.get("position", "N/A"),
        season_games=factors.get("season_games", "N/A"),
    )

    # Add recent form context if available
    if additional_context.get("last_5_values"):
        prompt += f"\n\nADDITIONAL CONTEXT:\n"
        prompt += f"- Last 5 games: {additional_context['last_5_values']}\n"
        prompt += f"- Recent opponents: {additional_context['recent_opponents']}"

    return prompt


def verify_pick_with_ai(prompt, dry_run=False):
    """Send pick to Claude for verification."""
    if dry_run or not HAS_ANTHROPIC:
        # Return mock response for testing
        return {
            "verdict": "CONFIRM",
            "confidence": "MEDIUM",
            "reason": "Dry run - no AI verification performed"
        }

    # Get API key from environment
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Warning: ANTHROPIC_API_KEY not set. Using mock response.")
        return {
            "verdict": "FLAG",
            "confidence": "LOW",
            "reason": "API key not configured"
        }

    try:
        client = anthropic.Anthropic(api_key=api_key)

        message = client.messages.create(
            model="claude-3-5-haiku-20241022",  # Fast and cheap for verification
            max_tokens=150,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )

        response_text = message.content[0].text.strip()
        return parse_verification_response(response_text)

    except Exception as e:
        print(f"  AI verification error: {e}")
        return {
            "verdict": "FLAG",
            "confidence": "LOW",
            "reason": f"Verification failed: {str(e)[:50]}"
        }


def parse_verification_response(response_text):
    """Parse the AI response into structured data."""
    lines = response_text.strip().split("\n")

    result = {
        "verdict": "FLAG",
        "confidence": "LOW",
        "reason": "Could not parse response"
    }

    for line in lines:
        line = line.strip()
        if line.startswith("VERDICT:"):
            verdict = line.replace("VERDICT:", "").strip().upper()
            if verdict in ["CONFIRM", "FLAG", "REJECT"]:
                result["verdict"] = verdict
        elif line.startswith("CONFIDENCE:"):
            conf = line.replace("CONFIDENCE:", "").strip().upper()
            if conf in ["HIGH", "MEDIUM", "LOW"]:
                result["confidence"] = conf
        elif line.startswith("REASON:"):
            result["reason"] = line.replace("REASON:", "").strip()

    return result


def save_verification_results(results, conn, target_date):
    """Save verification results to database."""
    # Create verification table if not exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS picks_verification (
            date TEXT,
            player_name TEXT,
            prop_type TEXT,
            verdict TEXT,
            ai_confidence TEXT,
            reason TEXT,
            PRIMARY KEY (date, player_name, prop_type)
        )
    """)

    # Clear existing results for today
    conn.execute("""
        DELETE FROM picks_verification WHERE date = ?
    """, (target_date,))

    # Insert new results
    for result in results:
        conn.execute("""
            INSERT OR REPLACE INTO picks_verification
            (date, player_name, prop_type, verdict, ai_confidence, reason)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            target_date,
            result["player_name"],
            result["prop_type"],
            result["verdict"],
            result["ai_confidence"],
            result["reason"]
        ))

    conn.commit()


def display_results(results):
    """Display verification results."""
    confirmed = [r for r in results if r["verdict"] == "CONFIRM"]
    flagged = [r for r in results if r["verdict"] == "FLAG"]
    rejected = [r for r in results if r["verdict"] == "REJECT"]

    print("\n" + "=" * 60)
    print("  AI VERIFICATION RESULTS")
    print("=" * 60)

    if confirmed:
        print("\n[CONFIRMED PICKS] - Ready for daily card")
        print("-" * 50)
        for r in confirmed:
            print(f"  {r['player_name']} {r['pick']} {r['line']} {r['prop_type']}")
            print(f"    Edge: {r['edge_pct']:+.1f}% | AI: {r['ai_confidence']}")
            print(f"    Reason: {r['reason']}")
            print()

    if flagged:
        print("\n[FLAGGED PICKS] - Risky plays, proceed with caution")
        print("-" * 50)
        for r in flagged:
            print(f"  {r['player_name']} {r['pick']} {r['line']} {r['prop_type']}")
            print(f"    Edge: {r['edge_pct']:+.1f}% | AI: {r['ai_confidence']}")
            print(f"    Reason: {r['reason']}")
            print()

    if rejected:
        print("\n[REJECTED PICKS] - Dropped from card")
        print("-" * 50)
        for r in rejected:
            print(f"  {r['player_name']} {r['pick']} {r['line']} {r['prop_type']}")
            print(f"    Reason: {r['reason']}")
            print()

    print("=" * 60)
    print(f"SUMMARY: {len(confirmed)} confirmed, {len(flagged)} flagged, {len(rejected)} rejected")
    print("=" * 60)


def verify_picks(conn, target_date=None, dry_run=False, verbose=True):
    """Main verification function."""
    if target_date is None:
        target_date = date.today().isoformat()

    picks = get_picks_to_verify(conn, target_date)

    if picks.empty:
        print("No HIGH/MEDIUM confidence picks to verify.")
        return []

    if verbose:
        print(f"\nVerifying {len(picks)} picks for {target_date}...")
        if dry_run:
            print("(DRY RUN - no AI calls)")

    results = []

    for _, pick in picks.iterrows():
        if verbose:
            print(f"\n  Verifying: {pick['player_name']} {pick['pick']} {pick['line']} {pick['prop_type']}...")

        # Get additional context
        additional_context = get_additional_context(
            pick["player_name"],
            pick["opponent"],
            pick["prop_type"],
            conn
        )

        # Build prompt
        prompt = build_verification_prompt(pick, pick["factors"], additional_context)

        # Verify with AI
        verification = verify_pick_with_ai(prompt, dry_run=dry_run)

        # Combine pick data with verification result
        result = {
            "player_name": pick["player_name"],
            "opponent": pick["opponent"],
            "prop_type": pick["prop_type"],
            "line": pick["line"],
            "projection": pick["projection"],
            "pick": pick["pick"],
            "edge_pct": pick["edge_pct"],
            "model_confidence": pick["confidence"],
            "stat_tier": pick["stat_tier"],
            "verdict": verification["verdict"],
            "ai_confidence": verification["confidence"],
            "reason": verification["reason"]
        }
        results.append(result)

        if verbose:
            print(f"    -> {verification['verdict']} ({verification['confidence']})")

    # Save results
    if not dry_run:
        save_verification_results(results, conn, target_date)

    return results


def get_verified_picks(conn, target_date=None, include_flagged=False):
    """Get verified picks for daily card generation."""
    if target_date is None:
        target_date = date.today().isoformat()

    verdicts = "('CONFIRM')" if not include_flagged else "('CONFIRM', 'FLAG')"

    picks = pd.read_sql(f"""
        SELECT e.*, v.verdict, v.ai_confidence, v.reason
        FROM props_edges e
        JOIN picks_verification v
          ON e.date = v.date
          AND e.player_name = v.player_name
          AND e.prop_type = v.prop_type
        WHERE e.date = ?
          AND v.verdict IN {verdicts}
        ORDER BY
            CASE v.verdict WHEN 'CONFIRM' THEN 1 ELSE 2 END,
            CASE e.stat_tier WHEN 'S_TIER' THEN 1 ELSE 2 END,
            e.confidence_score DESC
    """, conn, params=(target_date,))

    return picks


def main():
    parser = argparse.ArgumentParser(description="AI Pick Verification")
    parser.add_argument("--date", type=str, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Don't call AI or update database")
    parser.add_argument("--quiet", action="store_true", help="Minimal output")

    args = parser.parse_args()

    target_date = args.date or date.today().isoformat()

    conn = sqlite3.connect(DB_PATH)

    # Run verification
    results = verify_picks(
        conn,
        target_date=target_date,
        dry_run=args.dry_run,
        verbose=not args.quiet
    )

    # Display results
    if results and not args.quiet:
        display_results(results)

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
