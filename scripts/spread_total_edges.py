"""
AXIOM Spread & Total Edges
Validated edges from 200-query backtest analysis

SPREAD EDGES:
- DOGS_7: spread >= 7 -> HIGH confidence (63.5%, N=104, p=0.004)
- DOGS_6: spread >= 6 AND < 7 -> MEDIUM confidence (60.0%, N=130, p=0.014)

TOTAL EDGES:
- BOTH_LOW_PACE: both teams pace < 100 -> HIGH confidence (77.3%, N=22, p=0.009) [small sample]
- PACE_SUM_LOW: team1_pace + team2_pace < 200 -> HIGH confidence (63.6%, N=66, p=0.018)
- UNDER_235: total_line < 235 -> MEDIUM confidence (59.5%, N=116, p=0.025)
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional

DB_PATH = "data/NBA_AI_current.sqlite"

# Team name to code mapping for pace lookup
TEAM_NAME_TO_CODE = {
    'Atlanta Hawks': 'ATL', 'Boston Celtics': 'BOS', 'Brooklyn Nets': 'BKN',
    'Charlotte Hornets': 'CHA', 'Chicago Bulls': 'CHI', 'Cleveland Cavaliers': 'CLE',
    'Dallas Mavericks': 'DAL', 'Denver Nuggets': 'DEN', 'Detroit Pistons': 'DET',
    'Golden State Warriors': 'GSW', 'Houston Rockets': 'HOU', 'Indiana Pacers': 'IND',
    'LA Clippers': 'LAC', 'Los Angeles Lakers': 'LAL', 'Memphis Grizzlies': 'MEM',
    'Miami Heat': 'MIA', 'Milwaukee Bucks': 'MIL', 'Minnesota Timberwolves': 'MIN',
    'New Orleans Pelicans': 'NOP', 'New York Knicks': 'NYK', 'Oklahoma City Thunder': 'OKC',
    'Orlando Magic': 'ORL', 'Philadelphia 76ers': 'PHI', 'Phoenix Suns': 'PHX',
    'Portland Trail Blazers': 'POR', 'Sacramento Kings': 'SAC', 'San Antonio Spurs': 'SAS',
    'Toronto Raptors': 'TOR', 'Utah Jazz': 'UTA', 'Washington Wizards': 'WAS'
}

CODE_TO_NAME = {v: k for k, v in TEAM_NAME_TO_CODE.items()}


class SpreadTotalEdges:
    """Analyzes games for validated spread and total edges"""

    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
        self.team_stats = self._load_team_stats()

    def _load_team_stats(self) -> pd.DataFrame:
        """Load team advanced stats with pace data"""
        try:
            df = pd.read_sql_query('SELECT * FROM team_advanced_stats', self.conn)
            df['team_code'] = df['team_name'].map(TEAM_NAME_TO_CODE)
            return df
        except Exception as e:
            print(f"Warning: Could not load team_advanced_stats: {e}")
            return pd.DataFrame()

    def get_team_pace(self, team_code: str) -> Optional[float]:
        """Get pace for a team"""
        if self.team_stats.empty:
            return None
        match = self.team_stats[self.team_stats['team_code'] == team_code]
        if len(match) > 0:
            return match['pace'].values[0]
        return None

    def analyze_spread_edge(self, spread: float) -> Dict:
        """
        Analyze spread for underdog edges

        Returns dict with:
        - edge_type: DOGS_7, DOGS_6, or None
        - confidence: HIGH, MEDIUM, or None
        - pick: team to bet (underdog side)
        - historical: backtest stats
        """
        abs_spread = abs(spread)

        if abs_spread >= 7:
            return {
                'edge_type': 'DOGS_7',
                'confidence': 'HIGH',
                'pick': 'UNDERDOG',
                'historical': {
                    'hit_rate': 63.5,
                    'sample': 104,
                    'p_value': 0.004,
                    'roi': 22.0
                }
            }
        elif abs_spread >= 6:
            return {
                'edge_type': 'DOGS_6',
                'confidence': 'MEDIUM',
                'pick': 'UNDERDOG',
                'historical': {
                    'hit_rate': 60.0,
                    'sample': 130,
                    'p_value': 0.014,
                    'roi': 15.0
                }
            }

        return {'edge_type': None, 'confidence': None, 'pick': None}

    def analyze_total_edge(self, home_team: str, away_team: str,
                           total_line: Optional[float]) -> Dict:
        """
        Analyze total for under edges

        Returns dict with:
        - edge_type: BOTH_LOW_PACE, PACE_SUM_LOW, UNDER_235, or None
        - confidence: HIGH, MEDIUM, or None
        - pick: OVER or UNDER
        - historical: backtest stats
        - warning: any flags (e.g., small sample)
        """
        edges = []

        home_pace = self.get_team_pace(home_team)
        away_pace = self.get_team_pace(away_team)

        # Check pace-based edges
        if home_pace is not None and away_pace is not None:
            pace_sum = home_pace + away_pace

            # BOTH_LOW_PACE: both teams pace < 100
            if home_pace < 100 and away_pace < 100:
                edges.append({
                    'edge_type': 'BOTH_LOW_PACE',
                    'confidence': 'HIGH',
                    'pick': 'UNDER',
                    'warning': 'Small sample (N=22), monitor results',
                    'historical': {
                        'hit_rate': 77.3,
                        'sample': 22,
                        'p_value': 0.009,
                        'roi': 49.0
                    },
                    'detail': f'Both teams pace < 100 ({home_pace:.1f} + {away_pace:.1f})'
                })

            # PACE_SUM_LOW: combined pace < 200
            if pace_sum < 200:
                edges.append({
                    'edge_type': 'PACE_SUM_LOW',
                    'confidence': 'HIGH',
                    'pick': 'UNDER',
                    'historical': {
                        'hit_rate': 63.6,
                        'sample': 66,
                        'p_value': 0.018,
                        'roi': 22.0
                    },
                    'detail': f'Pace sum {pace_sum:.1f} < 200'
                })

        # UNDER_235: total line < 235
        if total_line is not None and total_line < 235:
            edges.append({
                'edge_type': 'UNDER_235',
                'confidence': 'MEDIUM',
                'pick': 'UNDER',
                'historical': {
                    'hit_rate': 59.5,
                    'sample': 116,
                    'p_value': 0.025,
                    'roi': 14.0
                },
                'detail': f'Total {total_line} < 235'
            })

        # Return the strongest edge (by ROI)
        if edges:
            edges.sort(key=lambda x: x['historical']['roi'], reverse=True)
            return edges[0]

        return {'edge_type': None, 'confidence': None, 'pick': None}

    def _get_ai_warnings(self, game: Dict, edge_type: str) -> Optional[str]:
        """
        Check for AI verification warnings based on backtest findings.

        Returns warning string if risk factor found, None otherwise.

        Based on ai_verification_backtest.py v2 results:
        - Large spread unders (>12): 33% hit rate -> WARN
        - Dog on B2B vs 4+ rest: rare but risky -> WARN
        """
        spread = abs(game.get('spread') or 0)

        # Check for total/under edges
        if edge_type in ['PACE_SUM_LOW', 'BOTH_LOW_PACE', 'UNDER_235']:
            # Large spread under - blowouts go over in garbage time
            if spread > 12:
                return 'CAUTION: Large spread game (>12) - garbage time may push over'

        # Check for spread/dog edges
        if edge_type in ['DOGS_7', 'DOGS_6']:
            is_home_dog = game.get('spread', 0) > 0
            home_b2b = game.get('home_b2b', False)
            away_b2b = game.get('away_b2b', False)
            home_rest = game.get('home_rest')
            away_rest = game.get('away_rest')

            dog_on_b2b = (is_home_dog and home_b2b) or (not is_home_dog and away_b2b)
            fav_very_rested = (is_home_dog and away_rest and away_rest >= 4) or \
                              (not is_home_dog and home_rest and home_rest >= 4)

            if dog_on_b2b and fav_very_rested:
                return 'CAUTION: Dog on B2B vs well-rested (4+ days) favorite'

        return None

    def analyze_game(self, game: Dict) -> Dict:
        """
        Full analysis of a single game for all edges

        game should have: home_team, away_team, spread, total_line
        Optional: home_b2b, away_b2b, home_rest, away_rest

        Returns comprehensive edge analysis
        """
        home_team = game.get('home_team')
        away_team = game.get('away_team')
        spread = game.get('spread')
        total_line = game.get('total_line')

        result = {
            'home_team': home_team,
            'away_team': away_team,
            'spread': spread,
            'total_line': total_line,
            'spread_edge': None,
            'total_edge': None,
            'multi_edge': False,
            'edges': [],
            'conviction': 'NONE',
            'warnings': []
        }

        # Determine underdog
        if spread is not None:
            if spread > 0:
                underdog = home_team
                favorite = away_team
            else:
                underdog = away_team
                favorite = home_team
            result['underdog'] = underdog
            result['favorite'] = favorite

        # Analyze spread edge
        if spread is not None:
            spread_edge = self.analyze_spread_edge(spread)
            if spread_edge['edge_type']:
                spread_edge['bet_team'] = result.get('underdog')

                # Check for AI verification warnings
                warning = self._get_ai_warnings(game, spread_edge['edge_type'])
                if warning:
                    spread_edge['warning'] = warning
                    result['warnings'].append(warning)

                result['spread_edge'] = spread_edge
                result['edges'].append(spread_edge)

        # Analyze total edge
        total_edge = self.analyze_total_edge(home_team, away_team, total_line)
        if total_edge.get('edge_type'):
            # Check for AI verification warnings
            warning = self._get_ai_warnings(game, total_edge['edge_type'])
            if warning:
                total_edge['warning'] = warning
                if warning not in result['warnings']:
                    result['warnings'].append(warning)

            result['total_edge'] = total_edge
            result['edges'].append(total_edge)

        # Check for multi-edge
        if result['spread_edge'] and result['total_edge']:
            result['multi_edge'] = True
            result['conviction'] = 'HIGHEST'
        elif result['spread_edge'] and result['spread_edge']['confidence'] == 'HIGH':
            result['conviction'] = 'HIGH'
        elif result['total_edge'] and result['total_edge']['confidence'] == 'HIGH':
            result['conviction'] = 'HIGH'
        elif result['spread_edge'] or result['total_edge']:
            result['conviction'] = 'MEDIUM'

        # Downgrade conviction if there are warnings
        if result['warnings'] and result['conviction'] in ['HIGH', 'HIGHEST']:
            result['conviction'] = 'MEDIUM'

        return result

    def get_todays_games(self, target_date: Optional[str] = None, include_final: bool = False) -> List[Dict]:
        """Get games with betting lines for a date

        Args:
            target_date: Date in YYYY-MM-DD format
            include_final: If True, include completed games (for backtesting)
        """
        if target_date is None:
            target_date = datetime.now().strftime('%Y-%m-%d')

        # Get games with B2B detection via CTEs
        sql = '''
        WITH all_team_games AS (
            SELECT home_team as team, game_id, DATE(date_time_utc) as game_date
            FROM Games WHERE status_text = 'Final'
            UNION ALL
            SELECT away_team as team, game_id, DATE(date_time_utc) as game_date
            FROM Games WHERE status_text = 'Final'
        ),
        team_schedule AS (
            SELECT team, game_id, game_date,
                JULIANDAY(game_date) - JULIANDAY(LAG(game_date) OVER (PARTITION BY team ORDER BY game_date)) as days_rest
            FROM all_team_games
        )
        SELECT
            g.game_id,
            g.home_team,
            g.away_team,
            DATE(g.date_time_utc) as game_date,
            g.status_text,
            COALESCE(b.espn_current_spread, b.espn_closing_spread, b.espn_opening_spread) as spread,
            COALESCE(b.espn_current_total, b.espn_closing_total, b.espn_opening_total) as total_line,
            b.espn_opening_spread,
            b.espn_opening_total,
            hs.days_rest as home_rest,
            aws.days_rest as away_rest
        FROM Games g
        LEFT JOIN Betting b ON g.game_id = b.game_id
        LEFT JOIN team_schedule hs ON g.game_id = hs.game_id AND g.home_team = hs.team
        LEFT JOIN team_schedule aws ON g.game_id = aws.game_id AND g.away_team = aws.team
        WHERE DATE(g.date_time_utc) = ?
        '''

        if not include_final:
            sql += " AND g.status_text != 'Final'"

        sql += " ORDER BY g.date_time_utc"

        df = pd.read_sql_query(sql, self.conn, params=[target_date])

        games = []
        for _, row in df.iterrows():
            games.append({
                'game_id': row['game_id'],
                'home_team': row['home_team'],
                'away_team': row['away_team'],
                'game_date': row['game_date'],
                'spread': row['spread'] if pd.notna(row['spread']) else row['espn_opening_spread'],
                'total_line': row['total_line'] if pd.notna(row['total_line']) else row['espn_opening_total'],
                'home_rest': row.get('home_rest'),
                'away_rest': row.get('away_rest'),
                'home_b2b': row.get('home_rest') == 1 if pd.notna(row.get('home_rest')) else False,
                'away_b2b': row.get('away_rest') == 1 if pd.notna(row.get('away_rest')) else False
            })

        return games

    def generate_edge_report(self, target_date: Optional[str] = None) -> str:
        """Generate formatted edge report for the day"""
        games = self.get_todays_games(target_date)

        if not games:
            return f"No games found for {target_date or 'today'}"

        # Analyze all games
        analyses = []
        for game in games:
            analysis = self.analyze_game(game)
            analyses.append(analysis)

        # Categorize
        underdog_plays = [a for a in analyses if a['spread_edge']]
        under_plays = [a for a in analyses if a['total_edge']]
        multi_edge = [a for a in analyses if a['multi_edge']]

        # Sort by spread size (biggest dogs first)
        underdog_plays.sort(key=lambda x: abs(x['spread'] or 0), reverse=True)

        # Build report
        lines = []
        date_str = target_date or datetime.now().strftime('%Y-%m-%d')
        lines.append("=" * 60)
        lines.append(f"SPREAD & TOTAL EDGES - {date_str}")
        lines.append("=" * 60)
        lines.append("")

        # Multi-edge (highest conviction)
        if multi_edge:
            lines.append("*** MULTI-EDGE (HIGHEST CONVICTION) ***")
            lines.append("-" * 40)
            for a in multi_edge:
                matchup = f"{a['away_team']} @ {a['home_team']}"
                spread_str = f"{a['spread']:+.1f}" if a['spread'] else "N/A"
                total_str = f"{a['total_line']:.1f}" if a['total_line'] else "N/A"

                lines.append(f"{matchup}")
                lines.append(f"  Spread: {spread_str} | Total: {total_str}")
                lines.append(f"  -> BET: {a['underdog']} +{abs(a['spread']):.1f}")
                lines.append(f"  -> BET: UNDER {total_str}")

                # Show edge details
                se = a['spread_edge']
                te = a['total_edge']
                lines.append(f"  Spread Edge: {se['edge_type']} ({se['historical']['hit_rate']}% hit rate)")
                lines.append(f"  Total Edge: {te['edge_type']} ({te['historical']['hit_rate']}% hit rate)")
                if te.get('warning'):
                    lines.append(f"  [!] {te['warning']}")
                lines.append("")
            lines.append("")

        # Underdog plays
        lines.append("UNDERDOG PLAYS")
        lines.append("-" * 40)
        if underdog_plays:
            for a in underdog_plays:
                if a['multi_edge']:
                    continue  # Already shown above

                matchup = f"{a['away_team']} @ {a['home_team']}"
                spread_str = f"{a['spread']:+.1f}" if a['spread'] else "N/A"
                se = a['spread_edge']
                conf = se['confidence']
                conf_emoji = "***" if conf == 'HIGH' else "**"

                lines.append(f"{conf_emoji} {matchup} | Spread: {spread_str}")
                lines.append(f"   -> BET: {a['underdog']} +{abs(a['spread']):.1f}")
                lines.append(f"   Edge: {se['edge_type']} | {se['historical']['hit_rate']}% ({se['historical']['sample']} games)")
                lines.append(f"   Expected ROI: +{se['historical']['roi']:.0f}%")
                lines.append("")
        else:
            lines.append("No underdog edges today")
            lines.append("")

        # Under plays
        lines.append("")
        lines.append("UNDER PLAYS")
        lines.append("-" * 40)
        under_only = [a for a in under_plays if not a['multi_edge']]
        if under_only:
            for a in under_only:
                matchup = f"{a['away_team']} @ {a['home_team']}"
                total_str = f"{a['total_line']:.1f}" if a['total_line'] else "N/A"
                te = a['total_edge']
                conf = te['confidence']
                conf_emoji = "***" if conf == 'HIGH' else "**"

                lines.append(f"{conf_emoji} {matchup} | Total: {total_str}")
                lines.append(f"   -> BET: UNDER {total_str}")
                lines.append(f"   Edge: {te['edge_type']} | {te['historical']['hit_rate']}% ({te['historical']['sample']} games)")
                if te.get('detail'):
                    lines.append(f"   Detail: {te['detail']}")
                if te.get('warning'):
                    lines.append(f"   [!] {te['warning']}")
                lines.append("")
        else:
            lines.append("No under edges today")
            lines.append("")

        # Summary
        lines.append("")
        lines.append("=" * 60)
        lines.append("SUMMARY")
        lines.append("=" * 60)
        lines.append(f"Total games: {len(games)}")
        lines.append(f"Multi-edge plays: {len(multi_edge)}")
        lines.append(f"Underdog plays: {len(underdog_plays)}")
        lines.append(f"Under plays: {len(under_plays)}")
        lines.append("")
        lines.append("Confidence Legend:")
        lines.append("  *** = HIGH (63%+ hit rate, p < 0.02)")
        lines.append("  **  = MEDIUM (59%+ hit rate, p < 0.10)")
        lines.append("")

        return "\n".join(lines)

    def get_edge_picks(self, target_date: Optional[str] = None) -> List[Dict]:
        """Get structured edge picks for integration with other systems"""
        games = self.get_todays_games(target_date)
        picks = []

        for game in games:
            analysis = self.analyze_game(game)

            if analysis['spread_edge']:
                picks.append({
                    'type': 'SPREAD',
                    'game': f"{analysis['away_team']} @ {analysis['home_team']}",
                    'pick': f"{analysis['underdog']} +{abs(analysis['spread']):.1f}",
                    'edge': analysis['spread_edge']['edge_type'],
                    'confidence': analysis['spread_edge']['confidence'],
                    'hit_rate': analysis['spread_edge']['historical']['hit_rate'],
                    'multi_edge': analysis['multi_edge']
                })

            if analysis['total_edge']:
                picks.append({
                    'type': 'TOTAL',
                    'game': f"{analysis['away_team']} @ {analysis['home_team']}",
                    'pick': f"UNDER {analysis['total_line']:.1f}",
                    'edge': analysis['total_edge']['edge_type'],
                    'confidence': analysis['total_edge']['confidence'],
                    'hit_rate': analysis['total_edge']['historical']['hit_rate'],
                    'multi_edge': analysis['multi_edge'],
                    'warning': analysis['total_edge'].get('warning')
                })

        return picks

    def close(self):
        """Close database connection"""
        self.conn.close()


def main():
    """Run edge analysis for today"""
    import argparse

    parser = argparse.ArgumentParser(description='AXIOM Spread & Total Edge Analyzer')
    parser.add_argument('--date', help='Date to analyze (YYYY-MM-DD)', default=None)
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    parser.add_argument('--backtest', action='store_true', help='Include completed games (for backtesting)')
    args = parser.parse_args()

    analyzer = SpreadTotalEdges()

    # Override get_todays_games to include final games if backtesting
    if args.backtest:
        original_func = analyzer.get_todays_games
        analyzer.get_todays_games = lambda d: original_func(d, include_final=True)

    if args.json:
        import json
        picks = analyzer.get_edge_picks(args.date)
        print(json.dumps(picks, indent=2))
    else:
        report = analyzer.generate_edge_report(args.date)
        print(report)

    analyzer.close()


if __name__ == "__main__":
    main()
