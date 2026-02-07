"""
app.py

This module sets up a Flask web application to display NBA game data, including game schedules, team details, and predictions.
It integrates with external APIs to fetch and process data, and utilizes a machine learning predictor for game predictions.

Core Functions:
- create_app(predictor): Initializes and configures the Flask application, including setting up routes and the app secret key.

Routes:
- home(): Renders the home page with the NBA game schedule for a specific date.
- get_game_data(): Fetches game data for a given date or game ID and processes it for display.

Helper Functions:
- add_header(response): Adds headers to the response to prevent caching of the pages.

Usage:
Typically run via a entry point in the root directory of the project.
"""

import csv
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask, flash, jsonify, render_template, request

from src.config import config
from src.games_api.api import api as api_blueprint
from src.games_api.games import get_games, get_games_for_date
from src.utils import validate_date_format
from src.web_app.game_data_processor import get_user_datetime, process_game_data

# Configuration variables
DB_PATH = config["database"]["path"]
WEB_APP_SECRET_KEY = config["web_app"]["secret_key"]
PROJECT_ROOT = Path(__file__).parent.parent.parent
PREDICTIONS_DIR = PROJECT_ROOT / "outputs" / "predictions"
PERFORMANCE_DIR = PROJECT_ROOT / "outputs" / "performance"
RESULTS_CSV = PROJECT_ROOT / "data" / "results.csv"


def create_app(predictor):
    """
    Initializes and configures the Flask application.

    Args:
        predictor (str): A predictor used for generating game predictions.

    Returns:
        Flask: The configured Flask application instance.
    """
    app = Flask(__name__)
    app.secret_key = WEB_APP_SECRET_KEY

    # Store the predictor in the app configuration
    app.config["PREDICTOR"] = predictor

    # Register the API blueprint
    app.register_blueprint(api_blueprint, url_prefix="/api")

    @app.route("/")
    def home():
        """
        Renders the home page with NBA game schedule and details for a specific date.

        - Defaults to the current date if no date is provided or if an invalid date is entered.
        - Displays a list of games, including links to detailed game information.

        Returns:
            str: Rendered HTML page of the home screen with games table.
        """
        current_date_local = get_user_datetime(as_eastern_tz=False)
        current_date_str = current_date_local.strftime("%Y-%m-%d")
        query_date_str = request.args.get("date", current_date_str)

        try:
            validate_date_format(query_date_str)
            query_date = datetime.strptime(query_date_str, "%Y-%m-%d")
        except Exception as e:
            flash("Invalid date format. Showing games for today.", "error")
            query_date_str = current_date_str
            query_date = current_date_local

        query_date_display_str = query_date.strftime("%b %d")
        next_date = query_date + timedelta(days=1)
        prev_date = query_date - timedelta(days=1)
        next_date_str = next_date.strftime("%Y-%m-%d")
        prev_date_str = prev_date.strftime("%Y-%m-%d")

        return render_template(
            "index.html",
            query_date_str=query_date_str,
            query_date_display_str=query_date_display_str,
            prev_date=prev_date_str,
            next_date=next_date_str,
        )

    @app.route("/get-game-data")
    def get_game_data():
        """
        Fetches and processes game data for a given date or game ID.

        - Supports querying by either 'date' or 'game_id'.
        - Retrieves game data directly from games module (no internal HTTP call).

        Returns:
            Response: JSON response containing processed game data or error message.
        """
        try:
            predictor = app.config["PREDICTOR"]

            # Determine the type of input (date or game_id)
            if "date" in request.args:
                # Use provided date or default to the current date if not provided
                inbound_query_date_str = request.args.get("date")
                if inbound_query_date_str is None or inbound_query_date_str == "":
                    current_date_local = get_user_datetime(as_eastern_tz=False)
                    query_date_str = current_date_local.strftime("%Y-%m-%d")
                else:
                    query_date_str = inbound_query_date_str

                # Call get_games_for_date directly (no HTTP overhead)
                # Note: This triggers database updates which log their own timing
                game_data = get_games_for_date(
                    query_date_str,
                    predictor=predictor,
                    update_predictions=True,
                )
                log_context = query_date_str

            elif "game_id" in request.args:
                game_id = request.args.get("game_id")
                game_ids = [g.strip() for g in game_id.split(",") if g.strip()]

                # Validate we have at least one game_id
                if not game_ids:
                    return (
                        jsonify({"error": "game_id parameter cannot be empty."}),
                        400,
                    )

                # Call get_games directly (no HTTP overhead)
                game_data = get_games(
                    game_ids,
                    predictor=predictor,
                    update_predictions=True,
                )
                log_context = (
                    game_ids[0] if len(game_ids) == 1 else f"{len(game_ids)} games"
                )

            else:
                return (
                    jsonify({"error": "Either 'date' or 'game_id' must be provided."}),
                    400,
                )

            # Get user timezone from request (passed from browser)
            user_tz = request.args.get("user_tz", None)

            # Time only the frontend processing (data transformation + JSON serialization)
            frontend_start = time.perf_counter()
            outbound_game_data = process_game_data(game_data, user_tz=user_tz)
            frontend_elapsed = time.perf_counter() - frontend_start

            # Summary log line at INFO level (similar style to pipeline stages)
            logging.info(
                f"[Frontend] {log_context}: {len(game_data)} games | {frontend_elapsed:.1f}s"
            )

            return jsonify(outbound_game_data)

        except ValueError as e:
            return (
                jsonify({"error": str(e)}),
                400,
            )
        except Exception as e:
            logging.exception("Error in get_game_data")
            return (
                jsonify({"error": f"Unable to fetch game data: {str(e)}"}),
                500,
            )

    @app.route("/picks")
    def picks():
        """Renders the picks page with today's or queried date's picks."""
        current_date_local = get_user_datetime(as_eastern_tz=False)
        current_date_str = current_date_local.strftime("%Y-%m-%d")
        query_date_str = request.args.get("date", current_date_str)

        try:
            validate_date_format(query_date_str)
            query_date = datetime.strptime(query_date_str, "%Y-%m-%d")
        except Exception:
            flash("Invalid date format. Showing picks for today.", "error")
            query_date_str = current_date_str
            query_date = current_date_local

        next_date = (query_date + timedelta(days=1)).strftime("%Y-%m-%d")
        prev_date = (query_date - timedelta(days=1)).strftime("%Y-%m-%d")

        # Read picks CSV
        picks_file = PREDICTIONS_DIR / f"picks_{query_date_str}.csv"
        spreads = []
        props = []

        if picks_file.exists():
            with open(picks_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    bet_type = row.get('bet_type', '')
                    tier = row.get('tier', '')
                    if tier == 'SKIP':
                        continue
                    if bet_type == 'SPREAD':
                        spreads.append(row)
                    elif bet_type == 'PROP':
                        props.append(row)

        return render_template(
            "picks.html",
            query_date=query_date_str,
            query_date_display=query_date.strftime("%b %d, %Y"),
            prev_date=prev_date,
            next_date=next_date,
            spreads=spreads,
            props=props,
        )

    @app.route("/performance")
    def performance():
        """Renders the performance dashboard with running record."""
        stats = None
        daily_rows = []

        if RESULTS_CSV.exists():
            with open(RESULTS_CSV, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                results = [r for r in reader if r.get('result') in ['W', 'L']]

            if results:
                # Calculate stats
                spread_w = sum(1 for r in results if r.get('bet_type') == 'SPREAD' and r['result'] == 'W')
                spread_l = sum(1 for r in results if r.get('bet_type') == 'SPREAD' and r['result'] == 'L')
                prop_w = sum(1 for r in results if r.get('bet_type', 'PROP') == 'PROP' and r['result'] == 'W')
                prop_l = sum(1 for r in results if r.get('bet_type', 'PROP') == 'PROP' and r['result'] == 'L')
                total_w = spread_w + prop_w
                total_l = spread_l + prop_l

                def calc_pct(w, l):
                    return (w / (w + l) * 100) if (w + l) > 0 else 0

                def calc_roi(w, l):
                    profit = (w * 100) - (l * 110)
                    risked = (w + l) * 110
                    return (profit / risked * 100) if risked > 0 else 0

                # Current streak
                sorted_results = sorted(results, key=lambda x: x['date'], reverse=True)
                streak = 0
                streak_type = sorted_results[0]['result']
                for r in sorted_results:
                    if r['result'] == streak_type:
                        streak += 1
                    else:
                        break

                stats = {
                    'total_w': total_w, 'total_l': total_l,
                    'total_pct': calc_pct(total_w, total_l),
                    'total_roi': calc_roi(total_w, total_l),
                    'spread_w': spread_w, 'spread_l': spread_l,
                    'spread_pct': calc_pct(spread_w, spread_l),
                    'spread_roi': calc_roi(spread_w, spread_l),
                    'prop_w': prop_w, 'prop_l': prop_l,
                    'prop_pct': calc_pct(prop_w, prop_l),
                    'prop_roi': calc_roi(prop_w, prop_l),
                    'streak': streak, 'streak_type': streak_type,
                }

                # Daily breakdown
                daily = defaultdict(lambda: {'SPREAD': {'W': 0, 'L': 0}, 'PROP': {'W': 0, 'L': 0}})
                for r in results:
                    bt = r.get('bet_type', 'PROP')
                    if bt not in ('SPREAD', 'PROP'):
                        bt = 'PROP'
                    daily[r['date']][bt][r['result']] += 1

                cum = {'SPREAD': {'W': 0, 'L': 0}, 'PROP': {'W': 0, 'L': 0}}
                for dt in sorted(daily.keys()):
                    d = daily[dt]
                    cum['SPREAD']['W'] += d['SPREAD']['W']
                    cum['SPREAD']['L'] += d['SPREAD']['L']
                    cum['PROP']['W'] += d['PROP']['W']
                    cum['PROP']['L'] += d['PROP']['L']

                    day_w = d['SPREAD']['W'] + d['PROP']['W']
                    day_l = d['SPREAD']['L'] + d['PROP']['L']
                    cum_w = cum['SPREAD']['W'] + cum['PROP']['W']
                    cum_l = cum['SPREAD']['L'] + cum['PROP']['L']

                    daily_rows.append({
                        'date': dt,
                        'spread_daily': f"{d['SPREAD']['W']}-{d['SPREAD']['L']}",
                        'prop_daily': f"{d['PROP']['W']}-{d['PROP']['L']}",
                        'total_daily': f"{day_w}-{day_l}",
                        'total_cumulative': f"{cum_w}-{cum_l}",
                        'total_pct': f"{calc_pct(cum_w, cum_l):.1f}%",
                    })

        return render_template(
            "performance.html",
            stats=stats,
            daily_rows=daily_rows,
        )

    @app.after_request
    def add_header(response):
        """
        Adds headers to the response to prevent caching of the pages.

        Args:
            response (Response): The HTTP response object.

        Returns:
            Response: The modified response object with added headers.
        """
        response.headers["Cache-Control"] = "no-store"
        return response

    return app
