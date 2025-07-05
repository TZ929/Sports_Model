import sys
import logging
import json
from pathlib import Path
import pandas as pd

# Add project root to the Python path
sys.path.append(str(Path(__file__).resolve().parent))

from src.prediction.predict import load_model, fetch_prediction_data, engineer_features, make_prediction
from src.utils.player_matching import player_matcher
from src.utils.game_finder import game_finder
from src.utils.odds_conversion import find_value_opportunity, american_to_implied_probability

# --- Main Application Logger ---
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage()
        }
        if "extra_data" in record.__dict__:
            log_record.update(record.__dict__["extra_data"])
        return json.dumps(log_record)

def setup_logger():
    logger = logging.getLogger() # Get root logger
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()
        
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    return logger

# --- Value Predictions Logger ---
def setup_value_logger():
    """Sets up a logger to output value bets to a dedicated file."""
    value_logger = logging.getLogger('ValueLogger')
    value_logger.setLevel(logging.INFO)
    
    # Prevent logs from propagating to the root logger
    value_logger.propagate = False
    
    # Clear existing handlers
    if value_logger.hasHandlers():
        value_logger.handlers.clear()
        
    # File handler for the value bets
    log_file = Path("logs/predictions.log")
    log_file.parent.mkdir(exist_ok=True)
    file_handler = logging.FileHandler(log_file)
    
    # Simple formatter for the log file, as requested in the plan
    formatter = logging.Formatter('%(message)s')
    file_handler.setFormatter(formatter)
    
    value_logger.addHandler(file_handler)
    
    # Add a header to the log file if it's new/empty
    if log_file.stat().st_size == 0:
        header = "Player | Prop | Line | Sportsbook | Model Probability | Implied Probability | Bet | Edge"
        value_logger.info(header)
        value_logger.info("-" * len(header))

    return value_logger

logger = setup_logger()
value_logger = setup_value_logger()
# --- End of Logging Setup ---

def find_latest_model() -> tuple[Path, str] | tuple[None, None]:
    """
    Finds the path to the latest model and its corresponding season.
    Returns a tuple of (model_path, season_name) or (None, None).
    """
    models_root = Path("data/models")
    if not models_root.exists():
        return None, None

    season_dirs = [d for d in models_root.iterdir() if d.is_dir()]
    
    if not season_dirs:
        # Fallback for non-versioned model
        model_path = models_root / "advanced_model.joblib"
        if model_path.exists():
            return model_path, "latest" # Use "latest" as a placeholder season
        return None, None

    latest_season_dir = max(season_dirs, key=lambda d: d.name)
    
    try:
        model_path = next(latest_season_dir.glob("*.joblib"))
        return model_path, latest_season_dir.name
    except StopIteration:
        return None, None

def run_predictions_for_odds(odds_df: pd.DataFrame):
    """
    Runs the prediction pipeline for all props in the odds DataFrame.
    """
    # 1. Find the latest model
    model_path, season = find_latest_model()
    if not model_path:
        logger.error("Prediction pipeline failed: No trained model found.")
        return

    logger.info("Loading latest model.", extra={"extra_data": {"model_path": str(model_path), "season": season}})
    model = load_model(model_path)
    if model is None:
        logger.error("Prediction pipeline failed: Could not load the model.")
        return

    # 2. Iterate through each prop bet
    for _, row in odds_df.iterrows():
        player_name = str(row['player_name'])
        game_date = str(row['game_date'])
        
        # 3. Match player name to player_id
        player_id = player_matcher.get_player_id(player_name, score_cutoff=80)
        if not player_id:
            logger.warning(f"Skipping prediction for '{player_name}': Could not find player_id.", extra={"extra_data": row.to_dict()})
            continue

        # 4. Find game_id
        game_id = game_finder.find_game_id(player_id, game_date)
        if not game_id:
            logger.warning(f"Skipping prediction for '{player_name}': Could not find game_id for date {game_date}.", extra={"extra_data": row.to_dict()})
            continue
            
        logger.info(
            "Making prediction for player.", 
            extra={"extra_data": {"game_id": game_id, "player_id": player_id, "player_name": player_name}}
        )

        # 5. Fetch data, engineer features, and predict
        raw_data = fetch_prediction_data(game_id, player_id)
        if raw_data is None:
            logger.error("Prediction failed: Could not fetch the required data.", extra={"extra_data": {"game_id": game_id, "player_id": player_id}})
            continue

        features = engineer_features(raw_data)
        if features.empty:
            logger.error("Prediction failed: Could not engineer features.", extra={"extra_data": {"game_id": game_id, "player_id": player_id}})
            continue
            
        result = make_prediction(model, features)

        if result:
            prediction, proba = result
            outcome = 'OVER' if prediction[0] == 1 else 'UNDER'
            logger.info(
                "Prediction successful.",
                extra={"extra_data": {
                    "game_id": game_id,
                    "player_name": player_name,
                    "prop_type": row['prop_type'],
                    "line": row['line'],
                    "outcome": outcome,
                    "probability_over": f"{proba[0][1]:.4f}",
                    "probability_under": f"{proba[0][0]:.4f}",
                }}
            )

            # 7. Find Value Opportunity
            over_odds = int(row['over_odds'])
            under_odds = int(row['under_odds'])
            
            value_bet = find_value_opportunity(proba[0][1], over_odds, under_odds)
            
            if value_bet:
                bet_type, edge = value_bet
                
                # Determine the relevant probabilities for logging
                if bet_type == 'OVER':
                    model_prob_log = proba[0][1]
                    implied_prob_log = american_to_implied_probability(over_odds)
                else: # UNDER
                    model_prob_log = 1 - proba[0][1]
                    implied_prob_log = american_to_implied_probability(under_odds)

                # Log to the dedicated predictions file
                log_message = (
                    f"{player_name} | {row['prop_type']} | {row['line']} | "
                    f"{row['sportsbook']} | {model_prob_log:.2%} | "
                    f"{implied_prob_log:.2%} | {bet_type} | {edge:.2%}"
                )
                value_logger.info(log_message)
                
                # Also log to the main console logger for real-time visibility
                logger.info("Value bet identified.", extra={"extra_data": row.to_dict()})

        else:
            logger.error("Prediction failed during the final step.", extra={"extra_data": {"game_id": game_id, "player_id": player_id}})


def main():
    """Main function to run the daily prediction pipeline."""
    # This script now reads from a file instead of taking CLI arguments.
    odds_file = Path("data/raw/daily_odds.json")
    if not odds_file.exists():
        logger.error(f"Prediction pipeline failed: Odds file not found at '{odds_file}'.")
        # As a fallback, we could try to run the scraper here.
        # from src.data_collection.scrape_daily_odds import get_daily_odds
        # logger.info("Attempting to scrape daily odds now...")
        # get_daily_odds()
        # if not odds_file.exists():
        #     logger.error("Failed to generate odds file. Exiting.")
        #     return
        return # For now, we just exit.

    try:
        odds_df = pd.read_json(odds_file)
        # The date in the sample file is already a string 'YYYY-MM-DD'
        # If it were a datetime object, we would format it:
        # odds_df['game_date'] = pd.to_datetime(odds_df['game_date']).dt.strftime('%Y-%m-%d')
        logger.info(f"Loaded {len(odds_df)} props from '{odds_file}'.")
    except Exception as e:
        logger.error(f"Failed to read or process odds file: {e}")
        return

    run_predictions_for_odds(odds_df)
    

if __name__ == "__main__":
    main() 