import argparse
import sys
import logging
import json
from pathlib import Path

# Add project root to the Python path
sys.path.append(str(Path(__file__).resolve().parent))

from src.prediction.predict import load_model, fetch_prediction_data, engineer_features, make_prediction

# --- Structured Logging Setup ---
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

logger = setup_logger()
# --- End of Logging Setup ---

def main():
    """Main function to run the prediction CLI."""
    parser = argparse.ArgumentParser(description="Make predictions for player performance in a given game.")
    parser.add_argument("game_id", type=str, help="The ID of the game to predict.")
    parser.add_argument("player_id", type=str, help="The ID of the player to predict for.")
    parser.add_argument("--model_path", type=str, default="data/models/advanced_model.joblib", help="Path to the trained model file.")
    
    args = parser.parse_args()

    logger.info(
        "Starting prediction process.", 
        extra={"extra_data": {"game_id": args.game_id, "player_id": args.player_id, "model_path": args.model_path}}
    )

    # 1. Load Model
    model = load_model(args.model_path)
    if model is None:
        logger.error("Prediction failed: Could not load the model.")
        return

    # 2. Fetch Data
    raw_data = fetch_prediction_data(args.game_id, args.player_id)
    if raw_data is None:
        logger.error("Prediction failed: Could not fetch the required data.")
        return

    # 3. Engineer Features
    features = engineer_features(raw_data)
    if features.empty:
        logger.error("Prediction failed: Could not engineer features from the data.")
        return
        
    # 4. Make Prediction
    result = make_prediction(model, features)

    if result:
        prediction, proba = result
        outcome = 'OVER' if prediction[0] == 1 else 'UNDER'
        logger.info(
            "Prediction successful.",
            extra={"extra_data": {
                "outcome": outcome,
                "probability_over": f"{proba[0][1]:.4f}",
                "probability_under": f"{proba[0][0]:.4f}",
            }}
        )
    else:
        logger.error("Prediction failed during the final step.")

if __name__ == "__main__":
    main() 