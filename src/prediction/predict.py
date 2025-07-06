import pandas as pd
import joblib
from pathlib import Path
import logging
import sys

# Add project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.utils.database import db_manager
from src.feature_engineering.player_features import PlayerFeatures
from src.feature_engineering.game_features import GameFeatures
from src.feature_engineering.team_features import TeamFeatures
from sqlalchemy import text

# Setup logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "prediction.log"),
        logging.StreamHandler()
    ]
)

def load_model(model_path="data/models/advanced_model.joblib"):
    """Loads the trained model from the specified path."""
    logging.info(f"Loading model from {model_path}...")
    try:
        model = joblib.load(model_path)
        logging.info("Model loaded successfully.")
        return model
    except FileNotFoundError:
        logging.error(f"Model file not found at {model_path}.")
        return None

def get_player_game_log(session, player_id, game_date):
    """Fetches the game log for a player up to a certain date."""
    query = f"""
    SELECT s.*, g.date
    FROM player_game_stats s
    JOIN games g ON s.game_id = g.game_id
    WHERE s.player_id = '{player_id}' AND g.date < '{game_date}'
    ORDER BY g.date DESC
    LIMIT 20;
    """
    return pd.read_sql(text(query), session.bind)

def get_game_info(session, game_id):
    """Fetches information about a specific game."""
    return pd.read_sql(text(f"SELECT * FROM games WHERE game_id = '{game_id}'"), session.bind)

def get_player_info(session, player_id):
    """Fetches information about a specific player."""
    return pd.read_sql(text(f"SELECT * FROM players WHERE player_id = '{player_id}'"), session.bind)

def fetch_prediction_data(game_id, player_id):
    """
    Fetches and prepares the data needed for a prediction from the database.
    """
    logging.info(f"Fetching data for game_id={game_id} and player_id={player_id}...")
    
    with db_manager.get_session() as session:
        game_info = get_game_info(session, game_id)
        if game_info.empty:
            logging.error(f"No game found with game_id={game_id}")
            return None
        
        game_date = game_info['date'].iloc[0]
        
        player_log = get_player_game_log(session, player_id, game_date)
        
        # Create a single-row DataFrame with the game and player info
        prediction_instance = game_info.copy()
        prediction_instance['player_id'] = player_id

        # Get player's team_id and add it to the instance
        player_info = get_player_info(session, player_id)
        if not player_info.empty:
            prediction_instance['team_id'] = player_info['team_id'].iloc[0]
        else:
            # If player not found, we can't determine their team.
            # This is a problem, but for now we'll use a placeholder.
            logging.warning(f"Player with id {player_id} not found in the database.")
            prediction_instance['team_id'] = 'UNKNOWN'


        if player_log.empty:
            logging.warning(f"No recent game log found for player_id={player_id}. Creating an empty record.")
            # Define the columns that the feature engineering steps expect
            stats_cols = ['points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers', 'offensive_rebounds']
            for col in stats_cols:
                prediction_instance[col] = 0
            
            full_data_for_features = prediction_instance
        else:
            # Combine the game info with the player's historical stats
            full_data_for_features = pd.concat([player_log, prediction_instance], ignore_index=True)

        return full_data_for_features

def engineer_features(raw_data):
    """
    Applies the same feature engineering logic used during training.
    """
    logging.info("Engineering features for the prediction data...")
    
    # Instantiate feature creators
    player_feature_creator = PlayerFeatures(config={})
    game_feature_creator = GameFeatures(config={})
    team_feature_creator = TeamFeatures(config={})

    # Apply feature engineering steps
    features_df = player_feature_creator.create_rolling_averages(raw_data)
    features_df = game_feature_creator.create_game_context_features(features_df)
    features_df = team_feature_creator.create_team_strength_features(features_df)
    
    # Return only the last row, which corresponds to the game we want to predict
    return features_df.tail(1)

def make_prediction(model, data):
    """Makes a prediction using the loaded model and input data."""
    if model is None:
        logging.error("Model is not loaded. Cannot make a prediction.")
        return None
    
    # Ensure data has the same columns as the training data
    # This is a simplified approach; a more robust solution would save/load column lists
    model_features = model.feature_name_
    
    # Check for missing columns and add them with a default value (e.g., 0)
    missing_cols = set(model_features) - set(data.columns)
    for c in missing_cols:
        data[c] = 0
        
    # Ensure the order of columns is the same as in the model
    data = data[model_features]

    logging.info("Making a prediction...")
    try:
        prediction = model.predict(data)
        prediction_proba = model.predict_proba(data)
        logging.info(f"Prediction: {'Over' if prediction[0] == 1 else 'Under'}")
        logging.info(f"Prediction probability: {prediction_proba[0]}")
        return prediction, prediction_proba
    except Exception as e:
        logging.error(f"An error occurred during prediction: {e}")
        return None

if __name__ == '__main__':
    # Example usage
    model = load_model()
    if model:
        # These would be the inputs for the prediction
        example_game_id = "401585609" # Example game
        example_player_id = "3947271" # Example player (e.g., Nikola Jokic)
        
        # 1. Fetch data
        raw_data = fetch_prediction_data(example_game_id, example_player_id)
        
        # 2. Engineer features
        features = engineer_features(raw_data)
        
        # 3. Make prediction
        make_prediction(model, features) 