import logging
import pandas as pd
from src.utils.database import db_manager
from src.preprocessing.data_cleaner import DataCleaner
from src.preprocessing.data_validator import DataValidator
from src.preprocessing.data_integrator import DataIntegrator
from src.feature_engineering.player_features import PlayerFeatures
from src.feature_engineering.game_features import GameFeatures
from src.feature_engineering.team_features import TeamFeatures
from src.utils.config import config
from sqlalchemy import text
from typing import Dict

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data_from_db() -> Dict[str, pd.DataFrame]:
    """Loads all necessary tables from the database into pandas DataFrames."""
    logger.info("Loading data from database...")
    with db_manager.get_session() as session:
        games_df = pd.read_sql(text("SELECT * FROM games"), session.bind)
        stats_df = pd.read_sql(text("SELECT * FROM player_game_stats"), session.bind)
        # Add other tables if needed
    logger.info(f"Loaded {len(games_df)} games and {len(stats_df)} player stats records.")
    return {'games': games_df, 'player_stats': stats_df}

def run_pipeline():
    """
    Executes the full data processing and feature engineering pipeline.
    """
    logger.info("Starting data pipeline...")

    # 1. Load data
    dataframes = load_data_from_db()
    
    # 2. Preprocessing
    cleaner = DataCleaner(config.get('preprocessing.cleaning', {}))
    validator = DataValidator(config.get('preprocessing.validation', {}))
    integrator = DataIntegrator(config.get('preprocessing.integration', {}))

    cleaned_stats = cleaner.clean_player_game_stats(dataframes['player_stats'])
    if not validator.validate_player_game_stats(cleaned_stats):
        logger.error("Data validation failed. Halting pipeline.")
        return

    integrated_df = integrator.integrate_game_data(cleaned_stats, dataframes['games'])
    
    # 3. Feature Engineering
    player_feature_creator = PlayerFeatures(config.get('feature_engineering.player_features', {}))
    game_feature_creator = GameFeatures(config.get('feature_engineering.game_features', {}))
    team_feature_creator = TeamFeatures(config.get('feature_engineering.team_features', {}))

    features_df = player_feature_creator.create_rolling_averages(integrated_df)
    features_df = game_feature_creator.create_game_context_features(features_df)
    features_df = team_feature_creator.create_team_strength_features(features_df)

    # 4. Save processed data
    output_path = config.get_data_path('processed') / 'featured_data.csv'
    features_df.to_csv(output_path, index=False)
    logger.info(f"Pipeline complete. Processed data saved to {output_path}")

if __name__ == "__main__":
    # This script assumes the database is populated.
    # The data collection script should be run before this.
    run_pipeline() 