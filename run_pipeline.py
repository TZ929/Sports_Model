import logging
import pandas as pd
import sys
from pathlib import Path

# Add project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[0]))

from src.utils.database import db_manager
from src.preprocessing.data_cleaner import DataCleaner
from src.preprocessing.data_validator import DataValidator
from src.preprocessing.data_integrator import DataIntegrator
from src.feature_engineering.player_features import PlayerFeatures
from src.feature_engineering.game_features import GameFeatures
from src.feature_engineering.team_features import TeamFeatures
from src.data_collection.sportsbook_scraper import scrape_all_sportsbooks
from src.utils.config import config
from sqlalchemy import text
from typing import Dict, List, Any

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

def integrate_sportsbook_odds(features_df: pd.DataFrame, odds_data: Dict[str, List[Dict[str, Any]]]) -> pd.DataFrame:
    """
    Integrates sportsbook odds into the features DataFrame.
    This is a placeholder and needs a robust implementation to match players and games.
    """
    logger.info("Integrating sportsbook odds...")
    # This is a complex task. For now, we will just log a message.
    # A real implementation would require matching player names, teams, and game dates
    # between our database and the scraped data. It would also involve handling
    # different prop bet types (e.g., points, rebounds).
    
    # For now, let's assume we can extract a 'points_over_under_odds' column.
    # This is a placeholder for the real logic.
    if not features_df.empty:
        features_df['fanduel_points_over_odds'] = -110  # Placeholder value
        features_df['fanduel_points_under_odds'] = -110 # Placeholder value

    logger.info("Sportsbook odds integration placeholder complete.")
    return features_df

def run_pipeline():
    """
    Executes the full data processing and feature engineering pipeline.
    """
    logger.info("Starting data pipeline...")

    # 1. Scrape Sportsbook Odds
    odds_data = scrape_all_sportsbooks()

    # 2. Load data
    dataframes = load_data_from_db()
    
    # 3. Preprocessing
    cleaner = DataCleaner(config.get('preprocessing.cleaning', {}))
    validator = DataValidator(config.get('preprocessing.validation', {}))
    integrator = DataIntegrator(config.get('preprocessing.integration', {}))

    cleaned_stats = cleaner.clean_player_game_stats(dataframes['player_stats'])
    if not validator.validate_player_game_stats(cleaned_stats):
        logger.error("Data validation failed. Halting pipeline.")
        return

    integrated_df = integrator.integrate_game_data(cleaned_stats, dataframes['games'])
    
    # 4. Feature Engineering
    player_feature_creator = PlayerFeatures(config.get('feature_engineering.player_features', {}))
    game_feature_creator = GameFeatures(config.get('feature_engineering.game_features', {}))
    team_feature_creator = TeamFeatures(config.get('feature_engineering.team_features', {}))

    features_df = player_feature_creator.create_rolling_averages(integrated_df)
    features_df = game_feature_creator.create_game_context_features(features_df)
    features_df = team_feature_creator.create_team_strength_features(features_df)

    # 5. Integrate Sportsbook Odds
    features_df = integrate_sportsbook_odds(features_df, odds_data)

    # 6. Save processed data
    output_path = config.get_data_path('processed') / 'featured_data.csv'
    features_df.to_csv(output_path, index=False)
    logger.info(f"Pipeline complete. Processed data saved to {output_path}")

if __name__ == "__main__":
    # This script assumes the database is populated.
    # The data collection script should be run before this.
    run_pipeline() 