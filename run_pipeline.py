import logging
import pandas as pd
import sys
from pathlib import Path
import os

# Add project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[0]))

from src.utils.database import db_manager, PropOdds
from src.preprocessing.data_cleaner import DataCleaner
from src.preprocessing.data_validator import DataValidator
from src.preprocessing.data_integrator import DataIntegrator
from src.feature_engineering.player_features import PlayerFeatures
from src.feature_engineering.game_features import GameFeatures
from src.feature_engineering.team_features import TeamFeatures
from src.data_collection.sports_game_odds_api import SportsGameOddsAPICollector
from src.utils.config import config
from sqlalchemy import text, func
from typing import Dict, List, Any, Optional

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_historical_odds_collection():
    """Checks if historical odds need to be collected and runs the collector if so."""
    with db_manager.get_session() as session:
        prop_count = session.query(func.count(PropOdds.game_id)).scalar()
    
    if prop_count == 0:
        logger.info("Prop odds table is empty. Running historical odds collector...")
        api_key = os.environ.get('SPORTS_GAME_ODDS_API_KEY')
        if not api_key:
            logger.error("SPORTS_GAME_ODDS_API_KEY environment variable not set. Cannot collect historical odds.")
            return
        
        collector = SportsGameOddsAPICollector(api_key=api_key)
        # Fetch for the entire 2023-2024 season
        collector.collect_and_store_odds(start_date="2023-10-24", end_date="2024-04-14")
    else:
        logger.info("Prop odds table already contains data. Skipping historical collection.")

def load_data_from_db() -> Dict[str, pd.DataFrame]:
    """Loads all necessary tables from the database into pandas DataFrames."""
    logger.info("Loading data from database...")
    with db_manager.get_session() as session:
        games_df = pd.read_sql(text("SELECT * FROM games"), session.bind)
        stats_df = pd.read_sql(text("SELECT * FROM player_game_stats"), session.bind)
        players_df = pd.read_sql(text("SELECT * FROM players"), session.bind)
        teams_df = pd.read_sql(text("SELECT * FROM teams"), session.bind)
        odds_df = pd.read_sql(text("SELECT * FROM prop_odds"), session.bind)
    logger.info(f"Loaded {len(games_df)} games, {len(stats_df)} player stats, {len(players_df)} players, {len(teams_df)} teams, and {len(odds_df)} prop odds.")
    return {'games': games_df, 'player_stats': stats_df, 'players': players_df, 'teams': teams_df, 'prop_odds': odds_df}

def integrate_sportsbook_odds(features_df: pd.DataFrame, odds_df: pd.DataFrame) -> pd.DataFrame:
    """
    Integrates historical sportsbook odds into the features DataFrame.
    """
    logger.info("Integrating historical sportsbook odds...")
    if odds_df.empty:
        logger.warning("Odds DataFrame is empty. Skipping integration.")
        # Still need to create the columns for the model
        features_df['fanduel_points_line'] = 0
        features_df['fanduel_points_over_odds'] = -110
        features_df['fanduel_points_under_odds'] = -110
        return features_df

    # We will just use FanDuel for now for simplicity
    fanduel_odds = odds_df[odds_df['sportsbook'] == 'FanDuel'].copy()
    fanduel_odds = fanduel_odds.rename(columns={
        'line': 'fanduel_points_line',
        'over_odds': 'fanduel_points_over_odds',
        'under_odds': 'fanduel_points_under_odds'
    })

    # Select relevant columns and merge
    odds_to_merge = fanduel_odds[['game_id', 'player_id', 'fanduel_points_line', 'fanduel_points_over_odds', 'fanduel_points_under_odds']]
    
    # Need to make sure player_id types match for merging
    features_df['player_id'] = features_df['player_id'].astype(str)
    odds_to_merge['player_id'] = odds_to_merge['player_id'].astype(str)
    
    merged_df = pd.merge(features_df, odds_to_merge, on=['game_id', 'player_id'], how='left')

    logger.info("Sportsbook odds integration complete.")
    return merged_df

def run_pipeline():
    """
    Executes the full data processing and feature engineering pipeline.
    """
    logger.info("Starting data pipeline...")
    
    # 1. Collect historical odds if needed
    run_historical_odds_collection()

    # 2. Load data from DB (now including odds)
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
    features_df = integrate_sportsbook_odds(features_df, dataframes['prop_odds'])

    # 6. Save processed data
    output_path = config.get_data_path('processed') / 'featured_data.csv'
    features_df.to_csv(output_path, index=False)
    logger.info(f"Pipeline complete. Processed data saved to {output_path}")

if __name__ == "__main__":
    run_pipeline() 