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
from typing import Dict, List, Any, Optional
from fuzzywuzzy import process, fuzz

def find_best_match(name_to_match: str, candidates: Dict[str, str], scorer, cutoff=75) -> Optional[str]:
    """Find the best match for a name from a list of candidates."""
    best_match = process.extractOne(name_to_match, candidates.keys(), scorer=scorer, score_cutoff=cutoff)
    if best_match:
        logger.info(f"Matched '{name_to_match}' to '{best_match[0]}' with score {best_match[1]}")
        return candidates[best_match[0]]
    return None

def get_mock_odds_data() -> Dict[str, List[Dict[str, Any]]]:
    """Returns a mock dictionary of sportsbook odds for development."""
    logger.info("Using mock sportsbook odds data.")
    return {
        'fanduel': [
            {
                'player_name': 'Nikola Jokic',
                'market_type': 'Points',
                'line': 26.5,
                'over_odds': -115,
                'under_odds': -105,
                'game_info': {'home_team': 'Denver Nuggets', 'away_team': 'Boston Celtics'}
            },
            {
                'player_name': 'Jayson Tatum',
                'market_type': 'Points',
                'line': 28.5,
                'over_odds': -110,
                'under_odds': -110,
                'game_info': {'home_team': 'Denver Nuggets', 'away_team': 'Boston Celtics'}
            }
        ],
        'espnbet': [] # Not yet implemented
    }

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data_from_db() -> Dict[str, pd.DataFrame]:
    """Loads all necessary tables from the database into pandas DataFrames."""
    logger.info("Loading data from database...")
    with db_manager.get_session() as session:
        games_df = pd.read_sql(text("SELECT * FROM games"), session.bind)
        stats_df = pd.read_sql(text("SELECT * FROM player_game_stats"), session.bind)
        players_df = pd.read_sql(text("SELECT * FROM players"), session.bind)
        teams_df = pd.read_sql(text("SELECT * FROM teams"), session.bind)
    logger.info(f"Loaded {len(games_df)} games, {len(stats_df)} player stats, {len(players_df)} players, and {len(teams_df)} teams.")
    return {'games': games_df, 'player_stats': stats_df, 'players': players_df, 'teams': teams_df}

def integrate_sportsbook_odds(features_df: pd.DataFrame, odds_data: Dict[str, List[Dict[str, Any]]], dataframes: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Integrates sportsbook odds into the features DataFrame using fuzzy matching.
    """
    logger.info("Integrating sportsbook odds...")

    # Prepare candidate names from our database for matching
    player_names = {str(p['full_name']): str(p['player_id']) for _, p in dataframes['players'].iterrows()}
    team_names = {str(t['team_name']): str(t['team_id']) for _, t in dataframes['teams'].iterrows()}

    # Initialize odds columns
    features_df['fanduel_points_line'] = None
    features_df['fanduel_points_over_odds'] = None
    features_df['fanduel_points_under_odds'] = None

    fanduel_props = odds_data.get('fanduel', [])
    for prop in fanduel_props:
        player_name = prop.get('player_name')
        if not player_name or prop.get('market_type') != 'Points':
            continue

        matched_player_id = find_best_match(player_name, player_names, scorer=fuzz.token_sort_ratio)
        if not matched_player_id:
            logger.warning(f"Could not find a good match for player: {player_name}")
            continue
            
        # This matching logic for games is simplified. A robust solution would also match teams.
        # For now, we assume the prop is for a game already in our features_df for that player.
        prop_rows = features_df[features_df['player_id'] == matched_player_id]
        
        if not prop_rows.empty:
            # Apply the odds to all games we have for this player (a simplification)
            features_df.loc[prop_rows.index, 'fanduel_points_line'] = prop.get('line')
            features_df.loc[prop_rows.index, 'fanduel_points_over_odds'] = prop.get('over_odds')
            features_df.loc[prop_rows.index, 'fanduel_points_under_odds'] = prop.get('under_odds')

    logger.info("Sportsbook odds integration complete.")
    return features_df

def run_pipeline():
    """
    Executes the full data processing and feature engineering pipeline.
    """
    logger.info("Starting data pipeline...")

    # 1. Scrape Sportsbook Odds (or use mock data)
    # In a production system, you might pass a specific date.
    # odds_data = scrape_all_sportsbooks()
    odds_data = get_mock_odds_data()

    # 2. Load data from DB
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
    features_df = integrate_sportsbook_odds(features_df, odds_data, dataframes)

    # 6. Save processed data
    output_path = config.get_data_path('processed') / 'featured_data.csv'
    features_df.to_csv(output_path, index=False)
    logger.info(f"Pipeline complete. Processed data saved to {output_path}")

if __name__ == "__main__":
    # This script assumes the database is populated.
    # The data collection script should be run before this.
    run_pipeline() 