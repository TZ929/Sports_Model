import logging
import pandas as pd
from typing import Dict, Any

logger = logging.getLogger(__name__)

class GameFeatures:
    """Creates game-context features for modeling."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the GameFeatures class.
        
        Args:
            config: Configuration dictionary for feature engineering parameters.
        """
        self.config = config

    def create_game_context_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates features related to the context of the game.
        
        Args:
            df: DataFrame with integrated game and player data.
            
        Returns:
            DataFrame with added game context features.
        """
        logger.info("Creating game context features...")

        # 1. Add home/away flag
        df = self._add_home_away_flag(df)
        
        # 2. Calculate rest days
        df = self._calculate_rest_days(df)

        logger.info("Game context features created successfully.")
        return df

    def _add_home_away_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds a flag indicating if the player's team is home or away."""
        if 'team_id' in df.columns and 'home_team_id' in df.columns:
            df['is_home'] = (df['team_id'] == df['home_team_id']).astype(int)
            logger.info("Added 'is_home' feature.")
        else:
            logger.warning("Could not create 'is_home' feature. Missing 'team_id' or 'home_team_id'.")
        return df

    def _calculate_rest_days(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculates the number of rest days for a team before a game."""
        if 'date' in df.columns and 'team_id' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values(by=['team_id', 'date'])
            df['rest_days'] = df.groupby('team_id')['date'].diff().dt.days.fillna(0)
            # Cap rest days at a reasonable number, e.g., 7
            df['rest_days'] = df['rest_days'].clip(upper=self.config.get('max_rest_days', 7))
            logger.info("Added 'rest_days' feature.")
        else:
            logger.warning("Could not create 'rest_days' feature. Missing 'date' or 'team_id'.")
        return df

def main():
    """
    Main function to test the GameFeatures class.
    This is for demonstration purposes.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Example DataFrame
    data = {
        'game_id': ['g1', 'g1', 'g2', 'g2'],
        'player_id': [1, 2, 1, 2],
        'team_id': ['t1', 't2', 't1', 't2'],
        'home_team_id': ['t1', 't1', 't3', 't3'],
        'date': pd.to_datetime(['2023-10-25', '2023-10-25', '2023-10-28', '2023-10-28'])
    }
    df = pd.DataFrame(data)
    
    config = {'max_rest_days': 7}
    
    feature_creator = GameFeatures(config)
    features_df = feature_creator.create_game_context_features(df)
    
    logger.info("DataFrame with game context features:")
    logger.info(features_df)

if __name__ == "__main__":
    main() 