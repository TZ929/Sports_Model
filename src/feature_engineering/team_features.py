import logging
import pandas as pd
from typing import Dict, Any

logger = logging.getLogger(__name__)

class TeamFeatures:
    """Creates team-level features for modeling."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the TeamFeatures class.
        
        Args:
            config: Configuration dictionary for feature engineering parameters.
        """
        self.config = config

    def create_team_strength_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates features related to team strength and recent form.
        
        This is a placeholder for more complex calculations like ELO ratings
        or team-based rolling averages.
        
        Args:
            df: DataFrame with integrated game and team data.
            
        Returns:
            DataFrame with added team strength features.
        """
        logger.info("Creating team strength features...")

        # Placeholder: a simple example of a team feature.
        # This could be expanded to calculate rolling win percentages, point differentials, etc.
        if 'is_win' in df.columns and 'team_id' in df.columns:
            df = df.sort_values(by=['team_id', 'date'])
            df['team_win_rate_rolling'] = df.groupby('team_id')['is_win'].transform(
                lambda x: x.rolling(window=self.config.get('team_form_window', 10), min_periods=1).mean()
            )
            logger.info("Created 'team_win_rate_rolling' feature.")
        else:
            logger.warning("Could not create 'team_win_rate_rolling'. Missing 'is_win' or 'team_id'.")
            
        logger.info("Team strength feature creation complete.")
        return df

def main():
    """
    Main function to test the TeamFeatures class.
    This is for demonstration purposes.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Example DataFrame
    data = {
        'game_id': ['g1', 'g2', 'g3', 'g4'],
        'team_id': ['t1', 't1', 't1', 't1'],
        'date': pd.to_datetime(['2023-10-25', '2023-10-27', '2023-10-29', '2023-10-31']),
        'is_win': [1, 0, 1, 1]
    }
    df = pd.DataFrame(data)
    
    config = {'team_form_window': 3}
    
    feature_creator = TeamFeatures(config)
    features_df = feature_creator.create_team_strength_features(df)
    
    logger.info("DataFrame with team features:")
    logger.info(features_df)

if __name__ == "__main__":
    main() 