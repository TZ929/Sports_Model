import logging
import pandas as pd
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class PlayerFeatures:
    """Creates player-centric features for modeling."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the PlayerFeatures class.
        
        Args:
            config: Configuration dictionary for feature engineering parameters.
        """
        self.config = config

    def create_rolling_averages(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates rolling average features for player stats.
        
        Args:
            df: DataFrame with player game stats.
            
        Returns:
            DataFrame with added rolling average features.
        """
        logger.info("Creating rolling average features...")
        
        rolling_windows = self.config.get('rolling_windows', [])
        stats_to_average = self.config.get('stats_to_average', [])
        
        if not rolling_windows or not stats_to_average:
            logger.warning("Rolling windows or stats to average not configured. Skipping.")
            return df
            
        # Ensure data is sorted by date for rolling calculations
        df = df.sort_values(by=['player_id', 'date'])
        
        for window in rolling_windows:
            for stat in stats_to_average:
                if stat in df.columns:
                    col_name = f'{stat}_rolling_avg_{window}'
                    # Group by player, then calculate rolling average
                    df[col_name] = df.groupby('player_id')[stat].transform(
                        lambda x: x.rolling(window=window, min_periods=1).mean()
                    )
        
        logger.info(f"Created rolling averages for {stats_to_average} with windows {rolling_windows}.")
        return df

def main():
    """
    Main function to test the PlayerFeatures class.
    This is for demonstration purposes.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Example DataFrame
    data = {
        'player_id': [1, 1, 1, 1, 2, 2, 2, 2],
        'date': pd.to_datetime(['2023-10-25', '2023-10-26', '2023-10-28', '2023-10-30', 
                                '2023-10-25', '2023-10-27', '2023-10-29', '2023-10-31']),
        'points': [10, 15, 12, 18, 20, 22, 25, 28],
        'rebounds': [5, 7, 6, 8, 10, 11, 12, 13]
    }
    df = pd.DataFrame(data)
    
    # Example feature engineering config
    config = {
        'rolling_windows': [2, 4],
        'stats_to_average': ['points', 'rebounds']
    }
    
    feature_creator = PlayerFeatures(config)
    features_df = feature_creator.create_rolling_averages(df)
    
    logger.info("DataFrame with player features:")
    logger.info(features_df)

if __name__ == "__main__":
    main() 