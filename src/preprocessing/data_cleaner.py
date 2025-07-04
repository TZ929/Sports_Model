import logging
import pandas as pd
from typing import Dict, Any

logger = logging.getLogger(__name__)

class DataCleaner:
    """Cleans and prepares data for feature engineering."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the DataCleaner.
        
        Args:
            config: Configuration dictionary for cleaning parameters.
        """
        self.config = config

    def clean_player_game_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the player game stats DataFrame.
        
        Args:
            df: DataFrame with raw player game stats.
            
        Returns:
            Cleaned DataFrame.
        """
        logger.info("Starting player game stats cleaning...")
        
        # 1. Handle missing values
        # Example: Fill missing numerical stats with 0
        numeric_cols = df.select_dtypes(include=['number']).columns
        df[numeric_cols] = df[numeric_cols].fillna(0)
        logger.info(f"Filled missing values in {len(numeric_cols)} numeric columns.")
        
        # 2. Convert data types
        # Example: Ensure categorical features are strings
        categorical_cols = ['player_id', 'game_id', 'team_id']
        for col in categorical_cols:
            if col in df.columns:
                df[col] = df[col].astype(str)
        logger.info(f"Converted {categorical_cols} to string type.")

        # 3. Handle outliers (optional, based on config)
        if self.config.get('clean_outliers', False):
            df = self._remove_outliers(df)
            
        logger.info("Player game stats cleaning complete.")
        return df

    def _remove_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Removes outliers from the DataFrame based on specified columns.
        """
        # Example outlier removal logic (e.g., using IQR)
        # This is a placeholder for a more sophisticated implementation.
        logger.warning("Outlier removal is not yet implemented.")
        return df

def main():
    """
    Main function to test the DataCleaner.
    This is for demonstration purposes.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Example DataFrame
    data = {
        'player_id': [1, 2, 3, 4],
        'game_id': ['g1', 'g1', 'g2', 'g2'],
        'team_id': ['t1', 't2', 't1', 't2'],
        'points': [10, 15, None, 22],
        'rebounds': [5, None, 8, 4]
    }
    df = pd.DataFrame(data)
    
    # Example config
    config = {'clean_outliers': True}
    
    cleaner = DataCleaner(config)
    cleaned_df = cleaner.clean_player_game_stats(df)
    
    logger.info("Original DataFrame:")
    logger.info(df.head())
    
    logger.info("Cleaned DataFrame:")
    logger.info(cleaned_df.head())
    logger.info(cleaned_df.info())

if __name__ == "__main__":
    main() 