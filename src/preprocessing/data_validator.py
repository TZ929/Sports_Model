import logging
import pandas as pd
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class DataValidator:
    """Validates the integrity and quality of the data."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the DataValidator.
        
        Args:
            config: Configuration dictionary for validation rules.
        """
        self.config = config

    def validate_player_game_stats(self, df: pd.DataFrame) -> bool:
        """
        Validates the player game stats DataFrame.
        
        Args:
            df: DataFrame with cleaned player game stats.
            
        Returns:
            True if validation passes, False otherwise.
        """
        logger.info("Starting player game stats validation...")
        
        # 1. Check for required columns
        required_cols = self.config.get('required_columns', [])
        if not self._check_required_columns(df, required_cols):
            return False
        
        # 2. Check data ranges
        range_checks = self.config.get('range_checks', {})
        if not self._check_data_ranges(df, range_checks):
            return False
            
        # 3. Check for uniqueness constraints
        unique_constraints = self.config.get('unique_constraints', [])
        if not self._check_uniqueness(df, unique_constraints):
            return False

        logger.info("Player game stats validation successful.")
        return True

    def _check_required_columns(self, df: pd.DataFrame, required_cols: List[str]) -> bool:
        """Checks if all required columns are present."""
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Validation failed: Missing required columns: {missing_cols}")
            return False
        logger.info("Required columns check passed.")
        return True

    def _check_data_ranges(self, df: pd.DataFrame, range_checks: Dict[str, Dict[str, float]]) -> bool:
        """Checks if data values are within specified ranges."""
        for col, ranges in range_checks.items():
            if col in df.columns:
                min_val = ranges.get('min', -float('inf'))
                max_val = ranges.get('max', float('inf'))
                if not df[col].between(min_val, max_val).all():
                    logger.error(f"Validation failed: Column '{col}' has values out of range [{min_val}, {max_val}].")
                    return False
        logger.info("Data range checks passed.")
        return True
        
    def _check_uniqueness(self, df: pd.DataFrame, unique_constraints: List[List[str]]) -> bool:
        """Checks for uniqueness on a set of columns."""
        for cols in unique_constraints:
            if df.duplicated(subset=cols).any():
                logger.error(f"Validation failed: Duplicates found for columns {cols}")
                return False
        logger.info("Uniqueness checks passed.")
        return True

def main():
    """
    Main function to test the DataValidator.
    This is for demonstration purposes.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Example DataFrame
    data = {
        'player_id': [1, 2, 1, 2],
        'game_id': ['g1', 'g1', 'g2', 'g2'],
        'points': [10, 15, 5, 22],
        'minutes_played': [25, 30, 28, -5] # Invalid minute
    }
    df = pd.DataFrame(data)
    
    # Example validation config
    config = {
        'required_columns': ['player_id', 'game_id', 'points'],
        'range_checks': {
            'points': {'min': 0, 'max': 100},
            'minutes_played': {'min': 0, 'max': 100}
        },
        'unique_constraints': [['player_id', 'game_id']]
    }
    
    validator = DataValidator(config)
    
    logger.info("Validating DataFrame...")
    is_valid = validator.validate_player_game_stats(df)
    
    logger.info(f"Validation result: {'Success' if is_valid else 'Failed'}")
    
if __name__ == "__main__":
    main() 