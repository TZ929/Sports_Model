import logging
import pandas as pd
from typing import Dict, Any

logger = logging.getLogger(__name__)

class DataIntegrator:
    """Integrates different data sources into a cohesive dataset."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the DataIntegrator.
        
        Args:
            config: Configuration dictionary for integration parameters.
        """
        self.config = config

    def integrate_game_data(self, player_stats_df: pd.DataFrame, games_df: pd.DataFrame) -> pd.DataFrame:
        """
        Integrates player game stats with game-level information.
        
        Args:
            player_stats_df: DataFrame with player game stats.
            games_df: DataFrame with game details.
            
        Returns:
            A merged DataFrame with integrated data.
        """
        logger.info("Starting data integration...")
        
        if 'game_id' not in player_stats_df.columns or 'game_id' not in games_df.columns:
            logger.error("Integration failed: 'game_id' column missing in one of the DataFrames.")
            raise ValueError("Both DataFrames must contain a 'game_id' column.")
            
        # Merge player stats with game data
        merged_df = pd.merge(
            player_stats_df, 
            games_df, 
            on='game_id', 
            how=self.config.get('merge_strategy', 'left')
        )
        
        logger.info(f"Integration complete. Merged {len(merged_df)} records.")
        
        return merged_df

def main():
    """
    Main function to test the DataIntegrator.
    This is for demonstration purposes.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Example DataFrames
    player_stats_data = {
        'player_id': [1, 2, 1, 2],
        'game_id': ['g1', 'g1', 'g2', 'g2'],
        'points': [10, 15, 5, 22]
    }
    player_stats_df = pd.DataFrame(player_stats_data)

    games_data = {
        'game_id': ['g1', 'g2'],
        'date': ['2023-10-25', '2023-10-26'],
        'home_team': ['Team A', 'Team C'],
        'away_team': ['Team B', 'Team D']
    }
    games_df = pd.DataFrame(games_data)
    
    # Example config
    config = {'merge_strategy': 'left'}
    
    integrator = DataIntegrator(config)
    integrated_df = integrator.integrate_game_data(player_stats_df, games_df)
    
    logger.info("Integrated DataFrame:")
    logger.info(integrated_df.head())
    logger.info(integrated_df.info())

if __name__ == "__main__":
    main() 