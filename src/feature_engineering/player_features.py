import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Any

# Setup logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "feature_engineering.log"),
        logging.StreamHandler()
    ]
)

class PlayerFeatures:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.stats_to_average = self.config.get('stats_to_average', 
            ['points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers'])
        self.rolling_windows = self.config.get('rolling_windows', [3, 5, 10])

    def create_rolling_averages(self, integrated_df: pd.DataFrame) -> pd.DataFrame:
        """Create player-level features, like rolling averages."""
        logging.info("Creating player-level rolling average features...")
        
        if 'date' not in integrated_df.columns:
            logging.error("The 'date' column is missing from the input DataFrame.")
            return integrated_df
            
        player_game_stats = integrated_df.sort_values(by=['player_id', 'date'])
        
        for stat in self.stats_to_average:
            for window in self.rolling_windows:
                col_name = f'{stat}_roll_avg_{window}g'
                player_game_stats[col_name] = player_game_stats.groupby('player_id')[stat].transform(
                    lambda x: x.shift(1).rolling(window, min_periods=1).mean()
                )
                
        logging.info("Finished creating player-level features.")
        return player_game_stats

# The following functions are for standalone execution of this script
def load_processed_data():
    """Load processed data from CSV files."""
    logging.info("Loading processed data from CSV files...")
    processed_dir = Path("data/processed")
    
    try:
        players_df = pd.read_csv(processed_dir / "players_cleaned.csv")
        player_stats_df = pd.read_csv(processed_dir / "player_stats_cleaned.csv")
        game_features_df = pd.read_csv(processed_dir / "game_features.csv", parse_dates=['date'])
        logging.info("Processed data loaded successfully.")
        return players_df, player_stats_df, game_features_df
    except FileNotFoundError as e:
        logging.error(f"Error loading processed data: {e}. Please ensure previous steps ran successfully.")
        return None, None, None

if __name__ == '__main__':
    players, player_stats, game_features = load_processed_data()
    
    if player_stats is not None and game_features is not None:
        # For standalone run, we merge stats and games first
        integrated_data = pd.merge(player_stats, game_features, on='game_id')
        
        feature_creator = PlayerFeatures(config={})
        player_features_df = feature_creator.create_rolling_averages(integrated_data)
        
        # Save final dataset
        output_dir = Path("data/processed")
        player_features_df.to_csv(output_dir / "master_player_features.csv", index=False)
        logging.info(f"Master player features dataset saved to {output_dir / 'master_player_features.csv'}") 