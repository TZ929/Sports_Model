import pandas as pd
import logging
from pathlib import Path

# Setup logging (can be shared across modules)
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

def create_player_features(player_stats_df, games_df):
    """Create player-level features, like rolling averages."""
    logging.info("Creating player-level features...")
    
    # Merge stats with games to get dates
    player_game_stats = pd.merge(player_stats_df, games_df[['game_id', 'date']], on='game_id')
    player_game_stats = player_game_stats.sort_values(by=['player_id', 'date'])
    
    # Calculate rolling averages
    stats_to_average = ['points', 'rebounds', 'assists', 'steals', 'blocks', 'turnovers']
    rolling_windows = [3, 5, 10]
    
    for stat in stats_to_average:
        for window in rolling_windows:
            col_name = f'{stat}_roll_avg_{window}g'
            player_game_stats[col_name] = player_game_stats.groupby('player_id')[stat].transform(
                lambda x: x.shift(1).rolling(window, min_periods=1).mean()
            )
            
    logging.info("Finished creating player-level features.")
    logging.info(f"\nSample of player features:\n{player_game_stats.head().to_string()}")
    
    return player_game_stats


if __name__ == '__main__':
    players, player_stats, game_features = load_processed_data()
    
    if player_stats is not None and game_features is not None:
        player_features_df = create_player_features(player_stats, game_features)
        
        # Merge with game features
        final_df = pd.merge(game_features, player_features_df, on=['game_id', 'date'], how='left')
        
        # Save final dataset
        output_dir = Path("data/processed")
        final_df.to_csv(output_dir / "master_feature_dataset.csv", index=False)
        logging.info(f"Master feature dataset saved to {output_dir / 'master_feature_dataset.csv'}") 