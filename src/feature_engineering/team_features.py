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

class TeamFeatures:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.stats_to_average = self.config.get('stats_to_average', ['points_for', 'points_against'])
        self.rolling_windows = self.config.get('rolling_windows', [3, 5, 10])

    def create_team_strength_features(self, integrated_df: pd.DataFrame) -> pd.DataFrame:
        """Create team-level features."""
        logging.info("Creating team-level strength features...")

        if 'date' not in integrated_df.columns:
            logging.error("The 'date' column is missing from the input DataFrame.")
            return integrated_df

        # Ensure games are sorted by date
        games_df = integrated_df.sort_values(by='date')

        # Create a DataFrame with one row per team per game
        home_teams = games_df[['game_id', 'date', 'home_team_id', 'home_score', 'away_score']].rename(
            columns={'home_team_id': 'team_id', 'home_score': 'points_for', 'away_score': 'points_against'}
        )
        away_teams = games_df[['game_id', 'date', 'away_team_id', 'home_score', 'away_score']].rename(
            columns={'away_team_id': 'team_id', 'away_score': 'points_for', 'home_score': 'points_against'}
        )
        
        team_game_stats = pd.concat([home_teams, away_teams]).sort_values(by=['team_id', 'date'])
        
        # Calculate rolling averages for team stats
        for stat in self.stats_to_average:
            for window in self.rolling_windows:
                col_name = f'team_{stat}_roll_avg_{window}g'
                team_game_stats[col_name] = team_game_stats.groupby('team_id')[stat].transform(
                    lambda x: x.shift(1).rolling(window, min_periods=1).mean()
                )

        # Merge these team features back into the main DataFrame
        home_features = team_game_stats.copy()
        home_feature_cols = {col: f'home_{col}' for col in home_features.columns if col not in ['game_id', 'date']}
        home_features = home_features.rename(columns=home_feature_cols).rename(columns={'home_team_id': 'team_id'})

        away_features = team_game_stats.copy()
        away_feature_cols = {col: f'away_{col}' for col in away_features.columns if col not in ['game_id', 'date']}
        away_features = away_features.rename(columns=away_feature_cols).rename(columns={'away_team_id': 'team_id'})

        final_df = integrated_df.merge(home_features, left_on=['game_id', 'date', 'home_team_id'], right_on=['game_id', 'date', 'team_id'], how='left')
        final_df = final_df.merge(away_features, left_on=['game_id', 'date', 'away_team_id'], right_on=['game_id', 'date', 'team_id'], how='left')
        
        # Clean up redundant columns from merges
        final_df = final_df.drop(columns=['team_id_x', 'team_id_y'])

        logging.info("Finished creating team-level features.")
        return final_df

# Standalone execution logic
def load_master_dataset():
    """Load the master feature dataset."""
    logging.info("Loading master feature dataset...")
    processed_dir = Path("data/processed")
    try:
        df = pd.read_csv(processed_dir / "master_feature_dataset.csv", parse_dates=['date'])
        logging.info("Master feature dataset loaded successfully.")
        return df
    except FileNotFoundError as e:
        logging.error(f"Error loading master dataset: {e}. Please ensure previous steps ran successfully.")
        return None

if __name__ == '__main__':
    master_df = load_master_dataset()
    
    if master_df is not None:
        feature_creator = TeamFeatures(config={})
        team_features_df = feature_creator.create_team_strength_features(master_df)
        
        output_dir = Path("data/processed")
        team_features_df.to_csv(output_dir / "master_feature_dataset_v2.csv", index=False)
        logging.info(f"Master feature dataset with team features saved to {output_dir / 'master_feature_dataset_v2.csv'}") 