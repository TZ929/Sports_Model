import pandas as pd
import logging
from pathlib import Path

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

def create_team_features(games_df):
    """Create team-level features."""
    logging.info("Creating team-level features...")

    # Ensure games are sorted by date
    games_df = games_df.sort_values(by='date')

    # Create a DataFrame with one row per team per game
    home_teams = games_df[['game_id', 'date', 'home_team_id', 'home_score', 'away_score']].rename(
        columns={'home_team_id': 'team_id', 'home_score': 'points_for', 'away_score': 'points_against'}
    )
    away_teams = games_df[['game_id', 'date', 'away_team_id', 'home_score', 'away_score']].rename(
        columns={'away_team_id': 'team_id', 'away_score': 'points_for', 'home_score': 'points_against'}
    )
    
    team_game_stats = pd.concat([home_teams, away_teams]).sort_values(by=['team_id', 'date'])
    
    # Calculate rolling averages for team stats
    stats_to_average = ['points_for', 'points_against']
    rolling_windows = [3, 5, 10]

    for stat in stats_to_average:
        for window in rolling_windows:
            col_name = f'team_{stat}_roll_avg_{window}g'
            team_game_stats[col_name] = team_game_stats.groupby('team_id')[stat].transform(
                lambda x: x.shift(1).rolling(window, min_periods=1).mean()
            )

    logging.info("Finished creating team-level features.")
    return team_game_stats

if __name__ == '__main__':
    master_df = load_master_dataset()
    
    if master_df is not None:
        # Create a unique games dataframe to avoid duplicate calculations
        games_unique_df = master_df[['game_id', 'date', 'home_team_id', 'away_team_id', 'home_score', 'away_score']].drop_duplicates()
        team_features_df = create_team_features(games_unique_df)
        
        # Merge team features back into the master dataset
        # Need to merge for both home and away teams
        
        home_features = team_features_df.rename(columns=lambda x: f'home_{x}' if x not in ['game_id', 'date'] else x)
        away_features = team_features_df.rename(columns=lambda x: f'away_{x}' if x not in ['game_id', 'date'] else x)

        final_df = pd.merge(master_df, home_features, on=['game_id', 'date'], suffixes=('', '_home_y'))
        final_df = pd.merge(final_df, away_features, on=['game_id', 'date'], suffixes=('', '_away_y'))

        # Clean up columns from merge
        final_df = final_df.loc[:, ~final_df.columns.str.endswith('_y')]
        
        # Save final dataset
        output_dir = Path("data/processed")
        final_df.to_csv(output_dir / "master_feature_dataset_v2.csv", index=False)
        logging.info(f"Master feature dataset with team features saved to {output_dir / 'master_feature_dataset_v2.csv'}") 