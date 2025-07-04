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

class GameFeatures:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def create_game_context_features(self, integrated_df: pd.DataFrame) -> pd.DataFrame:
        """Create game-level features like rest days."""
        logging.info("Creating game-level context features...")

        if 'date' not in integrated_df.columns:
            logging.error("The 'date' column is missing from the input DataFrame.")
            return integrated_df
            
        # Sort games by date to calculate rest days correctly
        games_sorted = integrated_df.sort_values(by='date')
        
        # Calculate rest days for each team
        all_games_with_rest = []
        # Need a unique list of all team IDs involved in the games
        team_ids = pd.unique(games_sorted[['home_team_id', 'away_team_id']].values.ravel('K'))

        for team_id in team_ids:
            team_games = games_sorted[(games_sorted['home_team_id'] == team_id) | (games_sorted['away_team_id'] == team_id)].copy()
            team_games = team_games.sort_values('date')

            # Ensure columns are datetime objects
            team_games['date'] = pd.to_datetime(team_games['date'])
            team_games['last_game_date'] = pd.to_datetime(team_games['date'].shift(1))
            
            team_games['rest_days'] = (team_games['date'] - team_games['last_game_date']).dt.days
            all_games_with_rest.append(team_games)

        if not all_games_with_rest:
            logging.warning("No games to process for feature engineering.")
            return integrated_df

        rest_days_df = pd.concat(all_games_with_rest).drop_duplicates(subset=['game_id', 'player_id'])
        
        # Assign rest days to home and away teams
        home_rest = rest_days_df[rest_days_df['home_team_id'] == rest_days_df['team_id']][['game_id', 'rest_days']].rename(columns={'rest_days': 'home_rest_days'})
        away_rest = rest_days_df[rest_days_df['away_team_id'] == rest_days_df['team_id']][['game_id', 'rest_days']].rename(columns={'rest_days': 'away_rest_days'})
        
        # Merge back into the original DataFrame
        final_df = integrated_df.merge(home_rest, on='game_id', how='left')
        final_df = final_df.merge(away_rest, on='game_id', how='left')

        logging.info("Finished creating game-level features.")
        return final_df

# Standalone execution logic
def load_cleaned_data():
    """Load cleaned data from CSV files."""
    logging.info("Loading cleaned data from CSV files...")
    processed_dir = Path("data/processed")
    
    try:
        teams_df = pd.read_csv(processed_dir / "teams_cleaned.csv")
        games_df = pd.read_csv(processed_dir / "games_cleaned.csv", parse_dates=['date'])
        logging.info("Cleaned data loaded successfully.")
        return teams_df, games_df
    except FileNotFoundError as e:
        logging.error(f"Error loading cleaned data: {e}. Please run the data cleaner script first.")
        return None, None

if __name__ == '__main__':
    teams, games = load_cleaned_data()
    if games is not None and teams is not None:
        # For standalone run, we don't have an "integrated_df", so we work with games_df
        feature_creator = GameFeatures(config={})
        game_features_df = feature_creator.create_game_context_features(games.assign(player_id=0, team_id=games['home_team_id'])) # Dummy columns for compatibility
        
        output_dir = Path("data/processed")
        output_dir.mkdir(parents=True, exist_ok=True)
        game_features_df.to_csv(output_dir / "game_features.csv", index=False)
        logging.info(f"Game features saved to {output_dir / 'game_features.csv'}") 