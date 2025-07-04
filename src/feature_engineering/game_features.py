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

def load_cleaned_data():
    """Load cleaned data from CSV files."""
    logging.info("Loading cleaned data from CSV files...")
    processed_dir = Path("data/processed")
    
    try:
        teams_df = pd.read_csv(processed_dir / "teams_cleaned.csv")
        players_df = pd.read_csv(processed_dir / "players_cleaned.csv")
        games_df = pd.read_csv(processed_dir / "games_cleaned.csv", parse_dates=['date'])
        player_stats_df = pd.read_csv(processed_dir / "player_stats_cleaned.csv")
        logging.info("Cleaned data loaded successfully.")
        return teams_df, players_df, games_df, player_stats_df
    except FileNotFoundError as e:
        logging.error(f"Error loading cleaned data: {e}. Please run the data cleaner script first.")
        return None, None, None, None

def create_game_features(games_df, teams_df):
    """Create game-level features."""
    logging.info("Creating game-level features...")
    
    # Sort games by date for each team
    games_df = games_df.sort_values(by='date')
    
    # Calculate rest days
    all_games = []
    for team_id in teams_df['team_id']:
        team_games = games_df[(games_df['home_team_id'] == team_id) | (games_df['away_team_id'] == team_id)].copy()
        team_games.loc[:, 'last_game_date'] = team_games['date'].shift(1)
        team_games.loc[:, 'rest_days'] = (team_games['date'] - team_games['last_game_date']).dt.days
        all_games.append(team_games)
        
    if not all_games:
        logging.warning("No games found to engineer features for.")
        return pd.DataFrame()

    # Concatenate and drop duplicates
    featured_games_df = pd.concat(all_games).drop_duplicates(subset=['game_id']).sort_values('date')
    
    # Separate rest days for home and away teams
    home_rest = featured_games_df.rename(columns={'rest_days': 'home_rest_days'})[['game_id', 'home_rest_days']]
    away_rest = featured_games_df.rename(columns={'rest_days': 'away_rest_days'})[['game_id', 'away_rest_days']]

    final_df = games_df.merge(home_rest, on='game_id').merge(away_rest, on='game_id')
    
    logging.info("Finished creating game-level features.")
    logging.info(f"\nSample of game features:\n{final_df.head().to_string()}")
    
    return final_df


if __name__ == '__main__':
    teams, players, games, player_stats = load_cleaned_data()
    if games is not None:
        game_features_df = create_game_features(games, teams)
        
        # Save features to a new CSV
        output_dir = Path("data/processed")
        output_dir.mkdir(parents=True, exist_ok=True)
        game_features_df.to_csv(output_dir / "game_features.csv", index=False)
        logging.info(f"Game features saved to {output_dir / 'game_features.csv'}") 