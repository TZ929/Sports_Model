import pandas as pd
from src.utils.database import db_manager, Teams, Players, Games, PlayerGameStats
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
        logging.FileHandler(log_dir / "data_cleaner.log"),
        logging.StreamHandler()
    ]
)

class DataCleaner:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def clean_player_game_stats(self, player_stats_df: pd.DataFrame) -> pd.DataFrame:
        """Cleans the player game stats DataFrame."""
        logging.info("Cleaning player stats data...")
        # Example cleaning step:
        player_stats_df['defensive_rebounds'] = player_stats_df['rebounds'] - player_stats_df['offensive_rebounds']
        
        # Drop timestamps
        for col in ['created_at', 'updated_at']:
            if col in player_stats_df.columns:
                player_stats_df = player_stats_df.drop(columns=col)
                
        logging.info("Player game stats cleaning complete.")
        return player_stats_df

    def clean_all_data(self, teams_df, players_df, games_df, player_stats_df):
        """Clean all dataframes."""
        logging.info("Starting data cleaning...")

        # --- Clean Teams Data ---
        logging.info("Cleaning teams data...")
        teams_df['team_name'] = teams_df['team_name'].str.replace('*', '', regex=False)
        
        # --- Clean Players Data ---
        logging.info("Cleaning players data...")
        players_df = players_df[~players_df['full_name'].str.contains('Celtics', na=False)].copy()
        
        team_name_map = {}
        for _, row in teams_df.iterrows():
            team_name_map[row['team_name']] = row['team_id']
            nickname = row['team_name'].split(' ')[-1]
            team_name_map[nickname] = row['team_id']

        team_name_map.update({
            'Nets': 'BKN', 'Hornets': 'CHA', 'Warriors': 'GSW', 'Pelicans': 'NOP',
            'Knicks': 'NYK', 'Suns': 'PHX', 'Spurs': 'SAS', 'Jazz': 'UTA', 'Wizards': 'WAS'
        })
        
        players_df['team_id'] = players_df['team_name'].map(team_name_map)

        unmapped_players = players_df[players_df['team_id'].isnull()]
        if not unmapped_players.empty:
            logging.warning(f"Could not map team_id for {len(unmapped_players)} players.")

        players_df = players_df.drop(columns=['team_name', 'height', 'weight', 'birth_date'])

        # --- Clean Player Stats Data ---
        player_stats_df = self.clean_player_game_stats(player_stats_df)

        # --- General Cleaning (drop timestamps) ---
        logging.info("Dropping timestamp columns...")
        for df in [teams_df, players_df, games_df]:
            for col in ['created_at', 'updated_at']:
                if col in df.columns:
                    df.drop(columns=col, inplace=True)

        logging.info("Data cleaning complete.")
        return teams_df, players_df, games_df, player_stats_df

def load_data_from_db():
    """Load all tables from the database into pandas DataFrames."""
    logging.info("Loading data from database...")
    with db_manager.get_session() as session:
        connection = session.connection()
        teams_df = pd.read_sql_table(Teams.__tablename__, connection)
        players_df = pd.read_sql_table(Players.__tablename__, connection)
        games_df = pd.read_sql_table(Games.__tablename__, connection)
        player_stats_df = pd.read_sql_table(PlayerGameStats.__tablename__, connection)
    logging.info("Data loaded successfully.")
    return teams_df, players_df, games_df, player_stats_df

def save_cleaned_data(teams_df, players_df, games_df, player_stats_df):
    """Save cleaned data to CSV files."""
    logging.info("Saving cleaned data to CSV files...")
    output_dir = Path("data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)

    teams_df.to_csv(output_dir / "teams_cleaned.csv", index=False)
    players_df.to_csv(output_dir / "players_cleaned.csv", index=False)
    games_df.to_csv(output_dir / "games_cleaned.csv", index=False)
    player_stats_df.to_csv(output_dir / "player_stats_cleaned.csv", index=False)
    
    logging.info(f"Cleaned data saved to {output_dir}")

if __name__ == '__main__':
    teams, players, games, player_stats = load_data_from_db()
    cleaner = DataCleaner(config={})  # Add a dummy config for now
    teams_c, players_c, games_c, player_stats_c = cleaner.clean_all_data(teams, players, games, player_stats)
    save_cleaned_data(teams_c, players_c, games_c, player_stats_c)
    logging.info("Data cleaning process finished.") 