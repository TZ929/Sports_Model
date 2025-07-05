import logging
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy.orm import Session

# Add project root to path to allow imports
sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.utils.database import DatabaseManager
from src.utils.mlb_database_models import MlbGame

# Configure logging
log_file_path = Path(__file__).resolve().parents[3] / 'logs' / 'mlb_game_feature_engineering.log'
log_file_path.parent.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

def calculate_rest_days(session: Session) -> pd.DataFrame:
    """
    Calculates home and away rest days for each game.
    """
    logging.info("Calculating rest days for all teams.")
    
    # Load all games from the database
    games_query = session.query(MlbGame).statement
    df_games = pd.read_sql(games_query, session.bind)
    
    if df_games.empty:
        logging.warning("No games found in the database. Aborting rest day calculation.")
        return pd.DataFrame()

    df_games['game_date'] = pd.to_datetime(df_games['game_date'])
    df_games = df_games.sort_values(by='game_date')

    # Create a DataFrame with one row per team per game
    home_teams = df_games[['game_id', 'game_date', 'home_team_id']].rename(columns={'home_team_id': 'team_id'})
    away_teams = df_games[['game_id', 'game_date', 'away_team_id']].rename(columns={'away_team_id': 'team_id'})
    
    df_team_games = pd.concat([home_teams, away_teams], ignore_index=True)
    df_team_games = df_team_games.sort_values(by=['team_id', 'game_date'])

    # Calculate rest days
    df_team_games['last_game_date'] = df_team_games.groupby('team_id')['game_date'].shift(1)
    df_team_games['rest_days'] = (df_team_games['game_date'] - df_team_games['last_game_date']).dt.days - 1
    
    # Merge back into the original games DataFrame
    home_rest = df_team_games[['game_id', 'team_id', 'rest_days']].rename(columns={'team_id': 'home_team_id', 'rest_days': 'home_rest_days'})
    away_rest = df_team_games[['game_id', 'team_id', 'rest_days']].rename(columns={'team_id': 'away_team_id', 'rest_days': 'away_rest_days'})

    df_games = pd.merge(df_games, home_rest, on=['game_id', 'home_team_id'], how='left')
    df_games = pd.merge(df_games, away_rest, on=['game_id', 'away_team_id'], how='left')
    
    logging.info("Finished calculating rest days.")
    return df_games

def main():
    """
    Main function to orchestrate game feature engineering.
    """
    logging.info("Starting MLB game feature engineering.")
    db_manager = DatabaseManager()
    session = db_manager.get_session()

    try:
        df_game_features = calculate_rest_days(session)
        
        if not df_game_features.empty:
            # Save the features to a CSV file
            output_path = Path(__file__).resolve().parents[3] / 'data' / 'processed' / 'mlb' / 'game_features.csv'
            df_game_features.to_csv(output_path, index=False)
            logging.info(f"Game features saved to {output_path}")
            print("Sample of game features:")
            print(df_game_features.head())

    except Exception as e:
        logging.error(f"An error occurred during game feature engineering: {e}")
    finally:
        session.close()
        logging.info("Game feature engineering finished.")

if __name__ == '__main__':
    main() 