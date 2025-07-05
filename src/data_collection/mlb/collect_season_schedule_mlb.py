import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path to allow imports
sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.data_collection.mlb.espn_api_mlb import ESPNApiMlb
from src.utils.database import DatabaseManager
from src.utils.mlb_database_models import MlbGame

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def collect_mlb_schedule(season: int):
    """
    Collects the MLB schedule for a given season and stores it in the database.
    """
    logging.info(f"Starting MLB schedule collection for the {season} season.")
    
    collector = ESPNApiMlb()
    db_manager = DatabaseManager()
    db_manager.create_tables()  # Ensure tables are created
    
    # Fetch the schedule from the API
    games = collector.get_schedule(season=season)
    
    if not games:
        logging.warning(f"No games found for the {season} season. Exiting.")
        return
        
    logging.info(f"Fetched {len(games)} games from the API. Preparing to save to database.")
    
    session = db_manager.get_session()
    games_added = 0
    processed_game_ids_in_run = set()
    
    try:
        for game_data in games:
            game_id = game_data.get('game_id')
            if not game_id:
                continue

            # If we've already processed this game in this batch, skip it.
            if game_id in processed_game_ids_in_run:
                continue

            # Check if game already exists
            existing_game = session.query(MlbGame).filter_by(game_id=game_id).first()
            if existing_game:
                continue

            # Convert date string to datetime object
            date_str = game_data.get('date')
            if not date_str:
                logging.warning(f"Game {game_id} is missing a date. Skipping.")
                continue

            try:
                game_date = datetime.strptime(date_str, '%Y-%m-%dT%H:%MZ')
            except (ValueError, TypeError):
                logging.warning(f"Could not parse date '{date_str}' for game {game_id}. Skipping.")
                continue

            new_game = MlbGame(
                game_id=game_id,
                game_date=game_date,
                season=season,
                home_team_id=game_data.get('home_team_id'),
                away_team_id=game_data.get('away_team_id'),
                home_team_score=game_data.get('home_score'),
                away_team_score=game_data.get('away_score'),
            )
            session.add(new_game)
            processed_game_ids_in_run.add(game_id)
            games_added += 1

        session.commit()
        logging.info(f"Successfully added {games_added} new games to the database.")

    except Exception as e:
        logging.error(f"An error occurred during database insertion: {e}")
        session.rollback()
    finally:
        session.close()


if __name__ == '__main__':
    # Example: Collect data for the 2025 season
    target_season = 2025
    collect_mlb_schedule(target_season) 