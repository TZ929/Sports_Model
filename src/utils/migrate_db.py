import logging
import sys
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# Add project root to path to allow imports
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.utils.database import DatabaseManager

# Configure logging
log_file_path = Path(__file__).resolve().parents[1] / 'logs' / 'migration.log'
log_file_path.parent.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

def add_throws_to_mlb_players():
    """Adds the throws column to the mlb_players table."""
    logging.info("Starting database migration for mlb_players table.")
    
    db_manager = DatabaseManager()
    engine = db_manager.engine

    with engine.connect() as connection:
        with connection.begin() as transaction:
            try:
                logging.info("Attempting to add 'throws' column...")
                connection.execute(text('ALTER TABLE mlb_players ADD COLUMN throws VARCHAR'))
                logging.info("'throws' column added successfully.")
            except OperationalError as e:
                if "duplicate column name" in str(e):
                    logging.warning("Column 'throws' already exists. Skipping.")
                else:
                    raise
        
        logging.info("Migration completed successfully.")


def add_columns_to_mlb_games():
    """Adds the starting pitcher columns to the mlb_games table."""
    logging.info("Starting database migration for mlb_games table.")
    
    db_manager = DatabaseManager()
    engine = db_manager.engine

    with engine.connect() as connection:
        with connection.begin() as transaction:
            try:
                logging.info("Attempting to add 'home_starting_pitcher_id' column...")
                connection.execute(text('ALTER TABLE mlb_games ADD COLUMN home_starting_pitcher_id VARCHAR'))
                logging.info("'home_starting_pitcher_id' column added successfully.")
            except OperationalError as e:
                if "duplicate column name" in str(e):
                    logging.warning("Column 'home_starting_pitcher_id' already exists. Skipping.")
                else:
                    raise

            try:
                logging.info("Attempting to add 'away_starting_pitcher_id' column...")
                connection.execute(text('ALTER TABLE mlb_games ADD COLUMN away_starting_pitcher_id VARCHAR'))
                logging.info("'away_starting_pitcher_id' column added successfully.")
            except OperationalError as e:
                if "duplicate column name" in str(e):
                    logging.warning("Column 'away_starting_pitcher_id' already exists. Skipping.")
                else:
                    raise
        
        logging.info("Migration completed successfully.")

if __name__ == '__main__':
    add_throws_to_mlb_players()
    add_columns_to_mlb_games() 