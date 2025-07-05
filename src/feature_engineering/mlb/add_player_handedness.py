import logging
import sys
from pathlib import Path
from sqlalchemy.orm import Session

# Add project root to path to allow imports
sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.utils.database import DatabaseManager
from src.utils.mlb_database_models import MlbPlayer
from src.data_collection.mlb.espn_api_mlb import ESPNApiMlb

# Configure logging
log_file_path = Path(__file__).resolve().parents[1] / 'logs' / 'add_player_handedness.log'
log_file_path.parent.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

def populate_player_handedness():
    """
    Populates the 'throws' column for players in the database.
    """
    logging.info("Starting to populate player handedness.")
    
    db_manager = DatabaseManager()
    session = db_manager.get_session()
    api = ESPNApiMlb()

    try:
        logging.info("Fetching all players from ESPN API...")
        all_players_data = api.get_players()
        
        if not all_players_data:
            logging.error("No player data received from API. Aborting.")
            return

        logging.info(f"Received data for {len(all_players_data)} players. Updating database...")
        
        for player_data in all_players_data:
            player_id = player_data.get('player_id')
            if not player_id:
                continue
            
            player = session.query(MlbPlayer).filter_by(player_id=player_id).first()
            if player:
                player.throws = player_data.get('throws', 'R') # Default to Right-handed
        
        session.commit()
        logging.info("Successfully updated player handedness in the database.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == '__main__':
    populate_player_handedness() 