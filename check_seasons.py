import logging
import sys
from pathlib import Path
from sqlalchemy import distinct

# Add project root to path to allow imports
sys.path.append(str(Path(__file__).resolve().parents[0]))

from src.utils.database import DatabaseManager
from src.utils.mlb_database_models import MlbGame

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_seasons_in_db():
    """
    Checks for unique seasons present in the MlbGame table.
    """
    logging.info("Checking for existing seasons in the database.")
    
    db_manager = DatabaseManager()
    session = db_manager.get_session()
    
    try:
        seasons = session.query(distinct(MlbGame.season)).all()
        
        if not seasons:
            logging.info("No seasons found in the database.")
        else:
            season_list = [s[0] for s in seasons]
            logging.info(f"Seasons found in database: {season_list}")
            
    except Exception as e:
        logging.error(f"An error occurred while checking seasons: {e}")
    finally:
        session.close()

if __name__ == '__main__':
    check_seasons_in_db() 