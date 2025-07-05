import logging
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy.orm import sessionmaker, Session

# Add project root to path to allow imports
sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.utils.database import DatabaseManager
from src.utils.mlb_database_models import MlbBatterStats, MlbPitcherStats

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def calculate_rolling_player_features():
    """
    Calculates rolling averages for MLB player stats.
    """
    logging.info("Starting MLB player feature engineering.")
    
    db_manager = DatabaseManager()
    session = db_manager.get_session()

    try:
        # Load batter and pitcher stats into pandas DataFrames
        batter_query = session.query(MlbBatterStats).statement
        pitcher_query = session.query(MlbPitcherStats).statement

        df_batters = pd.read_sql(batter_query, session.bind)
        df_pitchers = pd.read_sql(pitcher_query, session.bind)

        logging.info(f"Loaded {len(df_batters)} batter stat entries.")
        logging.info(f"Loaded {len(df_pitchers)} pitcher stat entries.")

        if df_batters.empty and df_pitchers.empty:
            logging.warning("No player stats found in the database. Aborting feature engineering.")
            return

        # The core logic for calculating rolling averages will be implemented next.
        # For now, we will just confirm the data has been loaded.

        logging.info("Successfully loaded data. Feature calculation logic to be implemented.")

    except Exception as e:
        logging.error(f"An error occurred during player feature engineering: {e}")
    finally:
        session.close()
        logging.info("Player feature engineering finished.")

if __name__ == '__main__':
    calculate_rolling_player_features() 