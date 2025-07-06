#!/usr/bin/env python3
"""
Clean up database to ensure Phase 2 completion with valid ESPN data only.
"""

import logging
from src.utils.database import db_manager
from sqlalchemy import text
import argparse

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_tables():
    """Truncates the games and player_game_stats tables."""
    try:
        with db_manager.get_session() as session:
            session.execute(text("DELETE FROM player_game_stats;"))
            session.execute(text("DELETE FROM games;"))
            session.commit()
            logging.info("Successfully truncated 'games' and 'player_game_stats' tables.")
    except Exception as e:
        logging.error(f"Failed to clean tables: {e}")

def clean_database(tables_to_clean=None):
    """
    Cleans the database by dropping specified tables or all tables if none are specified.
    """
    if tables_to_clean is None:
        # Default behavior: clean all tables
        all_tables = ['player_game_stats', 'games', 'players', 'teams']
    else:
        # Clean only specified tables
        all_tables = tables_to_clean

    logger.info(f"Attempting to clean the following tables: {', '.join(all_tables)}")
    
    with db_manager.engine.connect() as connection:
        for table in all_tables:
            try:
                # Use "IF EXISTS" to avoid errors if the table doesn't exist
                connection.execute(text(f'DROP TABLE IF EXISTS {table}'))
                logger.info(f"Successfully dropped table: {table}")
            except Exception as e:
                logger.error(f"Error dropping table {table}: {e}")
    
    logger.info("Re-initializing the database schema.")
    db_manager.create_tables()
    logger.info("Database cleaning and re-initialization complete.")

def verify_phase2_completion():
    """Verify that Phase 2 is 100% complete."""
    
    with db_manager.get_session() as session:
        try:
            # Check all required data
            checks = {
                'teams': "SELECT COUNT(*) FROM teams",
                'players': "SELECT COUNT(*) FROM players WHERE player_id NOT LIKE 'BR_%'",
                'games': "SELECT COUNT(*) FROM games",
                'valid_player_ids': "SELECT COUNT(*) FROM players WHERE player_id REGEXP '^[0-9]+$'"
            }
            
            results = {}
            for name, query in checks.items():
                result = session.execute(text(query))
                results[name] = result.scalar() or 0
            
            logger.info("=== PHASE 2 COMPLETION VERIFICATION ===")
            logger.info(f"Teams: {results['teams']}")
            logger.info(f"Valid Players: {results['players']}")
            logger.info(f"Games: {results['games']}")
            logger.info(f"Players with numeric IDs: {results['valid_player_ids']}")
            
            # Phase 2 completion criteria
            phase2_complete = (
                results['teams'] >= 30 and
                results['players'] >= 100 and
                results['games'] >= 50 and
                results['valid_player_ids'] >= 100
            )
            
            if phase2_complete:
                logger.info("✅ PHASE 2 IS 100% COMPLETE!")
                logger.info("All required data is available for modeling.")
            else:
                logger.info("❌ PHASE 2 NOT COMPLETE")
                logger.info("Missing required data for modeling.")
            
            return phase2_complete
            
        except Exception as e:
            logger.error(f"Error verifying Phase 2: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(description="Clean specified tables from the database.")
    parser.add_argument(
        '--tables', 
        nargs='*', 
        default=None, 
        help='A list of table names to clean. If not provided, all tables will be cleaned.'
    )
    args = parser.parse_args()

    clean_database(args.tables)

if __name__ == "__main__":
    main() 