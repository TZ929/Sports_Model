import logging
import argparse
from src.data_collection.espn_api import ESPNAPICollector
from src.utils.database import db_manager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def collect_season_schedule(season: str):
    """
    Collects the full game schedule for a given season and stores it in the database.

    Args:
        season (str): The season to collect data for (e.g., "2024").
    """
    collector = ESPNAPICollector()
    
    logging.info(f"Starting season schedule collection for the {season} season.")
    
    games = collector.get_games(season=season)
    
    if not games:
        logging.warning(f"No games found for the {season} season.")
        return
        
    game_count = 0
    for game in games:
        if db_manager.insert_game(game):
            game_count += 1
            
    logging.info(f"Successfully inserted {game_count} games for the {season} season.")

def main():
    parser = argparse.ArgumentParser(description="Collect the full season schedule from ESPN.")
    parser.add_argument("--season", type=str, required=True, help="The season to collect data for (e.g., '2024' for the 2023-24 season).")
    args = parser.parse_args()
    
    collect_season_schedule(args.season)

if __name__ == "__main__":
    main() 