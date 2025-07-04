import logging
import argparse
import concurrent.futures
import threading
from src.data_collection.espn_api import ESPNAPICollector
from src.utils.database import db_manager
from sqlalchemy import text
from typing import List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_and_save_stats(collector: ESPNAPICollector, game_id: str, db_lock: threading.Lock):
    """Fetches stats for a single game and saves them to the database."""
    try:
        player_stats = collector.get_box_score(game_id)
        if not player_stats:
            logging.warning(f"No player stats returned for game: {game_id}")
            return 0
        
        count = 0
        with db_lock:
            for stats in player_stats:
                if db_manager.insert_player_game_stats(stats):
                    count += 1
                else:
                    logging.error(f"  > Failed to save stats for player {stats.get('player_id', 'N/A')} in game {game_id}")
        
        if count > 0:
            logging.info(f"Successfully saved {count} player stat entries for game {game_id}.")
        return count
    except Exception as e:
        logging.error(f"An error occurred while processing game {game_id}: {e}", exc_info=True)
        return 0

def collect_all_player_stats_for_season(season: str, max_workers: int = 10):
    """
    Orchestrates the concurrent collection of player game stats for an entire season.
    """
    collector = ESPNAPICollector()
    logging.info(f"Starting player game stats collection for the {season} season.")
    
    with db_manager.get_session() as session:
        games = session.execute(text("SELECT game_id FROM games WHERE season = :season"), {"season": season}).fetchall()
        game_ids = [g[0] for g in games]
    
    logging.info(f"Found {len(game_ids)} games for the {season} season. Starting concurrent processing...")
    
    total_stats_saved = 0
    db_lock = threading.Lock()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_game = {executor.submit(fetch_and_save_stats, collector, game_id, db_lock): game_id for game_id in game_ids}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_game)):
            game_id = future_to_game[future]
            try:
                stats_saved_count = future.result()
                total_stats_saved += stats_saved_count
                logging.info(f"Progress: ({i+1}/{len(game_ids)}) - Game {game_id} processed. Saved {stats_saved_count} new stats.")
            except Exception as exc:
                logging.error(f'Game {game_id} generated an exception: {exc}')

    logging.info(f"Completed player game stats collection for the {season} season. Total stats saved: {total_stats_saved}.")

def main():
    parser = argparse.ArgumentParser(description="Collect player game stats for a given season from ESPN concurrently.")
    parser.add_argument("--season", type=str, required=True, help="The season to collect data for (e.g., '2024').")
    parser.add_argument("--workers", type=int, default=10, help="Number of concurrent workers.")
    args = parser.parse_args()
    
    collect_all_player_stats_for_season(args.season, args.workers)

if __name__ == "__main__":
    main() 