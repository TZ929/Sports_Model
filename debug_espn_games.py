#!/usr/bin/env python3
"""
Debug ESPN game log collection to understand why we're getting limited games.
"""

import logging
from src.data_collection.espn_api import ESPNAPICollector
import json
import requests

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def debug_single_game(game_id: str):
    """
    Fetches and prints the box score for a single game for debugging purposes.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"--- Debugging Game ID: {game_id} ---")
    
    collector = ESPNAPICollector()
    
    # Get the raw API response
    url = f"{collector.base_url}/summary?event={game_id}"
    try:
        response = requests.get(url, headers=collector.headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        logger.info("Successfully fetched API data.")
        
        # Save the full response to a file for inspection
        with open(f"debug_game_{game_id}.json", "w") as f:
            json.dump(data, f, indent=4)
        logger.info(f"Full API response saved to debug_game_{game_id}.json")

    except Exception as e:
        logger.error(f"Failed to fetch or save API data for game {game_id}: {e}", exc_info=True)
        return

    # Call the actual get_box_score method
    logger.info("--- Calling get_box_score ---")
    player_stats = collector.get_box_score(game_id)
    
    if player_stats:
        logger.info(f"Successfully parsed {len(player_stats)} player stat entries.")
        for stat in player_stats:
            logger.info(stat)
    else:
        logger.error("get_box_score returned no stats.")
        
    logger.info("--- Debugging Complete ---")


if __name__ == "__main__":
    # A known game ID from the 2024 season that should have stats
    # (e.g., from the NBA Finals or a regular season game)
    # Let's use a game ID from the previous logs that was failing.
    test_game_id = "401585609" 
    debug_single_game(test_game_id) 