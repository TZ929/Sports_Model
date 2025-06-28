#!/usr/bin/env python3
"""
Collect and store all ESPN player game stats for valid player IDs in the database.
"""

import logging
from src.data_collection.espn_api import ESPNAPICollector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting ESPN player game stats collection...")
    collector = ESPNAPICollector()
    # You can adjust the limit as needed (None or a large number for all players)
    collector.collect_all_player_game_stats(season="2024", limit=200, save_to_db=True)
    logger.info("ESPN player game stats collection complete.") 