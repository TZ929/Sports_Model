import logging
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy.orm import Session
import json

# Add project root to path to allow imports
sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.utils.database import DatabaseManager
from src.utils.mlb_database_models import MlbGame, MlbPitcherStats
from src.data_collection.mlb.espn_api_mlb import ESPNApiMlb

# Configure logging
log_file_path = Path(__file__).resolve().parents[1] / 'logs' / 'add_starting_pitchers.log'
log_file_path.parent.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

def identify_and_store_starting_pitchers():
    """
    Identifies the starting pitcher for each game and stores it in the database.
    """
    logging.info("Starting to identify and store starting pitchers.")
    
    db_manager = DatabaseManager()
    session = db_manager.get_session()
    api = ESPNApiMlb()

    try:
        games_to_update = session.query(MlbGame).filter(MlbGame.home_starting_pitcher_id == None).all()
        total_games = len(games_to_update)
        logging.info(f"Found {total_games} games to update with starting pitcher information.")

        for i, game in enumerate(games_to_update):
            logging.info(f"Processing game {i + 1}/{total_games}: {game.game_id}")
            
            box_score_data = api.get_box_score(str(game.game_id))
            
            if not box_score_data or 'boxscore' not in box_score_data or 'players' not in box_score_data['boxscore']:
                logging.warning(f"Could not get box score or player data for game {game.game_id}. Skipping.")
                continue

            player_groups = box_score_data['boxscore'].get('players', [])
            team_groups = box_score_data['boxscore'].get('teams', [])
            
            if len(player_groups) != len(team_groups) or len(team_groups) != 2:
                logging.warning(f"Box score data for game {game.game_id} does not have exactly two teams. Skipping.")
                continue

            home_pitchers = []
            away_pitchers = []

            for i, player_group in enumerate(player_groups):
                team_info = team_groups[i]
                is_home = team_info.get('homeAway') == 'home'

                for stats_table in player_group.get('statistics', []):
                    # For MLB, the pitching stats are under a 'pitching' type or label.
                    # As we saw differences between sports, let's check both name and type.
                    stats_type = stats_table.get('name', '') or stats_table.get('type', '')
                    if 'pitching' in stats_type.lower():
                        for athlete_data in stats_table.get('athletes', []):
                            # The first pitcher listed is the starter
                            player_id = athlete_data.get('athlete', {}).get('id')
                            if player_id:
                                if is_home:
                                    if game.home_starting_pitcher_id is None:
                                        game.home_starting_pitcher_id = player_id
                                else:
                                    if game.away_starting_pitcher_id is None:
                                        game.away_starting_pitcher_id = player_id
                                # Break after finding the first pitcher for the team
                                break 
                
                # After iterating through a team's stats, check if we are done
                if game.home_starting_pitcher_id is not None and game.away_starting_pitcher_id is not None:
                    break
            
            session.commit()
            logging.info(f"Updated game {game.game_id} with home SP: {game.home_starting_pitcher_id} and away SP: {game.away_starting_pitcher_id}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == '__main__':
    identify_and_store_starting_pitchers() 