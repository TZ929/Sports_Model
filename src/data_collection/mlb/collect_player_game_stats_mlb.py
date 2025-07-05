import logging
import sys
from pathlib import Path
from sqlalchemy.orm import sessionmaker, Session
import json
from typing import Dict, Any, Optional

# Add project root to path to allow imports
sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.data_collection.mlb.espn_api_mlb import ESPNApiMlb
from src.utils.database import DatabaseManager
from src.utils.mlb_database_models import MlbGame, MlbPlayer, MlbBatterStats, MlbPitcherStats

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def _clean_and_convert(value: Any, target_type: type) -> Optional[Any]:
    """Attempts to clean and convert a value to a target type, returning None on failure."""
    if value is None or value in ['---', '']:
        return None
    try:
        return target_type(value)
    except (ValueError, TypeError):
        return None

def _get_team_id_from_player_group(player_group: Dict[str, Any]) -> Optional[str]:
    """Helper to extract team ID from the player group structure."""
    return player_group.get('team', {}).get('id')

def parse_and_store_stats(session: Session, game_id: str, box_score_data: dict):
    """Parses the box score data and stores it in the database."""
    if 'boxscore' not in box_score_data or 'players' not in box_score_data['boxscore']:
        logging.warning(f"No player data in box score for game {game_id}")
        return

    player_groups = box_score_data['boxscore']['players']
    batter_stats_added = 0
    pitcher_stats_added = 0

    for player_group in player_groups:
        team_id = _get_team_id_from_player_group(player_group)
        if not team_id:
            logging.warning("Could not determine team ID for a player group.")
            continue

        for stats_table in player_group.get('statistics', []):
            table_type = stats_table.get('type')
            labels = stats_table.get('labels', [])
            
            for athlete_data in stats_table.get('athletes', []):
                player_id = athlete_data.get('athlete', {}).get('id')
                if not player_id:
                    continue

                stats_values = athlete_data.get('stats', [])
                stats_dict = dict(zip(labels, stats_values))

                if table_type == 'batting':
                    batter_stat = MlbBatterStats(
                        game_id=game_id,
                        player_id=player_id,
                        team_id=team_id,
                        at_bats=_clean_and_convert(stats_dict.get('AB'), int),
                        runs=_clean_and_convert(stats_dict.get('R'), int),
                        hits=_clean_and_convert(stats_dict.get('H'), int),
                        rbi=_clean_and_convert(stats_dict.get('RBI'), int),
                        home_runs=_clean_and_convert(stats_dict.get('HR'), int),
                        walks=_clean_and_convert(stats_dict.get('BB'), int),
                        strikeouts=_clean_and_convert(stats_dict.get('K'), int),
                        stolen_bases=_clean_and_convert(stats_dict.get('SB'), int),
                        batting_avg=_clean_and_convert(stats_dict.get('AVG'), float),
                        on_base_plus_slugging=_clean_and_convert(stats_dict.get('OPS'), float)
                    )
                    session.add(batter_stat)
                    batter_stats_added += 1
                
                elif table_type == 'pitching':
                    pitcher_stat = MlbPitcherStats(
                        game_id=game_id,
                        player_id=player_id,
                        team_id=team_id,
                        innings_pitched=_clean_and_convert(stats_dict.get('IP'), float),
                        hits_allowed=_clean_and_convert(stats_dict.get('H'), int),
                        runs_allowed=_clean_and_convert(stats_dict.get('R'), int),
                        earned_runs=_clean_and_convert(stats_dict.get('ER'), int),
                        walks=_clean_and_convert(stats_dict.get('BB'), int),
                        strikeouts=_clean_and_convert(stats_dict.get('K'), int),
                        home_runs_allowed=_clean_and_convert(stats_dict.get('HR'), int),
                        era=_clean_and_convert(stats_dict.get('ERA'), float),
                        win=stats_dict.get('W') == '1',
                        loss=stats_dict.get('L') == '1',
                        save=stats_dict.get('SV') == '1'
                    )
                    session.add(pitcher_stat)
                    pitcher_stats_added += 1

    logging.info(f"Added {batter_stats_added} batter stats and {pitcher_stats_added} pitcher stats for game {game_id}.")

def collect_player_game_stats(use_local_file=False):
    """
    Collects player game stats (batting and pitching) for all MLB games in the database.
    """
    logging.info("Starting player game stats collection for MLB.")
    
    db_manager = DatabaseManager()
    db_manager.create_tables() 
    api = ESPNApiMlb()
    session = db_manager.get_session()

    try:
        games = session.query(MlbGame.game_id).all()
        game_ids = [game.game_id for game in games]
        total_games = len(game_ids)
        logging.info(f"Found {total_games} games to process.")

        for i, game_id in enumerate(game_ids):
            logging.info(f"Processing game {i + 1}/{total_games}: {game_id}")

            existing_batter_stats = session.query(MlbBatterStats).filter_by(game_id=game_id).first()
            if existing_batter_stats:
                logging.info(f"Stats already exist for game {game_id}. Skipping.")
                continue
            
            if use_local_file:
                logging.info("Using local box score file for development.")
                try:
                    with open('box_score_401472911.json', 'r') as f:
                        box_score_data = json.load(f)
                    game_id = box_score_data['header']['competitions'][0]['id']
                except (FileNotFoundError, KeyError) as e:
                    logging.error(f"Could not load local box score file: {e}")
                    return
            else:
                box_score_data = api.get_box_score(game_id)

            if not box_score_data:
                logging.warning(f"Could not retrieve box score for game {game_id}. Skipping.")
                continue
            
            parse_and_store_stats(session, game_id, box_score_data)
            session.commit()

            if use_local_file:
                logging.info("Processed local file. Stopping.")
                break

    except Exception as e:
        logging.error(f"An error occurred during player stats collection: {e}")
        session.rollback()
    finally:
        session.close()
        logging.info("Player game stats collection finished.")

if __name__ == '__main__':
    collect_player_game_stats(use_local_file=False) 