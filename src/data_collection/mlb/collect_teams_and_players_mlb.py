import logging
import sys
from pathlib import Path

# Add project root to path to allow imports
sys.path.append(str(Path(__file__).resolve().parents[3]))

from src.data_collection.mlb.espn_api_mlb import ESPNApiMlb
from src.utils.database import DatabaseManager
from src.utils.mlb_database_models import MlbTeam, MlbPlayer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def populate_teams_and_players():
    """
    Fetches all MLB teams and their players from the ESPN API
    and populates the corresponding database tables.
    """
    logging.info("Starting MLB teams and players population.")
    collector = ESPNApiMlb()
    db_manager = DatabaseManager()
    db_manager.create_tables()  # Ensure tables are created
    session = db_manager.get_session()
    
    # 1. Fetch and store teams
    teams = collector.get_teams()
    teams_added = 0
    if not teams:
        logging.error("Could not fetch teams. Aborting.")
        return
        
    try:
        for team_data in teams:
            team_id = team_data.get('team_id')
            if not team_id:
                continue
            
            existing_team = session.query(MlbTeam).filter_by(team_id=team_id).first()
            if not existing_team:
                new_team = MlbTeam(
                    team_id=team_id,
                    team_name=team_data.get('team_name'),
                    team_abbreviation=team_data.get('team_abbreviation')
                )
                session.add(new_team)
                teams_added += 1
        session.commit()
        logging.info(f"Successfully added {teams_added} new teams.")
    except Exception as e:
        logging.error(f"Error adding teams to database: {e}")
        session.rollback()
    
    # 2. Fetch and store players
    players = collector.get_players()
    players_added = 0
    if not players:
        logging.error("Could not fetch players. Aborting.")
        session.close()
        return
        
    try:
        for player_data in players:
            player_id = player_data.get('player_id')
            if not player_id:
                continue

            existing_player = session.query(MlbPlayer).filter_by(player_id=player_id).first()
            if not existing_player:
                # Find the corresponding team_id from the teams table
                team_abbr = player_data.get('team_abbreviation')
                team = session.query(MlbTeam).filter_by(team_abbreviation=team_abbr).first()
                
                new_player = MlbPlayer(
                    player_id=player_id,
                    full_name=player_data.get('full_name'),
                    position=player_data.get('position'),
                    team_id=team.team_id if team else None
                )
                session.add(new_player)
                players_added += 1
        session.commit()
        logging.info(f"Successfully added {players_added} new players.")
    except Exception as e:
        logging.error(f"Error adding players to database: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == '__main__':
    populate_teams_and_players() 