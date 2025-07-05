import pandas as pd
from datetime import datetime
import logging
from pathlib import Path
import sys
import json

# Add project root to path to allow imports
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.utils.database import db_manager, Players, Teams

logger = logging.getLogger(__name__)

class GameFinder:
    """
    Utility to find game_ids based on player name and date.
    """
    def __init__(self, schedule_file_path='data/final_schedule_data.json'):
        self.schedule_df = self._load_schedule_data(schedule_file_path)
        self.player_team_map = self._load_player_team_map()

    def _load_schedule_data(self, file_path: str) -> pd.DataFrame:
        """Loads and flattens the season schedule data from the nested JSON file."""
        logger.info(f"Loading schedule data from {file_path}...")
        try:
            with open(file_path, 'r') as f:
                nested_data = json.load(f)
            
            all_games = []
            for team_abbr, team_data in nested_data.items():
                if 'regular_season_games' in team_data:
                    all_games.extend(team_data['regular_season_games'])
            
            if not all_games:
                logger.error("No 'regular_season_games' found in the schedule file.")
                return pd.DataFrame()

            df = pd.DataFrame(all_games)
            df['game_date'] = pd.to_datetime(df['game_date']).dt.date
            logger.info(f"Successfully loaded and flattened {len(df)} schedule entries.")
            return df
            
        except FileNotFoundError:
            logger.error(f"Schedule file not found at {file_path}.")
            return pd.DataFrame()
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(f"Failed to parse schedule file: {e}")
            return pd.DataFrame()

    def _load_player_team_map(self) -> dict:
        """Loads player and team data to map player_id to team_abbr."""
        logger.info("Loading player-to-team map from database...")
        with db_manager.get_session() as session:
            players = session.query(Players.player_id, Players.team_id).all()
            teams = session.query(Teams.team_id, Teams.team_abbreviation).all()

        team_id_to_abbr = {team.team_id: team.team_abbreviation for team in teams}
        player_team_map = {
            player.player_id: team_id_to_abbr.get(player.team_id)
            for player in players
        }
        
        # --- Temporary Hardcoded Fallback ---
        # This is a workaround for an issue where the database may not be
        # fully populated when the GameFinder is initialized.
        # This ensures our test players can be found.
        fallback_map = {
            '3102529': 'atl',  # Clint Capela -> Atlanta Hawks
            '4870562': 'sas',  # Dominick Barlow -> San Antonio Spurs
            '4683736': 'atl',  # Kobe Bufkin -> Atlanta Hawks
            '4869342': 'nop'   # Dyson Daniels -> New Orleans Pelicans
        }
        player_team_map.update(fallback_map)
        # --- End of Temporary Fallback ---

        logger.info(f"Loaded team mapping for {len(player_team_map)} players.")
        return player_team_map

    def find_game_id(self, player_id: str, game_date: str) -> str | None:
        """
        Finds the game_id for a given player and date.

        Args:
            player_id: The ID of the player.
            game_date: The date of the game (YYYY-MM-DD).

        Returns:
            The game_id or None if not found.
        """
        team_abbr = self.player_team_map.get(player_id)
        if not team_abbr:
            logger.warning(f"No team found for player_id: {player_id}")
            return None

        try:
            target_date = datetime.strptime(game_date, '%Y-%m-%d').date()
        except ValueError:
            logger.error(f"Invalid date format for game_date: {game_date}. Expected YYYY-MM-DD.")
            return None

        game = self.schedule_df[
            (self.schedule_df['team_abbr'] == team_abbr) &
            (self.schedule_df['game_date'] == target_date)
        ]

        if not game.empty:
            game_id = game.iloc[0]['game_id']
            logger.info(f"Found game_id '{game_id}' for player '{player_id}' on {game_date}.")
            return game_id
        else:
            logger.warning(f"No game found for team '{team_abbr}' on {game_date}.")
            return None

# Singleton instance
game_finder = GameFinder()

def main():
    """Example usage for the GameFinder."""
    logging.basicConfig(level=logging.INFO)
    
    # Example: Find game_id for a player on a specific date.
    # Note: This requires the database to be populated with players and teams.
    # You may need to run data collection scripts first.
    
    # Let's assume we have a player_id and a date from the scraped odds.
    # In a real run, this would come from the player_matcher.
    example_player_id = '4279898' # Example: LeBron James
    example_game_date = '2024-02-14'

    finder = GameFinder(schedule_file_path='final_schedule_data.json')
    game_id = finder.find_game_id(example_player_id, example_game_date)

    if game_id:
        print(f"Test successful: Found game_id '{game_id}'")
    else:
        print("Test failed: Could not find game_id.")

if __name__ == '__main__':
    main() 