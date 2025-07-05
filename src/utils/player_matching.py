import logging
from fuzzywuzzy import process
from typing import Optional, Dict

# Add project root to path to allow imports
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.utils.database import db_manager, Players

logger = logging.getLogger(__name__)

class PlayerMatcher:
    """
    Matches player names from external sources to player_ids in the database.
    """
    def __init__(self):
        self.player_map = self._load_player_map()

    def _load_player_map(self) -> Dict[str, str]:
        """Loads all players from the database into a name-to-ID map."""
        logger.info("Loading player map from database...")
        with db_manager.get_session() as session:
            players = session.query(Players.player_id, Players.full_name).all()
        
        player_map = {player.full_name: player.player_id for player in players}
        logger.info(f"Loaded {len(player_map)} players into the matcher.")
        return player_map

    def get_player_id(self, name: str, score_cutoff: int = 85) -> Optional[str]:
        """
        Finds the best match for a player name and returns the player_id.

        Args:
            name: The player name to match.
            score_cutoff: The minimum fuzzy match score required.

        Returns:
            The matched player_id or None if no good match is found.
        """
        if not self.player_map:
            logger.warning("Player map is empty. Cannot perform matching.")
            return None
            
        best_match = process.extractOne(name, self.player_map.keys(), score_cutoff=score_cutoff)
        
        if best_match:
            matched_name, score = best_match
            player_id = self.player_map[matched_name]
            logger.debug(f"Matched '{name}' to '{matched_name}' with score {score} -> player_id: {player_id}")
            return player_id
        else:
            logger.warning(f"No good match found for player name: '{name}' (cutoff: {score_cutoff})")
            return None

# Singleton instance
player_matcher = PlayerMatcher()

def main():
    """Example usage for the PlayerMatcher."""
    logging.basicConfig(level=logging.INFO)
    
    # Example names to match
    names_to_test = ["LeBron James", "Lebron J.", "Steph Curry", "Nikola Jokic (Joker)"]
    
    for name in names_to_test:
        player_id = player_matcher.get_player_id(name)
        if player_id:
            print(f"Successfully matched '{name}' -> player_id: {player_id}")
        else:
            print(f"Failed to match '{name}'")

if __name__ == '__main__':
    main() 