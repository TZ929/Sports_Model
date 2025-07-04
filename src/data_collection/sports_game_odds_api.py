import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import logging

# Add project root to path to allow imports
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.utils.database import db_manager, PropOdds
from src.utils.player_matching import player_matcher

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SportsGameOddsAPICollector:
    """
    Collects historical player prop odds from the sportsgameodds.com API.
    """
    API_URL = "https://api.sportsgameodds.com/v1"
    
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("API key for sportsgameodds.com is required.")
        self.api_key = api_key
        self.headers = {'X-API-Key': self.api_key}

    def get_events_for_date(self, date: str):
        """Fetches all NBA events for a specific date."""
        url = f"{self.API_URL}/events?league=NBA&date={date}"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            # Ensure we return a dictionary
            return data if isinstance(data, dict) else {}
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching events for {date}: {e}")
            return {}

    def collect_and_store_odds(self, start_date: str, end_date: str):
        """
        Collects and stores prop odds for a range of dates.
        """
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        logging.info(f"Starting historical odds collection from {start_date} to {end_date}.")

        with db_manager.get_session() as session:
            for date in (start + timedelta(n) for n in range(int((end - start).days) + 1)):
                date_str = date.strftime('%Y-%m-%d')
                logger.info(f"Fetching data for {date_str}...")
                events_data = self.get_events_for_date(date_str)
                events = events_data.get('data', []) if events_data else []

                if not events:
                    logger.info(f"No events found for {date_str}. Skipping.")
                    time.sleep(5)
                    continue
                
                for event in events:
                    game_id = event.get('eventID')
                    props = event.get('props', [])
                    
                    for prop in props:
                        # Logic to find and store player points props from FanDuel or ESPNBet
                        player_name = prop.get('participantName')
                        bookmaker = prop.get('bookmakerID')
                        prop_name = prop.get('propName', '').lower()

                        if not all([player_name, bookmaker, 'points' in prop_name]):
                            continue
                        
                        if bookmaker not in ['FanDuel', 'ESPNBet']:
                            continue
                            
                        player_id = player_matcher.get_player_id(player_name)
                        if not player_id:
                            logger.warning(f"Could not find a match for player: {player_name}. Skipping.")
                            continue
                        
                        # The structure of over/under might need adjustment based on real API response
                        over_odds = prop.get('overOdds')
                        under_odds = prop.get('underOdds')
                        line = prop.get('line')

                        if over_odds and under_odds and line:
                            db_prop = PropOdds(
                                game_id=game_id,
                                player_id=player_id,
                                sportsbook=bookmaker,
                                prop_type='player_points', # Simplified
                                line=float(line),
                                over_odds=int(over_odds),
                                under_odds=int(under_odds),
                                timestamp=datetime.fromisoformat(prop['lastUpdated'].replace('Z', '+00:00'))
                            )
                            logger.info(f"Storing prop for {player_name} in game {game_id}")
                            session.merge(db_prop) # Use merge to handle potential duplicates

                time.sleep(5) # API rate limit

            session.commit()
        logging.info("Historical odds collection finished.")

def main():
    """Main function to run the collector."""
    api_key = os.environ.get('SPORTS_GAME_ODDS_API_KEY')
    
    if not api_key:
        logger.error("Please set the SPORTS_GAME_ODDS_API_KEY environment variable.")
        return

    collector = SportsGameOddsAPICollector(api_key=api_key)
    # Example: Collect data for one week of the 2023 season
    collector.collect_and_store_odds(start_date="2023-10-24", end_date="2023-10-31")

if __name__ == '__main__':
    main() 