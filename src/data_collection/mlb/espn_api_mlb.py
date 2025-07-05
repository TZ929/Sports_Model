"""
ESPN API data collector for MLB statistics.
"""

import requests
import time
import logging
from typing import List, Dict, Any
import calendar

logger = logging.getLogger(__name__)

class ESPNApiMlb:
    """Collect MLB data from ESPN API."""

    def __init__(self):
        self.base_url = "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def get_teams(self) -> List[Dict[str, Any]]:
        """
        Get all MLB teams.

        Returns:
            List of team dictionaries.
        """
        url = f"{self.base_url}/teams"
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()

            teams = []
            for team_data in data.get('sports', [{}])[0].get('leagues', [{}])[0].get('teams', []):
                team = team_data.get('team', {})
                team_info = {
                    'team_id': team.get('id'),
                    'team_name': team.get('displayName'),
                    'team_abbreviation': team.get('abbreviation'),
                }
                teams.append(team_info)
            
            logger.info(f"Retrieved {len(teams)} MLB teams from ESPN API")
            return teams

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching MLB teams from ESPN: {e}")
            return []

    def get_players(self) -> List[Dict[str, Any]]:
        """
        Get all MLB players by iterating through each team's roster.

        Returns:
            List of player dictionaries.
        """
        teams = self.get_teams()
        if not teams:
            logger.error("Could not fetch teams, aborting player fetch.")
            return []

        all_players = []
        logger.info(f"Fetching rosters for {len(teams)} teams...")

        for team in teams:
            team_abbr = team.get('team_abbreviation')
            if not team_abbr:
                continue

            url = f"{self.base_url}/teams/{team_abbr}/roster"
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                data = response.json()

                # The roster data is nested under 'athletes' and then by position
                position_groups = data.get('athletes', [])
                for group in position_groups:
                    for athlete in group.get('items', []):
                        position_data = athlete.get('position', {})
                        position = position_data.get('abbreviation') if isinstance(position_data, dict) else position_data

                        player_info = {
                            'player_id': athlete.get('id'),
                            'full_name': athlete.get('fullName'),
                            'team_abbreviation': team_abbr,
                            'position': position,
                        }
                        all_players.append(player_info)
                
                logger.info(f"Successfully processed roster for {team_abbr}.")
                time.sleep(1)  # Be respectful to the API

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching players for team {team_abbr}: {e}")
                continue
        
        logger.info(f"Retrieved a total of {len(all_players)} MLB players.")
        return all_players

    def get_schedule(self, season: int = 2023) -> List[Dict[str, Any]]:
        """
        Get the MLB schedule for an entire season by iterating through each day.

        Args:
            season (int): The year of the season to fetch.

        Returns:
            List of game dictionaries.
        """
        logger.info(f"Fetching MLB schedule for the {season} season...")
        all_games = []
        # MLB season typically runs from April (4) to the start of November (10)
        for month in range(3, 11): # March to October to be safe
            num_days = calendar.monthrange(season, month)[1]
            for day in range(1, num_days + 1):
                date_str = f"{season}{month:02d}{day:02d}"
                url = f"{self.base_url}/scoreboard?dates={date_str}"
                
                try:
                    response = requests.get(url, headers=self.headers, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                    
                    for event in data.get('events', []):
                        try:
                            competition = event.get('competitions', [{}])[0]
                            competitors = competition.get('competitors', [])
                            
                            if len(competitors) != 2:
                                continue
                            
                            home_team, away_team = None, None
                            for competitor in competitors:
                                if competitor.get('homeAway') == 'home':
                                    home_team = competitor
                                else:
                                    away_team = competitor
                            
                            if not home_team or not away_team:
                                continue

                            game_info = {
                                'game_id': event.get('id'),
                                'date': event.get('date'),
                                'name': event.get('name'),
                                'home_team_id': home_team.get('team', {}).get('abbreviation'),
                                'away_team_id': away_team.get('team', {}).get('abbreviation'),
                                'home_score': home_team.get('score'),
                                'away_score': away_team.get('score'),
                            }
                            all_games.append(game_info)
                        except (IndexError, TypeError) as e:
                            logger.error(f"Error parsing a game event on {date_str}: {e}")
                            continue

                    logger.info(f"Fetched and processed {len(data.get('events', []))} events for {date_str}")
                    time.sleep(0.5) # Be respectful

                except requests.exceptions.RequestException as e:
                    logger.warning(f"Could not fetch games for {date_str}: {e}")
                    continue
        
        logger.info(f"Successfully fetched a total of {len(all_games)} games for the {season} season.")
        return all_games

    def get_box_score(self, game_id: str) -> Dict[str, Any]:
        """
        Get the box score for a specific MLB game.

        Args:
            game_id: The ESPN game_id.

        Returns:
            A dictionary containing parsed box score data.
        """
        logger.info(f"Fetching box score for game_id: {game_id}")
        url = f"https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event={game_id}"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # For now, we'll just return the raw data to inspect its structure.
            # Parsing logic will be complex and added in a later step.
            logger.info(f"Successfully fetched box score data for game_id: {game_id}")
            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching box score for game_id {game_id}: {e}")
            return {}


if __name__ == '__main__':
    import json

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    collector = ESPNApiMlb()
    
    # Test get_box_score with a hardcoded game_id to speed up testing
    test_game_id = "401472911" # A real game_id from the 2023 season
    print(f"\nTesting with game_id: {test_game_id}")
    
    box_score = collector.get_box_score(test_game_id)
    
    if box_score:
        print("Successfully fetched box score data. Root keys:", box_score.keys())
        
        # Save the full JSON to a file for inspection
        output_path = f"box_score_{test_game_id}.json"
        with open(output_path, 'w') as f:
            json.dump(box_score, f, indent=4)
        print(f"Full box score data saved to {output_path}")
    else:
        print("\nFailed to fetch MLB box score.") 