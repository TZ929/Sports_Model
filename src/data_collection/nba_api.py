"""
NBA API data collector for historical game and player data.
"""

import logging
import time
import requests
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

from ..utils.config import config
from ..utils.database import db_manager

# Set up logging
logger = logging.getLogger(__name__)


class NBADataCollector:
    """Collector for NBA data from stats.nba.com API."""
    
    def __init__(self):
        """Initialize the NBA data collector."""
        self.base_url = config.get('data_sources.nba_api.base_url')
        self.headers = config.get('data_sources.nba_api.headers', {})
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        """Make a request to the NBA API.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            API response as dictionary or None if failed
        """
        try:
            url = f"{self.base_url}{endpoint}"
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            # Add delay to be respectful to the API
            time.sleep(config.get('scraping.delay_between_requests', 1.0))
            
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {endpoint}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in API request: {e}")
            return None
    
    def get_teams(self) -> List[Dict[str, Any]]:
        """Get all NBA teams.
        
        Returns:
            List of team dictionaries
        """
        endpoint = "leaguedashteamstats"
        params = {
            "Season": "2023-24",
            "SeasonType": "Regular Season",
            "PerMode": "PerGame"
        }
        
        response = self._make_request(endpoint, params)
        if not response or 'resultSets' not in response:
            return []
        
        teams_data = response['resultSets'][0]['rowSet']
        teams = []
        
        for team_data in teams_data:
            team = {
                'team_id': team_data[0],
                'team_name': team_data[1],
                'team_abbreviation': team_data[2],
                'conference': team_data[3],
                'division': team_data[4]
            }
            teams.append(team)
        
        logger.info(f"Retrieved {len(teams)} teams")
        return teams
    
    def get_players(self, season: str = "2023-24") -> List[Dict[str, Any]]:
        """Get all NBA players for a season.
        
        Args:
            season: NBA season (e.g., "2023-24")
            
        Returns:
            List of player dictionaries
        """
        endpoint = "leaguedashplayerstats"
        params = {
            "Season": season,
            "SeasonType": "Regular Season",
            "PerMode": "PerGame"
        }
        
        response = self._make_request(endpoint, params)
        if not response or 'resultSets' not in response:
            return []
        
        players_data = response['resultSets'][0]['rowSet']
        players = []
        
        for player_data in players_data:
            player = {
                'player_id': player_data[0],
                'full_name': player_data[1],
                'team_id': player_data[2],
                'team_name': player_data[3],
                'position': player_data[4],
                'age': player_data[5],
                'height': player_data[6],
                'weight': player_data[7],
                'season': season
            }
            players.append(player)
        
        logger.info(f"Retrieved {len(players)} players for {season}")
        return players
    
    def get_games(self, season: str = "2023-24", season_type: str = "Regular Season") -> List[Dict[str, Any]]:
        """Get all games for a season.
        
        Args:
            season: NBA season (e.g., "2023-24")
            season_type: Season type (Regular Season, Playoffs, etc.)
            
        Returns:
            List of game dictionaries
        """
        endpoint = "leaguegamefinder"
        params = {
            "Season": season,
            "SeasonType": season_type,
            "LeagueID": "00"  # NBA
        }
        
        response = self._make_request(endpoint, params)
        if not response or 'resultSets' not in response:
            return []
        
        games_data = response['resultSets'][0]['rowSet']
        games = []
        
        for game_data in games_data:
            game = {
                'game_id': game_data[1],
                'date': datetime.strptime(game_data[2], '%Y-%m-%d'),
                'home_team_id': game_data[6],
                'away_team_id': game_data[3],
                'home_team_name': game_data[7],
                'away_team_name': game_data[4],
                'home_score': game_data[8],
                'away_score': game_data[5],
                'season': season,
                'league': 'NBA'
            }
            games.append(game)
        
        logger.info(f"Retrieved {len(games)} games for {season} {season_type}")
        return games
    
    def get_player_game_stats(self, game_id: str) -> List[Dict[str, Any]]:
        """Get player statistics for a specific game.
        
        Args:
            game_id: Game identifier
            
        Returns:
            List of player statistics dictionaries
        """
        endpoint = "boxscoretraditionalv2"
        params = {
            "GameID": game_id
        }
        
        response = self._make_request(endpoint, params)
        if not response or 'resultSets' not in response:
            return []
        
        stats = []
        
        # Process both home and away team stats
        for result_set in response['resultSets']:
            if 'PlayerStats' in result_set['name']:
                player_stats_data = result_set['rowSet']
                
                for player_data in player_stats_data:
                    stat = {
                        'game_id': game_id,
                        'player_id': player_data[0],
                        'team_id': player_data[1],
                        'minutes_played': self._parse_minutes(player_data[8]),
                        'field_goals_made': player_data[9],
                        'field_goals_attempted': player_data[10],
                        'three_pointers_made': player_data[11],
                        'three_pointers_attempted': player_data[12],
                        'free_throws_made': player_data[13],
                        'free_throws_attempted': player_data[14],
                        'rebounds': player_data[20],
                        'offensive_rebounds': player_data[18],
                        'defensive_rebounds': player_data[19],
                        'assists': player_data[21],
                        'steals': player_data[22],
                        'blocks': player_data[23],
                        'turnovers': player_data[24],
                        'personal_fouls': player_data[25],
                        'points': player_data[26],
                        'plus_minus': player_data[27]
                    }
                    stats.append(stat)
        
        logger.info(f"Retrieved {len(stats)} player stats for game {game_id}")
        return stats
    
    def _parse_minutes(self, minutes_str: str) -> Optional[Union[int, float]]:
        """Parse minutes played string to total minutes.
        
        Args:
            minutes_str: Minutes string in format "MM:SS"
            
        Returns:
            Total minutes as float or None if invalid
        """
        if not minutes_str or minutes_str == '':
            return None
        
        try:
            parts = minutes_str.split(':')
            if len(parts) == 2:
                minutes = int(parts[0])
                seconds = int(parts[1])
                return minutes + (seconds / 60)
            return None
        except (ValueError, IndexError):
            return None
    
    def collect_season_data(self, season: str = "2023-24", save_to_db: bool = True) -> Dict[str, int]:
        """Collect complete season data including teams, players, games, and stats.
        
        Args:
            season: NBA season to collect
            save_to_db: Whether to save data to database
            
        Returns:
            Dictionary with counts of collected data
        """
        logger.info(f"Starting data collection for {season}")
        
        counts = {
            'teams': 0,
            'players': 0,
            'games': 0,
            'player_stats': 0
        }
        
        # Collect teams
        teams = self.get_teams()
        counts['teams'] = len(teams)
        
        # Collect players
        players = self.get_players(season)
        counts['players'] = len(players)
        
        # Collect games
        games = self.get_games(season)
        counts['games'] = len(games)
        
        # Collect player stats for each game
        total_stats = 0
        for i, game in enumerate(games):
            logger.info(f"Collecting stats for game {i+1}/{len(games)}: {game['game_id']}")
            
            game_stats = self.get_player_game_stats(game['game_id'])
            total_stats += len(game_stats)
            
            if save_to_db:
                # Save game to database
                db_manager.insert_game(game)
                
                # Save player stats to database
                for stat in game_stats:
                    db_manager.insert_player_stats(stat)
        
        counts['player_stats'] = total_stats
        
        logger.info(f"Data collection completed for {season}: {counts}")
        return counts
    
    def save_to_csv(self, data: List[Dict], filename: str, output_dir: str = "data/raw"):
        """Save data to CSV file.
        
        Args:
            data: List of dictionaries to save
            filename: Output filename
            output_dir: Output directory
        """
        if not data:
            logger.warning(f"No data to save for {filename}")
            return
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        file_path = output_path / filename
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)
        logger.info(f"Saved {len(data)} records to {file_path}")


# Example usage
if __name__ == "__main__":
    collector = NBADataCollector()
    
    # Test with a small dataset
    teams = collector.get_teams()
    print(f"Retrieved {len(teams)} teams")
    
    # Save teams to CSV
    collector.save_to_csv(teams, "nba_teams.csv") 