"""
ESPN API data collector for NBA statistics.
"""

import requests
import time
import logging
from typing import List, Dict, Any
from datetime import datetime
from src.utils.database import db_manager
import calendar

logger = logging.getLogger(__name__)

class ESPNAPICollector:
    """Collect NBA data from ESPN API."""
    
    def __init__(self):
        self.base_url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def get_teams(self, season: str = "2024") -> List[Dict[str, Any]]:
        """Get all NBA teams for a season.
        
        Args:
            season: NBA season (e.g., "2024" for 2023-24 season)
            
        Returns:
            List of team dictionaries
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
                    'team_id': team.get('abbreviation', ''),
                    'team_name': team.get('name', ''),
                    'team_abbreviation': team.get('abbreviation', ''),
                    'league': 'NBA',
                    'season': season
                }
                teams.append(team_info)
            
            logger.info(f"Retrieved {len(teams)} teams from ESPN API")
            return teams
            
        except Exception as e:
            logger.error(f"Error getting teams from ESPN: {e}")
            return []
    
    def get_players(self, season: str = "2024") -> List[Dict[str, Any]]:
        """Get all NBA players for a season.
        
        Args:
            season: NBA season (e.g., "2024" for 2023-24 season)
            
        Returns:
            List of player dictionaries
        """
        # ESPN API doesn't have a direct endpoint for all players
        # We'll get players by team
        teams = self.get_teams(season)
        all_players = []
        
        for team in teams[:5]:  # Limit to first 5 teams for testing
            try:
                team_abbr = team['team_abbreviation']
                url = f"{self.base_url}/teams/{team_abbr}/roster"
                
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                athletes = data.get('athletes', [])
                for athlete in athletes:
                    player_info = {
                        'player_id': athlete.get('id', ''),
                        'full_name': athlete.get('fullName', ''),
                        'team_name': team['team_name'],
                        'position': athlete.get('position', {}).get('abbreviation', ''),
                        'season': season
                    }
                    all_players.append(player_info)
                
                time.sleep(1)  # Be respectful
                
            except Exception as e:
                logger.error(f"Error getting players for team {team['team_name']}: {e}")
                continue
        
        logger.info(f"Retrieved {len(all_players)} players from ESPN API")
        return all_players
    
    def get_games(self, season: str = "2024") -> List[Dict[str, Any]]:
        """Get NBA games for a season by iterating through the season dates.
        
        Args:
            season (str): NBA season (e.g., "2024" for 2023-24 season)
            
        Returns:
            List of game dictionaries
        """
        all_games = []
        season_start_year = int(season) - 1
        season_year = int(season)
        
        # Iterate from October (start of season) to June of the next year (end of season)
        # Months for the first part of the season (e.g., Oct, Nov, Dec 2023)
        for month in range(10, 13):
            num_days = calendar.monthrange(season_start_year, month)[1]
            for day in range(1, num_days + 1):
                date_str = f"{season_start_year}{month:02d}{day:02d}"
                url = f"{self.base_url}/scoreboard?dates={date_str}"
                all_games.extend(self._fetch_games_from_url(url, season))
                time.sleep(0.5) # Be respectful to the API

        # Months for the second part of the season (e.g., Jan-Jun 2024)
        for month in range(1, 7):
            num_days = calendar.monthrange(season_year, month)[1]
            for day in range(1, num_days + 1):
                date_str = f"{season_year}{month:02d}{day:02d}"
                url = f"{self.base_url}/scoreboard?dates={date_str}"
                all_games.extend(self._fetch_games_from_url(url, season))
                time.sleep(0.5) # Be respectful to the API
            
        logger.info(f"Retrieved {len(all_games)} games from ESPN API for the {season} season.")
        return all_games

    def _fetch_games_from_url(self, url: str, season: str) -> List[Dict[str, Any]]:
        """Helper to fetch and parse games from a specific scoreboard URL."""
        retries = 3
        for i in range(retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                games = []
                events = data.get('events', [])
                
                for event in events:
                    try:
                        competitions = event.get('competitions', [{}])[0]
                        competitors = competitions.get('competitors', [])
                        
                        if len(competitors) != 2:
                            continue
                        
                        home_team, away_team = None, None
                        for competitor in competitors:
                            if competitor.get('homeAway') == 'home':
                                home_team = competitor.get('team', {})
                            else:
                                away_team = competitor.get('team', {})
                        
                        if not home_team or not away_team:
                            continue
                        
                        date_str = event.get('date', '')
                        game_date = datetime.fromisoformat(date_str.replace('Z', '+00:00')) if date_str else datetime.now()
                        
                        game_info = {
                            'game_id': event.get('id', ''),
                            'date': game_date,
                            'home_team_id': home_team.get('abbreviation', ''),
                            'away_team_id': away_team.get('abbreviation', ''),
                            'home_team_name': home_team.get('name', ''),
                            'away_team_name': away_team.get('name', ''),
                            'home_score': int(home_team.get('score', '0')),
                            'away_score': int(away_team.get('score', '0')),
                            'season': season,
                            'league': 'NBA'
                        }
                        games.append(game_info)
                        
                    except Exception as e:
                        logger.error(f"Error parsing a game event: {e}")
                        continue
                
                return games
                
            except Exception as e:
                logger.warning(f"Attempt {i+1} failed for URL {url}: {e}")
                if i < retries - 1:
                    time.sleep(5)  # Wait 5 seconds before retrying
                else:
                    logger.error(f"Error getting games from URL {url} after {retries} retries: {e}")
                    return []
        return []
    
    def get_player_stats(self, player_id: str, season: str = "2024") -> List[Dict[str, Any]]:
        """Get game-by-game statistics for a specific player.
        
        Args:
            player_id: ESPN player ID
            season: NBA season
            
        Returns:
            List of game statistics dictionaries
        """
        url = f"{self.base_url}/athletes/{player_id}/stats"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            stats = []
            # ESPN API structure may vary, this is a basic implementation
            # You might need to adjust based on actual API response
            
            logger.info(f"Retrieved stats for player {player_id}")
            return stats
            
        except Exception as e:
            logger.error(f"Error getting stats for player {player_id}: {e}")
            return []
    
    def collect_season_data(self, season: str = "2024", save_to_db: bool = True) -> Dict[str, int]:
        """Collect complete season data including teams, players, and games.
        
        Args:
            season: NBA season to collect
            save_to_db: Whether to save data to database
            
        Returns:
            Dictionary with counts of collected data
        """
        logger.info(f"Starting ESPN API data collection for {season}")
        
        counts = {
            'teams': 0,
            'players': 0,
            'games': 0,
            'player_stats': 0
        }
        
        # Collect teams
        teams = self.get_teams(season)
        if save_to_db:
            for team in teams:
                team_db = dict(team)
                team_db.pop('season', None)  # Remove 'season' key if present
                if db_manager.insert_team(team_db):
                    counts['teams'] += 1
        
        # Collect players
        players = self.get_players(season)
        if save_to_db:
            for player in players:
                player_db = dict(player)
                player_db.pop('season', None)  # Remove 'season' key if present
                if db_manager.insert_player(player_db):
                    counts['players'] += 1
        
        # Collect games
        games = self.get_games(season)
        if save_to_db:
            for game in games:
                game_db = dict(game)
                if db_manager.insert_game(game_db):
                    counts['games'] += 1
        
        # Collect player stats (now by game)
        # This part will be orchestrated by the new script
        
        logger.info(f"ESPN API data collection summary for {season}: {counts}")
        return counts

    def get_player_game_stats(self, player_id: str, season: str = "2024") -> list:
        """
        [DEPRECATED] This method is not reliable for comprehensive data collection.
        It is kept for legacy purposes but should not be used for new development.
        """
        logger.warning("The 'get_player_game_stats' method is deprecated and should not be used.")
        return []

    def collect_all_player_game_stats(self, season: str = "2024", limit: int = 10, save_to_db: bool = True):
        """
        [DEPRECATED] This method is not reliable for comprehensive data collection.
        It is kept for legacy purposes but should not be used for new development.
        """
        logger.warning("The 'collect_all_player_game_stats' method is deprecated and should not be used.")
        pass

    def get_box_score(self, game_id: str) -> List[Dict[str, Any]]:
        """Get box score for a specific game.
        
        Args:
            game_id: ESPN game ID
            
        Returns:
            List of player stats dictionaries
        """
        url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={game_id}"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            player_stats = []
            
            # The player data is nested inside the 'boxscore' -> 'players' structure
            boxscore_players = data.get('boxscore', {}).get('players', [])

            for team_stats in boxscore_players:
                team_info = team_stats.get('team', {})
                if not team_info:
                    continue
                
                team_id = team_info.get('id')

                for player_data in team_stats.get('statistics', []):
                    # The first item in statistics is the labels, not a player
                    labels = [label.lower() for label in player_data.get('labels', [])]
                    
                    for athlete_stats in player_data.get('athletes', []):
                        try:
                            athlete_info = athlete_stats.get('athlete', {})
                            player_id = athlete_info.get('id')

                            if not player_id or athlete_stats.get('didNotPlay'):
                                continue

                            stats_values = athlete_stats.get('stats', [])
                            stats_dict = dict(zip(labels, stats_values))

                            fgm_fga = stats_dict.get('fg', '0-0').split('-')
                            tpm_tpa = stats_dict.get('3pt', '0-0').split('-')
                            ftm_fta = stats_dict.get('ft', '0-0').split('-')

                            player_entry = {
                                'game_id': game_id,
                                'player_id': player_id,
                                'team_id': team_id,
                                'minutes_played': int(stats_dict.get('min', 0)),
                                'field_goals_made': int(fgm_fga[0]),
                                'field_goals_attempted': int(fgm_fga[1]),
                                'three_pointers_made': int(tpm_tpa[0]),
                                'three_pointers_attempted': int(tpm_tpa[1]),
                                'free_throws_made': int(ftm_fta[0]),
                                'free_throws_attempted': int(ftm_fta[1]),
                                'rebounds': int(stats_dict.get('reb', 0)),
                                'offensive_rebounds': int(stats_dict.get('oreb', 0)),
                                'assists': int(stats_dict.get('ast', 0)),
                                'steals': int(stats_dict.get('stl', 0)),
                                'blocks': int(stats_dict.get('blk', 0)),
                                'turnovers': int(stats_dict.get('to', 0)),
                                'personal_fouls': int(stats_dict.get('pf', 0)),
                                'points': int(stats_dict.get('pts', 0)),
                                'plus_minus': int(stats_dict.get('+/-', 0))
                            }
                            player_stats.append(player_entry)
                        except (ValueError, IndexError) as e:
                            logger.error(f"Error parsing stats for player in game {game_id}: {e}", exc_info=True)
                            continue
            return player_stats
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request error for game {game_id}: {e}")
            return []
        except Exception as e:
            logger.error(f"An unexpected error occurred processing game {game_id}: {e}", exc_info=True)
            return [] 