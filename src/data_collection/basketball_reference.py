"""
Basketball Reference data collector for historical game and player data.
"""

import logging
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from pathlib import Path
from bs4 import BeautifulSoup
import re

from ..utils.config import config
from ..utils.database import db_manager

# Set up logging
logger = logging.getLogger(__name__)


class BasketballReferenceCollector:
    """Collector for Basketball Reference data."""
    
    def __init__(self):
        """Initialize the Basketball Reference collector."""
        self.base_url = config.get('data_sources.basketball_reference.base_url')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
    def _make_request(self, url: str, params: Dict[str, Any] = None) -> Optional[BeautifulSoup]:
        """Make a request to Basketball Reference.
        
        Args:
            url: Full URL to request
            params: Query parameters
            
        Returns:
            BeautifulSoup object or None if failed
        """
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            # Add delay to be respectful
            time.sleep(config.get('scraping.delay_between_requests', 2.0))
            
            return BeautifulSoup(response.content, 'html.parser')
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in request: {e}")
            return None
    
    def get_teams(self, season: str = "2024") -> List[Dict[str, Any]]:
        """Get all NBA teams for a season.
        
        Args:
            season: NBA season (e.g., "2024" for 2023-24 season)
            
        Returns:
            List of team dictionaries
        """
        url = f"{self.base_url}/leagues/NBA_{season}.html"
        soup = self._make_request(url)
        
        if not soup:
            return []
        
        teams = []
        
        try:
            # Find the team stats table
            table = soup.find('table', {'id': 'per_game-team'})
            if not table:
                logger.warning("Team stats table not found")
                return []
            
            rows = table.find('tbody').find_all('tr')
            
            for row in rows:
                # Skip header rows
                if 'thead' in row.get('class', []):
                    continue
                
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue
                
                team_name = cells[1].get_text(strip=True)
                team_abbr = cells[1].find('a')['href'].split('/')[-2] if cells[1].find('a') else team_name[:3].upper()
                
                team = {
                    'team_id': team_abbr,
                    'team_name': team_name,
                    'team_abbreviation': team_abbr,
                    'season': season
                }
                teams.append(team)
                
        except Exception as e:
            logger.error(f"Error parsing teams: {e}")
        
        logger.info(f"Retrieved {len(teams)} teams for {season}")
        return teams
    
    def _normalize_team_name(self, team_name: str) -> str:
        """Normalize team name for consistent mapping.
        
        Args:
            team_name: Raw team name
            
        Returns:
            Normalized team name
        """
        # Remove asterisks and extra whitespace
        normalized = team_name.replace('*', '').strip()
        return normalized
    
    def get_players(self, season: str = "2024") -> List[Dict[str, Any]]:
        """Get all NBA players for a season.
        
        Args:
            season: NBA season (e.g., "2024" for 2023-24 season)
            
        Returns:
            List of player dictionaries
        """
        # Try different approaches to get player data
        players = []
        
        # Approach 1: Try the main league page with different table IDs
        url = f"{self.base_url}/leagues/NBA_{season}.html"
        soup = self._make_request(url)
        
        if soup:
            players = self._extract_players_from_page(soup, season)
        
        # Approach 2: If no players found, try team pages
        if not players:
            logger.info("No players found on main page, trying team pages...")
            players = self._get_players_from_team_pages(season)
        
        logger.info(f"Retrieved {len(players)} players for {season}")
        return players
    
    def _extract_players_from_page(self, soup: BeautifulSoup, season: str) -> List[Dict[str, Any]]:
        """Extract players from a page."""
        players = []
        
        try:
            # Try different common table IDs for player stats
            table_ids = ['per_game_stats', 'stats_per_game', 'per_game', 'player_stats', 'stats']
            table = None
            
            for table_id in table_ids:
                table = soup.find('table', {'id': table_id})
                if table:
                    logger.info(f"Found player stats table with ID: {table_id}")
                    break
            
            if not table:
                # Try to find any table that might contain player data
                tables = soup.find_all('table')
                for t in tables:
                    if t.find('tbody') and t.find('tbody').find_all('tr'):
                        first_row = t.find('tbody').find_all('tr')[0]
                        if first_row.find('a'):  # Has links, might be player data
                            table = t
                            logger.info("Found potential player table")
                            break
            
            if not table:
                return []
            
            rows = table.find('tbody').find_all('tr')
            
            for row in rows:
                # Skip header rows
                if 'thead' in row.get('class', []):
                    continue
                
                cells = row.find_all(['td', 'th'])
                if len(cells) < 5:
                    continue
                
                # Get player name and link
                name_cell = cells[0]
                player_name = name_cell.get_text(strip=True)
                player_link = name_cell.find('a')
                
                # Only add players with valid links (skip summary rows, etc.)
                if not player_link or not player_link.get('href'):
                    continue
                
                # Extract correct player ID from href
                href = player_link.get('href')
                # Extract player ID from URL like /players/j/jamesle01.html
                player_id = href.split('/')[-1].replace('.html', '')
                
                # Get team
                team = cells[1].get_text(strip=True) if len(cells) > 1 else "Unknown"
                
                # Get position
                position = cells[2].get_text(strip=True) if len(cells) > 2 else "Unknown"
                
                player = {
                    'player_id': player_id,
                    'full_name': player_name,
                    'team_name': team,
                    'position': position,
                    'season': season
                }
                players.append(player)
                
        except Exception as e:
            logger.error(f"Error parsing players: {e}")
        
        return players
    
    def _get_players_from_team_pages(self, season: str) -> List[Dict[str, Any]]:
        """Get players by visiting individual team pages."""
        players = []
        
        # Get teams first
        teams = self.get_teams(season)
        
        for team in teams[:5]:  # Limit to first 5 teams for testing
            try:
                team_abbr = team['team_abbreviation']
                url = f"{self.base_url}/teams/{team_abbr}/{season}.html"
                soup = self._make_request(url)
                
                if soup:
                    team_players = self._extract_players_from_page(soup, season)
                    for player in team_players:
                        player['team_name'] = team['team_name']
                    players.extend(team_players)
                    
            except Exception as e:
                logger.error(f"Error getting players for team {team['team_name']}: {e}")
                continue
        
        return players
    
    def get_games(self, season: str = "2024") -> List[Dict[str, Any]]:
        """Get all NBA games for a season.
        
        Args:
            season: NBA season (e.g., "2024" for 2023-24 season)
            
        Returns:
            List of game dictionaries
        """
        url = f"{self.base_url}/leagues/NBA_{season}_games.html"
        soup = self._make_request(url)
        
        if not soup:
            return []
        
        games = []
        
        try:
            # Find the games table
            table = soup.find('table', {'id': 'schedule'})
            if not table:
                logger.warning("Games table not found")
                return []
            
            rows = table.find('tbody').find_all('tr')
            
            for row in rows:
                # Skip header rows
                if 'thead' in row.get('class', []):
                    continue
                
                cells = row.find_all(['td', 'th'])
                if len(cells) < 8:
                    continue
                
                try:
                    # Parse date
                    date_text = cells[0].get_text(strip=True)
                    game_date = datetime.strptime(date_text, '%a, %b %d, %Y')
                    
                    # Get teams
                    away_team = cells[2].get_text(strip=True)
                    home_team = cells[4].get_text(strip=True)
                    
                    # Get scores
                    away_score = int(cells[3].get_text(strip=True)) if cells[3].get_text(strip=True) else None
                    home_score = int(cells[5].get_text(strip=True)) if cells[5].get_text(strip=True) else None
                    
                    # Create game ID
                    game_id = f"BR_{away_team}_{home_team}_{game_date.strftime('%Y%m%d')}"
                    
                    game = {
                        'game_id': game_id,
                        'date': game_date,
                        'away_team_name': away_team,
                        'home_team_name': home_team,
                        'away_score': away_score,
                        'home_score': home_score,
                        'season': season,
                        'league': 'NBA'
                    }
                    games.append(game)
                    
                except (ValueError, IndexError) as e:
                    logger.warning(f"Error parsing game row: {e}")
                    continue
                
        except Exception as e:
            logger.error(f"Error parsing games: {e}")
        
        logger.info(f"Retrieved {len(games)} games for {season}")
        return games
    
    def get_player_game_stats(self, game_url: str) -> List[Dict[str, Any]]:
        """Get player statistics for a specific game.
        
        Args:
            game_url: URL to the game's box score page
            
        Returns:
            List of player statistics dictionaries
        """
        soup = self._make_request(game_url)
        
        if not soup:
            return []
        
        stats = []
        
        try:
            # Find both team box scores
            box_scores = soup.find_all('table', {'class': 'stats_table'})
            
            for box_score in box_scores:
                # Check if this is a player stats table
                caption = box_score.find('caption')
                if not caption or 'Basic Stats' not in caption.get_text():
                    continue
                
                rows = box_score.find('tbody').find_all('tr')
                
                for row in rows:
                    # Skip header rows
                    if 'thead' in row.get('class', []):
                        continue
                    
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < 10:
                        continue
                    
                    try:
                        # Extract player stats
                        player_name = cells[0].get_text(strip=True)
                        minutes = self._parse_minutes(cells[1].get_text(strip=True))
                        points = int(cells[26].get_text(strip=True)) if cells[26].get_text(strip=True) else 0
                        rebounds = int(cells[20].get_text(strip=True)) if cells[20].get_text(strip=True) else 0
                        assists = int(cells[21].get_text(strip=True)) if cells[21].get_text(strip=True) else 0
                        
                        # Get team from the table context
                        team = "Unknown"  # Would need to parse from table context
                        
                        stat = {
                            'player_name': player_name,
                            'team_name': team,
                            'minutes_played': minutes,
                            'points': points,
                            'rebounds': rebounds,
                            'assists': assists,
                            'game_url': game_url
                        }
                        stats.append(stat)
                        
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Error parsing player stat row: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"Error parsing game stats: {e}")
        
        return stats
    
    def _parse_minutes(self, minutes_str: str) -> Optional[float]:
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
    
    def collect_season_data(self, season: str = "2024", save_to_db: bool = True) -> Dict[str, int]:
        """Collect complete season data including teams, players, and games.
        
        Args:
            season: NBA season to collect
            save_to_db: Whether to save data to database
            
        Returns:
            Dictionary with counts of collected data
        """
        logger.info(f"Starting Basketball Reference data collection for {season}")
        
        counts = {
            'teams': 0,
            'players': 0,
            'games': 0,
            'player_stats': 0
        }
        
        # Collect teams
        teams = self.get_teams(season)
        counts['teams'] = len(teams)
        
        # Create team name to ID mapping (normalized)
        team_mapping = {self._normalize_team_name(team['team_name']): team['team_id'] for team in teams}
        
        # Collect players
        players = self.get_players(season)
        counts['players'] = len(players)
        
        # Collect games
        games = self.get_games(season)
        counts['games'] = len(games)
        
        # Save to database if requested
        if save_to_db:
            for team in teams:
                team_data = {
                    'team_id': team['team_id'],
                    'team_name': team['team_name'],
                    'team_abbreviation': team['team_abbreviation'],
                    'league': 'NBA'
                }
                db_manager.insert_team(team_data)
            
            for player in players:
                player_data = {
                    'player_id': player['player_id'],
                    'full_name': player['full_name'],
                    'team_id': team_mapping.get(self._normalize_team_name(player.get('team_name', '')), None),
                    'team_name': player.get('team_name', None),
                    'position': player.get('position', None),
                    'league': 'NBA',
                    'active': True
                }
                db_manager.insert_player(player_data)
            
            for game in games:
                game_data = {
                    'game_id': game['game_id'],
                    'date': game['date'],
                    'home_team_id': team_mapping.get(self._normalize_team_name(game['home_team_name']), None),
                    'away_team_id': team_mapping.get(self._normalize_team_name(game['away_team_name']), None),
                    'home_team_name': game['home_team_name'],
                    'away_team_name': game['away_team_name'],
                    'home_score': game['home_score'],
                    'away_score': game['away_score'],
                    'season': game['season'],
                    'league': game.get('league', 'NBA')
                }
                db_manager.insert_game(game_data)
        
        logger.info(f"Basketball Reference data collection completed for {season}: {counts}")
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
    collector = BasketballReferenceCollector()
    
    # Test with a small dataset
    teams = collector.get_teams("2024")
    print(f"Retrieved {len(teams)} teams")
    
    # Save teams to CSV
    collector.save_to_csv(teams, "br_teams_2024.csv") 