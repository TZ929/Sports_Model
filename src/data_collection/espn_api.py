"""
ESPN API data collector for NBA statistics.
"""

import requests
import time
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from src.utils.database import db_manager
from sqlalchemy import text
from bs4 import BeautifulSoup

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
        """Get NBA games for a season.
        
        Args:
            season: NBA season (e.g., "2024" for 2023-24 season)
            
        Returns:
            List of game dictionaries
        """
        url = f"{self.base_url}/scoreboard"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            games = []
            events = data.get('events', [])
            
            for event in events:
                try:
                    # Get team info
                    competitions = event.get('competitions', [{}])[0]
                    competitors = competitions.get('competitors', [])
                    
                    if len(competitors) != 2:
                        continue
                    
                    home_team = None
                    away_team = None
                    
                    for competitor in competitors:
                        if competitor.get('homeAway') == 'home':
                            home_team = competitor.get('team', {})
                        else:
                            away_team = competitor.get('team', {})
                    
                    if not home_team or not away_team:
                        continue
                    
                    # Get scores
                    home_score = home_team.get('score', '0')
                    away_score = away_team.get('score', '0')
                    
                    # Get game date
                    date_str = event.get('date', '')
                    game_date = datetime.fromisoformat(date_str.replace('Z', '+00:00')) if date_str else datetime.now()
                    
                    game_info = {
                        'game_id': f"ESPN_{event.get('id', '')}",
                        'game_date': game_date,
                        'home_team_id': home_team.get('abbreviation', ''),
                        'away_team_id': away_team.get('abbreviation', ''),
                        'home_team_name': home_team.get('name', ''),
                        'away_team_name': away_team.get('name', ''),
                        'home_score': int(home_score) if home_score.isdigit() else 0,
                        'away_score': int(away_score) if away_score.isdigit() else 0,
                        'season': season,
                        'league': 'NBA'
                    }
                    games.append(game_info)
                    
                except Exception as e:
                    logger.error(f"Error parsing game: {e}")
                    continue
            
            logger.info(f"Retrieved {len(games)} games from ESPN API")
            return games
            
        except Exception as e:
            logger.error(f"Error getting games from ESPN: {e}")
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
                # Rename 'game_date' to 'date' to match the Games table
                if 'game_date' in game_db:
                    game_db['date'] = game_db.pop('game_date')
                if db_manager.insert_game(game_db):
                    counts['games'] += 1
        
        logger.info(f"ESPN API collection completed: {counts}")
        return counts

    def get_player_game_stats(self, player_id: str, season: str = "2024") -> list:
        """Fetch game-by-game stats for a player from ESPN."""
        url = f"https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/athletes/{player_id}/stats"
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            stats = []
            
            # ESPN API structure: data['categories'][0]['events'] contains game-by-game stats
            for category in data.get('categories', []):
                for event in category.get('events', []):
                    event_id = event.get('eventId', '')
                    event_stats = event.get('stats', [])
                    
                    # ESPN stats array format (from glossary):
                    # [MIN, FG, FG%, 3PT, 3P%, FT, FT%, REB, AST, STL, BLK, TO, PF, PTS]
                    if len(event_stats) >= 14:
                        stat = {
                            'game_id': f"ESPN_{event_id}",
                            'player_id': player_id,
                            'team_id': '',  # Will need to get from team info
                            'minutes_played': int(event_stats[0]) if event_stats[0].isdigit() else 0,
                            'field_goals_made': int(event_stats[1].split('-')[0]) if '-' in event_stats[1] else 0,
                            'field_goals_attempted': int(event_stats[1].split('-')[1]) if '-' in event_stats[1] else 0,
                            'three_pointers_made': int(event_stats[3].split('-')[0]) if '-' in event_stats[3] else 0,
                            'three_pointers_attempted': int(event_stats[3].split('-')[1]) if '-' in event_stats[3] else 0,
                            'free_throws_made': int(event_stats[5].split('-')[0]) if '-' in event_stats[5] else 0,
                            'free_throws_attempted': int(event_stats[5].split('-')[1]) if '-' in event_stats[5] else 0,
                            'rebounds': int(event_stats[7]) if event_stats[7].isdigit() else 0,
                            'offensive_rebounds': 0,  # Not provided in ESPN API
                            'defensive_rebounds': 0,  # Not provided in ESPN API
                            'assists': int(event_stats[8]) if event_stats[8].isdigit() else 0,
                            'steals': int(event_stats[9]) if event_stats[9].isdigit() else 0,
                            'blocks': int(event_stats[10]) if event_stats[10].isdigit() else 0,
                            'turnovers': int(event_stats[11]) if event_stats[11].isdigit() else 0,
                            'personal_fouls': int(event_stats[12]) if event_stats[12].isdigit() else 0,
                            'points': int(event_stats[13]) if event_stats[13].isdigit() else 0,
                            'plus_minus': 0  # Not provided in ESPN API
                        }
                        stats.append(stat)
            
            logger.info(f"Retrieved {len(stats)} game stats for player {player_id}")
            return stats
        except Exception as e:
            logger.error(f"Error fetching game stats for player {player_id}: {e}")
            return []

    def collect_all_player_game_stats(self, season: str = "2024", limit: int = 10, save_to_db: bool = True):
        """Collect and store game stats for all players in the database (ESPN IDs)."""
        from src.utils.database import db_manager
        import time
        session = db_manager.get_session()
        # Get all non-BR player IDs and filter for numeric ones
        all_player_ids = [row[0] for row in session.execute(text("SELECT player_id FROM players WHERE player_id NOT LIKE 'BR_%' LIMIT :limit"), {'limit': limit * 2})]
        # Filter for valid ESPN player IDs (numeric only)
        player_ids = [pid for pid in all_player_ids if pid.isdigit()]
        session.close()
        total_stats = 0
        for player_id in player_ids:
            # Skip obviously invalid player IDs (like '2024' which is probably a team name)
            if player_id == '2024' or len(player_id) < 6:
                logger.info(f"Skipping invalid player ID: {player_id}")
                continue
                
            stats = self.get_player_game_stats(player_id, season)
            if save_to_db:
                for stat in stats:
                    db_manager.insert_player_stats(stat)
            total_stats += len(stats)
            time.sleep(1)
        logger.info(f"Collected and stored {total_stats} player game stats from ESPN.")

    def get_player_game_logs_web(self, player_id: str, player_name: str = "", season: str = "2024") -> List[Dict[str, Any]]:
        """Get game-by-game statistics for a specific player from ESPN web pages.
        
        Args:
            player_id: ESPN player ID
            player_name: Player name for URL (optional, will be derived if not provided)
            season: NBA season
            
        Returns:
            List of game statistics dictionaries
        """
        import requests
        from bs4 import BeautifulSoup
        import time
        
        # Construct the ESPN game log URL
        if player_name:
            # Convert player name to URL format (lowercase, hyphens)
            player_name_url = player_name.lower().replace(' ', '-')
            url = f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/{player_name_url}"
        else:
            # Try with just the ID
            url = f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the game log table
            # ESPN typically uses a table with class containing 'gamelog' or similar
            game_log_table = soup.find('table', class_=lambda x: x and 'gamelog' in x.lower() if x else False)
            
            if not game_log_table:
                # Try alternative table selectors
                game_log_table = soup.find('table', class_='Table')
            
            if not game_log_table:
                logger.warning(f"Game log table not found for player {player_id}")
                return []
            
            stats = []
            tbody = game_log_table.find('tbody')
            if not tbody:
                logger.warning(f"No tbody found in game log table for player {player_id}")
                return []
                
            rows = tbody.find_all('tr')
            
            for row in rows:
                # Skip header rows
                row_classes = row.get('class', [])
                if isinstance(row_classes, str):
                    row_classes = [row_classes]
                if 'thead' in row_classes:
                    continue
                
                cells = row.find_all(['td', 'th'])
                if len(cells) < 10:  # Need at least basic stats
                    continue
                
                try:
                    # Extract game information
                    # ESPN game log format typically: Date, OPP, RESULT, MIN, FG, FG%, 3PT, 3P%, FT, FT%, REB, AST, STL, BLK, TO, PF, PTS, +/- 
                    game_date = self._parse_espn_date(cells[0].get_text(strip=True))
                    if not game_date:
                        continue
                    
                    opponent = cells[1].get_text(strip=True)
                    result = cells[2].get_text(strip=True)
                    minutes = self._parse_minutes(cells[3].get_text(strip=True))
                    
                    # Parse shooting stats (FG, 3PT, FT)
                    fg_str = cells[4].get_text(strip=True)  # e.g., "8-15"
                    fg_made, fg_attempted = self._parse_shot_attempts(fg_str)
                    
                    three_pt_str = cells[6].get_text(strip=True)  # e.g., "2-5"
                    three_made, three_attempted = self._parse_shot_attempts(three_pt_str)
                    
                    ft_str = cells[8].get_text(strip=True)  # e.g., "4-6"
                    ft_made, ft_attempted = self._parse_shot_attempts(ft_str)
                    
                    # Other stats
                    rebounds = int(cells[10].get_text(strip=True)) if cells[10].get_text(strip=True).isdigit() else 0
                    assists = int(cells[11].get_text(strip=True)) if cells[11].get_text(strip=True).isdigit() else 0
                    steals = int(cells[12].get_text(strip=True)) if cells[12].get_text(strip=True).isdigit() else 0
                    blocks = int(cells[13].get_text(strip=True)) if cells[13].get_text(strip=True).isdigit() else 0
                    turnovers = int(cells[14].get_text(strip=True)) if cells[14].get_text(strip=True).isdigit() else 0
                    personal_fouls = int(cells[15].get_text(strip=True)) if cells[15].get_text(strip=True).isdigit() else 0
                    points = int(cells[16].get_text(strip=True)) if cells[16].get_text(strip=True).isdigit() else 0
                    
                    # Plus/minus (if available)
                    plus_minus = 0
                    if len(cells) > 17:
                        pm_text = cells[17].get_text(strip=True)
                        if pm_text and pm_text != '-':
                            try:
                                plus_minus = int(pm_text)
                            except ValueError:
                                plus_minus = 0
                    
                    # Create game stat record
                    game_stat = {
                        'game_id': f"ESPN_{player_id}_{game_date.strftime('%Y%m%d')}",
                        'player_id': player_id,
                        'game_date': game_date,
                        'opponent': opponent,
                        'result': result,
                        'minutes_played': minutes,
                        'field_goals_made': fg_made,
                        'field_goals_attempted': fg_attempted,
                        'three_pointers_made': three_made,
                        'three_pointers_attempted': three_attempted,
                        'free_throws_made': ft_made,
                        'free_throws_attempted': ft_attempted,
                        'rebounds': rebounds,
                        'assists': assists,
                        'steals': steals,
                        'blocks': blocks,
                        'turnovers': turnovers,
                        'personal_fouls': personal_fouls,
                        'points': points,
                        'plus_minus': plus_minus,
                        'season': season
                    }
                    
                    stats.append(game_stat)
                    
                except (ValueError, IndexError) as e:
                    logger.warning(f"Error parsing game stat row for {player_id}: {e}")
                    continue
            
            logger.info(f"Retrieved {len(stats)} game stats for player {player_id}")
            return stats
            
        except Exception as e:
            logger.error(f"Error getting game logs for player {player_id}: {e}")
            return []
    
    def _parse_espn_date(self, date_str: str) -> Optional[datetime]:
        """Parse ESPN date string to datetime object.
        
        Args:
            date_str: Date string from ESPN (e.g., "Oct 25, 2023")
            
        Returns:
            Datetime object or None if invalid
        """
        if not date_str or date_str == '':
            return None
        
        try:
            # ESPN format: "Oct 25, 2023"
            return datetime.strptime(date_str, '%b %d, %Y')
        except ValueError:
            try:
                # Alternative format: "10/25/2023"
                return datetime.strptime(date_str, '%m/%d/%Y')
            except ValueError:
                logger.warning(f"Could not parse date: {date_str}")
                return None
    
    def _parse_shot_attempts(self, shot_str: str) -> tuple:
        """Parse shot attempts string (e.g., "8-15") to made and attempted.
        
        Args:
            shot_str: Shot string in format "made-attempted"
            
        Returns:
            Tuple of (made, attempted) or (0, 0) if invalid
        """
        if not shot_str or shot_str == '' or shot_str == '-':
            return (0, 0)
        
        try:
            if '-' in shot_str:
                made, attempted = shot_str.split('-')
                return (int(made), int(attempted))
            else:
                # Single number (probably made)
                return (int(shot_str), 0)
        except (ValueError, IndexError):
            return (0, 0)

    def collect_player_game_logs_web(self, season: str = "2024", limit: int = 10, save_to_db: bool = True):
        """Collect game logs for players using ESPN web scraping."""
        from src.utils.database import db_manager
        from sqlalchemy import text
        import time
        
        session = db_manager.get_session()
        
        # Get valid ESPN player IDs with names
        query = text("""
            SELECT player_id, full_name FROM players 
            WHERE player_id NOT LIKE 'BR_%' 
            AND player_id REGEXP '^[0-9]+$'
            AND full_name IS NOT NULL
            LIMIT :limit
        """)
        
        result = session.execute(query, {'limit': limit})
        players = result.fetchall()
        session.close()
        
        total_stats = 0
        successful_players = 0
        
        for i, (player_id, player_name) in enumerate(players):
            try:
                logger.info(f"Collecting game logs for player {i+1}/{len(players)}: {player_name} ({player_id})")
                
                stats = self.get_player_game_logs_web(player_id, player_name, season)
                
                if stats:
                    total_stats += len(stats)
                    successful_players += 1
                    
                    if save_to_db:
                        for stat in stats:
                            # Convert to database format
                            db_stat = {
                                'game_id': stat['game_id'],
                                'player_id': stat['player_id'],
                                'game_date': stat['game_date'],
                                'minutes_played': stat['minutes_played'],
                                'field_goals_made': stat['field_goals_made'],
                                'field_goals_attempted': stat['field_goals_attempted'],
                                'three_pointers_made': stat['three_pointers_made'],
                                'three_pointers_attempted': stat['three_pointers_attempted'],
                                'free_throws_made': stat['free_throws_made'],
                                'free_throws_attempted': stat['free_throws_attempted'],
                                'rebounds': stat['rebounds'],
                                'assists': stat['assists'],
                                'steals': stat['steals'],
                                'blocks': stat['blocks'],
                                'turnovers': stat['turnovers'],
                                'personal_fouls': stat['personal_fouls'],
                                'points': stat['points'],
                                'plus_minus': stat['plus_minus']
                            }
                            db_manager.insert_player_stats(db_stat)
                
                # Be respectful with requests
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Error collecting game logs for {player_name} ({player_id}): {e}")
                continue
        
        logger.info(f"Web scraping completed: {successful_players} players, {total_stats} game stats collected")
        return {'players': successful_players, 'stats': total_stats} 