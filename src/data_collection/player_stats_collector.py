"""
Player game statistics collector for Basketball Reference.
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


class PlayerStatsCollector:
    """Collector for individual player game statistics."""
    
    def __init__(self):
        """Initialize the player stats collector."""
        self.base_url = config.get('data_sources.basketball_reference.base_url')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
    def _make_request(self, url: str) -> Optional[BeautifulSoup]:
        """Make a request to Basketball Reference.
        
        Args:
            url: Full URL to request
            
        Returns:
            BeautifulSoup object or None if failed
        """
        try:
            response = self.session.get(url, timeout=30)
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
    
    def get_player_game_logs(self, player_id: str, season: str = "2024") -> List[Dict[str, Any]]:
        """Get game-by-game statistics for a specific player.
        
        Args:
            player_id: Player identifier from Basketball Reference
            season: NBA season (e.g., "2024" for 2023-24 season)
            
        Returns:
            List of game statistics dictionaries
        """
        # Basketball Reference uses the year the season ends
        # So "2024" means 2023-24 season
        url = f"{self.base_url}/players/{player_id}/gamelog/{season}/"
        soup = self._make_request(url)
        
        if not soup:
            return []
        
        game_stats = []
        
        try:
            # Find the game log table
            table = soup.find('table', {'id': 'pgl_basic'})
            if not table:
                logger.warning(f"Game log table not found for player {player_id}")
                return []
            
            rows = table.find('tbody').find_all('tr')
            
            for row in rows:
                # Skip header rows and non-game rows
                if 'thead' in row.get('class', []) or 'full_table' in row.get('class', []):
                    continue
                
                cells = row.find_all(['td', 'th'])
                if len(cells) < 15:
                    continue
                
                try:
                    # Extract game information
                    game_date = self._parse_date(cells[0].get_text(strip=True))
                    if not game_date:
                        continue
                    
                    # Get opponent and game result
                    opponent = cells[3].get_text(strip=True)
                    result = cells[4].get_text(strip=True)
                    
                    # Extract basic stats
                    minutes = self._parse_minutes(cells[5].get_text(strip=True))
                    fg_made = int(cells[6].get_text(strip=True)) if cells[6].get_text(strip=True) else 0
                    fg_attempted = int(cells[7].get_text(strip=True)) if cells[7].get_text(strip=True) else 0
                    fg_pct = float(cells[8].get_text(strip=True)) if cells[8].get_text(strip=True) else 0.0
                    
                    # Three pointers
                    three_made = int(cells[9].get_text(strip=True)) if cells[9].get_text(strip=True) else 0
                    three_attempted = int(cells[10].get_text(strip=True)) if cells[10].get_text(strip=True) else 0
                    three_pct = float(cells[11].get_text(strip=True)) if cells[11].get_text(strip=True) else 0.0
                    
                    # Free throws
                    ft_made = int(cells[12].get_text(strip=True)) if cells[12].get_text(strip=True) else 0
                    ft_attempted = int(cells[13].get_text(strip=True)) if cells[13].get_text(strip=True) else 0
                    ft_pct = float(cells[14].get_text(strip=True)) if cells[14].get_text(strip=True) else 0.0
                    
                    # Rebounds
                    rebounds = int(cells[15].get_text(strip=True)) if cells[15].get_text(strip=True) else 0
                    offensive_rebounds = int(cells[16].get_text(strip=True)) if cells[16].get_text(strip=True) else 0
                    defensive_rebounds = int(cells[17].get_text(strip=True)) if cells[17].get_text(strip=True) else 0
                    
                    # Other stats
                    assists = int(cells[18].get_text(strip=True)) if cells[18].get_text(strip=True) else 0
                    steals = int(cells[19].get_text(strip=True)) if cells[19].get_text(strip=True) else 0
                    blocks = int(cells[20].get_text(strip=True)) if cells[20].get_text(strip=True) else 0
                    turnovers = int(cells[21].get_text(strip=True)) if cells[21].get_text(strip=True) else 0
                    personal_fouls = int(cells[22].get_text(strip=True)) if cells[22].get_text(strip=True) else 0
                    points = int(cells[23].get_text(strip=True)) if cells[23].get_text(strip=True) else 0
                    plus_minus = int(cells[24].get_text(strip=True)) if cells[24].get_text(strip=True) else 0
                    
                    # Create game stat record
                    game_stat = {
                        'player_id': player_id,
                        'game_date': game_date,
                        'opponent': opponent,
                        'result': result,
                        'minutes_played': minutes,
                        'field_goals_made': fg_made,
                        'field_goals_attempted': fg_attempted,
                        'field_goal_percentage': fg_pct,
                        'three_pointers_made': three_made,
                        'three_pointers_attempted': three_attempted,
                        'three_point_percentage': three_pct,
                        'free_throws_made': ft_made,
                        'free_throws_attempted': ft_attempted,
                        'free_throw_percentage': ft_pct,
                        'rebounds': rebounds,
                        'offensive_rebounds': offensive_rebounds,
                        'defensive_rebounds': defensive_rebounds,
                        'assists': assists,
                        'steals': steals,
                        'blocks': blocks,
                        'turnovers': turnovers,
                        'personal_fouls': personal_fouls,
                        'points': points,
                        'plus_minus': plus_minus,
                        'season': season
                    }
                    
                    game_stats.append(game_stat)
                    
                except (ValueError, IndexError) as e:
                    logger.warning(f"Error parsing game stat row for {player_id}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error parsing game logs for {player_id}: {e}")
        
        logger.info(f"Retrieved {len(game_stats)} game stats for player {player_id} in {season}")
        return game_stats
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime object.
        
        Args:
            date_str: Date string in format "YYYY-MM-DD"
            
        Returns:
            Datetime object or None if invalid
        """
        if not date_str or date_str == '':
            return None
        
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return None
    
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
    
    def collect_player_stats_batch(self, player_ids: List[str] = None, season: str = "2024", save_to_db: bool = True, limit: int = 10) -> Dict[str, int]:
        """Collect game statistics for multiple players.
        
        Args:
            player_ids: List of player IDs to collect stats for (if None, get from database)
            season: NBA season to collect
            save_to_db: Whether to save to database
            limit: Maximum number of players to collect (if getting from database)
            
        Returns:
            Dictionary with collection statistics
        """
        # If no player IDs provided, get them from the database
        if player_ids is None:
            player_ids = self._get_player_ids_from_db(limit)
            logger.info(f"Retrieved {len(player_ids)} player IDs from database")
        
        logger.info(f"Starting batch collection for {len(player_ids)} players in {season}")
        
        total_games = 0
        successful_players = 0
        failed_players = 0
        
        for i, player_id in enumerate(player_ids):
            try:
                logger.info(f"Collecting stats for player {i+1}/{len(player_ids)}: {player_id}")
                
                game_stats = self.get_player_game_logs(player_id, season)
                
                if game_stats:
                    total_games += len(game_stats)
                    successful_players += 1
                    
                    if save_to_db:
                        # Save to database
                        for stat in game_stats:
                            # Convert to database format
                            db_stat = {
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
                                'offensive_rebounds': stat['offensive_rebounds'],
                                'defensive_rebounds': stat['defensive_rebounds'],
                                'assists': stat['assists'],
                                'steals': stat['steals'],
                                'blocks': stat['blocks'],
                                'turnovers': stat['turnovers'],
                                'personal_fouls': stat['personal_fouls'],
                                'points': stat['points'],
                                'plus_minus': stat['plus_minus']
                            }
                            db_manager.insert_player_stats(db_stat)
                else:
                    failed_players += 1
                    
            except Exception as e:
                logger.error(f"Error collecting stats for {player_id}: {e}")
                failed_players += 1
                continue
        
        stats = {
            'total_players': len(player_ids),
            'successful_players': successful_players,
            'failed_players': failed_players,
            'total_games': total_games
        }
        
        logger.info(f"Batch collection completed: {stats}")
        return stats
    
    def _get_player_ids_from_db(self, limit: int = 10) -> List[str]:
        """Get player IDs from the database.
        
        Args:
            limit: Maximum number of player IDs to return
            
        Returns:
            List of player IDs
        """
        try:
            # Get a session and query for player IDs
            session = db_manager.get_session()
            
            # Query for players with valid IDs (not BR_1, BR_2, etc.)
            from sqlalchemy import text
            query = text("""
                SELECT player_id FROM players 
                WHERE player_id NOT LIKE 'BR_%' 
                AND player_id IS NOT NULL 
                LIMIT :limit
            """)
            
            result = session.execute(query, {'limit': limit})
            player_ids = [row[0] for row in result]
            
            session.close()
            return player_ids
            
        except Exception as e:
            logger.error(f"Error getting player IDs from database: {e}")
            # Fallback to some known player IDs
            return ['jamesle01', 'curryst01', 'duranke01', 'giannke01', 'embiijo01']
    
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
    collector = PlayerStatsCollector()
    
    # Test with a few players
    test_players = ["jamesle01", "curryst01", "duranke01"]  # LeBron, Curry, Durant
    
    stats = collector.collect_player_stats_batch(test_players, "2024")
    print(f"Collection results: {stats}") 