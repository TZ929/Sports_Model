#!/usr/bin/env python3
"""
Collect game stats for all OKC Thunder players.
"""

import requests
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Any, Optional
from src.utils.database import db_manager
from sqlalchemy import text
import time
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OKCThunderCollector:
    """Collect game stats for OKC Thunder players."""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.team_name = "Oklahoma City Thunder"
    
    def get_okc_players(self) -> List[tuple]:
        """Get all OKC Thunder players from the database."""
        
        with db_manager.get_session() as session:
            query = text("""
                SELECT player_id, full_name 
                FROM players 
                WHERE team_name LIKE '%Thunder%' 
                AND player_id REGEXP '^[0-9]+$'
                AND full_name IS NOT NULL
                ORDER BY full_name
            """)
            
            result = session.execute(query)
            players = result.fetchall()
            
            if not players:
                # Try alternative team names
                query2 = text("""
                    SELECT player_id, full_name 
                    FROM players 
                    WHERE team_name LIKE '%OKC%' 
                    AND player_id REGEXP '^[0-9]+$'
                    AND full_name IS NOT NULL
                    ORDER BY full_name
                """)
                
                result = session.execute(query2)
                players = result.fetchall()
            
            logger.info(f"Found {len(players)} OKC Thunder players")
            for player_id, name in players:
                logger.info(f"  - {name} (ID: {player_id})")
            
            return players
    
    def get_player_game_logs(self, player_id: str, player_name: str) -> List[Dict[str, Any]]:
        """Get game-by-game statistics for a specific player from ESPN web pages."""
        
        # Construct the ESPN game log URL
        player_name_url = player_name.lower().replace(' ', '-')
        url = f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/{player_name_url}"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the game log table - try multiple selectors
            game_log_table = None
            for selector in ['table.Table', 'table[class*="gamelog"]', 'table']:
                game_log_table = soup.select_one(selector)
                if game_log_table:
                    break
            
            if not game_log_table:
                logger.warning(f"Game log table not found for player {player_id}")
                return []
            
            stats = []
            rows = game_log_table.find_all('tr')
            
            for row in rows:
                # Skip header rows
                if 'thead' in str(row.get('class', [])):
                    continue
                
                cells = row.find_all(['td', 'th'])
                if len(cells) < 10:  # Need at least basic stats
                    continue
                
                try:
                    # Extract game information
                    # ESPN game log format: Date, OPP, RESULT, MIN, FG, FG%, 3PT, 3P%, FT, FT%, REB, AST, STL, BLK, TO, PF, PTS, +/-
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
                        'team_id': 'OKC',  # OKC Thunder team ID
                        'minutes_played': int(minutes) if minutes else 0,
                        'field_goals_made': fg_made,
                        'field_goals_attempted': fg_attempted,
                        'three_pointers_made': three_made,
                        'three_pointers_attempted': three_attempted,
                        'free_throws_made': ft_made,
                        'free_throws_attempted': ft_attempted,
                        'rebounds': rebounds,
                        'offensive_rebounds': 0,  # Not provided in ESPN data
                        'defensive_rebounds': 0,  # Not provided in ESPN data
                        'assists': assists,
                        'steals': steals,
                        'blocks': blocks,
                        'turnovers': turnovers,
                        'personal_fouls': personal_fouls,
                        'points': points,
                        'plus_minus': plus_minus
                    }
                    
                    stats.append(game_stat)
                    
                except (ValueError, IndexError) as e:
                    logger.warning(f"Error parsing game stat row for {player_id}: {e}")
                    continue
            
            logger.info(f"Retrieved {len(stats)} game stats for {player_name} ({player_id})")
            return stats
            
        except Exception as e:
            logger.error(f"Error getting game logs for player {player_id}: {e}")
            return []
    
    def _parse_espn_date(self, date_str: str) -> Optional[datetime]:
        """Parse ESPN date string to datetime object."""
        if not date_str or date_str == '' or date_str.lower() == 'date':
            return None
        
        try:
            # ESPN format: "Oct 25, 2023"
            return datetime.strptime(date_str, '%b %d, %Y')
        except ValueError:
            try:
                # Alternative format: "10/25/2023"
                return datetime.strptime(date_str, '%m/%d/%Y')
            except ValueError:
                try:
                    # Current ESPN format: "Tue 4/15" (assume current year)
                    match = re.search(r'(\d{1,2})/(\d{1,2})', date_str)
                    if match:
                        month = int(match.group(1))
                        day = int(match.group(2))
                        # Assume current year (2024)
                        return datetime(2024, month, day)
                except (ValueError, AttributeError):
                    pass
                
                logger.warning(f"Could not parse date: {date_str}")
                return None
    
    def _parse_minutes(self, minutes_str: str) -> Optional[float]:
        """Parse minutes played string to total minutes."""
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
    
    def _parse_shot_attempts(self, shot_str: str) -> tuple:
        """Parse shot attempts string (e.g., "8-15") to made and attempted."""
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
    
    def collect_all_okc_stats(self, save_to_db: bool = True):
        """Collect game stats for all OKC Thunder players."""
        
        players = self.get_okc_players()
        
        if not players:
            logger.warning("No OKC Thunder players found in database")
            return {'players': 0, 'stats': 0}
        
        total_stats = 0
        successful_players = 0
        
        for i, (player_id, player_name) in enumerate(players):
            try:
                logger.info(f"Collecting game logs for player {i+1}/{len(players)}: {player_name} ({player_id})")
                
                stats = self.get_player_game_logs(player_id, player_name)
                
                if stats:
                    total_stats += len(stats)
                    successful_players += 1
                    
                    if save_to_db:
                        for stat in stats:
                            db_manager.insert_player_stats(stat)
                    
                    logger.info(f"  ✅ {len(stats)} game stats collected and saved")
                else:
                    logger.warning(f"  ⚠️ No game stats found for {player_name}")
                
                # Be respectful with requests
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Error collecting game logs for {player_name} ({player_id}): {e}")
                continue
        
        logger.info(f"OKC Thunder collection completed: {successful_players} players, {total_stats} game stats collected")
        return {'players': successful_players, 'stats': total_stats}

def main():
    """Collect game stats for all OKC Thunder players."""
    
    logger.info("Starting OKC Thunder player game stats collection...")
    collector = OKCThunderCollector()
    results = collector.collect_all_okc_stats(save_to_db=True)
    
    logger.info(f"Collection complete: {results}")
    
    # Show summary
    print(f"\n{'='*50}")
    print("OKC THUNDER COLLECTION SUMMARY")
    print(f"{'='*50}")
    print(f"Players processed: {results['players']}")
    print(f"Total game stats: {results['stats']}")
    print(f"Average stats per player: {results['stats']/results['players']:.1f}" if results['players'] > 0 else "No players processed")

if __name__ == "__main__":
    main() 