#!/usr/bin/env python3
"""
ESPN Full Season Data Collector - Focus on getting complete 2023-24 season data.
"""

import requests
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Any, Optional
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ESPNFullSeasonCollector:
    """Collect complete 2023-2024 season data from ESPN."""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def get_player_full_season_data(self, player_id: str, player_name: str) -> List[Dict[str, Any]]:
        """Get complete 2023-24 season data for a player using multiple ESPN approaches."""
        
        print(f"\n{'='*80}")
        print(f"COLLECTING FULL SEASON DATA: {player_name} ({player_id})")
        print(f"{'='*80}")
        
        # Try multiple ESPN approaches
        approaches = [
            self._try_espn_api_gamelog,
            self._try_espn_web_gamelog_2024,
            self._try_espn_web_gamelog_2023,
            self._try_espn_web_gamelog_standard,
        ]
        
        for i, approach in enumerate(approaches):
            print(f"\n--- TRYING APPROACH {i+1}/{len(approaches)} ---")
            try:
                stats = approach(player_id, player_name)
                if stats and len(stats) > 10:  # We want substantial data
                    print(f"✅ SUCCESS! Found {len(stats)} games with approach {i+1}")
                    return stats
                elif stats:
                    print(f"⚠️ Found {len(stats)} games (too few, trying next approach)")
                else:
                    print(f"❌ No data found with approach {i+1}")
            except Exception as e:
                print(f"❌ Error with approach {i+1}: {e}")
                continue
        
        print("❌ Failed to get substantial data from any approach")
        return []
    
    def _try_espn_api_gamelog(self, player_id: str, player_name: str) -> List[Dict[str, Any]]:
        """Try ESPN API for game log data."""
        
        print("Testing ESPN API gamelog endpoint...")
        
        url = f"https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/athletes/{player_id}/gamelog"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            print(f"API Response keys: {list(data.keys())}")
            
            if 'events' in data:
                events = data['events']
                print(f"Found {len(events)} events in API response")
                
                stats = []
                for event in events:
                    try:
                        # Extract game data from API response
                        game_date_str = event.get('date', '')
                        if game_date_str:
                            game_date = datetime.strptime(game_date_str, '%Y-%m-%d')
                            
                            # Only include 2023-24 season
                            if game_date.year == 2023 and game_date.month >= 10:
                                pass  # Valid
                            elif game_date.year == 2024 and game_date.month <= 4:
                                pass  # Valid
                            else:
                                continue
                            
                            # Extract stats
                            stats_data = event.get('stats', [])
                            points = next((stat['value'] for stat in stats_data if stat['name'] == 'points'), 0)
                            rebounds = next((stat['value'] for stat in stats_data if stat['name'] == 'rebounds'), 0)
                            assists = next((stat['value'] for stat in stats_data if stat['name'] == 'assists'), 0)
                            
                            game_stat = {
                                'game_id': f"ESPN_API_{player_id}_{game_date.strftime('%Y%m%d')}",
                                'player_id': player_id,
                                'team_id': 'UNK',
                                'minutes_played': 0,  # Not available in API
                                'field_goals_made': 0,
                                'field_goals_attempted': 0,
                                'three_pointers_made': 0,
                                'three_pointers_attempted': 0,
                                'free_throws_made': 0,
                                'free_throws_attempted': 0,
                                'rebounds': int(rebounds) if rebounds else 0,
                                'offensive_rebounds': 0,
                                'defensive_rebounds': 0,
                                'assists': int(assists) if assists else 0,
                                'steals': 0,
                                'blocks': 0,
                                'turnovers': 0,
                                'personal_fouls': 0,
                                'points': int(points) if points else 0,
                                'plus_minus': 0
                            }
                            
                            stats.append(game_stat)
                            
                    except Exception as e:
                        print(f"Error parsing event: {e}")
                        continue
                
                return stats
            
            return []
            
        except Exception as e:
            print(f"API request failed: {e}")
            return []
    
    def _try_espn_web_gamelog_2024(self, player_id: str, player_name: str) -> List[Dict[str, Any]]:
        """Try ESPN web gamelog with 2024 season parameter."""
        
        print("Testing ESPN web gamelog with 2024 season...")
        
        url = f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/season/2024"
        
        return self._parse_espn_web_gamelog(url, player_id)
    
    def _try_espn_web_gamelog_2023(self, player_id: str, player_name: str) -> List[Dict[str, Any]]:
        """Try ESPN web gamelog with 2023 season parameter."""
        
        print("Testing ESPN web gamelog with 2023 season...")
        
        url = f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/season/2023"
        
        return self._parse_espn_web_gamelog(url, player_id)
    
    def _try_espn_web_gamelog_standard(self, player_id: str, player_name: str) -> List[Dict[str, Any]]:
        """Try standard ESPN web gamelog URL."""
        
        print("Testing standard ESPN web gamelog...")
        
        player_name_url = player_name.lower().replace(' ', '-')
        url = f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/{player_name_url}"
        
        return self._parse_espn_web_gamelog(url, player_id)
    
    def _parse_espn_web_gamelog(self, url: str, player_id: str) -> List[Dict[str, Any]]:
        """Parse ESPN web gamelog page."""
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Check if page exists
            if "Page Not Found" in soup.get_text() or "404" in soup.get_text():
                print("Page not found")
                return []
            
            # Find the game log table
            game_log_table = None
            for selector in ['table.Table', 'table[class*="gamelog"]', 'table']:
                game_log_table = soup.select_one(selector)
                if game_log_table:
                    break
            
            if not game_log_table:
                print("No game log table found")
                return []
            
            stats = []
            rows = game_log_table.find_all('tr')
            print(f"Found {len(rows)} rows in table")
            
            for row in rows:
                # Skip header rows
                if 'thead' in str(row.get('class', [])):
                    continue
                
                cells = row.find_all(['td', 'th'])
                if len(cells) < 10:
                    continue
                
                try:
                    # Extract game information
                    game_date = self._parse_espn_date(cells[0].get_text(strip=True))
                    if not game_date:
                        continue
                    
                    # Only collect 2023-2024 season data
                    if game_date.year == 2023 and game_date.month < 10:
                        continue  # Skip pre-season 2023
                    if game_date.year == 2024 and game_date.month > 4:
                        continue  # Skip post-season 2024
                    if game_date.year not in [2023, 2024]:
                        continue  # Skip other years
                    
                    opponent = cells[1].get_text(strip=True)
                    result = cells[2].get_text(strip=True)
                    minutes = self._parse_minutes(cells[3].get_text(strip=True))
                    
                    # Parse shooting stats
                    fg_str = cells[4].get_text(strip=True)
                    fg_made, fg_attempted = self._parse_shot_attempts(fg_str)
                    
                    three_pt_str = cells[6].get_text(strip=True)
                    three_made, three_attempted = self._parse_shot_attempts(three_pt_str)
                    
                    ft_str = cells[8].get_text(strip=True)
                    ft_made, ft_attempted = self._parse_shot_attempts(ft_str)
                    
                    # Other stats
                    rebounds = int(cells[10].get_text(strip=True)) if cells[10].get_text(strip=True).isdigit() else 0
                    assists = int(cells[11].get_text(strip=True)) if cells[11].get_text(strip=True).isdigit() else 0
                    steals = int(cells[12].get_text(strip=True)) if cells[12].get_text(strip=True).isdigit() else 0
                    blocks = int(cells[13].get_text(strip=True)) if cells[13].get_text(strip=True).isdigit() else 0
                    turnovers = int(cells[14].get_text(strip=True)) if cells[14].get_text(strip=True).isdigit() else 0
                    personal_fouls = int(cells[15].get_text(strip=True)) if cells[15].get_text(strip=True).isdigit() else 0
                    points = int(cells[16].get_text(strip=True)) if cells[16].get_text(strip=True).isdigit() else 0
                    
                    # Plus/minus
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
                        'game_id': f"ESPN_WEB_{player_id}_{game_date.strftime('%Y%m%d')}",
                        'player_id': player_id,
                        'team_id': 'UNK',
                        'minutes_played': int(minutes) if minutes else 0,
                        'field_goals_made': fg_made,
                        'field_goals_attempted': fg_attempted,
                        'three_pointers_made': three_made,
                        'three_pointers_attempted': three_attempted,
                        'free_throws_made': ft_made,
                        'free_throws_attempted': ft_attempted,
                        'rebounds': rebounds,
                        'offensive_rebounds': 0,
                        'defensive_rebounds': 0,
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
                    print(f"Error parsing row: {e}")
                    continue
            
            print(f"Parsed {len(stats)} valid games")
            return stats
            
        except Exception as e:
            print(f"Error parsing web gamelog: {e}")
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
                    # Current ESPN format: "Tue 4/15" (assume 2024)
                    match = re.search(r'(\d{1,2})/(\d{1,2})', date_str)
                    if match:
                        month = int(match.group(1))
                        day = int(match.group(2))
                        return datetime(2024, month, day)
                except (ValueError, AttributeError):
                    pass
                
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
                return (int(shot_str), 0)
        except (ValueError, IndexError):
            return (0, 0)

def main():
    """Test ESPN full season collection with sample players."""
    
    collector = ESPNFullSeasonCollector()
    
    # Test with players we know have data
    test_players = [
        ("3138196", "Cameron Johnson"),
        ("4432174", "Cam Thomas"),
        ("4432816", "LaMelo Ball"),
    ]
    
    for player_id, player_name in test_players:
        stats = collector.get_player_full_season_data(player_id, player_name)
        
        if stats:
            print(f"\n🎉 SUCCESS: {player_name} - {len(stats)} games collected")
            
            # Show date range
            dates = [stat['game_id'].split('_')[-1] for stat in stats]
            dates.sort()
            print(f"Date range: {dates[0]} to {dates[-1]}")
            
            # Show sample games
            print("Sample games:")
            for stat in stats[:5]:
                game_date = stat['game_id'].split('_')[-1]
                points = stat['points']
                print(f"  {game_date}: {points} points")
        else:
            print(f"\n❌ FAILED: {player_name} - No data collected")

if __name__ == "__main__":
    main() 