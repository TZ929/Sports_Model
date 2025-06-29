#!/usr/bin/env python3
"""
ESPN JSON Data Collector - Extract complete season data from embedded JSON.
"""

import requests
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Any, Optional
import re
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ESPNJSONDataCollector:
    """Extract complete season data from embedded JSON in ESPN pages."""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def get_player_complete_data(self, player_id: str, player_name: str) -> List[Dict[str, Any]]:
        """Get complete season data by extracting from embedded JSON."""
        
        print(f"\n{'='*80}")
        print(f"COLLECTING JSON DATA: {player_name} ({player_id})")
        print(f"{'='*80}")
        
        player_name_url = player_name.lower().replace(' ', '-')
        url = f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/{player_name_url}"
        
        print(f"URL: {url}")
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for embedded JSON data
            print("🔍 Searching for embedded JSON data...")
            
            # Method 1: Look for script tags with JSON data
            scripts = soup.find_all('script')
            json_data = None
            
            for script in scripts:
                script_text = script.get_text()
                
                # Look for JSON data patterns
                if 'window.espn.scoreboardData' in script_text:
                    print("Found window.espn.scoreboardData")
                    # Extract JSON from this pattern
                    match = re.search(r'window\.espn\.scoreboardData\s*=\s*({.*?});', script_text, re.DOTALL)
                    if match:
                        try:
                            json_data = json.loads(match.group(1))
                            print("✅ Successfully parsed scoreboardData JSON")
                            break
                        except json.JSONDecodeError:
                            continue
                
                # Look for other JSON patterns
                elif '"events":' in script_text and '"stats":' in script_text:
                    print("Found events/stats JSON pattern")
                    # Try to extract JSON from this script
                    try:
                        # Look for JSON object boundaries
                        start = script_text.find('{')
                        if start != -1:
                            # Find matching closing brace
                            brace_count = 0
                            end = start
                            for i, char in enumerate(script_text[start:], start):
                                if char == '{':
                                    brace_count += 1
                                elif char == '}':
                                    brace_count -= 1
                                    if brace_count == 0:
                                        end = i + 1
                                        break
                            
                            if end > start:
                                json_str = script_text[start:end]
                                json_data = json.loads(json_str)
                                print("✅ Successfully parsed events/stats JSON")
                                break
                    except json.JSONDecodeError:
                        continue
            
            if json_data:
                print(f"📊 JSON data keys: {list(json_data.keys())}")
                return self._parse_json_data(json_data, player_id)
            else:
                print("❌ No JSON data found in scripts")
                
                # Method 2: Look for data attributes
                print("🔍 Searching for data attributes...")
                data_elements = soup.find_all(attrs={'data-games': True})
                if data_elements:
                    print(f"Found {len(data_elements)} elements with data-games attribute")
                    for elem in data_elements:
                        games_data = elem.get('data-games')
                        print(f"  Data: {games_data}")
                
                # Method 3: Look for any JSON-like strings
                print("🔍 Searching for JSON-like strings...")
                page_text = soup.get_text()
                json_patterns = re.findall(r'\{[^{}]*"events"[^{}]*\}', page_text)
                print(f"Found {len(json_patterns)} JSON-like patterns")
                
                for pattern in json_patterns[:3]:
                    print(f"  Pattern: {pattern[:100]}...")
            
            return []
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return []
    
    def _parse_json_data(self, json_data: Dict[str, Any], player_id: str) -> List[Dict[str, Any]]:
        """Parse JSON data to extract game statistics."""
        
        stats = []
        
        # Look for events in the JSON structure
        events = []
        
        # Try different possible JSON structures
        if 'events' in json_data:
            events = json_data['events']
        elif 'groups' in json_data:
            # ESPN often structures data in groups
            for group in json_data['groups']:
                if 'tbls' in group:
                    for tbl in group['tbls']:
                        if 'events' in tbl:
                            events.extend(tbl['events'])
        
        print(f"Found {len(events)} events in JSON data")
        
        for event in events:
            try:
                # Extract game date
                date_str = event.get('dt', '')
                if not date_str:
                    continue
                
                # Parse date (ESPN format: "2025-03-29T23:00:00.000+00:00")
                try:
                    game_date = datetime.strptime(date_str.split('T')[0], '%Y-%m-%d')
                except ValueError:
                    continue
                
                # Only include 2023-24 season
                if game_date.year == 2023 and game_date.month >= 10:
                    pass  # Valid
                elif game_date.year == 2024 and game_date.month <= 4:
                    pass  # Valid
                else:
                    continue
                
                # Extract stats
                stats_array = event.get('stats', [])
                if len(stats_array) < 17:  # Need at least basic stats
                    continue
                
                # Parse stats array (ESPN format: [min, fg, fg%, 3pt, 3p%, ft, ft%, reb, ast, blk, stl, pf, to, pts, +/-])
                try:
                    minutes_str = stats_array[0] if len(stats_array) > 0 else "0"
                    minutes = self._parse_minutes_from_str(minutes_str)
                    
                    # Parse shooting stats
                    fg_str = stats_array[1] if len(stats_array) > 1 else "0-0"
                    fg_made, fg_attempted = self._parse_shot_attempts(fg_str)
                    
                    three_pt_str = stats_array[3] if len(stats_array) > 3 else "0-0"
                    three_made, three_attempted = self._parse_shot_attempts(three_pt_str)
                    
                    ft_str = stats_array[5] if len(stats_array) > 5 else "0-0"
                    ft_made, ft_attempted = self._parse_shot_attempts(ft_str)
                    
                    # Other stats
                    rebounds = int(stats_array[7]) if len(stats_array) > 7 and stats_array[7].isdigit() else 0
                    assists = int(stats_array[8]) if len(stats_array) > 8 and stats_array[8].isdigit() else 0
                    blocks = int(stats_array[9]) if len(stats_array) > 9 and stats_array[9].isdigit() else 0
                    steals = int(stats_array[10]) if len(stats_array) > 10 and stats_array[10].isdigit() else 0
                    personal_fouls = int(stats_array[11]) if len(stats_array) > 11 and stats_array[11].isdigit() else 0
                    turnovers = int(stats_array[12]) if len(stats_array) > 12 and stats_array[12].isdigit() else 0
                    points = int(stats_array[13]) if len(stats_array) > 13 and stats_array[13].isdigit() else 0
                    plus_minus = int(stats_array[14]) if len(stats_array) > 14 and stats_array[14].isdigit() else 0
                    
                    # Create game stat record
                    game_stat = {
                        'game_id': f"ESPN_JSON_{player_id}_{game_date.strftime('%Y%m%d')}",
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
                    print(f"Error parsing stats array: {e}")
                    continue
                
            except Exception as e:
                print(f"Error parsing event: {e}")
                continue
        
        print(f"Successfully parsed {len(stats)} games from JSON data")
        return stats
    
    def _parse_minutes_from_str(self, minutes_str: str) -> Optional[float]:
        """Parse minutes from string format."""
        if not minutes_str or minutes_str == '':
            return None
        
        try:
            # Try to parse as float first
            return float(minutes_str)
        except ValueError:
            try:
                # Try to parse as MM:SS format
                parts = minutes_str.split(':')
                if len(parts) == 2:
                    minutes = int(parts[0])
                    seconds = int(parts[1])
                    return minutes + (seconds / 60)
            except (ValueError, IndexError):
                pass
            
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
    """Test the JSON data collector with sample players."""
    
    collector = ESPNJSONDataCollector()
    
    # Test with players we know have data
    test_players = [
        ("3138196", "Cameron Johnson"),
        ("4432174", "Cam Thomas"),
    ]
    
    for player_id, player_name in test_players:
        stats = collector.get_player_complete_data(player_id, player_name)
        
        if stats:
            print(f"\n🎉 SUCCESS: {player_name} - {len(stats)} games collected from JSON")
            
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
            print(f"\n❌ FAILED: {player_name} - No JSON data found")

if __name__ == "__main__":
    main() 