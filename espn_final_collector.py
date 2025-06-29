#!/usr/bin/env python3
"""
ESPN Final Collector - Comprehensive approach to get complete 2023-24 season data.
"""

import requests
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Any, Optional
import re
import time
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ESPNFinalCollector:
    """Final comprehensive ESPN collector for complete season data."""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def get_player_complete_data(self, player_id: str, player_name: str) -> List[Dict[str, Any]]:
        """Get complete season data using all available ESPN methods."""
        
        print(f"\n{'='*80}")
        print(f"FINAL ESPN COLLECTOR: {player_name} ({player_id})")
        print(f"{'='*80}")
        
        # Method 1: Multi-table extraction (best so far)
        print("\n🔍 METHOD 1: Multi-table extraction...")
        multi_table_stats = self._extract_from_all_tables(player_id, player_name)
        print(f"Multi-table result: {len(multi_table_stats)} games")
        
        # Method 2: Try different URL patterns
        print("\n🔍 METHOD 2: Alternative URL patterns...")
        url_pattern_stats = self._try_url_patterns(player_id, player_name)
        print(f"URL patterns result: {len(url_pattern_stats)} games")
        
        # Method 3: Look for AJAX endpoints
        print("\n🔍 METHOD 3: AJAX endpoints...")
        ajax_stats = self._try_ajax_endpoints(player_id, player_name)
        print(f"AJAX result: {len(ajax_stats)} games")
        
        # Method 4: Try to find "Load More" functionality
        print("\n🔍 METHOD 4: Load More functionality...")
        load_more_stats = self._try_load_more_functionality(player_id, player_name)
        print(f"Load More result: {len(load_more_stats)} games")
        
        # Combine all results and remove duplicates
        all_stats = []
        seen_game_ids = set()
        
        for stats_list in [multi_table_stats, url_pattern_stats, ajax_stats, load_more_stats]:
            for stat in stats_list:
                game_id = stat['game_id']
                if game_id not in seen_game_ids:
                    all_stats.append(stat)
                    seen_game_ids.add(game_id)
        
        print(f"\n📊 FINAL COMBINED RESULT: {len(all_stats)} unique games")
        
        if all_stats:
            # Sort by date
            all_stats.sort(key=lambda x: x['game_id'].split('_')[-1])
            
            # Show date range
            dates = [stat['game_id'].split('_')[-1] for stat in all_stats]
            print(f"Date range: {dates[0]} to {dates[-1]}")
            
            # Show breakdown by month
            months = {}
            for stat in all_stats:
                game_date = stat['game_id'].split('_')[-1]
                month = game_date[:6]  # YYYYMM
                months[month] = months.get(month, 0) + 1
            
            print("Games by month:")
            for month, count in sorted(months.items()):
                print(f"  {month}: {count} games")
            
            # Show total games vs expected
            total_games = len(all_stats)
            expected_games = 82  # Full NBA season
            coverage = (total_games / expected_games) * 100
            print(f"\n📈 COVERAGE: {total_games}/{expected_games} games ({coverage:.1f}%)")
            
            if coverage < 50:
                print("⚠️ Low coverage - ESPN may not provide complete season data by default")
            elif coverage < 80:
                print("⚠️ Moderate coverage - Some games may be missing")
            else:
                print("✅ Good coverage - Most season data collected")
        
        return all_stats
    
    def _extract_from_all_tables(self, player_id: str, player_name: str) -> List[Dict[str, Any]]:
        """Extract data from all tables on ESPN page."""
        
        player_name_url = player_name.lower().replace(' ', '-')
        url = f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/{player_name_url}"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all tables
            tables = soup.find_all('table')
            print(f"  Found {len(tables)} tables")
            
            all_stats = []
            seen_game_ids = set()
            
            for i, table in enumerate(tables):
                table_stats = self._extract_stats_from_table(table, player_id, f"T{i+1}")
                
                for stat in table_stats:
                    game_id = stat['game_id']
                    if game_id not in seen_game_ids:
                        all_stats.append(stat)
                        seen_game_ids.add(game_id)
            
            return all_stats
            
        except Exception as e:
            print(f"  Error: {e}")
            return []
    
    def _try_url_patterns(self, player_id: str, player_name: str) -> List[Dict[str, Any]]:
        """Try different URL patterns to get more data."""
        
        url_patterns = [
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}",
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/type/1",
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/view/all",
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/season/2024/type/1",
        ]
        
        all_stats = []
        seen_game_ids = set()
        
        for i, url in enumerate(url_patterns):
            try:
                print(f"  Trying URL {i+1}: {url}")
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Check if page exists
                if "Page Not Found" in soup.get_text() or "404" in soup.get_text():
                    print(f"    Page not found")
                    continue
                
                # Find game log table
                game_log_table = None
                for selector in ['table.Table', 'table[class*="gamelog"]', 'table']:
                    game_log_table = soup.select_one(selector)
                    if game_log_table:
                        break
                
                if game_log_table:
                    table_stats = self._extract_stats_from_table(game_log_table, player_id, f"URL{i+1}")
                    
                    for stat in table_stats:
                        game_id = stat['game_id']
                        if game_id not in seen_game_ids:
                            all_stats.append(stat)
                            seen_game_ids.add(game_id)
                
                time.sleep(1)  # Small delay
                
            except Exception as e:
                print(f"    Error: {e}")
                continue
        
        return all_stats
    
    def _try_ajax_endpoints(self, player_id: str, player_name: str) -> List[Dict[str, Any]]:
        """Try to find and use AJAX endpoints."""
        
        # Try different ESPN API endpoints
        api_endpoints = [
            f"https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/athletes/{player_id}/gamelog",
            f"https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/athletes/{player_id}/stats",
            f"https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/athletes/{player_id}/gamelog?limit=100",
        ]
        
        all_stats = []
        
        for i, endpoint in enumerate(api_endpoints):
            try:
                print(f"  Trying API endpoint {i+1}: {endpoint}")
                response = requests.get(endpoint, headers=self.headers, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                print(f"    Response keys: {list(data.keys())}")
                
                if 'events' in data:
                    events = data['events']
                    print(f"    Found {len(events)} events")
                    api_stats = self._parse_api_events(events, player_id)
                    all_stats.extend(api_stats)
                
                time.sleep(1)
                
            except Exception as e:
                print(f"    Error: {e}")
                continue
        
        return all_stats
    
    def _try_load_more_functionality(self, player_id: str, player_name: str) -> List[Dict[str, Any]]:
        """Try to find and use "Load More" functionality."""
        
        player_name_url = player_name.lower().replace(' ', '-')
        url = f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/{player_name_url}"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for "Load More" buttons or similar
            load_more_selectors = [
                'button[class*="load"]',
                'button[class*="more"]',
                'a[class*="load"]',
                'a[class*="more"]',
                '[data-action="load-more"]',
                '[data-action="show-more"]',
            ]
            
            load_more_found = False
            for selector in load_more_selectors:
                elements = soup.select(selector)
                if elements:
                    print(f"  Found {len(elements)} potential load more elements")
                    load_more_found = True
                    break
            
            if not load_more_found:
                print("  No load more functionality found")
                return []
            
            # For now, return empty as we'd need to implement click simulation
            # This would require Selenium or Playwright for full implementation
            print("  Load more functionality detected but not implemented")
            return []
            
        except Exception as e:
            print(f"  Error: {e}")
            return []
    
    def _extract_stats_from_table(self, table, player_id: str, table_id: str) -> List[Dict[str, Any]]:
        """Extract stats from a single table."""
        
        stats = []
        rows = table.find_all('tr')
        
        # Check if this looks like a game log table
        table_text = table.get_text().lower()
        if 'date' not in table_text or ('opp' not in table_text and 'opponent' not in table_text):
            return stats  # Not a game log table
        
        for row in rows:
            # Skip header rows
            if 'thead' in str(row.get('class', [])):
                continue
            
            cells = row.find_all(['td', 'th'])
            if len(cells) < 10:  # Need at least basic stats
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
                    'game_id': f"ESPN_{table_id}_{player_id}_{game_date.strftime('%Y%m%d')}",
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
                continue
        
        return stats
    
    def _parse_api_events(self, events: List[Dict], player_id: str) -> List[Dict[str, Any]]:
        """Parse events from ESPN API response."""
        
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
                        'minutes_played': 0,
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
                continue
        
        return stats
    
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
    """Test the final ESPN collector with sample players."""
    
    collector = ESPNFinalCollector()
    
    # Test with players we know have data
    test_players = [
        ("3138196", "Cameron Johnson"),
        ("4432174", "Cam Thomas"),
    ]
    
    for player_id, player_name in test_players:
        stats = collector.get_player_complete_data(player_id, player_name)
        
        if stats:
            print(f"\n🎉 FINAL SUCCESS: {player_name} - {len(stats)} total games collected")
        else:
            print(f"\n❌ FINAL FAILED: {player_name} - No data collected")

if __name__ == "__main__":
    main() 