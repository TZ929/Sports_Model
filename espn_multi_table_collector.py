#!/usr/bin/env python3
"""
ESPN Multi-Table Collector - Extract data from all tables to get complete season data.
"""

import requests
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Any, Optional
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ESPNMultiTableCollector:
    """Collect data from all tables on ESPN pages to get complete season data."""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def get_player_complete_data(self, player_id: str, player_name: str) -> List[Dict[str, Any]]:
        """Get complete season data by extracting from all tables on the page."""
        
        print(f"\n{'='*80}")
        print(f"COLLECTING COMPLETE DATA: {player_name} ({player_id})")
        print(f"{'='*80}")
        
        player_name_url = player_name.lower().replace(' ', '-')
        url = f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/{player_name_url}"
        
        print(f"URL: {url}")
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all tables
            tables = soup.find_all('table')
            print(f"Found {len(tables)} tables on the page")
            
            all_stats = []
            table_stats = []
            
            for i, table in enumerate(tables):
                print(f"\n--- PROCESSING TABLE {i+1}/{len(tables)} ---")
                
                table_stats_count = self._extract_stats_from_table(table, player_id, i+1)
                table_stats.append(table_stats_count)
                
                if len(table_stats_count) > 0:
                    print(f"✅ Table {i+1}: {len(table_stats_count)} games extracted")
                else:
                    print(f"❌ Table {i+1}: No valid games found")
            
            # Combine all stats and remove duplicates
            all_stats = []
            seen_game_ids = set()
            
            for table_data in table_stats:
                for stat in table_data:
                    game_id = stat['game_id']
                    if game_id not in seen_game_ids:
                        all_stats.append(stat)
                        seen_game_ids.add(game_id)
            
            print("\n📊 FINAL RESULTS:")
            print(f"Total unique games collected: {len(all_stats)}")
            
            if all_stats:
                # Sort by date
                all_stats.sort(key=lambda x: x['game_id'].split('_')[-1])
                
                # Show date range
                dates = [stat['game_id'].split('_')[-1] for stat in all_stats]
                print(f"Date range: {dates[0]} to {dates[-1]}")
                
                # Show sample games
                print("Sample games:")
                for stat in all_stats[:5]:
                    game_date = stat['game_id'].split('_')[-1]
                    points = stat['points']
                    print(f"  {game_date}: {points} points")
            
            return all_stats
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return []
    
    def _extract_stats_from_table(self, table, player_id: str, table_num: int) -> List[Dict[str, Any]]:
        """Extract stats from a single table."""
        
        stats = []
        rows = table.find_all('tr')
        
        # Check if this looks like a game log table
        table_text = table.get_text().lower()
        if 'date' not in table_text or ('opp' not in table_text and 'opponent' not in table_text):
            return stats  # Not a game log table
        
        print(f"  Analyzing {len(rows)} rows...")
        
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
                    'game_id': f"ESPN_T{table_num}_{player_id}_{game_date.strftime('%Y%m%d')}",
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
                print(f"    Error parsing row: {e}")
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
    """Test the multi-table collector with sample players."""
    
    collector = ESPNMultiTableCollector()
    
    # Test with players we know have data
    test_players = [
        ("3138196", "Cameron Johnson"),
        ("4432174", "Cam Thomas"),
    ]
    
    for player_id, player_name in test_players:
        stats = collector.get_player_complete_data(player_id, player_name)
        
        if stats:
            print(f"\n🎉 SUCCESS: {player_name} - {len(stats)} total games collected")
            
            # Show breakdown by month
            months = {}
            for stat in stats:
                game_date = stat['game_id'].split('_')[-1]
                month = game_date[:6]  # YYYYMM
                months[month] = months.get(month, 0) + 1
            
            print("Games by month:")
            for month, count in sorted(months.items()):
                print(f"  {month}: {count} games")
        else:
            print(f"\n❌ FAILED: {player_name} - No data collected")

if __name__ == "__main__":
    main() 