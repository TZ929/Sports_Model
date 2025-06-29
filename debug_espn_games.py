#!/usr/bin/env python3
"""
Debug ESPN game log collection to understand why we're getting limited games.
"""

import requests
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Any, Optional
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ESPNGameDebugger:
    """Debug ESPN game log collection issues."""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def debug_player_games(self, player_id: str, player_name: str):
        """Debug a specific player's game log page."""
        
        # Construct the ESPN game log URL
        player_name_url = player_name.lower().replace(' ', '-')
        url = f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/{player_name_url}"
        
        print(f"\n{'='*80}")
        print(f"DEBUGGING: {player_name} ({player_id})")
        print(f"URL: {url}")
        print(f"{'='*80}")
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Check if page exists
            if "Page Not Found" in soup.get_text() or "404" in soup.get_text():
                print("❌ Page not found - player may not exist on ESPN")
                return
            
            # Find all tables on the page
            tables = soup.find_all('table')
            print(f"📊 Found {len(tables)} tables on the page")
            
            # Look for game log table
            game_log_table = None
            for i, table in enumerate(tables):
                table_text = table.get_text().lower()
                if 'date' in table_text and ('opp' in table_text or 'opponent' in table_text):
                    game_log_table = table
                    print(f"✅ Found game log table at index {i}")
                    break
            
            if not game_log_table:
                print("❌ No game log table found")
                # Try alternative selectors
                for selector in ['table.Table', 'table[class*="gamelog"]', '.gamelog table']:
                    game_log_table = soup.select_one(selector)
                    if game_log_table:
                        print(f"✅ Found game log table with selector: {selector}")
                        break
                
                if not game_log_table:
                    print("❌ Still no game log table found")
                    return
            
            # Analyze the table structure
            rows = game_log_table.find_all('tr')
            print(f"📋 Found {len(rows)} rows in game log table")
            
            # Show first few rows to understand structure
            print(f"\n📋 TABLE STRUCTURE ANALYSIS:")
            for i, row in enumerate(rows[:5]):  # Show first 5 rows
                cells = row.find_all(['td', 'th'])
                cell_texts = [cell.get_text(strip=True) for cell in cells]
                print(f"Row {i}: {len(cells)} cells - {cell_texts}")
            
            # Count games by year
            games_by_year = {}
            total_games = 0
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 3:  # Need at least date, opponent, result
                    continue
                
                try:
                    date_text = cells[0].get_text(strip=True)
                    if not date_text or date_text.lower() in ['date', '']:
                        continue
                    
                    game_date = self._parse_espn_date(date_text)
                    if game_date:
                        year = game_date.year
                        games_by_year[year] = games_by_year.get(year, 0) + 1
                        total_games += 1
                        
                        # Show sample of recent games
                        if total_games <= 10:
                            opponent = cells[1].get_text(strip=True) if len(cells) > 1 else "N/A"
                            result = cells[2].get_text(strip=True) if len(cells) > 2 else "N/A"
                            print(f"  {game_date.strftime('%Y-%m-%d')} vs {opponent} - {result}")
                    
                except Exception as e:
                    print(f"Error parsing row: {e}")
                    continue
            
            print(f"\n📊 GAMES BY YEAR:")
            for year, count in sorted(games_by_year.items()):
                print(f"  {year}: {count} games")
            
            print(f"📈 TOTAL GAMES: {total_games}")
            
            # Check for pagination or "load more" buttons
            load_more = soup.find_all(text=re.compile(r'load more|show more|view all', re.I))
            if load_more:
                print(f"⚠️ Found potential 'load more' elements: {len(load_more)}")
                for elem in load_more[:3]:
                    print(f"  - {elem}")
            
            # Check for season filters
            season_filters = soup.find_all(text=re.compile(r'season|2023|2024', re.I))
            if season_filters:
                print(f"📅 Found season-related elements: {len(season_filters)}")
                for elem in season_filters[:5]:
                    print(f"  - {elem}")
            
        except Exception as e:
            print(f"❌ Error accessing page: {e}")
    
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

def main():
    """Debug ESPN game log collection for sample players."""
    
    debugger = ESPNGameDebugger()
    
    # Test with some known players
    test_players = [
        ("4432816", "LaMelo Ball"),  # Nets player we collected
        ("3138196", "Cameron Johnson"),  # Another Nets player
        ("4066218", "Grant Williams"),  # Another Nets player
        ("4432174", "Cam Thomas"),  # Another Nets player
    ]
    
    for player_id, player_name in test_players:
        debugger.debug_player_games(player_id, player_name)
        print("\n" + "="*80 + "\n")

if __name__ == "__main__":
    main() 