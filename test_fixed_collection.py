#!/usr/bin/env python3
"""
Test the fixed collection method with a single player.
"""

import requests
import logging
from bs4 import BeautifulSoup
from datetime import datetime
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_player_collection(player_id: str, player_name: str):
    """Test collecting data for a single player with the fixed method."""
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Try multiple ESPN URL formats to get 2023-24 season data
    urls_to_try = [
        # Format 1: Direct 2023-24 season URL
        f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/season/2024",
        # Format 2: Standard URL (might default to current season)
        f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/{player_name.lower().replace(' ', '-')}",
        # Format 3: Alternative format
        f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}",
    ]
    
    for i, url in enumerate(urls_to_try):
        print(f"\n{'='*60}")
        print(f"TESTING URL {i+1}: {url}")
        print(f"{'='*60}")
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Check if page exists
            if "Page Not Found" in soup.get_text() or "404" in soup.get_text():
                print("❌ Page not found")
                continue
            
            # Find the game log table
            game_log_table = None
            for selector in ['table.Table', 'table[class*="gamelog"]', 'table']:
                game_log_table = soup.select_one(selector)
                if game_log_table:
                    print(f"✅ Found table with selector: {selector}")
                    break
            
            if not game_log_table:
                print("❌ No game log table found")
                continue
            
            # Analyze the table
            rows = game_log_table.find_all('tr')
            print(f"📋 Found {len(rows)} rows")
            
            # Count games by year
            games_by_year = {}
            total_games = 0
            valid_games = 0
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 3:
                    continue
                
                try:
                    date_text = cells[0].get_text(strip=True)
                    if not date_text or date_text.lower() in ['date', '']:
                        continue
                    
                    # Parse date
                    game_date = None
                    try:
                        game_date = datetime.strptime(date_text, '%b %d, %Y')
                    except ValueError:
                        try:
                            game_date = datetime.strptime(date_text, '%m/%d/%Y')
                        except ValueError:
                            match = re.search(r'(\d{1,2})/(\d{1,2})', date_text)
                            if match:
                                month = int(match.group(1))
                                day = int(match.group(2))
                                game_date = datetime(2024, month, day)
                    
                    if game_date:
                        year = game_date.year
                        games_by_year[year] = games_by_year.get(year, 0) + 1
                        total_games += 1
                        
                        # Check if it's 2023-24 season
                        if game_date.year == 2023 and game_date.month >= 10:
                            valid_games += 1
                        elif game_date.year == 2024 and game_date.month <= 4:
                            valid_games += 1
                        
                        # Show first few games
                        if total_games <= 5:
                            opponent = cells[1].get_text(strip=True) if len(cells) > 1 else "N/A"
                            result = cells[2].get_text(strip=True) if len(cells) > 2 else "N/A"
                            print(f"  {game_date.strftime('%Y-%m-%d')} vs {opponent} - {result}")
                    
                except Exception as e:
                    print(f"Error parsing row: {e}")
                    continue
            
            print("\n📊 RESULTS:")
            print(f"  Total games found: {total_games}")
            print(f"  Valid 2023-24 games: {valid_games}")
            print("  Games by year:")
            for year, count in sorted(games_by_year.items()):
                print(f"    {year}: {count} games")
            
            if valid_games > 0:
                print(f"✅ SUCCESS! Found {valid_games} valid 2023-24 season games")
                return True
            else:
                print("❌ No valid 2023-24 season games found")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            continue
    
    print("❌ Failed to get valid data from any URL")
    return False

def main():
    """Test the fixed collection method."""
    
    # Test with Cameron Johnson who we know has data
    test_players = [
        ("3138196", "Cameron Johnson"),
        ("4432174", "Cam Thomas"),
    ]
    
    for player_id, player_name in test_players:
        print(f"\n{'='*80}")
        print(f"TESTING PLAYER: {player_name} ({player_id})")
        print(f"{'='*80}")
        
        success = test_player_collection(player_id, player_name)
        
        if success:
            print(f"✅ {player_name} test PASSED")
        else:
            print(f"❌ {player_name} test FAILED")

if __name__ == "__main__":
    main() 