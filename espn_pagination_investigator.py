#!/usr/bin/env python3
"""
Investigate ESPN pagination and "view all" functionality for complete season data.
"""

import requests
import logging
from bs4 import BeautifulSoup
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def investigate_espn_pagination(player_id: str, player_name: str):
    """Investigate ESPN page for pagination or "view all" functionality."""
    
    print(f"\n{'='*80}")
    print(f"INVESTIGATING ESPN PAGINATION: {player_name} ({player_id})")
    print(f"{'='*80}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Try the standard URL that worked best
    player_name_url = player_name.lower().replace(' ', '-')
    url = f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/{player_name_url}"
    
    print(f"URL: {url}")
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Check for pagination elements
        print(f"\n🔍 SEARCHING FOR PAGINATION ELEMENTS:")
        
        # Look for "Load More" buttons
        load_more_buttons = soup.find_all(text=re.compile(r'load more|show more|view all|see all', re.I))
        print(f"Load More buttons found: {len(load_more_buttons)}")
        for btn in load_more_buttons[:3]:
            print(f"  - {btn}")
        
        # Look for pagination controls
        pagination = soup.find_all(['nav', 'div'], class_=re.compile(r'pagination|pager', re.I))
        print(f"Pagination controls found: {len(pagination)}")
        
        # Look for season selectors
        season_selectors = soup.find_all(text=re.compile(r'season|2023|2024', re.I))
        print(f"Season-related elements found: {len(season_selectors)}")
        for elem in season_selectors[:5]:
            print(f"  - {elem}")
        
        # Look for "View All Games" links
        view_all_links = soup.find_all('a', href=re.compile(r'gamelog|games', re.I))
        print(f"Game log links found: {len(view_all_links)}")
        for link in view_all_links[:3]:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            print(f"  - {text}: {href}")
        
        # Look for JavaScript that might load more data
        scripts = soup.find_all('script')
        print(f"Scripts found: {len(scripts)}")
        
        # Check for data attributes that might indicate total games
        data_attrs = soup.find_all(attrs={'data-games': True})
        print(f"Elements with data-games attribute: {len(data_attrs)}")
        
        # Look for any text indicating total games
        total_games_text = soup.find_all(text=re.compile(r'\d+ games?|total games?', re.I))
        print(f"Total games references found: {len(total_games_text)}")
        for text in total_games_text[:3]:
            print(f"  - {text}")
        
        # Check the actual table structure
        print(f"\n📊 TABLE ANALYSIS:")
        tables = soup.find_all('table')
        print(f"Total tables found: {len(tables)}")
        
        for i, table in enumerate(tables):
            rows = table.find_all('tr')
            print(f"Table {i+1}: {len(rows)} rows")
            
            # Check if this looks like a game log table
            table_text = table.get_text().lower()
            if 'date' in table_text and ('opp' in table_text or 'opponent' in table_text):
                print(f"  ✅ This looks like a game log table")
                
                # Count actual game rows (excluding headers)
                game_rows = 0
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) > 5:  # Looks like a data row
                        date_cell = cells[0].get_text(strip=True)
                        if date_cell and date_cell.lower() not in ['date', '']:
                            game_rows += 1
                
                print(f"  📋 Actual game rows: {game_rows}")
                
                # Check if there are any indicators of more data
                if game_rows < 20:  # Suspiciously few games
                    print(f"  ⚠️ Only {game_rows} games found - likely incomplete")
                    
                    # Look for any text suggesting there are more games
                    surrounding_text = table.find_parent().get_text() if table.find_parent() else ""
                    if 'more' in surrounding_text.lower() or 'all' in surrounding_text.lower():
                        print(f"  🔍 Found text suggesting more data available")
        
        # Check for any AJAX endpoints or API calls
        print(f"\n🔌 LOOKING FOR API ENDPOINTS:")
        for script in scripts:
            script_text = script.get_text()
            if 'gamelog' in script_text or 'api' in script_text:
                print(f"  Found script with potential API references")
                # Look for URLs in the script
                urls = re.findall(r'https?://[^\s"\']+', script_text)
                for url in urls[:3]:
                    if 'espn' in url:
                        print(f"    - {url}")
        
        # Check if there's a different URL format we should try
        print(f"\n🔗 ALTERNATIVE URL FORMATS TO TRY:")
        alternative_urls = [
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}",
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/season/2024/type/1",
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/type/1",
            f"https://www.espn.com/nba/player/gamelog/_/id/{player_id}/view/all",
        ]
        
        for alt_url in alternative_urls:
            print(f"  - {alt_url}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

def main():
    """Investigate ESPN pagination for sample players."""
    
    # Test with Cameron Johnson who we know has some data
    test_players = [
        ("3138196", "Cameron Johnson"),
    ]
    
    for player_id, player_name in test_players:
        investigate_espn_pagination(player_id, player_name)

if __name__ == "__main__":
    main() 