"""
Test script to inspect Basketball Reference page structure.
"""

import requests
from bs4 import BeautifulSoup
import time

def inspect_basketball_reference():
    """Inspect the Basketball Reference page to find table IDs."""
    
    # Try different URLs for player statistics
    urls_to_test = [
        "https://www.basketball-reference.com/leagues/NBA_2024.html",
        "https://www.basketball-reference.com/leagues/NBA_2024_per_game.html",
        "https://www.basketball-reference.com/leagues/NBA_2024_advanced.html",
        "https://www.basketball-reference.com/leagues/NBA_2024_totals.html",
        # Try 2023 season
        "https://www.basketball-reference.com/leagues/NBA_2023_per_game.html",
        "https://www.basketball-reference.com/leagues/NBA_2023.html"
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for url in urls_to_test:
        print(f"\nTesting URL: {url}")
        print("=" * 60)
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for player-specific tables
            tables = soup.find_all('table')
            player_tables = []
            
            for table in tables:
                table_id = table.get('id', '')
                if any(keyword in table_id.lower() for keyword in ['player', 'per_game', 'stats']):
                    tbody = table.find('tbody')
                    if tbody:
                        rows = tbody.find_all('tr')
                        if rows:
                            first_row = rows[0]
                            cells = first_row.find_all(['td', 'th'])
                            if cells:
                                first_cell_text = cells[0].get_text(strip=True)
                                has_links = bool(first_row.find('a'))
                                
                                if has_links and not first_cell_text.isdigit():
                                    player_tables.append((table_id, len(rows), first_cell_text))
            
            if player_tables:
                print(f"Found {len(player_tables)} potential player tables:")
                for table_id, row_count, first_cell in player_tables:
                    print(f"  - {table_id}: {row_count} rows, first: '{first_cell}'")
                    
                    # Show sample player data
                    table = soup.find('table', {'id': table_id})
                    if table:
                        tbody = table.find('tbody')
                        if tbody:
                            rows = tbody.find_all('tr')[:3]
                            for j, row in enumerate(rows):
                                cells = row.find_all(['td', 'th'])
                                if cells:
                                    player_cell = cells[0]
                                    player_name = player_cell.get_text(strip=True)
                                    player_link = player_cell.find('a')
                                    
                                    if player_link:
                                        href = player_link.get('href', '')
                                        player_id = href.split('/')[-1].replace('.html', '') if href else 'NO_ID'
                                        print(f"    Row {j+1}: '{player_name}' -> {player_id}")
            else:
                print("No player tables found on this page")
                
        except Exception as e:
            print(f"Error: {e}")
        
        time.sleep(2)  # Be respectful

if __name__ == "__main__":
    inspect_basketball_reference() 