#!/usr/bin/env python3
"""
Test ESPN game log scraping for a single player.
"""

import requests
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test with Shai Gilgeous-Alexander (ID: 4278073)
PLAYER_ID = "4278073"
PLAYER_NAME = "shai-gilgeous-alexander"

url = f"https://www.espn.com/nba/player/gamelog/_/id/{PLAYER_ID}/{PLAYER_NAME}"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

print(f"Testing URL: {url}")

try:
    response = requests.get(url, headers=headers, timeout=30)
    print(f"Status code: {response.status_code}")
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for game log table
        tables = soup.find_all('table')
        print(f"Found {len(tables)} tables on the page")
        
        for i, table in enumerate(tables):
            print(f"Table {i}: classes = {table.get('class', 'No class')}")
            
            # Check if this looks like a game log table
            rows = table.find_all('tr')
            if rows:
                first_row = rows[0]
                cells = first_row.find_all(['td', 'th'])
                if cells:
                    headers = [cell.get_text(strip=True) for cell in cells]
                    print(f"  Headers: {headers}")
                    
                    # If we see date-like headers, this might be our table
                    if any('date' in h.lower() or 'opp' in h.lower() for h in headers):
                        print("  This looks like a game log table!")
                        print(f"  Number of rows: {len(rows)}")
                        
                        # Show first few data rows
                        for j, row in enumerate(rows[1:6]):  # Skip header, show next 5
                            cells = row.find_all(['td', 'th'])
                            if cells:
                                data = [cell.get_text(strip=True) for cell in cells]
                                print(f"  Row {j+1}: {data}")
                        break
    else:
        print(f"Failed to load page: {response.status_code}")
        
except Exception as e:
    print(f"Error: {e}") 