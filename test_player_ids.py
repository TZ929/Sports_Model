"""
Test script to find correct player IDs for Basketball Reference.
"""

import requests
from bs4 import BeautifulSoup
import time

def test_player_urls():
    """Test different player ID formats to find the correct ones."""
    
    base_url = "https://www.basketball-reference.com"
    
    # Test different player ID formats
    test_players = [
        ("jamesle01", "LeBron James"),
        ("curryst01", "Stephen Curry"), 
        ("duranke01", "Kevin Durant"),
        ("giannke01", "Giannis Antetokounmpo"),
        ("embiijo01", "Joel Embiid"),
        # Try alternative formats
        ("jamesle", "LeBron James alt"),
        ("curryst", "Stephen Curry alt"),
        ("duranke", "Kevin Durant alt"),
    ]
    
    for player_id, name in test_players:
        # Test 2024 season (2023-24)
        url_2024 = f"{base_url}/players/{player_id}/gamelog/2024/"
        
        # Test 2023 season (2022-23)
        url_2023 = f"{base_url}/players/{player_id}/gamelog/2023/"
        
        print(f"\nTesting {name} ({player_id}):")
        
        # Test 2024
        try:
            response = requests.get(url_2024, timeout=10)
            if response.status_code == 200:
                print(f"  ✅ 2024: {url_2024}")
                # Check if it has game data
                soup = BeautifulSoup(response.content, 'html.parser')
                table = soup.find('table', {'id': 'pgl_basic'})
                if table:
                    rows = table.find('tbody').find_all('tr')
                    game_count = len([r for r in rows if 'thead' not in r.get('class', [])])
                    print(f"     Found {game_count} games")
                else:
                    print(f"     No game table found")
            else:
                print(f"  ❌ 2024: {response.status_code}")
        except Exception as e:
            print(f"  ❌ 2024: Error - {e}")
        
        # Test 2023
        try:
            response = requests.get(url_2023, timeout=10)
            if response.status_code == 200:
                print(f"  ✅ 2023: {url_2023}")
                # Check if it has game data
                soup = BeautifulSoup(response.content, 'html.parser')
                table = soup.find('table', {'id': 'pgl_basic'})
                if table:
                    rows = table.find('tbody').find_all('tr')
                    game_count = len([r for r in rows if 'thead' not in r.get('class', [])])
                    print(f"     Found {game_count} games")
                else:
                    print(f"     No game table found")
            else:
                print(f"  ❌ 2023: {response.status_code}")
        except Exception as e:
            print(f"  ❌ 2023: Error - {e}")
        
        time.sleep(2)  # Be respectful

if __name__ == "__main__":
    test_player_urls() 