"""
Script to find correct player IDs from our collected data.
"""

import requests
from bs4 import BeautifulSoup
import time

def find_player_ids():
    """Find correct player IDs by searching Basketball Reference."""
    
    base_url = "https://www.basketball-reference.com"
    
    # Search for players by name
    search_terms = [
        "LeBron James",
        "Stephen Curry", 
        "Kevin Durant",
        "Giannis Antetokounmpo",
        "Joel Embiid"
    ]
    
    for search_term in search_terms:
        print(f"\nSearching for: {search_term}")
        
        # Search URL
        search_url = f"{base_url}/search/search.fcgi?search={search_term.replace(' ', '+')}"
        
        try:
            response = requests.get(search_url, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Look for player links
                player_links = soup.find_all('a', href=True)
                
                for link in player_links:
                    href = link.get('href', '')
                    if '/players/' in href and '.html' in href:
                        # Extract player ID from URL
                        player_id = href.split('/players/')[1].split('/')[0]
                        player_name = link.get_text(strip=True)
                        
                        if search_term.lower() in player_name.lower():
                            print(f"  Found: {player_name} -> {player_id}")
                            
                            # Test if this player ID works for game logs
                            test_url = f"{base_url}/players/{player_id}/gamelog/2024/"
                            test_response = requests.get(test_url, timeout=10)
                            
                            if test_response.status_code == 200:
                                print(f"    ✅ Game log works: {test_url}")
                            else:
                                print(f"    ❌ Game log fails: {test_response.status_code}")
                            
                            break
            else:
                print(f"  ❌ Search failed: {response.status_code}")
                
        except Exception as e:
            print(f"  ❌ Error: {e}")
        
        time.sleep(2)  # Be respectful

if __name__ == "__main__":
    find_player_ids() 