#!/usr/bin/env python3
"""
Investigate alternative data sources for complete 2023-24 season data.
"""

import requests
import logging
from bs4 import BeautifulSoup
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_basketball_reference():
    """Test Basketball Reference for complete season data."""
    
    print(f"\n{'='*60}")
    print(f"TESTING BASKETBALL REFERENCE")
    print(f"{'='*60}")
    
    # Test with a known player
    player_url = "https://www.basketball-reference.com/players/j/johnsca01/gamelog/2024/"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(player_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the game log table
        table = soup.find('table', {'id': 'pgl_basic'})
        if table:
            rows = table.find_all('tr', class_=lambda x: x != 'thead')
            print(f"✅ Found {len(rows)} games on Basketball Reference")
            
            # Show sample games
            for i, row in enumerate(rows[:5]):
                cells = row.find_all('td')
                if len(cells) > 2:
                    date = cells[0].get_text(strip=True) if cells[0] else "N/A"
                    opponent = cells[1].get_text(strip=True) if cells[1] else "N/A"
                    points = cells[26].get_text(strip=True) if len(cells) > 26 else "N/A"
                    print(f"  {date} vs {opponent} - {points} pts")
            
            return True
        else:
            print("❌ No game log table found")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_nba_api():
    """Test NBA API for complete season data."""
    
    print(f"\n{'='*60}")
    print(f"TESTING NBA API")
    print(f"{'='*60}")
    
    # Test with a known player ID
    player_id = "1629637"  # Ja Morant
    url = f"https://stats.nba.com/stats/playergamelog?PlayerID={player_id}&Season=2023-24&SeasonType=Regular%20Season"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://www.nba.com/',
        'Accept': 'application/json, text/plain, */*',
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if 'resultSets' in data and len(data['resultSets']) > 0:
            games = data['resultSets'][0]['rowSet']
            print(f"✅ Found {len(games)} games via NBA API")
            
            # Show sample games
            for i, game in enumerate(games[:5]):
                date = game[1] if len(game) > 1 else "N/A"
                opponent = game[2] if len(game) > 2 else "N/A"
                points = game[26] if len(game) > 26 else "N/A"
                print(f"  {date} vs {opponent} - {points} pts")
            
            return True
        else:
            print("❌ No data found in NBA API response")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_espn_api():
    """Test ESPN API for complete season data."""
    
    print(f"\n{'='*60}")
    print(f"TESTING ESPN API")
    print(f"{'='*60}")
    
    # Test with a known player ID
    player_id = "3138196"  # Cameron Johnson
    url = f"https://site.web.api.espn.com/apis/site/v2/sports/basketball/nba/athletes/{player_id}/gamelog"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        if 'events' in data:
            games = data['events']
            print(f"✅ Found {len(games)} games via ESPN API")
            
            # Show sample games
            for i, game in enumerate(games[:5]):
                date = game.get('date', 'N/A')
                opponent = game.get('competitions', [{}])[0].get('competitors', [{}])[1].get('team', {}).get('name', 'N/A')
                stats = game.get('stats', [])
                points = next((stat['value'] for stat in stats if stat['name'] == 'points'), 'N/A')
                print(f"  {date} vs {opponent} - {points} pts")
            
            return True
        else:
            print("❌ No events found in ESPN API response")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    """Test different data sources for complete season data."""
    
    print("INVESTIGATING DATA SOURCES FOR COMPLETE 2023-24 SEASON DATA")
    print("="*80)
    
    sources = [
        ("Basketball Reference", test_basketball_reference),
        ("NBA API", test_nba_api),
        ("ESPN API", test_espn_api),
    ]
    
    results = {}
    
    for source_name, test_func in sources:
        try:
            success = test_func()
            results[source_name] = success
        except Exception as e:
            print(f"❌ {source_name} test failed: {e}")
            results[source_name] = False
    
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    
    for source_name, success in results.items():
        status = "✅ WORKING" if success else "❌ FAILED"
        print(f"{source_name}: {status}")
    
    print(f"\nRECOMMENDATIONS:")
    if results.get("Basketball Reference", False):
        print("✅ Basketball Reference appears to have complete season data")
        print("   - Consider using Basketball Reference scraper")
    if results.get("NBA API", False):
        print("✅ NBA API appears to have complete season data")
        print("   - Consider using NBA API (may have rate limits)")
    if results.get("ESPN API", False):
        print("✅ ESPN API appears to have complete season data")
        print("   - Consider using ESPN API instead of web scraping")
    
    if not any(results.values()):
        print("❌ All tested sources failed")
        print("   - May need to investigate other sources or methods")

if __name__ == "__main__":
    main() 