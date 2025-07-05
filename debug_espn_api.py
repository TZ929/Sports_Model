"""
Debug script to test ESPN API and print raw responses.
"""

import requests
import json

def debug_espn_player_stats():
    """Debug ESPN player stats API."""
    
    # Test with Clint Capela (ID: 3102529)
    player_id = "3102529"
    url = f"https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/athletes/{player_id}/gamelog"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    print(f"Testing ESPN API for player {player_id}")
    print(f"URL: {url}")
    print("=" * 60)
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("Raw JSON Response:")
            print(json.dumps(data, indent=2))
            
            # Check for gamelog data
            if 'gamelog' in data:
                print(f"\nFound {len(data['gamelog'])} games in gamelog")
                if data['gamelog']:
                    print("Sample game data:")
                    print(json.dumps(data['gamelog'][0], indent=2))
            else:
                print("\nNo 'gamelog' key found in response")
                print("Available keys:", list(data.keys()))
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            
    except Exception as e:
        print(f"Exception: {e}")

def debug_espn_alternative_endpoints():
    """Try alternative ESPN endpoints for player stats."""
    
    player_id = "3102529"
    
    # Try different ESPN endpoints
    endpoints = [
        f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/athletes/{player_id}/stats",
        f"https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/athletes/{player_id}/stats",
        f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/athletes/{player_id}/gamelog"
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for i, url in enumerate(endpoints):
        print(f"\nTesting endpoint {i+1}: {url}")
        print("-" * 40)
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            print(f"Status Code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print("Available keys:", list(data.keys()))
                
                # Look for stats-related keys
                stats_keys = [k for k in data.keys() if 'stat' in k.lower() or 'game' in k.lower() or 'log' in k.lower()]
                if stats_keys:
                    print("Potential stats keys:", stats_keys)
                    
                    for key in stats_keys[:2]:  # Show first 2
                        print(f"\n{key} data:")
                        print(json.dumps(data[key][:1], indent=2))  # Show first item
            else:
                print(f"Error: {response.status_code}")
                
        except Exception as e:
            print(f"Exception: {e}")

def debug_player_ids():
    """Debug what player IDs are being selected from database."""
    from src.utils.database import db_manager
    from sqlalchemy import text
    
    session = db_manager.get_session()
    
    # Get all non-BR player IDs
    all_player_ids = [row[0] for row in session.execute(text("SELECT player_id FROM players WHERE player_id NOT LIKE 'BR_%' LIMIT 10"))]
    print("All non-BR player IDs:")
    for pid in all_player_ids:
        print(f"  '{pid}' (isdigit: {pid.isdigit()})")
    
    # Filter for numeric ones
    numeric_ids = [pid for pid in all_player_ids if pid.isdigit()]
    print(f"\nNumeric player IDs ({len(numeric_ids)}):")
    for pid in numeric_ids:
        print(f"  {pid}")
    
    session.close()

def debug_espn_stats_parsing():
    """Debug ESPN stats parsing step by step."""
    from src.data_collection.espn_api import ESPNAPICollector
    
    collector = ESPNAPICollector()
    player_id = "3102529"  # Clint Capela
    
    print(f"Debugging ESPN stats parsing for player {player_id}")
    print("=" * 60)
    
    # Get raw data
    url = f"https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/athletes/{player_id}/stats"
    response = requests.get(url, headers=collector.headers, timeout=30)
    
    if response.status_code == 200:
        data = response.json()
        
        print(f"Categories found: {len(data.get('categories', []))}")
        
        for i, category in enumerate(data.get('categories', [])):
            print(f"\nCategory {i+1}: {category.get('displayName', 'Unknown')}")
            print(f"  Type: {category.get('type', 'Unknown')}")
            print(f"  Events: {len(category.get('events', []))}")
            
            if category.get('events'):
                print(f"  Sample event stats: {category['events'][0].get('stats', [])}")
                
                # Test parsing
                event = category['events'][0]
                event_stats = event.get('stats', [])
                
                if len(event_stats) >= 14:
                    print("  Parsed stats:")
                    print(f"    Minutes: {event_stats[0]}")
                    print(f"    FG: {event_stats[1]}")
                    print(f"    3PT: {event_stats[3]}")
                    print(f"    FT: {event_stats[5]}")
                    print(f"    REB: {event_stats[7]}")
                    print(f"    AST: {event_stats[8]}")
                    print(f"    PTS: {event_stats[13]}")
                else:
                    print(f"  Not enough stats: {len(event_stats)} < 14")
    else:
        print(f"Error: {response.status_code}")

def debug_espn_game_logs():
    """Debug ESPN game logs endpoints."""
    from src.data_collection.espn_api import ESPNAPICollector
    
    collector = ESPNAPICollector()
    player_id = "3102529"  # Clint Capela
    
    print(f"Debugging ESPN game logs for player {player_id}")
    print("=" * 60)
    
    # Try different endpoints for game logs
    endpoints = [
        f"https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/athletes/{player_id}/gamelog",
        f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/athletes/{player_id}/gamelog",
        f"https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/athletes/{player_id}/stats?type=event",
        f"https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/athletes/{player_id}/stats?type=game"
    ]
    
    for i, url in enumerate(endpoints):
        print(f"\nTesting endpoint {i+1}: {url}")
        print("-" * 40)
        
        try:
            response = requests.get(url, headers=collector.headers, timeout=30)
            print(f"Status Code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print("Available keys:", list(data.keys()))
                
                # Look for events or gamelog
                if 'events' in data:
                    print(f"Events found: {len(data['events'])}")
                if 'gamelog' in data:
                    print(f"Gamelog found: {len(data['gamelog'])}")
                if 'categories' in data:
                    print(f"Categories found: {len(data['categories'])}")
                    for j, cat in enumerate(data['categories']):
                        print(f"  Category {j+1}: {cat.get('displayName', 'Unknown')} - Events: {len(cat.get('events', []))}")
            else:
                print(f"Error: {response.status_code}")
                
        except Exception as e:
            print(f"Exception: {e}")

if __name__ == "__main__":
    print("Debugging ESPN API...")
    debug_espn_player_stats()
    print("\n" + "="*60)
    debug_espn_alternative_endpoints()
    print("\n" + "="*60)
    debug_player_ids()
    print("\n" + "="*60)
    debug_espn_stats_parsing()
    print("\n" + "="*60)
    debug_espn_game_logs()

#!/usr/bin/env python3
"""
Debug ESPN API player stats endpoint for a known valid player ID.
"""


PLAYER_ID = "3102529"  # Clint Capela
SEASON = "2024"

url = f"https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/athletes/{PLAYER_ID}/stats"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

response = requests.get(url, headers=headers, timeout=30)
print(f"Status code: {response.status_code}")

try:
    data = response.json()
    print(json.dumps(data, indent=2))
except Exception as e:
    print(f"Error parsing JSON: {e}")
    print(response.text) 