"""
Test script to check NBA API connectivity.
"""

import requests
import time
from src.utils.config import config

def test_nba_api():
    """Test NBA API connectivity."""
    print("Testing NBA API connectivity...")
    
    # Get configuration
    base_url = config.get('data_sources.nba_api.base_url')
    headers = config.get('data_sources.nba_api.headers', {})
    
    print(f"Base URL: {base_url}")
    print(f"Headers: {headers}")
    
    # Test simple endpoint
    test_endpoint = "leaguedashteamstats"
    url = f"{base_url}{test_endpoint}"
    
    params = {
        "Season": "2023-24",
        "SeasonType": "Regular Season",
        "PerMode": "PerGame"
    }
    
    print(f"Testing URL: {url}")
    print(f"Parameters: {params}")
    
    try:
        # Test with shorter timeout first
        print("Testing with 10 second timeout...")
        response = requests.get(url, params=params, headers=headers, timeout=10)
        print(f"Response status: {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            if 'resultSets' in data:
                print(f"Number of result sets: {len(data['resultSets'])}")
                if data['resultSets']:
                    print(f"First result set name: {data['resultSets'][0].get('name', 'Unknown')}")
                    print(f"Number of rows: {len(data['resultSets'][0].get('rowSet', []))}")
        else:
            print(f"Error response: {response.text[:500]}")
            
    except requests.exceptions.Timeout:
        print("❌ Request timed out - API might be slow or blocked")
    except requests.exceptions.ConnectionError as e:
        print(f"❌ Connection error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

def test_alternative_approach():
    """Test alternative data sources."""
    print("\nTesting alternative approaches...")
    
    # Test Basketball Reference (might be more reliable)
    print("Testing Basketball Reference...")
    try:
        response = requests.get("https://www.basketball-reference.com/leagues/NBA_2024.html", timeout=10)
        print(f"Basketball Reference status: {response.status_code}")
        if response.status_code == 200:
            print("✅ Basketball Reference accessible")
        else:
            print("❌ Basketball Reference not accessible")
    except Exception as e:
        print(f"❌ Basketball Reference error: {e}")

if __name__ == "__main__":
    test_nba_api()
    test_alternative_approach() 