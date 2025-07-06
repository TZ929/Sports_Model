import requests
import re
import json
import pandas as pd

# Try different URL patterns that Baseball Savant might use
base_urls = [
    'https://baseballsavant.mlb.com/leaderboard/statcast-park-factors?year=2024',
    'https://baseballsavant.mlb.com/leaderboard/statcast-park-factors?year=2024&type=distance',
    'https://baseballsavant.mlb.com/leaderboard/statcast-park-factors?year=2024&type=runs',
    'https://baseballsavant.mlb.com/api/park-factors?year=2024',
    'https://baseballsavant.mlb.com/api/statcast-park-factors?year=2024',
]

# Also try the CSV download endpoints
csv_urls = [
    'https://baseballsavant.mlb.com/leaderboard/statcast-park-factors?year=2024&download=true',
    'https://baseballsavant.mlb.com/leaderboard/statcast-park-factors?year=2024&type=distance&download=true',
    'https://baseballsavant.mlb.com/leaderboard/statcast-park-factors?year=2024&type=runs&download=true',
]

print("=== Testing CSV Download URLs ===")
for url in csv_urls:
    print(f"\nTrying: {url}")
    try:
        response = requests.get(url)
        print(f"Status: {response.status_code}")
        print(f"Content-Type: {response.headers.get('content-type', 'unknown')}")
        print(f"Content length: {len(response.text)}")
        
        # Check if it looks like CSV
        if response.status_code == 200 and len(response.text) > 100:
            lines = response.text.split('\n')[:5]
            print(f"First 5 lines:")
            for i, line in enumerate(lines):
                print(f"  {i+1}: {line}")
            
            # Try to parse as CSV
            try:
                from io import StringIO
                df = pd.read_csv(StringIO(response.text))
                print(f"Successfully parsed as CSV: {df.shape}")
                print(f"Columns: {list(df.columns)}")
                if len(df) > 0:
                    print(f"Sample row: {df.iloc[0].to_dict()}")
                break
            except Exception as e:
                print(f"CSV parsing failed: {e}")
                
    except Exception as e:
        print(f"Request failed: {e}")

print("\n=== Testing Regular URLs ===")
for url in base_urls:
    print(f"\nTrying: {url}")
    try:
        response = requests.get(url)
        print(f"Status: {response.status_code}")
        print(f"Content-Type: {response.headers.get('content-type', 'unknown')}")
        
        if response.status_code == 200:
            content = response.text
            print(f"Content length: {len(content)}")
            
            # Look for JSON data
            if content.strip().startswith('{') or content.strip().startswith('['):
                try:
                    data = json.loads(content)
                    print(f"Successfully parsed as JSON")
                    if isinstance(data, list) and data:
                        print(f"List with {len(data)} items")
                        print(f"First item: {data[0]}")
                    elif isinstance(data, dict):
                        print(f"Dict with keys: {list(data.keys())}")
                    break
                except json.JSONDecodeError:
                    print("Not valid JSON")
            
            # Look for embedded data in script tags
            script_matches = re.findall(r'<script[^>]*>(.*?)</script>', content, re.DOTALL)
            for i, script in enumerate(script_matches):
                if 'park' in script.lower() or 'factor' in script.lower():
                    print(f"Found potentially relevant script {i}: {script[:200]}...")
                    
    except Exception as e:
        print(f"Request failed: {e}")

print("\n=== Manual CSV Creation ===")
print("If no automatic method works, we can create a manual CSV with 2024 park factors from multiple sources.") 