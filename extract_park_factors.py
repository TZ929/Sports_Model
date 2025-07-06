import requests
import re
import json
import pandas as pd
from io import StringIO

def extract_park_factors(year=2024, data_type='runs'):
    """Extract park factors data from Baseball Savant embedded JavaScript.
    
    Args:
        year: Season year (default 2024)
        data_type: 'runs', 'distance', or 'default' (default 'runs')
    
    Returns:
        pandas.DataFrame: Park factors data
    """
    # Build URL based on data type
    base_url = f'https://baseballsavant.mlb.com/leaderboard/statcast-park-factors?year={year}'
    if data_type == 'distance':
        url = f'{base_url}&type=distance'
    elif data_type == 'runs':
        url = f'{base_url}&type=runs'
    else:
        url = base_url
    
    print(f"Fetching: {url}")
    response = requests.get(url)
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code}")
    
    html = response.text
    
    # Look for the JavaScript variable containing the data
    # Pattern: var data = [{"key":"value",...}]
    pattern = r'var data = (\[.*?\]);'
    match = re.search(pattern, html, re.DOTALL)
    
    if not match:
        raise Exception("Could not find data variable in HTML")
    
    # Extract and parse the JSON data
    json_str = match.group(1)
    try:
        data = json.loads(json_str)
        print(f"Successfully extracted {len(data)} records")
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        print(f"DataFrame shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        return df
        
    except json.JSONDecodeError as e:
        print(f"JSON parsing failed: {e}")
        print(f"JSON string preview: {json_str[:500]}...")
        raise

def main():
    """Extract park factors for different data types and save to CSV."""
    
    # Create data directory if it doesn't exist
    import os
    os.makedirs('data/raw', exist_ok=True)
    
    # Extract different types of park factors
    data_types = ['runs', 'distance']
    
    for data_type in data_types:
        try:
            print(f"\n=== Extracting {data_type} park factors ===")
            df = extract_park_factors(year=2024, data_type=data_type)
            
            # Save to CSV
            filename = f'data/raw/park_factors_2024_{data_type}.csv'
            df.to_csv(filename, index=False)
            print(f"Saved to: {filename}")
            
            # Show sample data
            print(f"\nSample data:")
            print(df.head())
            
        except Exception as e:
            print(f"Failed to extract {data_type} park factors: {e}")

if __name__ == "__main__":
    main() 