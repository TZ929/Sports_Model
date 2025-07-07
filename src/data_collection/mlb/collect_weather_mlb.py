"""
MLB Weather Data Collection

This script collects historical and current weather data for MLB games using OpenWeatherMap API.
It matches games to ballpark coordinates and fetches weather conditions at game time.

Features:
- Historical weather data (1979-present) 
- Current/forecast weather for upcoming games
- Comprehensive weather metrics (temp, humidity, wind, precipitation)
- Ballpark coordinate mapping
- Rate limiting and error handling

Example usage:
    python collect_weather_mlb.py --year 2024 --save-dir data/raw
    python collect_weather_mlb.py --all-years --ballpark-only
"""

import argparse
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import json

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# MLB Ballpark Coordinates (latitude, longitude)
MLB_BALLPARK_COORDS = {
    'NYY': (40.829659, -73.926186),    # Yankee Stadium
    'NYM': (40.757256, -73.846237),    # Citi Field  
    'BOS': (42.346676, -71.097218),    # Fenway Park
    'TB': (27.768211, -82.653388),     # Tropicana Field
    'TOR': (43.641438, -79.389353),    # Rogers Centre
    'BAL': (39.283787, -76.621689),    # Oriole Park at Camden Yards
    'CWS': (41.830000, -87.633611),    # Guaranteed Rate Field
    'CLE': (41.495861, -81.685256),    # Progressive Field
    'DET': (42.339144, -83.048644),    # Comerica Park
    'KC': (39.051667, -94.480556),     # Kauffman Stadium
    'MIN': (44.981667, -93.277778),    # Target Field
    'HOU': (29.757222, -95.355556),    # Minute Maid Park
    'LAA': (33.800308, -117.882728),   # Angel Stadium
    'OAK': (37.751667, -122.200556),   # Oakland Coliseum
    'SEA': (47.591667, -122.332222),   # T-Mobile Park
    'TEX': (32.747222, -97.082500),    # Globe Life Field
    'ATL': (33.735000, -84.389722),    # Truist Park
    'MIA': (25.778135, -80.219650),    # loanDepot park
    'NYM': (40.757256, -73.846237),    # Citi Field
    'PHI': (39.906000, -75.166389),    # Citizens Bank Park
    'WSN': (38.873056, -77.007222),    # Nationals Park
    'CHC': (41.948333, -87.655556),    # Wrigley Field
    'CIN': (39.097222, -84.506944),    # Great American Ball Park
    'MIL': (43.028056, -87.971111),    # American Family Field
    'PIT': (40.447222, -80.005833),    # PNC Park
    'STL': (38.622500, -90.193056),    # Busch Stadium
    'ARI': (33.445564, -112.067413),   # Chase Field
    'COL': (39.756229, -104.994865),   # Coors Field
    'LAD': (34.073611, -118.240000),   # Dodger Stadium
    'SD': (32.707222, -117.157222),    # Petco Park
    'SF': (37.778572, -122.389717),    # Oracle Park
}

class WeatherDataCollector:
    """Collects weather data for MLB games using OpenWeatherMap API."""
    
    def __init__(self, api_key: str, rate_limit_delay: float = 1.0):
        """
        Initialize the weather data collector.
        
        Args:
            api_key: OpenWeatherMap API key
            rate_limit_delay: Delay between API calls in seconds
        """
        self.api_key = api_key
        self.rate_limit_delay = rate_limit_delay
        self.base_url = "https://api.openweathermap.org/data/2.5"
        self.historical_url = "https://history.openweathermap.org/data/2.5/history/city"
        self.onecall_url = "https://api.openweathermap.org/data/3.0/onecall"
        
        # Set up session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def get_historical_weather(self, lat: float, lon: float, timestamp: int) -> Optional[Dict]:
        """
        Get historical weather data for a specific location and time.
        
        Args:
            lat: Latitude
            lon: Longitude  
            timestamp: Unix timestamp
            
        Returns:
            Weather data dictionary or None if failed
        """
        try:
            # Use One Call API for historical data (more reliable)
            url = f"{self.onecall_url}/timemachine"
            params = {
                'lat': lat,
                'lon': lon,
                'dt': timestamp,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                return self._parse_historical_weather(data)
            elif response.status_code == 429:
                logger.warning("Rate limit exceeded, waiting...")
                time.sleep(60)  # Wait 1 minute for rate limit reset
                return self.get_historical_weather(lat, lon, timestamp)
            else:
                logger.error(f"API error {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching weather data: {e}")
            return None
    
    def get_current_weather(self, lat: float, lon: float) -> Optional[Dict]:
        """
        Get current weather data for a location.
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Weather data dictionary or None if failed
        """
        try:
            url = f"{self.base_url}/weather"
            params = {
                'lat': lat,
                'lon': lon,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                return self._parse_current_weather(data)
            else:
                logger.error(f"API error {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching current weather: {e}")
            return None
    
    def _parse_historical_weather(self, data: Dict) -> Dict:
        """Parse historical weather API response."""
        weather_data = data.get('data', [{}])[0]
        
        return {
            'temperature': weather_data.get('temp'),
            'feels_like': weather_data.get('feels_like'),
            'pressure': weather_data.get('pressure'),
            'humidity': weather_data.get('humidity'),
            'dew_point': weather_data.get('dew_point'),
            'uvi': weather_data.get('uvi'),
            'clouds': weather_data.get('clouds'),
            'visibility': weather_data.get('visibility'),
            'wind_speed': weather_data.get('wind_speed'),
            'wind_deg': weather_data.get('wind_deg'),
            'wind_gust': weather_data.get('wind_gust'),
            'weather_main': weather_data.get('weather', [{}])[0].get('main'),
            'weather_description': weather_data.get('weather', [{}])[0].get('description'),
            'weather_id': weather_data.get('weather', [{}])[0].get('id'),
            'rain_1h': weather_data.get('rain', {}).get('1h'),
            'snow_1h': weather_data.get('snow', {}).get('1h'),
            'dt': weather_data.get('dt')
        }
    
    def _parse_current_weather(self, data: Dict) -> Dict:
        """Parse current weather API response."""
        main = data.get('main', {})
        wind = data.get('wind', {})
        weather = data.get('weather', [{}])[0]
        
        return {
            'temperature': main.get('temp'),
            'feels_like': main.get('feels_like'),
            'pressure': main.get('pressure'),
            'humidity': main.get('humidity'),
            'temp_min': main.get('temp_min'),
            'temp_max': main.get('temp_max'),
            'clouds': data.get('clouds', {}).get('all'),
            'visibility': data.get('visibility'),
            'wind_speed': wind.get('speed'),
            'wind_deg': wind.get('deg'),
            'wind_gust': wind.get('gust'),
            'weather_main': weather.get('main'),
            'weather_description': weather.get('description'),
            'weather_id': weather.get('id'),
            'rain_1h': data.get('rain', {}).get('1h'),
            'snow_1h': data.get('snow', {}).get('1h'),
            'dt': data.get('dt')
        }
    
    def collect_game_weather(self, games_df: pd.DataFrame) -> pd.DataFrame:
        """
        Collect weather data for a DataFrame of games.
        
        Args:
            games_df: DataFrame with columns: game_date, home_team_id, start_time
            
        Returns:
            DataFrame with weather data added
        """
        weather_data = []
        total_games = len(games_df)
        
        logger.info(f"Collecting weather data for {total_games} games...")
        
        for idx, game in games_df.iterrows():
            row_num = idx if isinstance(idx, int) else 0
            if row_num % 100 == 0:
                logger.info(f"Processing game {row_num + 1}/{total_games}")
            
            # Get ballpark coordinates
            home_team = game.get('home_team_id')
            if home_team not in MLB_BALLPARK_COORDS:
                logger.warning(f"No coordinates found for team: {home_team}")
                weather_data.append({})
                continue
            
            lat, lon = MLB_BALLPARK_COORDS[home_team]
            
            # Convert game datetime to timestamp
            try:
                if 'game_datetime' in game:
                    game_dt = pd.to_datetime(game['game_datetime'])
                else:
                    # Combine date and time
                    game_date = pd.to_datetime(game['game_date'])
                    start_time = game.get('start_time', '19:00')  # Default 7 PM
                    if isinstance(game_date, pd.Timestamp):
                        game_dt = pd.to_datetime(f"{game_date.date()} {start_time}")
                    else:
                        game_dt = pd.to_datetime(f"{game_date} {start_time}")
                
                # Convert to UTC timestamp
                timestamp = int(pd.to_datetime(game_dt).timestamp())
                
                # Get weather data
                weather = self.get_historical_weather(lat, lon, timestamp)
                if weather is None:
                    weather = {}
                
                weather_data.append(weather)
                
                # Rate limiting
                time.sleep(self.rate_limit_delay)
                
            except Exception as e:
                logger.error(f"Error processing game {row_num}: {e}")
                weather_data.append({})
        
        # Add weather data to games DataFrame
        weather_df = pd.DataFrame(weather_data)
        
        # Add weather_ prefix to columns to avoid conflicts
        weather_df.columns = [f'weather_{col}' for col in weather_df.columns]
        
        # Combine with games data
        result_df = pd.concat([games_df.reset_index(drop=True), weather_df], axis=1)
        
        logger.info(f"Weather data collection complete. Added {len(weather_df.columns)} weather features.")
        
        return result_df
    
    def save_ballpark_coordinates(self, save_path: str):
        """Save ballpark coordinates to a CSV file."""
        coords_data = []
        for team, (lat, lon) in MLB_BALLPARK_COORDS.items():
            coords_data.append({
                'team_id': team,
                'latitude': lat,
                'longitude': lon
            })
        
        coords_df = pd.DataFrame(coords_data)
        coords_df.to_csv(save_path, index=False)
        logger.info(f"Saved ballpark coordinates to {save_path}")

def main():
    parser = argparse.ArgumentParser(description='Collect weather data for MLB games')
    parser.add_argument('--api-key', type=str, required=True, help='OpenWeatherMap API key')
    parser.add_argument('--games-file', type=str, help='Path to games CSV file')
    parser.add_argument('--save-dir', type=str, default='data/raw', help='Directory to save weather data')
    parser.add_argument('--year', type=int, help='Specific year to process')
    parser.add_argument('--ballpark-only', action='store_true', help='Only save ballpark coordinates')
    parser.add_argument('--rate-limit', type=float, default=1.0, help='Delay between API calls (seconds)')
    
    args = parser.parse_args()
    
    # Create save directory
    save_dir = Path(args.save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize collector
    collector = WeatherDataCollector(args.api_key, args.rate_limit)
    
    # Save ballpark coordinates if requested
    if args.ballpark_only:
        coords_path = save_dir / 'mlb_ballpark_coordinates.csv'
        collector.save_ballpark_coordinates(str(coords_path))
        return
    
    # Load games data
    if args.games_file:
        games_df = pd.read_csv(args.games_file)
    else:
        # Try to find games file
        possible_files = [
            f'data/raw/mlb_games_{args.year}.csv',
            'data/processed/mlb_games_with_features.csv',
            'data/raw/mlb_games.csv'
        ]
        
        games_df = None
        for file_path in possible_files:
            if Path(file_path).exists():
                games_df = pd.read_csv(file_path)
                logger.info(f"Loaded games from {file_path}")
                break
        
        if games_df is None:
            logger.error("No games file found. Please specify --games-file")
            return
    
    # Filter by year if specified
    if args.year:
        games_df['game_date'] = pd.to_datetime(games_df['game_date'])
        games_df = games_df[games_df['game_date'].dt.year == args.year]
        logger.info(f"Filtered to {len(games_df)} games for year {args.year}")
    
    # Collect weather data
    games_with_weather = collector.collect_game_weather(games_df)
    
    # Save results
    if args.year:
        output_file = save_dir / f'mlb_games_with_weather_{args.year}.csv'
    else:
        output_file = save_dir / 'mlb_games_with_weather.csv'
    
    games_with_weather.to_csv(output_file, index=False)
    logger.info(f"Saved weather data to {output_file}")
    
    # Print summary statistics
    weather_cols = [col for col in games_with_weather.columns if col.startswith('weather_')]
    logger.info(f"Weather data summary:")
    logger.info(f"- Total games: {len(games_with_weather)}")
    logger.info(f"- Weather features: {len(weather_cols)}")
    logger.info(f"- Games with weather data: {games_with_weather['weather_temperature'].notna().sum()}")

if __name__ == "__main__":
    main() 