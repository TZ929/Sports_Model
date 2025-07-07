"""
MLB Weather Features Engineering

This module adds weather-related features to MLB games data.
It can work with pre-collected weather data or simulate weather features
based on ballpark locations and seasonal patterns.

Key features added:
- Temperature and feels-like temperature
- Humidity and dew point
- Wind speed and direction
- Precipitation indicators
- Weather condition categories
- Seasonal weather patterns

Example usage:
    from weather_features_mlb import WeatherFeatureEngineer
    
    engineer = WeatherFeatureEngineer()
    games_with_weather = engineer.add_weather_features(games_df)
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict, Optional, Tuple
from datetime import datetime
import math

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MLB Ballpark Coordinates and Climate Info
MLB_BALLPARK_CLIMATE = {
    'NYY': {'lat': 40.829659, 'lon': -73.926186, 'climate': 'humid_continental'},
    'NYM': {'lat': 40.757256, 'lon': -73.846237, 'climate': 'humid_continental'},
    'BOS': {'lat': 42.346676, 'lon': -71.097218, 'climate': 'humid_continental'},
    'TB': {'lat': 27.768211, 'lon': -82.653388, 'climate': 'humid_subtropical'},
    'TOR': {'lat': 43.641438, 'lon': -79.389353, 'climate': 'humid_continental'},
    'BAL': {'lat': 39.283787, 'lon': -76.621689, 'climate': 'humid_subtropical'},
    'CWS': {'lat': 41.830000, 'lon': -87.633611, 'climate': 'humid_continental'},
    'CLE': {'lat': 41.495861, 'lon': -81.685256, 'climate': 'humid_continental'},
    'DET': {'lat': 42.339144, 'lon': -83.048644, 'climate': 'humid_continental'},
    'KC': {'lat': 39.051667, 'lon': -94.480556, 'climate': 'humid_continental'},
    'MIN': {'lat': 44.981667, 'lon': -93.277778, 'climate': 'humid_continental'},
    'HOU': {'lat': 29.757222, 'lon': -95.355556, 'climate': 'humid_subtropical'},
    'LAA': {'lat': 33.800308, 'lon': -117.882728, 'climate': 'mediterranean'},
    'OAK': {'lat': 37.751667, 'lon': -122.200556, 'climate': 'mediterranean'},
    'SEA': {'lat': 47.591667, 'lon': -122.332222, 'climate': 'oceanic'},
    'TEX': {'lat': 32.747222, 'lon': -97.082500, 'climate': 'humid_subtropical'},
    'ATL': {'lat': 33.735000, 'lon': -84.389722, 'climate': 'humid_subtropical'},
    'MIA': {'lat': 25.778135, 'lon': -80.219650, 'climate': 'tropical'},
    'PHI': {'lat': 39.906000, 'lon': -75.166389, 'climate': 'humid_subtropical'},
    'WSN': {'lat': 38.873056, 'lon': -77.007222, 'climate': 'humid_subtropical'},
    'CHC': {'lat': 41.948333, 'lon': -87.655556, 'climate': 'humid_continental'},
    'CIN': {'lat': 39.097222, 'lon': -84.506944, 'climate': 'humid_subtropical'},
    'MIL': {'lat': 43.028056, 'lon': -87.971111, 'climate': 'humid_continental'},
    'PIT': {'lat': 40.447222, 'lon': -80.005833, 'climate': 'humid_continental'},
    'STL': {'lat': 38.622500, 'lon': -90.193056, 'climate': 'humid_continental'},
    'ARI': {'lat': 33.445564, 'lon': -112.067413, 'climate': 'desert'},
    'COL': {'lat': 39.756229, 'lon': -104.994865, 'climate': 'semi_arid'},
    'LAD': {'lat': 34.073611, 'lon': -118.240000, 'climate': 'mediterranean'},
    'SD': {'lat': 32.707222, 'lon': -117.157222, 'climate': 'mediterranean'},
    'SF': {'lat': 37.778572, 'lon': -122.389717, 'climate': 'mediterranean'},
}

class WeatherFeatureEngineer:
    """Engineer weather features for MLB games."""
    
    def __init__(self, weather_data_path: Optional[str] = None):
        """
        Initialize the weather feature engineer.
        
        Args:
            weather_data_path: Path to pre-collected weather data CSV
        """
        self.weather_data_path = weather_data_path
        self.weather_data = None
        
        if weather_data_path and Path(weather_data_path).exists():
            self.weather_data = pd.read_csv(weather_data_path)
            logger.info(f"Loaded weather data from {weather_data_path}")
    
    def add_weather_features(self, games_df: pd.DataFrame) -> pd.DataFrame:
        """
        Add weather features to games DataFrame.
        
        Args:
            games_df: DataFrame with game data
            
        Returns:
            DataFrame with weather features added
        """
        logger.info("Adding weather features to games data...")
        
        # Make a copy to avoid modifying original
        result_df = games_df.copy()
        
        # Add ballpark climate info
        result_df = self._add_ballpark_climate_features(result_df)
        
        # Add weather features (real data if available, simulated otherwise)
        if self.weather_data is not None:
            result_df = self._add_real_weather_features(result_df)
        else:
            logger.info("No weather data found, generating simulated weather features...")
            result_df = self._add_simulated_weather_features(result_df)
        
        # Add derived weather features
        result_df = self._add_derived_weather_features(result_df)
        
        logger.info(f"Added weather features. Total columns: {len(result_df.columns)}")
        return result_df
    
    def _add_ballpark_climate_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add ballpark climate classification features."""
        # Map home team to climate info
        def get_climate(team_id):
            return MLB_BALLPARK_CLIMATE.get(team_id, {}).get('climate', 'unknown')
        
        df['ballpark_climate'] = df['home_team_id'].apply(get_climate)
        
        # Create climate dummy variables
        df['climate_humid_continental'] = (df['ballpark_climate'] == 'humid_continental').astype(int)
        df['climate_humid_subtropical'] = (df['ballpark_climate'] == 'humid_subtropical').astype(int)
        df['climate_mediterranean'] = (df['ballpark_climate'] == 'mediterranean').astype(int)
        df['climate_desert'] = (df['ballpark_climate'] == 'desert').astype(int)
        df['climate_tropical'] = (df['ballpark_climate'] == 'tropical').astype(int)
        df['climate_oceanic'] = (df['ballpark_climate'] == 'oceanic').astype(int)
        df['climate_semi_arid'] = (df['ballpark_climate'] == 'semi_arid').astype(int)
        
        return df
    
    def _add_real_weather_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add real weather features from collected data."""
        # This would merge with actual weather data
        # For now, just add placeholder columns
        weather_columns = [
            'weather_temperature', 'weather_feels_like', 'weather_humidity',
            'weather_pressure', 'weather_wind_speed', 'weather_wind_deg',
            'weather_precipitation', 'weather_visibility', 'weather_condition'
        ]
        
        for col in weather_columns:
            if col not in df.columns:
                df[col] = np.nan
        
        return df
    
    def _add_simulated_weather_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add simulated weather features based on location and season."""
        # Ensure game_date is datetime
        df['game_date'] = pd.to_datetime(df['game_date'])
        
        # Extract seasonal info
        df['month'] = df['game_date'].dt.month
        df['day_of_year'] = df['game_date'].dt.dayofyear
        
        # Simulate temperature based on location and season
        df['weather_temperature'] = df.apply(self._simulate_temperature, axis=1)
        
        # Simulate humidity based on climate
        df['weather_humidity'] = df.apply(self._simulate_humidity, axis=1)
        
        # Simulate wind speed
        df['weather_wind_speed'] = df.apply(self._simulate_wind_speed, axis=1)
        
        # Simulate precipitation probability
        df['weather_precipitation_prob'] = df.apply(self._simulate_precipitation, axis=1)
        
        # Simulate weather conditions
        df['weather_condition'] = df.apply(self._simulate_weather_condition, axis=1)
        
        return df
    
    def _simulate_temperature(self, row) -> float:
        """Simulate temperature based on location and season."""
        home_team = row['home_team_id']
        month = row['month']
        
        if home_team not in MLB_BALLPARK_CLIMATE:
            return 20.0  # Default temperature
        
        climate_info = MLB_BALLPARK_CLIMATE[home_team]
        lat = climate_info['lat']
        climate = climate_info['climate']
        
        # Base temperature varies by latitude and season
        seasonal_temp = 20 + 15 * math.cos(2 * math.pi * (month - 7) / 12)
        
        # Adjust for latitude (higher latitude = colder)
        latitude_adjustment = (45 - lat) * 0.5
        
        # Adjust for climate type
        climate_adjustments = {
            'humid_continental': -2,
            'humid_subtropical': 3,
            'mediterranean': 2,
            'desert': 8,
            'tropical': 10,
            'oceanic': -1,
            'semi_arid': 5
        }
        
        climate_adjustment = climate_adjustments.get(climate, 0)
        
        # Add some random variation
        random_variation = np.random.normal(0, 3)
        
        temperature = seasonal_temp + latitude_adjustment + climate_adjustment + random_variation
        
        return round(temperature, 1)
    
    def _simulate_humidity(self, row) -> float:
        """Simulate humidity based on climate."""
        climate = row.get('ballpark_climate', 'humid_continental')
        
        base_humidity = {
            'humid_continental': 65,
            'humid_subtropical': 75,
            'mediterranean': 55,
            'desert': 25,
            'tropical': 80,
            'oceanic': 70,
            'semi_arid': 35
        }
        
        humidity = base_humidity.get(climate, 60)
        
        # Add seasonal variation (summer = higher humidity)
        month = row['month']
        seasonal_adjustment = 10 * math.sin(2 * math.pi * (month - 1) / 12)
        
        # Add random variation
        random_variation = np.random.normal(0, 8)
        
        final_humidity = humidity + seasonal_adjustment + random_variation
        
        # Clamp between 20-95%
        return max(20, min(95, round(final_humidity, 1)))
    
    def _simulate_wind_speed(self, row) -> float:
        """Simulate wind speed."""
        # Base wind speed varies by location and season
        base_speed = np.random.gamma(2, 2)  # Gamma distribution for wind speed
        
        # Some ballparks are windier
        windy_parks = ['SF', 'CHC', 'CLE', 'MIN']
        if row['home_team_id'] in windy_parks:
            base_speed *= 1.5
        
        return round(max(0, base_speed), 1)
    
    def _simulate_precipitation(self, row) -> float:
        """Simulate precipitation probability."""
        climate = row.get('ballpark_climate', 'humid_continental')
        month = row['month']
        
        # Base precipitation probability by climate
        base_prob = {
            'humid_continental': 0.3,
            'humid_subtropical': 0.4,
            'mediterranean': 0.1,
            'desert': 0.05,
            'tropical': 0.5,
            'oceanic': 0.4,
            'semi_arid': 0.15
        }
        
        prob = base_prob.get(climate, 0.3)
        
        # Adjust for season (summer = more rain in some climates)
        if climate in ['humid_subtropical', 'tropical']:
            seasonal_adjustment = 0.2 * math.sin(2 * math.pi * (month - 1) / 12)
            prob += seasonal_adjustment
        
        return max(0, min(1, prob))
    
    def _simulate_weather_condition(self, row) -> str:
        """Simulate weather condition category."""
        precip_prob = row.get('weather_precipitation_prob', 0.3)
        
        if precip_prob > 0.7:
            return 'Rain'
        elif precip_prob > 0.4:
            return 'Cloudy'
        elif precip_prob > 0.2:
            return 'Partly Cloudy'
        else:
            return 'Clear'
    
    def _add_derived_weather_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived weather features."""
        # Temperature categories
        df['weather_temp_category'] = pd.cut(
            df['weather_temperature'], 
            bins=[-np.inf, 10, 20, 30, np.inf],
            labels=['Cold', 'Cool', 'Warm', 'Hot']
        )
        
        # Wind categories
        df['weather_wind_category'] = pd.cut(
            df['weather_wind_speed'],
            bins=[-np.inf, 5, 10, 15, np.inf],
            labels=['Calm', 'Light', 'Moderate', 'Strong']
        )
        
        # Weather favorability for hitting (warmer, less wind = better)
        df['weather_hitting_favorability'] = (
            (df['weather_temperature'] - df['weather_temperature'].mean()) / df['weather_temperature'].std() -
            (df['weather_wind_speed'] - df['weather_wind_speed'].mean()) / df['weather_wind_speed'].std()
        )
        
        # Dome/indoor indicator
        dome_parks = ['TB', 'HOU', 'TOR', 'SEA', 'ARI', 'MIA']  # Retractable/fixed roof
        df['weather_dome_game'] = df['home_team_id'].isin(dome_parks).astype(int)
        
        # Weather affects game (outdoor games with extreme conditions)
        df['weather_extreme_conditions'] = (
            (df['weather_dome_game'] == 0) & 
            (
                (df['weather_temperature'] < 5) | 
                (df['weather_temperature'] > 35) |
                (df['weather_wind_speed'] > 20) |
                (df['weather_precipitation_prob'] > 0.8)
            )
        ).astype(int)
        
        return df

def main():
    """Test the weather feature engineering."""
    # Create sample data
    sample_data = pd.DataFrame({
        'game_date': pd.date_range('2024-04-01', periods=100),
        'home_team_id': np.random.choice(['NYY', 'BOS', 'LAD', 'SF', 'ARI', 'COL'], 100),
        'away_team_id': np.random.choice(['NYY', 'BOS', 'LAD', 'SF', 'ARI', 'COL'], 100),
    })
    
    # Initialize engineer
    engineer = WeatherFeatureEngineer()
    
    # Add weather features
    result = engineer.add_weather_features(sample_data)
    
    # Display results
    print("Weather features added:")
    weather_cols = [col for col in result.columns if 'weather' in col or 'climate' in col]
    print(f"Total weather features: {len(weather_cols)}")
    print("\nSample data:")
    print(result[['game_date', 'home_team_id', 'weather_temperature', 'weather_condition', 'ballpark_climate']].head())
    
    print("\nWeather feature summary:")
    print(result[weather_cols].describe())

if __name__ == "__main__":
    main() 