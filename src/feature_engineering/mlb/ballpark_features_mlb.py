"""
MLB Ballpark Features Engineering

This module integrates Baseball Savant park factors data into the feature engineering pipeline.
It adds ballpark-specific features to games based on the home team's venue.

Key features added:
- Run factors (how much the park affects run scoring)
- Home run factors (park effects on HR frequency)
- Hit factors (park effects on hit frequency)
- Distance factors (environmental effects on ball travel)
- Temperature and elevation effects
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict, Optional

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BallparkFeatureEngineer:
    """Class to handle ballpark factor feature engineering."""
    
    def __init__(self, data_dir: str = "data/raw"):
        """Initialize with path to park factors data."""
        self.data_dir = Path(data_dir)
        self.runs_factors = None
        self.distance_factors = None
        self.team_venue_map = None
        
    def load_park_factors(self, year: int = 2024) -> None:
        """Load park factors data from CSV files."""
        try:
            # Load runs factors
            runs_file = self.data_dir / f"park_factors_{year}_runs.csv"
            if runs_file.exists():
                self.runs_factors = pd.read_csv(runs_file)
                logger.info(f"Loaded runs factors: {len(self.runs_factors)} ballparks")
            else:
                logger.warning(f"Runs factors file not found: {runs_file}")
                
            # Load distance factors
            distance_file = self.data_dir / f"park_factors_{year}_distance.csv"
            if distance_file.exists():
                self.distance_factors = pd.read_csv(distance_file)
                logger.info(f"Loaded distance factors: {len(self.distance_factors)} ballparks")
            else:
                logger.warning(f"Distance factors file not found: {distance_file}")
                
            # Create team to venue mapping
            self._create_team_venue_map()
            
        except Exception as e:
            logger.error(f"Error loading park factors: {e}")
            raise
            
    def _create_team_venue_map(self) -> None:
        """Create mapping from team_id to venue information."""
        if self.runs_factors is not None:
            # Use runs factors as the primary source for team-venue mapping
            self.team_venue_map = self.runs_factors[
                ['main_team_id', 'venue_id', 'venue_name', 'name_display_club']
            ].drop_duplicates().set_index('main_team_id')
            logger.info(f"Created team-venue mapping for {len(self.team_venue_map)} teams")
        else:
            logger.warning("Cannot create team-venue mapping without runs factors data")
            
    def add_ballpark_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add ballpark features to game data.
        
        Args:
            df: DataFrame with game data including 'home_team_id' column
            
        Returns:
            DataFrame with added ballpark features
        """
        if self.runs_factors is None or self.distance_factors is None:
            logger.warning("Park factors not loaded. Call load_park_factors() first.")
            return df
            
        df_with_features = df.copy()
        
        # Add runs factors
        df_with_features = self._add_runs_factors(df_with_features)
        
        # Add distance factors
        df_with_features = self._add_distance_factors(df_with_features)
        
        # Add derived features
        df_with_features = self._add_derived_features(df_with_features)
        
        logger.info(f"Added ballpark features to {len(df_with_features)} games")
        return df_with_features
        
    def _add_runs_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add run-based park factors."""
        if self.runs_factors is None:
            return df
            
        # Merge on home team ID - create features with renamed columns
        runs_features = self.runs_factors[[
            'main_team_id', 'venue_name', 'index_runs', 'index_hr', 'index_hits',
            'index_1b', 'index_2b', 'index_3b', 'index_woba', 'index_hardhit'
        ]].copy()
        
        # Rename columns manually to avoid linter issues
        column_mapping = {
            'main_team_id': 'home_team_id',
            'index_runs': 'ballpark_runs_factor',
            'index_hr': 'ballpark_hr_factor', 
            'index_hits': 'ballpark_hits_factor',
            'index_1b': 'ballpark_1b_factor',
            'index_2b': 'ballpark_2b_factor',
            'index_3b': 'ballpark_3b_factor',
            'index_woba': 'ballpark_woba_factor',
            'index_hardhit': 'ballpark_hardhit_factor'
        }
        runs_features.columns = [column_mapping.get(col, col) for col in runs_features.columns]
        
        df = df.merge(runs_features, on='home_team_id', how='left')
        return df
        
    def _add_distance_factors(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add distance/environmental park factors."""
        if self.distance_factors is None:
            return df
            
        # Merge on home team ID - create features with renamed columns
        distance_features = self.distance_factors[[
            'main_team_id', 'elevation_feet', 'avg_temperature', 'extra_distance',
            'temperature_extra_distance', 'elevation_extra_distance'
        ]].copy()
        
        # Rename columns manually to avoid linter issues
        distance_column_mapping = {
            'main_team_id': 'home_team_id',
            'elevation_feet': 'ballpark_elevation',
            'avg_temperature': 'ballpark_avg_temp',
            'extra_distance': 'ballpark_distance_factor',
            'temperature_extra_distance': 'ballpark_temp_distance_factor',
            'elevation_extra_distance': 'ballpark_elevation_distance_factor'
        }
        distance_features.columns = [distance_column_mapping.get(col, col) for col in distance_features.columns]
        
        df = df.merge(distance_features, on='home_team_id', how='left')
        return df
        
    def _add_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived ballpark features."""
        # Ballpark type categories
        if 'ballpark_hr_factor' in df.columns:
            df['ballpark_is_hitters_park'] = (df['ballpark_hr_factor'] > 105).astype(int)
            df['ballpark_is_pitchers_park'] = (df['ballpark_hr_factor'] < 95).astype(int)
            
        # Elevation categories
        if 'ballpark_elevation' in df.columns:
            df['ballpark_is_high_altitude'] = (df['ballpark_elevation'] > 3000).astype(int)
            
        # Temperature categories
        if 'ballpark_avg_temp' in df.columns:
            df['ballpark_is_warm_weather'] = (df['ballpark_avg_temp'] > 75).astype(int)
            df['ballpark_is_cold_weather'] = (df['ballpark_avg_temp'] < 60).astype(int)
            
        return df


def add_ballpark_features_to_games(
    games_df: pd.DataFrame, 
    data_dir: str = "data/raw",
    year: int = 2024
) -> pd.DataFrame:
    """Convenience function to add ballpark features to games DataFrame.
    
    Args:
        games_df: DataFrame with game data
        data_dir: Directory containing park factors CSV files
        year: Year of park factors data to use
        
    Returns:
        DataFrame with ballpark features added
    """
    engineer = BallparkFeatureEngineer(data_dir)
    engineer.load_park_factors(year)
    return engineer.add_ballpark_features(games_df)


def main():
    """Example usage and testing."""
    # Example usage
    print("Testing ballpark feature engineering...")
    
    # Create sample game data
    sample_games = pd.DataFrame({
        'game_id': ['game1', 'game2', 'game3'],
        'home_team_id': [121, 137, 147],  # Mets, Giants, Yankees
        'away_team_id': [111, 119, 144],
        'game_date': ['2024-06-01', '2024-06-02', '2024-06-03']
    })
    
    print("Sample games:")
    print(sample_games)
    
    # Add ballpark features
    try:
        games_with_features = add_ballpark_features_to_games(sample_games)
        print(f"\nGames with ballpark features ({games_with_features.shape[1]} columns):")
        print(games_with_features.head())
        
        # Show ballpark-specific columns
        ballpark_cols = [col for col in games_with_features.columns if 'ballpark' in col]
        print(f"\nBallpark feature columns: {ballpark_cols}")
        
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure park factors data is available in data/raw/")


if __name__ == "__main__":
    main() 