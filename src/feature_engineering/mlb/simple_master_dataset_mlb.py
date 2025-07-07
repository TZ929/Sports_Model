"""
Simple MLB Master Dataset Builder

This module combines the working feature engineering components into a dataset ready for modeling.
It integrates:

1. Base game data (rest days, home/away, day/night)
2. Ballpark factors (offensive/defensive park effects)
3. Weather features (temperature, humidity, wind, conditions)

Example usage:
    python simple_master_dataset_mlb.py
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict, Optional
from datetime import datetime
import sys
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# Import working feature engineering modules
from ballpark_features_mlb import BallparkFeatureEngineer
from weather_features_mlb import WeatherFeatureEngineer

class SimpleMasterDatasetBuilder:
    """Builds MLB dataset with ballpark and weather features."""
    
    def __init__(self, data_dir: str = "data", save_intermediate: bool = True):
        """Initialize the dataset builder."""
        self.data_dir = Path(data_dir)
        self.save_intermediate = save_intermediate
        
        # Initialize feature engineers
        self.ballpark_engineer = BallparkFeatureEngineer()
        self.weather_engineer = WeatherFeatureEngineer()
        
        # Load park factors for ballpark features
        try:
            self.ballpark_engineer.load_park_factors()
            logger.info("Park factors loaded successfully")
        except Exception as e:
            logger.warning(f"Could not load park factors: {e}")
            logger.warning("Ballpark features will be skipped")
        
        # Track feature counts
        self.feature_counts = {}
        
        logger.info("Simple Master Dataset Builder initialized")
    
    def load_base_games_data(self, games_file: str) -> pd.DataFrame:
        """Load and prepare base games data."""
        logger.info(f"Loading base games data from {games_file}")
        
        if not Path(games_file).exists():
            raise FileNotFoundError(f"Games file not found: {games_file}")
        
        # Load games data
        games_df = pd.read_csv(games_file)
        
        # Ensure required columns exist
        required_columns = ['game_date', 'home_team_id', 'away_team_id']
        missing_columns = [col for col in required_columns if col not in games_df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Convert date column
        games_df['game_date'] = pd.to_datetime(games_df['game_date'])
        
        # Add basic game context features if not present
        if 'is_home_game' not in games_df.columns:
            games_df['is_home_game'] = 1
        
        if 'is_away_game' not in games_df.columns:
            games_df['is_away_game'] = 0
        
        # Add day/night indicator if not present
        if 'is_day_game' not in games_df.columns:
            if 'start_time' in games_df.columns:
                games_df['start_time_parsed'] = pd.to_datetime(games_df['start_time'], format='%H:%M', errors='coerce')
                games_df['is_day_game'] = (games_df['start_time_parsed'].dt.hour < 18).astype(int)
            else:
                # Default: 70% night games, 30% day games
                np.random.seed(42)
                games_df['is_day_game'] = np.random.choice([0, 1], size=len(games_df), p=[0.7, 0.3])
        
        games_df['is_night_game'] = 1 - games_df['is_day_game']
        
        # Add rest days calculation if not present
        if 'home_rest_days' not in games_df.columns:
            games_df = self._calculate_rest_days(games_df)
        
        initial_features = len(games_df.columns)
        self.feature_counts['base'] = initial_features
        
        logger.info(f"Base games data loaded: {len(games_df):,} games with {initial_features} features")
        
        return games_df
    
    def _calculate_rest_days(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate rest days for each team."""
        logger.info("Calculating rest days...")
        
        # Sort by date
        df = df.sort_values('game_date').reset_index(drop=True)
        
        # Initialize rest days columns
        df['home_rest_days'] = 0
        df['away_rest_days'] = 0
        
        # Calculate rest days for each team
        teams = pd.concat([df['home_team_id'], df['away_team_id']]).unique()
        
        for team_id in teams:
            # Get all games for this team (home and away)
            team_home_games = df[df['home_team_id'] == team_id][['game_date']].copy()
            team_away_games = df[df['away_team_id'] == team_id][['game_date']].copy()
            
            # Combine and sort all games for this team
            all_team_games = pd.concat([team_home_games, team_away_games]).sort_values('game_date')
            
            if len(all_team_games) > 1:
                # Calculate days between games
                all_team_games['rest_days'] = all_team_games['game_date'].diff().dt.days.fillna(0)
                
                # Map back to original dataframe
                for _, row in all_team_games.iterrows():
                    game_date = row['game_date']
                    rest_days = row['rest_days']
                    
                    # Update home games
                    home_mask = (df['home_team_id'] == team_id) & (df['game_date'] == game_date)
                    df.loc[home_mask, 'home_rest_days'] = rest_days
                    
                    # Update away games
                    away_mask = (df['away_team_id'] == team_id) & (df['game_date'] == game_date)
                    df.loc[away_mask, 'away_rest_days'] = rest_days
        
        # Cap rest days at reasonable maximum
        df['home_rest_days'] = df['home_rest_days'].clip(0, 10)
        df['away_rest_days'] = df['away_rest_days'].clip(0, 10)
        
        return df
    
    def add_ballpark_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add ballpark factor features."""
        logger.info("Adding ballpark factor features...")
        
        try:
            df_with_ballpark = self.ballpark_engineer.add_ballpark_features(df)
            
            new_features = len(df_with_ballpark.columns) - len(df.columns)
            self.feature_counts['ballpark'] = new_features
            
            logger.info(f"Ballpark features added: {new_features} new features")
            
            if self.save_intermediate:
                output_path = self.data_dir / 'processed' / 'mlb_with_ballpark.csv'
                output_path.parent.mkdir(parents=True, exist_ok=True)
                df_with_ballpark.to_csv(output_path, index=False)
                logger.info(f"Intermediate dataset saved: {output_path}")
            
            return df_with_ballpark
            
        except Exception as e:
            logger.warning(f"Error adding ballpark features: {e}")
            logger.warning("Continuing without ballpark features")
            self.feature_counts['ballpark'] = 0
            return df
    
    def add_weather_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add weather features."""
        logger.info("Adding weather features...")
        
        try:
            df_with_weather = self.weather_engineer.add_weather_features(df)
            
            new_features = len(df_with_weather.columns) - len(df.columns)
            self.feature_counts['weather'] = new_features
            
            logger.info(f"Weather features added: {new_features} new features")
            
            if self.save_intermediate:
                output_path = self.data_dir / 'processed' / 'mlb_with_weather.csv'
                output_path.parent.mkdir(parents=True, exist_ok=True)
                df_with_weather.to_csv(output_path, index=False)
                logger.info(f"Intermediate dataset saved: {output_path}")
            
            return df_with_weather
            
        except Exception as e:
            logger.warning(f"Error adding weather features: {e}")
            logger.warning("Continuing without weather features")
            self.feature_counts['weather'] = 0
            return df
    
    def finalize_dataset(self, df: pd.DataFrame) -> pd.DataFrame:
        """Final dataset preparation and cleaning."""
        logger.info("Finalizing master dataset...")
        
        # Remove any duplicate columns
        df = df.loc[:, ~df.columns.duplicated()]
        
        # Sort columns for consistency
        feature_columns = [col for col in df.columns if col not in ['game_date', 'home_team_id', 'away_team_id']]
        ordered_columns = ['game_date', 'home_team_id', 'away_team_id'] + sorted(feature_columns)
        df = df[ordered_columns]
        
        # Add metadata columns
        df['dataset_version'] = '1.0'
        df['created_date'] = datetime.now().strftime('%Y-%m-%d')
        df['total_features'] = len(df.columns) - 5  # Exclude metadata and ID columns
        
        # Data quality checks
        logger.info("Performing data quality checks...")
        
        # Check for missing values
        missing_counts = df.isnull().sum()
        if missing_counts.sum() > 0:
            logger.warning(f"Missing values found in {missing_counts[missing_counts > 0].shape[0]} columns")
        
        # Fill missing values
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            if df[col].isnull().sum() > 0:
                df[col] = df[col].fillna(df[col].median())
        
        categorical_columns = df.select_dtypes(include=['object']).columns
        for col in categorical_columns:
            if df[col].isnull().sum() > 0:
                df[col] = df[col].fillna('unknown')
        
        logger.info("Dataset finalization complete")
        
        return df
    
    def generate_feature_report(self, df: pd.DataFrame) -> Dict:
        """Generate comprehensive feature report."""
        report = {
            'dataset_summary': {
                'total_games': len(df),
                'total_features': len(df.columns),
                'date_range': f"{df['game_date'].min()} to {df['game_date'].max()}",
                'teams_covered': df['home_team_id'].nunique(),
                'seasons_covered': df['game_date'].dt.year.nunique()
            },
            'feature_breakdown': self.feature_counts,
            'data_quality': {
                'missing_values': df.isnull().sum().sum(),
                'duplicate_rows': df.duplicated().sum(),
                'numeric_features': len(df.select_dtypes(include=[np.number]).columns),
                'categorical_features': len(df.select_dtypes(include=['object']).columns)
            }
        }
        
        return report
    
    def build_master_dataset(self, games_file: str, output_file: Optional[str] = None) -> pd.DataFrame:
        """Build the complete master dataset with all features."""
        logger.info("=" * 60)
        logger.info("BUILDING SIMPLE MLB MASTER DATASET")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        # Step 1: Load base games data
        df = self.load_base_games_data(games_file)
        
        # Step 2: Add feature categories
        df = self.add_ballpark_features(df)
        df = self.add_weather_features(df)
        
        # Step 3: Finalize dataset
        df = self.finalize_dataset(df)
        
        # Step 4: Generate report
        report = self.generate_feature_report(df)
        
        # Step 5: Save dataset
        if output_file:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False)
            logger.info(f"Master dataset saved: {output_path}")
        
        # Step 6: Print summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 60)
        logger.info("SIMPLE MASTER DATASET BUILD COMPLETE")
        logger.info("=" * 60)
        
        print(f"\n🎉 Master Dataset Successfully Built!")
        print(f"📊 Dataset Summary:")
        print(f"   • Total games: {report['dataset_summary']['total_games']:,}")
        print(f"   • Total features: {report['dataset_summary']['total_features']:,}")
        print(f"   • Date range: {report['dataset_summary']['date_range']}")
        print(f"   • Teams covered: {report['dataset_summary']['teams_covered']}")
        print(f"   • Seasons covered: {report['dataset_summary']['seasons_covered']}")
        
        print(f"\n🔧 Feature Breakdown:")
        for category, count in report['feature_breakdown'].items():
            print(f"   • {category.replace('_', ' ').title()}: {count} features")
        
        print(f"\n✅ Data Quality:")
        print(f"   • Missing values: {report['data_quality']['missing_values']:,}")
        print(f"   • Duplicate rows: {report['data_quality']['duplicate_rows']:,}")
        print(f"   • Numeric features: {report['data_quality']['numeric_features']:,}")
        print(f"   • Categorical features: {report['data_quality']['categorical_features']:,}")
        
        print(f"\n⏱️  Build time: {duration.total_seconds():.1f} seconds")
        
        if output_file:
            print(f"💾 Saved to: {output_file}")
        
        return df

def main():
    """Main function for building the master dataset."""
    # Initialize builder
    builder = SimpleMasterDatasetBuilder(
        data_dir="data",
        save_intermediate=True
    )
    
    # Try to find games file
    possible_files = [
        "data/processed/mlb_games_with_features.csv",
        "data/raw/mlb_games.csv",
        "data/raw/mlb_games_2024.csv"
    ]
    
    games_file = None
    for file_path in possible_files:
        if Path(file_path).exists():
            games_file = file_path
            break
    
    if not games_file:
        # Create sample data for testing
        logger.info("No games file found. Creating sample data for testing...")
        
        # Use proper MLB team IDs that match the ballpark features
        team_mapping = {
            'NYY': 147,  # New York Yankees
            'BOS': 111,  # Boston Red Sox  
            'LAD': 119,  # Los Angeles Dodgers
            'SF': 137,   # San Francisco Giants
            'ARI': 109,  # Arizona Diamondbacks
            'COL': 115   # Colorado Rockies
        }
        
        team_abbrevs = list(team_mapping.keys())
        home_teams = np.random.choice(team_abbrevs, 100)
        away_teams = np.random.choice(team_abbrevs, 100)
        
        sample_data = pd.DataFrame({
            'game_date': pd.date_range('2024-04-01', periods=100),
            'home_team_id': [team_mapping[team] for team in home_teams],
            'away_team_id': [team_mapping[team] for team in away_teams],
            'home_team_abbrev': home_teams,
            'away_team_abbrev': away_teams,
        })
        
        # Save sample data
        sample_file = "data/raw/sample_mlb_games.csv"
        Path(sample_file).parent.mkdir(parents=True, exist_ok=True)
        sample_data.to_csv(sample_file, index=False)
        games_file = sample_file
        logger.info(f"Sample data created: {sample_file}")
    
    # Build master dataset
    master_df = builder.build_master_dataset(
        games_file=games_file,
        output_file="data/processed/mlb_master_dataset.csv"
    )
    
    # Display sample
    print(f"\n📋 Sample of master dataset:")
    print(master_df.head())
    
    # Show feature categories
    feature_cols = [col for col in master_df.columns if col not in ['game_date', 'home_team_id', 'away_team_id']]
    ballpark_features = [col for col in feature_cols if 'ballpark' in col.lower()]
    weather_features = [col for col in feature_cols if 'weather' in col.lower() or 'climate' in col.lower()]
    
    print(f"\n🏟️  Ballpark Features ({len(ballpark_features)}):")
    print(f"   {ballpark_features[:5]}..." if len(ballpark_features) > 5 else f"   {ballpark_features}")
    
    print(f"\n🌤️  Weather Features ({len(weather_features)}):")
    print(f"   {weather_features[:5]}..." if len(weather_features) > 5 else f"   {weather_features}")
    
    return master_df

if __name__ == "__main__":
    main() 