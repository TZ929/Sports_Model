"""
MLB Master Dataset Builder

This module combines all feature engineering components into a single, comprehensive dataset
ready for machine learning modeling. It integrates:

1. Base game data (rest days, home/away, day/night)
2. Advanced statistics (team and player performance metrics)
3. Batter vs Pitcher (BvP) matchup data
4. Platoon splits (lefty/righty advantages)
5. Ballpark factors (offensive/defensive park effects)
6. Weather features (temperature, humidity, wind, conditions)

The resulting dataset contains all features needed for MLB game outcome prediction.

Example usage:
    from master_dataset_mlb import MasterDatasetBuilder
    
    builder = MasterDatasetBuilder()
    master_df = builder.build_master_dataset('data/raw/mlb_games.csv')
    master_df.to_csv('data/processed/mlb_master_dataset.csv', index=False)
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import sys
import os

# Set up logging first
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add the feature engineering modules to the path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# Import all feature engineering modules
try:
    from advanced_stats_mlb import AdvancedStatsEngineer
except ImportError:
    logger.warning("Advanced stats module not found, will skip advanced stats features")
    AdvancedStatsEngineer = None

try:
    from bvp_features_mlb import BvPFeatureEngineer
except ImportError:
    logger.warning("BvP features module not found, will skip BvP features")
    BvPFeatureEngineer = None

try:
    from platoon_features_mlb import PlatoonFeatureEngineer
except ImportError:
    logger.warning("Platoon features module not found, will skip platoon features")
    PlatoonFeatureEngineer = None

try:
    from ballpark_features_mlb import BallparkFeatureEngineer
except ImportError:
    logger.warning("Ballpark features module not found, will skip ballpark features")
    BallparkFeatureEngineer = None

try:
    from weather_features_mlb import WeatherFeatureEngineer
except ImportError:
    logger.warning("Weather features module not found, will skip weather features")
    WeatherFeatureEngineer = None

class MasterDatasetBuilder:
    """Builds comprehensive MLB dataset with all feature engineering components."""
    
    def __init__(self, 
                 data_dir: str = "data",
                 save_intermediate: bool = True,
                 verbose: bool = True):
        """
        Initialize the master dataset builder.
        
        Args:
            data_dir: Base directory for data files
            save_intermediate: Whether to save intermediate datasets
            verbose: Whether to print detailed progress
        """
        self.data_dir = Path(data_dir)
        self.save_intermediate = save_intermediate
        self.verbose = verbose
        
        # Initialize all feature engineers (handle None modules)
        self.advanced_stats_engineer = AdvancedStatsEngineer() if AdvancedStatsEngineer else None
        self.bvp_engineer = BvPFeatureEngineer() if BvPFeatureEngineer else None
        self.platoon_engineer = PlatoonFeatureEngineer() if PlatoonFeatureEngineer else None
        self.ballpark_engineer = BallparkFeatureEngineer() if BallparkFeatureEngineer else None
        self.weather_engineer = WeatherFeatureEngineer() if WeatherFeatureEngineer else None
        
        # Track feature counts for reporting
        self.feature_counts = {}
        
        logger.info("Master Dataset Builder initialized")
    
    def load_base_games_data(self, games_file: str) -> pd.DataFrame:
        """
        Load and prepare base games data.
        
        Args:
            games_file: Path to games CSV file
            
        Returns:
            Base games DataFrame
        """
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
            games_df['is_home_game'] = 1  # All games are from home team perspective
        
        if 'is_away_game' not in games_df.columns:
            games_df['is_away_game'] = 0  # Complement of is_home_game
        
        # Add day/night indicator if not present
        if 'is_day_game' not in games_df.columns:
            # Assume day games if start time is before 6 PM, otherwise night
            if 'start_time' in games_df.columns:
                games_df['start_time'] = pd.to_datetime(games_df['start_time'], format='%H:%M', errors='coerce')
                games_df['is_day_game'] = (games_df['start_time'].dt.hour < 18).astype(int)
            else:
                # Default assumption: 70% night games, 30% day games
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
        for team_id in df['home_team_id'].unique():
            # Home games
            home_games = df[df['home_team_id'] == team_id].copy()
            if len(home_games) > 1:
                home_games['days_since_last'] = home_games['game_date'].diff().dt.days
                df.loc[df['home_team_id'] == team_id, 'home_rest_days'] = home_games['days_since_last'].fillna(0)
            
            # Away games
            away_games = df[df['away_team_id'] == team_id].copy()
            if len(away_games) > 1:
                away_games['days_since_last'] = away_games['game_date'].diff().dt.days
                df.loc[df['away_team_id'] == team_id, 'away_rest_days'] = away_games['days_since_last'].fillna(0)
        
        # Cap rest days at reasonable maximum
        df['home_rest_days'] = df['home_rest_days'].clip(0, 10)
        df['away_rest_days'] = df['away_rest_days'].clip(0, 10)
        
        return df
    
    def add_advanced_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add advanced statistics features."""
        logger.info("Adding advanced statistics features...")
        
        try:
            df_with_stats = self.advanced_stats_engineer.add_advanced_stats_features(df)
            
            new_features = len(df_with_stats.columns) - len(df.columns)
            self.feature_counts['advanced_stats'] = new_features
            
            logger.info(f"Advanced stats added: {new_features} new features")
            
            if self.save_intermediate:
                output_path = self.data_dir / 'processed' / 'mlb_with_advanced_stats.csv'
                output_path.parent.mkdir(parents=True, exist_ok=True)
                df_with_stats.to_csv(output_path, index=False)
                logger.info(f"Intermediate dataset saved: {output_path}")
            
            return df_with_stats
            
        except Exception as e:
            logger.warning(f"Error adding advanced stats: {e}")
            logger.warning("Continuing without advanced stats features")
            self.feature_counts['advanced_stats'] = 0
            return df
    
    def add_bvp_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add Batter vs Pitcher features."""
        logger.info("Adding Batter vs Pitcher (BvP) features...")
        
        try:
            df_with_bvp = self.bvp_engineer.add_bvp_features(df)
            
            new_features = len(df_with_bvp.columns) - len(df.columns)
            self.feature_counts['bvp'] = new_features
            
            logger.info(f"BvP features added: {new_features} new features")
            
            if self.save_intermediate:
                output_path = self.data_dir / 'processed' / 'mlb_with_bvp.csv'
                output_path.parent.mkdir(parents=True, exist_ok=True)
                df_with_bvp.to_csv(output_path, index=False)
                logger.info(f"Intermediate dataset saved: {output_path}")
            
            return df_with_bvp
            
        except Exception as e:
            logger.warning(f"Error adding BvP features: {e}")
            logger.warning("Continuing without BvP features")
            self.feature_counts['bvp'] = 0
            return df
    
    def add_platoon_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add platoon split features."""
        logger.info("Adding platoon split features...")
        
        try:
            df_with_platoon = self.platoon_engineer.add_platoon_features(df)
            
            new_features = len(df_with_platoon.columns) - len(df.columns)
            self.feature_counts['platoon'] = new_features
            
            logger.info(f"Platoon features added: {new_features} new features")
            
            if self.save_intermediate:
                output_path = self.data_dir / 'processed' / 'mlb_with_platoon.csv'
                output_path.parent.mkdir(parents=True, exist_ok=True)
                df_with_platoon.to_csv(output_path, index=False)
                logger.info(f"Intermediate dataset saved: {output_path}")
            
            return df_with_platoon
            
        except Exception as e:
            logger.warning(f"Error adding platoon features: {e}")
            logger.warning("Continuing without platoon features")
            self.feature_counts['platoon'] = 0
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
            if self.verbose:
                print("\nMissing value summary:")
                print(missing_counts[missing_counts > 0])
        
        # Check for infinite values
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        inf_counts = pd.DataFrame({col: np.isinf(df[col]).sum() for col in numeric_columns}, index=[0])
        if inf_counts.sum(axis=1).iloc[0] > 0:
            logger.warning("Infinite values found in numeric columns")
            # Replace infinite values with NaN
            df[numeric_columns] = df[numeric_columns].replace([np.inf, -np.inf], np.nan)
        
        # Fill remaining NaN values with appropriate defaults
        for col in numeric_columns:
            if df[col].isnull().sum() > 0:
                df[col] = df[col].fillna(df[col].median())
        
        # Fill categorical NaN values
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
            },
            'feature_categories': {}
        }
        
        # Categorize features
        feature_categories = {
            'game_context': [col for col in df.columns if any(x in col.lower() for x in ['home', 'away', 'day', 'night', 'rest'])],
            'advanced_stats': [col for col in df.columns if any(x in col.lower() for x in ['era', 'whip', 'ops', 'avg', 'obp', 'slg'])],
            'bvp_features': [col for col in df.columns if 'bvp' in col.lower()],
            'platoon_features': [col for col in df.columns if any(x in col.lower() for x in ['platoon', 'vs_lhp', 'vs_rhp'])],
            'ballpark_features': [col for col in df.columns if 'ballpark' in col.lower()],
            'weather_features': [col for col in df.columns if any(x in col.lower() for x in ['weather', 'climate'])],
            'metadata': [col for col in df.columns if any(x in col.lower() for x in ['version', 'created', 'total_features'])]
        }
        
        for category, columns in feature_categories.items():
            report['feature_categories'][category] = len(columns)
        
        return report
    
    def build_master_dataset(self, 
                           games_file: str,
                           output_file: Optional[str] = None) -> pd.DataFrame:
        """
        Build the complete master dataset with all features.
        
        Args:
            games_file: Path to base games CSV file
            output_file: Optional path to save the master dataset
            
        Returns:
            Complete master dataset DataFrame
        """
        logger.info("=" * 60)
        logger.info("BUILDING MLB MASTER DATASET")
        logger.info("=" * 60)
        
        start_time = datetime.now()
        
        # Step 1: Load base games data
        df = self.load_base_games_data(games_file)
        
        # Step 2: Add all feature categories
        df = self.add_advanced_stats(df)
        df = self.add_bvp_features(df)
        df = self.add_platoon_features(df)
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
        logger.info("MASTER DATASET BUILD COMPLETE")
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
        
        print(f"\n📋 Feature Categories:")
        for category, count in report['feature_categories'].items():
            if count > 0:
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
    """Main function for testing the master dataset builder."""
    # Initialize builder
    builder = MasterDatasetBuilder(
        data_dir="data",
        save_intermediate=True,
        verbose=True
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
        logger.error("No games file found. Please ensure you have MLB games data available.")
        return
    
    # Build master dataset
    master_df = builder.build_master_dataset(
        games_file=games_file,
        output_file="data/processed/mlb_master_dataset.csv"
    )
    
    # Display sample
    print(f"\n📋 Sample of master dataset:")
    print(master_df.head())
    
    return master_df

if __name__ == "__main__":
    main() 