"""
Simple Real MLB Master Dataset Builder

This module creates a comprehensive MLB dataset using direct database queries.
It combines:

1. Real MLB games data (7,290+ games from 2023-2024 seasons)
2. Player statistics (169K+ batter stats, 66K+ pitcher stats)
3. Ballpark factors (offensive/defensive park effects)
4. Weather features (temperature, humidity, wind, conditions)

Example usage:
    python real_master_dataset_simple.py
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict, Optional
from datetime import datetime
import sys
import os
import sqlite3

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add current directory to path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# Import feature engineering modules
from ballpark_features_mlb import BallparkFeatureEngineer
from weather_features_mlb import WeatherFeatureEngineer

class SimpleRealMasterDatasetBuilder:
    """Builds MLB dataset using direct database queries."""
    
    def __init__(self, data_dir: str = "data", save_intermediate: bool = True):
        """Initialize the dataset builder."""
        self.data_dir = Path(data_dir)
        self.save_intermediate = save_intermediate
        self.db_path = self.data_dir / "sports_model.db"
        
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
        
        logger.info("Simple Real Master Dataset Builder initialized")
    
    def load_games_from_database(self, season_filter: Optional[str] = None) -> pd.DataFrame:
        """Load real MLB games data from the database."""
        logger.info("Loading MLB games from database...")
        
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")
        
        # Connect to database
        conn = sqlite3.connect(str(self.db_path))
        
        try:
            # Build query with optional season filter
            # CRITICAL: Exclude future games (0-0 scores) and only include completed games
            query = """
                SELECT 
                    game_id,
                    game_date,
                    home_team_id,
                    away_team_id,
                    home_team_score,
                    away_team_score,
                    season,
                    home_starting_pitcher_id,
                    away_starting_pitcher_id
                FROM mlb_games
                WHERE game_date <= date('now')
                AND NOT (home_team_score = 0 AND away_team_score = 0)
            """
            
            if season_filter:
                if season_filter == "2023":
                    query += " AND game_date >= '2023-03-01' AND game_date < '2024-03-01'"
                elif season_filter == "2024":
                    query += " AND game_date >= '2024-03-01' AND game_date < '2025-03-01'"
                elif season_filter == "2025":
                    query += " AND game_date >= '2025-03-01' AND game_date < '2026-03-01'"
                elif season_filter == "2023-2024":
                    query += " AND game_date >= '2023-03-01' AND game_date < '2025-03-01'"
                elif season_filter == "2023-2025":
                    query += " AND game_date >= '2023-03-01' AND game_date < '2026-03-01'"
            
            query += " ORDER BY game_date"
            
            # Load games
            games_df = pd.read_sql(query, conn)
            
            # Log filtering results
            total_query = "SELECT COUNT(*) FROM mlb_games"
            if season_filter and season_filter == "2023-2025":
                total_query += " WHERE game_date >= '2023-03-01' AND game_date < '2026-03-01'"
            total_games = pd.read_sql(total_query, conn).iloc[0, 0]
            
            future_query = "SELECT COUNT(*) FROM mlb_games WHERE game_date > date('now')"
            if season_filter and season_filter == "2023-2025":
                future_query += " AND game_date >= '2023-03-01' AND game_date < '2026-03-01'"
            future_games = pd.read_sql(future_query, conn).iloc[0, 0]
            
            zero_query = "SELECT COUNT(*) FROM mlb_games WHERE home_team_score = 0 AND away_team_score = 0"
            if season_filter and season_filter == "2023-2025":
                zero_query += " AND game_date >= '2023-03-01' AND game_date < '2026-03-01'"
            zero_games = pd.read_sql(zero_query, conn).iloc[0, 0]
            
            logger.info(f"Data filtering results:")
            logger.info(f"  • Total games in period: {total_games:,}")
            logger.info(f"  • Future games excluded: {future_games:,}")
            logger.info(f"  • 0-0 games excluded: {zero_games:,}")
            logger.info(f"  • Final dataset games: {len(games_df):,}")
            
            if games_df.empty:
                raise ValueError("No completed games found in database")
            
            # Convert date column
            games_df['game_date'] = pd.to_datetime(games_df['game_date'])
            
            # Add basic game context features
            games_df['is_home_game'] = 1
            games_df['is_away_game'] = 0
            
            # Add day/night indicator (assume 70% night games)
            np.random.seed(42)
            games_df['is_day_game'] = np.random.choice([0, 1], size=len(games_df), p=[0.7, 0.3])
            games_df['is_night_game'] = 1 - games_df['is_day_game']
            
            # Add rest days calculation
            games_df = self._calculate_rest_days(games_df)
            
            # Add game outcome features
            games_df['home_win'] = (games_df['home_team_score'] > games_df['away_team_score']).astype(int)
            games_df['away_win'] = (games_df['away_team_score'] > games_df['home_team_score']).astype(int)
            games_df['total_runs'] = games_df['home_team_score'] + games_df['away_team_score']
            games_df['run_differential'] = games_df['home_team_score'] - games_df['away_team_score']
            
            initial_features = len(games_df.columns)
            self.feature_counts['base'] = initial_features
            
            logger.info(f"Loaded {len(games_df):,} games from database with {initial_features} features")
            logger.info(f"Date range: {games_df['game_date'].min()} to {games_df['game_date'].max()}")
            logger.info(f"Teams: {games_df['home_team_id'].nunique()} unique teams")
            logger.info(f"Seasons: {sorted(games_df['game_date'].dt.year.unique())}")
            
            return games_df
            
        except Exception as e:
            logger.error(f"Error loading games from database: {e}")
            raise
        finally:
            conn.close()
    
    def _calculate_rest_days(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate rest days for each team."""
        logger.info("Calculating rest days from game sequence...")
        
        # Sort by date
        df = df.sort_values('game_date').reset_index(drop=True)
        
        # Initialize rest days columns
        df['home_rest_days'] = 0
        df['away_rest_days'] = 0
        
        # Get unique teams
        teams = pd.concat([df['home_team_id'], df['away_team_id']]).unique()
        
        for team_id in teams:
            # Get all games for this team (home and away)
            team_home_games = df[df['home_team_id'] == team_id][['game_date']].copy()
            team_away_games = df[df['away_team_id'] == team_id][['game_date']].copy()
            
            # Combine and sort all games for this team
            all_team_games = pd.concat([team_home_games, team_away_games])
            all_team_games = all_team_games.sort_values('game_date').drop_duplicates()
            
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
    
    def add_player_stats_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add aggregated player statistics features."""
        logger.info("Adding player statistics features...")
        
        if not self.db_path.exists():
            logger.warning("Database not found, skipping player stats")
            return df
        
        conn = sqlite3.connect(str(self.db_path))
        
        try:
            # Get team batting averages
            logger.info("Calculating team batting statistics...")
            batting_query = """
                SELECT 
                    team_id,
                    AVG(CAST(at_bats AS FLOAT)) as avg_at_bats,
                    AVG(CAST(runs AS FLOAT)) as avg_runs,
                    AVG(CAST(hits AS FLOAT)) as avg_hits,
                    AVG(CAST(rbi AS FLOAT)) as avg_rbi,
                    AVG(CAST(home_runs AS FLOAT)) as avg_home_runs,
                    AVG(CAST(walks AS FLOAT)) as avg_walks,
                    AVG(CAST(strikeouts AS FLOAT)) as avg_strikeouts,
                    AVG(CAST(batting_avg AS FLOAT)) as team_batting_avg,
                    AVG(CAST(on_base_plus_slugging AS FLOAT)) as team_ops,
                    COUNT(*) as total_batter_games
                FROM mlb_batter_stats 
                WHERE at_bats > 0
                GROUP BY team_id
            """
            
            batting_stats = pd.read_sql(batting_query, conn)
            
            # Get team pitching averages
            logger.info("Calculating team pitching statistics...")
            pitching_query = """
                SELECT 
                    team_id,
                    AVG(CAST(innings_pitched AS FLOAT)) as avg_innings_pitched,
                    AVG(CAST(hits_allowed AS FLOAT)) as avg_hits_allowed,
                    AVG(CAST(runs_allowed AS FLOAT)) as avg_runs_allowed,
                    AVG(CAST(earned_runs AS FLOAT)) as avg_earned_runs,
                    AVG(CAST(walks AS FLOAT)) as avg_walks_allowed,
                    AVG(CAST(strikeouts AS FLOAT)) as avg_strikeouts_pitched,
                    AVG(CAST(home_runs_allowed AS FLOAT)) as avg_hrs_allowed,
                    AVG(CAST(era AS FLOAT)) as team_era,
                    COUNT(*) as total_pitcher_games
                FROM mlb_pitcher_stats 
                WHERE innings_pitched > 0
                GROUP BY team_id
            """
            
            pitching_stats = pd.read_sql(pitching_query, conn)
            
            # Merge batting stats for home teams
            df = df.merge(
                batting_stats.add_prefix('home_'),
                left_on='home_team_id',
                right_on='home_team_id',
                how='left'
            )
            
            # Merge batting stats for away teams
            df = df.merge(
                batting_stats.add_prefix('away_'),
                left_on='away_team_id',
                right_on='away_team_id',
                how='left'
            )
            
            # Merge pitching stats for home teams
            df = df.merge(
                pitching_stats.add_prefix('home_pitching_'),
                left_on='home_team_id',
                right_on='home_pitching_team_id',
                how='left'
            )
            
            # Merge pitching stats for away teams
            df = df.merge(
                pitching_stats.add_prefix('away_pitching_'),
                left_on='away_team_id',
                right_on='away_pitching_team_id',
                how='left'
            )
            
            # Drop duplicate team_id columns
            df = df.drop(columns=[col for col in df.columns if col.endswith('_team_id') and col not in ['home_team_id', 'away_team_id']], errors='ignore')
            
            new_features = len(df.columns) - self.feature_counts.get('base', 0)
            self.feature_counts['player_stats'] = new_features - self.feature_counts.get('base', 0)
            
            logger.info(f"Player statistics features added: {self.feature_counts['player_stats']} new features")
            
            return df
            
        except Exception as e:
            logger.warning(f"Error adding player stats features: {e}")
            logger.warning("Continuing without player stats features")
            self.feature_counts['player_stats'] = 0
            return df
        finally:
            conn.close()
    
    def add_ballpark_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add ballpark factor features."""
        logger.info("Adding ballpark factor features...")
        
        try:
            df_with_ballpark = self.ballpark_engineer.add_ballpark_features(df)
            
            new_features = len(df_with_ballpark.columns) - len(df.columns)
            self.feature_counts['ballpark'] = new_features
            
            logger.info(f"Ballpark features added: {new_features} new features")
            
            if self.save_intermediate:
                output_path = self.data_dir / 'processed' / 'real_mlb_with_ballpark.csv'
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
                output_path = self.data_dir / 'processed' / 'real_mlb_with_weather.csv'
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
        logger.info("Finalizing real master dataset...")
        
        # Remove any duplicate columns
        df = df.loc[:, ~df.columns.duplicated()]
        
        # Sort columns for consistency
        id_columns = ['game_id', 'game_date', 'home_team_id', 'away_team_id']
        feature_columns = [col for col in df.columns if col not in id_columns]
        ordered_columns = id_columns + sorted(feature_columns)
        df = df[ordered_columns]
        
        # Add metadata columns
        df['dataset_version'] = '2.0'
        df['created_date'] = datetime.now().strftime('%Y-%m-%d')
        df['total_features'] = len(df.columns) - 6  # Exclude metadata and ID columns
        df['data_source'] = 'real_database'
        
        # Data quality checks
        logger.info("Performing data quality checks...")
        
        # Check for missing values
        missing_counts = df.isnull().sum()
        if missing_counts.sum() > 0:
            logger.warning(f"Missing values found in {missing_counts[missing_counts > 0].shape[0]} columns")
        
        # Fill missing values strategically
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            if df[col].isnull().sum() > 0:
                # Use median for most numeric columns
                df[col] = df[col].fillna(df[col].median())
        
        categorical_columns = df.select_dtypes(include=['object']).columns
        for col in categorical_columns:
            if df[col].isnull().sum() > 0:
                df[col] = df[col].fillna('unknown')
        
        logger.info("Real dataset finalization complete")
        
        return df
    
    def generate_feature_report(self, df: pd.DataFrame) -> Dict:
        """Generate comprehensive feature report."""
        report = {
            'dataset_summary': {
                'total_games': len(df),
                'total_features': len(df.columns),
                'date_range': f"{df['game_date'].min()} to {df['game_date'].max()}",
                'teams_covered': df['home_team_id'].nunique(),
                'seasons_covered': df['game_date'].dt.year.nunique(),
                'data_source': 'Real MLB Database'
            },
            'feature_breakdown': self.feature_counts,
            'data_quality': {
                'missing_values': df.isnull().sum().sum(),
                'duplicate_rows': df.duplicated().sum(),
                'numeric_features': len(df.select_dtypes(include=[np.number]).columns),
                'categorical_features': len(df.select_dtypes(include=['object']).columns)
            },
            'game_statistics': {
                'total_runs_avg': df['total_runs'].mean(),
                'home_win_rate': df['home_win'].mean(),
                'high_scoring_games': (df['total_runs'] > 10).sum(),
                'low_scoring_games': (df['total_runs'] < 6).sum()
            }
        }
        
        return report
    
    def build_master_dataset(self, season_filter: Optional[str] = "2023-2024", 
                           output_file: Optional[str] = None) -> pd.DataFrame:
        """Build the complete master dataset with all features using real data."""
        logger.info("=" * 70)
        logger.info("BUILDING REAL MLB MASTER DATASET FROM DATABASE")
        logger.info("=" * 70)
        
        start_time = datetime.now()
        
        # Step 1: Load real games data from database
        df = self.load_games_from_database(season_filter)
        
        # Step 2: Add player statistics features
        df = self.add_player_stats_features(df)
        
        # Step 3: Add ballpark features
        df = self.add_ballpark_features(df)
        
        # Step 4: Add weather features
        df = self.add_weather_features(df)
        
        # Step 5: Finalize dataset
        df = self.finalize_dataset(df)
        
        # Step 6: Generate report
        report = self.generate_feature_report(df)
        
        # Step 7: Save dataset
        if output_file:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False)
            logger.info(f"Real master dataset saved: {output_path}")
        
        # Step 8: Print comprehensive summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 70)
        logger.info("REAL MLB MASTER DATASET BUILD COMPLETE")
        logger.info("=" * 70)
        
        print(f"\n🎉 Real MLB Master Dataset Successfully Built!")
        print(f"📊 Dataset Summary:")
        print(f"   • Total games: {report['dataset_summary']['total_games']:,}")
        print(f"   • Total features: {report['dataset_summary']['total_features']:,}")
        print(f"   • Date range: {report['dataset_summary']['date_range']}")
        print(f"   • Teams covered: {report['dataset_summary']['teams_covered']}")
        print(f"   • Seasons covered: {report['dataset_summary']['seasons_covered']}")
        print(f"   • Data source: {report['dataset_summary']['data_source']}")
        
        print(f"\n🔧 Feature Breakdown:")
        for category, count in report['feature_breakdown'].items():
            print(f"   • {category.replace('_', ' ').title()}: {count} features")
        
        print(f"\n⚾ Game Statistics:")
        print(f"   • Average total runs: {report['game_statistics']['total_runs_avg']:.1f}")
        print(f"   • Home win rate: {report['game_statistics']['home_win_rate']:.1%}")
        print(f"   • High-scoring games (>10 runs): {report['game_statistics']['high_scoring_games']:,}")
        print(f"   • Low-scoring games (<6 runs): {report['game_statistics']['low_scoring_games']:,}")
        
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
    """Main function for building the real master dataset."""
    # Initialize builder
    builder = SimpleRealMasterDatasetBuilder(
        data_dir="data",
        save_intermediate=True
    )
    
    # Build master dataset using real data
    master_df = builder.build_master_dataset(
        season_filter="2023-2025",  # Use all three seasons
        output_file="data/processed/real_mlb_master_dataset_complete.csv"
    )
    
    # Display sample
    print(f"\n📋 Sample of real master dataset:")
    print(master_df.head())
    
    # Show feature categories
    feature_cols = [col for col in master_df.columns if col not in ['game_id', 'game_date', 'home_team_id', 'away_team_id']]
    
    # Categorize features
    player_features = [col for col in feature_cols if any(x in col.lower() for x in ['batting', 'pitching', 'avg_', 'era', 'ops'])]
    ballpark_features = [col for col in feature_cols if 'ballpark' in col.lower()]
    weather_features = [col for col in feature_cols if 'weather' in col.lower() or 'climate' in col.lower()]
    game_features = [col for col in feature_cols if any(x in col.lower() for x in ['rest', 'home', 'away', 'win', 'runs', 'day', 'night'])]
    
    print(f"\n🎯 Feature Categories:")
    print(f"   • Player Stats ({len(player_features)}): {player_features[:3]}..." if len(player_features) > 3 else f"   • Player Stats ({len(player_features)}): {player_features}")
    print(f"   • Ballpark Features ({len(ballpark_features)}): {ballpark_features[:3]}..." if len(ballpark_features) > 3 else f"   • Ballpark Features ({len(ballpark_features)}): {ballpark_features}")
    print(f"   • Weather Features ({len(weather_features)}): {weather_features[:3]}..." if len(weather_features) > 3 else f"   • Weather Features ({len(weather_features)}): {weather_features}")
    print(f"   • Game Context ({len(game_features)}): {game_features[:3]}..." if len(game_features) > 3 else f"   • Game Context ({len(game_features)}): {game_features}")
    
    return master_df

if __name__ == "__main__":
    main() 