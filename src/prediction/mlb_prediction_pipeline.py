"""
MLB Prediction Pipeline for Production Use

This module provides a complete pipeline for predicting MLB game outcomes
using the trained home_team_wins model that achieved sportsbook-beating performance.
"""

import pandas as pd
import numpy as np
import joblib
import json
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MLBPredictionPipeline:
    """
    Production pipeline for MLB game outcome predictions.
    
    Focuses on home_team_wins predictions which achieved 0.5657 AUC
    with real baseball intelligence.
    """
    
    def __init__(self, model_dir: str = "data/models/mlb", db_path: str = "data/sports_model.db"):
        """Initialize the prediction pipeline."""
        self.model_dir = Path(model_dir)
        self.db_path = Path(db_path)
        self.model = None
        self.scaler = None
        self.features = None
        self.model_metadata = None
        
    def load_model(self, target: str = "home_team_wins", model_type: str = "xgboost"):
        """Load the trained model and associated artifacts."""
        logger.info(f"Loading {model_type} model for {target}...")
        
        try:
            # Load model
            model_path = self.model_dir / f"{model_type}_{target}.joblib"
            self.model = joblib.load(model_path)
            
            # Load model parameters/metadata
            params_path = self.model_dir / f"{model_type}_{target}_params.json"
            with open(params_path, 'r') as f:
                self.model_metadata = json.load(f)
            
            # Load feature list from modeling info
            modeling_info_path = Path("data/processed/mlb") / f"modeling_info_{target}.json"
            with open(modeling_info_path, 'r') as f:
                modeling_info = json.load(f)
                self.features = modeling_info['features']
            
            # Load scaler if it exists
            scaler_path = Path("data/processed/mlb") / f"scaler_{target}.joblib"
            if scaler_path.exists():
                self.scaler = joblib.load(scaler_path)
                logger.info("Scaler loaded successfully")
            else:
                logger.info("No scaler found - using unscaled features")
            
            logger.info(f"Model loaded successfully:")
            logger.info(f"  - Model type: {model_type}")
            logger.info(f"  - Target: {target}")
            logger.info(f"  - Features: {len(self.features)}")
            logger.info(f"  - Expected AUC: {self.model_metadata.get('best_auc', 'Unknown') if self.model_metadata else 'Unknown'}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            return False
    
    def get_team_stats(self, team_id: str) -> Dict:
        """Get aggregated team statistics from the database."""
        logger.info(f"Fetching team stats for {team_id}")
        
        if not self.db_path.exists():
            logger.warning("Database not found, using default stats")
            return {}
        
        conn = sqlite3.connect(str(self.db_path))
        
        try:
            # Get team ID mapping (abbreviation to numeric ID)
            team_mapping_query = """
                SELECT team_id, team_abbreviation
                FROM mlb_teams
                WHERE team_abbreviation = ?
            """
            team_mapping = pd.read_sql(team_mapping_query, conn, params=[team_id])
            
            if team_mapping.empty:
                logger.warning(f"Team {team_id} not found in database")
                return {}
            
            numeric_team_id = str(team_mapping.iloc[0]['team_id'])
            
            # Get batting stats
            batting_query = """
                SELECT 
                    AVG(CAST(at_bats AS FLOAT)) as avg_at_bats,
                    AVG(CAST(runs AS FLOAT)) as avg_runs,
                    AVG(CAST(hits AS FLOAT)) as avg_hits,
                    AVG(CAST(rbi AS FLOAT)) as avg_rbi,
                    AVG(CAST(home_runs AS FLOAT)) as avg_home_runs,
                    AVG(CAST(walks AS FLOAT)) as avg_walks,
                    AVG(CAST(strikeouts AS FLOAT)) as avg_strikeouts,
                    AVG(CAST(batting_avg AS FLOAT)) as team_batting_avg,
                    AVG(CAST(on_base_plus_slugging AS FLOAT)) as team_ops
                FROM mlb_batter_stats 
                WHERE team_id = ? AND at_bats > 0
            """
            
            batting_stats = pd.read_sql(batting_query, conn, params=[numeric_team_id])
            
            # Get pitching stats
            pitching_query = """
                SELECT 
                    AVG(CAST(innings_pitched AS FLOAT)) as avg_innings_pitched,
                    AVG(CAST(hits_allowed AS FLOAT)) as avg_hits_allowed,
                    AVG(CAST(runs_allowed AS FLOAT)) as avg_runs_allowed,
                    AVG(CAST(earned_runs AS FLOAT)) as avg_earned_runs,
                    AVG(CAST(walks AS FLOAT)) as avg_walks_allowed,
                    AVG(CAST(strikeouts AS FLOAT)) as avg_strikeouts_pitched,
                    AVG(CAST(home_runs_allowed AS FLOAT)) as avg_hrs_allowed,
                    AVG(CAST(era AS FLOAT)) as team_era
                FROM mlb_pitcher_stats 
                WHERE team_id = ? AND innings_pitched > 0
            """
            
            pitching_stats = pd.read_sql(pitching_query, conn, params=[numeric_team_id])
            
            # Combine stats
            team_stats = {}
            
            # Add batting stats
            if not batting_stats.empty:
                for col in batting_stats.columns:
                    team_stats[col] = batting_stats.iloc[0][col] if not pd.isna(batting_stats.iloc[0][col]) else 0.0
            
            # Add pitching stats with prefix
            if not pitching_stats.empty:
                for col in pitching_stats.columns:
                    pitching_col = f"pitching_{col}"
                    team_stats[pitching_col] = pitching_stats.iloc[0][col] if not pd.isna(pitching_stats.iloc[0][col]) else 0.0
            
            logger.info(f"Retrieved {len(team_stats)} stats for {team_id}")
            return team_stats
            
        except Exception as e:
            logger.error(f"Error fetching team stats for {team_id}: {e}")
            return {}
        finally:
            conn.close()
    
    def prepare_game_features(self, game_data: Dict) -> pd.DataFrame:
        """
        Prepare features for a single game prediction using real team stats.
        
        Args:
            game_data: Dictionary containing game information with keys:
                - home_team_id: Team abbreviation (e.g., 'NYY')
                - away_team_id: Team abbreviation (e.g., 'BOS')
                - game_date: Date string (YYYY-MM-DD)
                - home_rest_days: Days since last game (optional)
                - away_rest_days: Days since last game (optional)
        
        Returns:
            DataFrame with prepared features matching model expectations
        """
        logger.info(f"Preparing features for {game_data['away_team_id']} @ {game_data['home_team_id']}")
        
        if not self.features:
            raise ValueError("Model not loaded. Call load_model() first.")
        
        # Initialize feature dictionary with zeros
        features_dict = {feature: 0.0 for feature in self.features}
        
        # Set basic game features
        # Handle both date-only and datetime formats
        game_date_str = game_data['game_date']
        if ' ' in game_date_str:
            # Full datetime format
            game_date = datetime.strptime(game_date_str.split(' ')[0], '%Y-%m-%d')
        else:
            # Date-only format
            game_date = datetime.strptime(game_date_str, '%Y-%m-%d')
        features_dict['month'] = game_date.month
        features_dict['home_field_advantage'] = 1.0
        features_dict['home_rest_days'] = game_data.get('home_rest_days', 1)
        features_dict['away_rest_days'] = game_data.get('away_rest_days', 1)
        
        # Get real team stats from database
        home_stats = self.get_team_stats(game_data['home_team_id'])
        away_stats = self.get_team_stats(game_data['away_team_id'])
        
        # Map home team stats
        for stat_name, stat_value in home_stats.items():
            home_feature = f"home_{stat_name}"
            if home_feature in features_dict:
                features_dict[home_feature] = stat_value
        
        # Map away team stats  
        for stat_name, stat_value in away_stats.items():
            away_feature = f"away_{stat_name}"
            if away_feature in features_dict:
                features_dict[away_feature] = stat_value
        
        # Set default weather and ballpark features to match EXACT training feature names
        # These are the exact 17 weather/ballpark features from the training data
        
        # Weather features (3 features)
        weather_defaults = {
            'weather_dome_game': 0,
            'weather_extreme_conditions': 0, 
            'weather_hitting_favorability': 1  # Assume favorable conditions
        }
        
        # Ballpark climate features (7 features)
        ballpark_climate_defaults = {
            'ballpark_climate_humid_continental': 1,  # Most common
            'ballpark_climate_humid_subtropical': 0,
            'ballpark_climate_mediterranean': 0,
            'ballpark_climate_oceanic': 0,
            'ballpark_climate_semi_arid': 0,
            'ballpark_climate_tropical': 0,
            'ballpark_climate_unknown': 0,
        }
        
        # Climate features (7 features) - these are separate from ballpark_climate
        climate_defaults = {
            'climate_desert': 0,
            'climate_humid_continental': 0,
            'climate_humid_subtropical': 1,  # Most common
            'climate_mediterranean': 0,
            'climate_oceanic': 0,
            'climate_semi_arid': 0,
            'climate_tropical': 0,
        }
        
        # Update with defaults that match training feature names exactly
        features_dict.update(weather_defaults)
        features_dict.update(ballpark_climate_defaults)
        features_dict.update(climate_defaults)
        
        # Override with any provided weather/ballpark data
        for key, value in game_data.items():
            if key in features_dict:
                features_dict[key] = value
        
        # Convert to DataFrame with exact feature order
        df = pd.DataFrame([features_dict])
        df_result = df[self.features].copy()  # Ensure exact order and features
        
        logger.info(f"Features prepared: {len(df_result.columns)} features")
        return df_result
    
    def predict_game(self, game_data: Dict) -> Dict:
        """
        Predict the outcome of a single MLB game using real team stats.
        
        Args:
            game_data: Dictionary containing game information
            
        Returns:
            Dictionary with prediction results
        """
        if self.model is None:
            raise ValueError("Model not loaded. Call load_model() first.")
        
        # Prepare features with real team stats
        features_df = self.prepare_game_features(game_data)
        
        # Apply scaling if scaler is available
        if self.scaler is not None:
            # Only scale features that the scaler was trained on
            scaler_features = self.scaler.feature_names_in_
            features_to_scale = [f for f in scaler_features if f in features_df.columns]
            features_not_scaled = [f for f in features_df.columns if f not in scaler_features]
            
            # Scale only the features the scaler knows about
            features_df_scaled = features_df.copy()
            if features_to_scale:
                scaled_values = self.scaler.transform(features_df[features_to_scale])
                features_df_scaled[features_to_scale] = scaled_values
            
            features_df = features_df_scaled
            logger.info(f"Scaled {len(features_to_scale)} features, left {len(features_not_scaled)} unscaled")
        
        # Make prediction
        try:
            # Get probability predictions
            probabilities = self.model.predict_proba(features_df)[0]
            home_win_prob = probabilities[1]  # Probability of home team winning
            away_win_prob = probabilities[0]  # Probability of away team winning
            
            # Get binary prediction
            prediction = self.model.predict(features_df)[0]
            
            # Calculate confidence and betting recommendation
            confidence = abs(home_win_prob - 0.5) * 2  # Scale to 0-1
            
            # Betting recommendation based on confidence threshold
            betting_threshold = 0.55  # Only bet if we're 55%+ confident
            should_bet = max(home_win_prob, away_win_prob) >= betting_threshold
            
            result = {
                'game_info': {
                    'home_team': game_data['home_team_id'],
                    'away_team': game_data['away_team_id'],
                    'date': game_data['game_date']
                },
                'prediction': {
                    'home_win_probability': round(home_win_prob, 4),
                    'away_win_probability': round(away_win_prob, 4),
                    'predicted_winner': game_data['home_team_id'] if prediction == 1 else game_data['away_team_id'],
                    'confidence': round(confidence, 4)
                },
                'betting': {
                    'should_bet': should_bet,
                    'recommended_bet': game_data['home_team_id'] if home_win_prob >= betting_threshold else (
                        game_data['away_team_id'] if away_win_prob >= betting_threshold else 'No bet'
                    ),
                    'edge': round(max(home_win_prob, away_win_prob) - 0.5, 4)
                },
                'model_info': {
                    'model_type': 'XGBoost',
                    'target': 'home_team_wins',
                    'expected_auc': self.model_metadata.get('best_auc', 'Unknown') if self.model_metadata else 'Unknown',
                    'features_used': len(self.features) if self.features else 0
                }
            }
            
            logger.info(f"Prediction completed: {result['prediction']['predicted_winner']} "
                       f"({result['prediction']['confidence']:.1%} confidence)")
            
            return result
            
        except Exception as e:
            logger.error(f"Error making prediction: {e}")
            raise
    
    def predict_multiple_games(self, games_data: List[Dict]) -> List[Dict]:
        """
        Predict outcomes for multiple games.
        
        Args:
            games_data: List of game data dictionaries
            
        Returns:
            List of prediction results
        """
        logger.info(f"Predicting outcomes for {len(games_data)} games...")
        
        results = []
        for game_data in games_data:
            try:
                result = self.predict_game(game_data)
                results.append(result)
            except Exception as e:
                logger.error(f"Error predicting game {game_data}: {e}")
                continue
        
        logger.info(f"Successfully predicted {len(results)} out of {len(games_data)} games")
        return results
    
    def generate_betting_report(self, predictions: List[Dict]) -> Dict:
        """
        Generate a betting strategy report from predictions.
        
        Args:
            predictions: List of prediction results
            
        Returns:
            Dictionary with betting analysis
        """
        logger.info("Generating betting report...")
        
        total_games = len(predictions)
        if total_games == 0:
            return {
                'summary': {'total_games_analyzed': 0, 'recommended_bets': 0},
                'error': 'No successful predictions to analyze'
            }
        
        recommended_bets = [p for p in predictions if p['betting']['should_bet']]
        high_confidence = [p for p in predictions if p['prediction']['confidence'] >= 0.3]
        
        # Calculate average confidence and edge
        avg_confidence = np.mean([p['prediction']['confidence'] for p in predictions])
        avg_edge = np.mean([p['betting']['edge'] for p in recommended_bets]) if recommended_bets else 0
        
        # Home vs away bias
        home_predictions = sum(1 for p in predictions if p['prediction']['predicted_winner'] == p['game_info']['home_team'])
        
        report = {
            'summary': {
                'total_games_analyzed': total_games,
                'recommended_bets': len(recommended_bets),
                'bet_percentage': round(len(recommended_bets) / total_games * 100, 1) if total_games > 0 else 0,
                'high_confidence_predictions': len(high_confidence),
                'average_confidence': round(avg_confidence, 3),
                'average_edge': round(avg_edge, 3)
            },
            'bias_analysis': {
                'home_team_predictions': home_predictions,
                'away_team_predictions': total_games - home_predictions,
                'home_bias_percentage': round(home_predictions / total_games * 100, 1) if total_games > 0 else 0
            },
            'recommended_bets': recommended_bets,
            'model_performance': {
                'expected_auc': self.model_metadata.get('best_auc', 'Unknown') if self.model_metadata else 'Unknown',
                'model_type': 'XGBoost',
                'features_count': len(self.features) if self.features else 0
            }
        }
        
        logger.info(f"Betting report generated: {len(recommended_bets)} recommended bets out of {total_games} games")
        return report

def main():
    """Example usage of the MLB prediction pipeline with real team stats."""
    # Initialize pipeline
    pipeline = MLBPredictionPipeline()
    
    # Load the successful home_team_wins model
    if not pipeline.load_model(target="home_team_wins", model_type="xgboost"):
        logger.error("Failed to load model")
        return
    
    # Example game data with real teams
    example_games = [
        {
            'home_team_id': 'NYY',  # New York Yankees
            'away_team_id': 'BOS',  # Boston Red Sox
            'game_date': '2024-07-15',
            'home_rest_days': 1,
            'away_rest_days': 2,
        },
        {
            'home_team_id': 'LAD',  # Los Angeles Dodgers
            'away_team_id': 'SF',   # San Francisco Giants
            'game_date': '2024-07-15',
            'home_rest_days': 0,
            'away_rest_days': 1,
        }
    ]
    
    # Make predictions using real team stats
    predictions = pipeline.predict_multiple_games(example_games)
    
    # Generate betting report
    report = pipeline.generate_betting_report(predictions)
    
    # Display results
    print("\n=== MLB PREDICTION RESULTS (WITH REAL TEAM STATS) ===")
    for pred in predictions:
        print(f"\n{pred['game_info']['away_team']} @ {pred['game_info']['home_team']}")
        print(f"Predicted Winner: {pred['prediction']['predicted_winner']}")
        print(f"Home Win Prob: {pred['prediction']['home_win_probability']:.1%}")
        print(f"Away Win Prob: {pred['prediction']['away_win_probability']:.1%}")
        print(f"Confidence: {pred['prediction']['confidence']:.1%}")
        print(f"Should Bet: {pred['betting']['should_bet']}")
        if pred['betting']['should_bet']:
            print(f"Recommended Bet: {pred['betting']['recommended_bet']}")
            print(f"Edge: {pred['betting']['edge']:.1%}")
    
    print(f"\n=== BETTING REPORT ===")
    print(f"Total Games: {report['summary']['total_games_analyzed']}")
    print(f"Recommended Bets: {report['summary']['recommended_bets']}")
    print(f"Bet Percentage: {report['summary']['bet_percentage']}%")
    print(f"Average Confidence: {report['summary']['average_confidence']:.1%}")
    print(f"Average Edge: {report['summary']['average_edge']:.1%}")
    print(f"Home Bias: {report['bias_analysis']['home_bias_percentage']}%")
    print(f"Expected Model AUC: {report['model_performance']['expected_auc']}")

if __name__ == "__main__":
    main() 