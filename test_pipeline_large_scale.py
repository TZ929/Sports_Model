"""
Large-scale testing of MLB prediction pipeline on real games.

This script tests the production pipeline on 1000+ real games from the database
to validate performance, reliability, and betting strategy effectiveness.
"""

import pandas as pd
import numpy as np
import sqlite3
import logging
from datetime import datetime
from pathlib import Path
import sys
import time

# Add src to path for imports
sys.path.append('src')
from prediction.mlb_prediction_pipeline import MLBPredictionPipeline

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_test_games(db_path="data/sports_model.db", min_games=1000):
    """Load real games from database for testing."""
    logger.info(f"Loading test games from database (minimum {min_games} games)...")
    
    conn = sqlite3.connect(db_path)
    
    # Get completed games with actual scores 
    # Note: mlb_games already has team abbreviations as IDs, no need to join
    query = """
        SELECT 
            g.game_id,
            g.game_date,
            g.home_team_id,
            g.away_team_id,
            g.home_team_score,
            g.away_team_score,
            g.home_team_id as home_team_abbr,
            g.away_team_id as away_team_abbr
        FROM mlb_games g
        WHERE (g.home_team_score > 0 OR g.away_team_score > 0)
        AND g.home_team_score IS NOT NULL 
        AND g.away_team_score IS NOT NULL
        ORDER BY g.game_date DESC
        LIMIT ?
    """
    
    df = pd.read_sql(query, conn, params=[min_games * 2])  # Get extra to ensure we have enough
    conn.close()
    
    logger.info(f"Loaded {len(df)} games from database")
    return df

def prepare_game_data_for_pipeline(games_df):
    """Convert database games to pipeline input format."""
    logger.info("Preparing game data for pipeline...")
    
    games_data = []
    for _, row in games_df.iterrows():
        game_data = {
            'game_id': row['game_id'],
            'home_team_id': row['home_team_abbr'],
            'away_team_id': row['away_team_abbr'],
            'game_date': row['game_date'],
            'home_rest_days': 1,  # Default - would normally calculate from schedule
            'away_rest_days': 1,  # Default - would normally calculate from schedule
            'actual_home_score': row['home_team_score'],
            'actual_away_score': row['away_team_score'],
            'actual_home_wins': 1 if row['home_team_score'] > row['away_team_score'] else 0
        }
        games_data.append(game_data)
    
    logger.info(f"Prepared {len(games_data)} games for pipeline testing")
    return games_data

def run_large_scale_test(games_data, pipeline):
    """Run predictions on large dataset and collect results."""
    logger.info(f"Running large-scale test on {len(games_data)} games...")
    
    start_time = time.time()
    results = []
    errors = []
    
    for i, game_data in enumerate(games_data):
        try:
            # Make prediction
            prediction = pipeline.predict_game(game_data)
            
            # Add actual outcome for evaluation
            prediction['actual_outcome'] = {
                'home_wins': game_data['actual_home_wins'],
                'home_score': game_data['actual_home_score'],
                'away_score': game_data['actual_away_score'],
                'total_runs': game_data['actual_home_score'] + game_data['actual_away_score']
            }
            
            results.append(prediction)
            
            # Progress update
            if (i + 1) % 100 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed
                logger.info(f"Processed {i + 1}/{len(games_data)} games ({rate:.1f} games/sec)")
                
        except Exception as e:
            error_info = {
                'game_id': game_data.get('game_id', 'unknown'),
                'error': str(e),
                'game_data': game_data
            }
            errors.append(error_info)
            logger.error(f"Error predicting game {game_data.get('game_id', 'unknown')}: {e}")
    
    total_time = time.time() - start_time
    logger.info(f"Large-scale test completed in {total_time:.1f} seconds")
    logger.info(f"Successfully predicted {len(results)} games, {len(errors)} errors")
    
    return results, errors

def evaluate_predictions(results):
    """Evaluate prediction accuracy and betting performance."""
    logger.info("Evaluating prediction performance...")
    
    if not results:
        logger.error("No results to evaluate!")
        return {}
    
    # Convert to DataFrame for analysis
    predictions_data = []
    for result in results:
        pred_data = {
            'game_id': result['game_info']['home_team'] + '_vs_' + result['game_info']['away_team'],
            'home_team': result['game_info']['home_team'],
            'away_team': result['game_info']['away_team'],
            'predicted_winner': result['prediction']['predicted_winner'],
            'home_win_prob': result['prediction']['home_win_probability'],
            'away_win_prob': result['prediction']['away_win_probability'],
            'confidence': result['prediction']['confidence'],
            'should_bet': result['betting']['should_bet'],
            'recommended_bet': result['betting']['recommended_bet'],
            'edge': result['betting']['edge'],
            'actual_home_wins': result['actual_outcome']['home_wins'],
            'actual_total_runs': result['actual_outcome']['total_runs'],
            'predicted_home_wins': 1 if result['prediction']['predicted_winner'] == result['game_info']['home_team'] else 0
        }
        predictions_data.append(pred_data)
    
    df = pd.DataFrame(predictions_data)
    
    # Calculate accuracy metrics
    total_games = len(df)
    correct_predictions = (df['predicted_home_wins'] == df['actual_home_wins']).sum()
    accuracy = correct_predictions / total_games
    
    # Betting analysis
    betting_games = df[df['should_bet'] == True]
    total_bets = len(betting_games)
    
    if total_bets > 0:
        correct_bets = 0
        for _, row in betting_games.iterrows():
            if row['recommended_bet'] == row['home_team'] and row['actual_home_wins'] == 1:
                correct_bets += 1
            elif row['recommended_bet'] == row['away_team'] and row['actual_home_wins'] == 0:
                correct_bets += 1
        
        betting_accuracy = correct_bets / total_bets
        avg_edge = betting_games['edge'].mean()
        avg_confidence = betting_games['confidence'].mean()
    else:
        betting_accuracy = 0
        avg_edge = 0
        avg_confidence = 0
    
    # Home field advantage analysis
    home_wins_actual = df['actual_home_wins'].mean()
    home_wins_predicted = df['predicted_home_wins'].mean()
    
    # Confidence distribution
    confidence_stats = df['confidence'].describe()
    
    # ROC AUC calculation (simplified)
    from sklearn.metrics import roc_auc_score
    try:
        auc_score = roc_auc_score(df['actual_home_wins'], df['home_win_prob'])
    except:
        auc_score = 0.5
    
    evaluation = {
        'overall_performance': {
            'total_games': total_games,
            'accuracy': round(accuracy, 4),
            'auc_score': round(auc_score, 4),
            'correct_predictions': correct_predictions
        },
        'betting_performance': {
            'total_bets_recommended': total_bets,
            'betting_percentage': round(total_bets / total_games * 100, 1),
            'betting_accuracy': round(betting_accuracy, 4),
            'average_edge': round(avg_edge, 4),
            'average_confidence': round(avg_confidence, 4)
        },
        'home_field_advantage': {
            'actual_home_win_rate': round(home_wins_actual, 4),
            'predicted_home_win_rate': round(home_wins_predicted, 4),
            'home_bias': round(abs(home_wins_predicted - home_wins_actual), 4)
        },
        'confidence_analysis': {
            'mean_confidence': round(confidence_stats['mean'], 4),
            'std_confidence': round(confidence_stats['std'], 4),
            'min_confidence': round(confidence_stats['min'], 4),
            'max_confidence': round(confidence_stats['max'], 4)
        }
    }
    
    return evaluation, df

def generate_detailed_report(evaluation, df, errors):
    """Generate comprehensive test report."""
    logger.info("Generating detailed test report...")
    
    report = f"""
=== MLB PREDICTION PIPELINE LARGE-SCALE TEST REPORT ===
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

OVERALL PERFORMANCE:
- Total Games Tested: {evaluation['overall_performance']['total_games']:,}
- Prediction Accuracy: {evaluation['overall_performance']['accuracy']:.1%}
- ROC AUC Score: {evaluation['overall_performance']['auc_score']:.4f}
- Correct Predictions: {evaluation['overall_performance']['correct_predictions']:,}

SPORTSBOOK PERFORMANCE:
- Recommended Bets: {evaluation['betting_performance']['total_bets_recommended']:,} ({evaluation['betting_performance']['betting_percentage']:.1f}% of games)
- Betting Accuracy: {evaluation['betting_performance']['betting_accuracy']:.1%}
- Average Edge: {evaluation['betting_performance']['average_edge']:.1%}
- Average Confidence: {evaluation['betting_performance']['average_confidence']:.1%}

HOME FIELD ADVANTAGE:
- Actual Home Win Rate: {evaluation['home_field_advantage']['actual_home_win_rate']:.1%}
- Predicted Home Win Rate: {evaluation['home_field_advantage']['predicted_home_win_rate']:.1%}
- Home Bias: {evaluation['home_field_advantage']['home_bias']:.1%}

CONFIDENCE ANALYSIS:
- Mean Confidence: {evaluation['confidence_analysis']['mean_confidence']:.1%}
- Std Confidence: {evaluation['confidence_analysis']['std_confidence']:.1%}
- Min Confidence: {evaluation['confidence_analysis']['min_confidence']:.1%}
- Max Confidence: {evaluation['confidence_analysis']['max_confidence']:.1%}

ERROR ANALYSIS:
- Total Errors: {len(errors)}
- Success Rate: {(1 - len(errors) / (evaluation['overall_performance']['total_games'] + len(errors))):.1%}

PERFORMANCE BENCHMARKS:
- Random Chance Accuracy: 50.0%
- Random Chance AUC: 0.5000
- Sportsbook Break-even: ~52.4% (accounting for juice)

VERDICT:
"""
    
    # Add verdict based on performance
    if evaluation['overall_performance']['auc_score'] > 0.55:
        verdict = "🎉 EXCELLENT - Sportsbook-beating performance achieved!"
    elif evaluation['overall_performance']['auc_score'] > 0.52:
        verdict = "✅ GOOD - Above-average performance with potential profitability"
    elif evaluation['overall_performance']['auc_score'] > 0.50:
        verdict = "⚠️ MARGINAL - Slightly better than random, needs improvement"
    else:
        verdict = "❌ POOR - Below random chance, significant issues detected"
    
    report += verdict
    
    # Add betting strategy analysis
    if evaluation['betting_performance']['total_bets_recommended'] > 0:
        if evaluation['betting_performance']['betting_accuracy'] > 0.55:
            report += "\n🎯 BETTING STRATEGY: High-confidence bets show strong performance!"
        elif evaluation['betting_performance']['betting_accuracy'] > 0.52:
            report += "\n💰 BETTING STRATEGY: Selective betting strategy shows promise"
        else:
            report += "\n⚠️ BETTING STRATEGY: Betting recommendations need improvement"
    
    return report

def main():
    """Run comprehensive large-scale test of MLB prediction pipeline."""
    logger.info("Starting large-scale MLB prediction pipeline test...")
    
    # Load test games
    games_df = load_test_games(min_games=1000)
    
    if len(games_df) < 100:
        logger.error("Insufficient test data - need at least 100 games")
        return
    
    # Prepare game data
    games_data = prepare_game_data_for_pipeline(games_df)
    
    # Initialize pipeline
    pipeline = MLBPredictionPipeline()
    
    # Load model
    if not pipeline.load_model(target="home_team_wins", model_type="xgboost"):
        logger.error("Failed to load model - cannot run test")
        return
    
    # Run predictions
    results, errors = run_large_scale_test(games_data, pipeline)
    
    if not results:
        logger.error("No successful predictions - test failed")
        return
    
    # Evaluate performance
    evaluation, df = evaluate_predictions(results)
    
    # Generate report
    report = generate_detailed_report(evaluation, df, errors)
    
    # Save results
    output_dir = Path("analysis_results")
    output_dir.mkdir(exist_ok=True)
    
    # Save detailed results
    df.to_csv(output_dir / "large_scale_test_results.csv", index=False)
    
    # Save report
    with open(output_dir / "large_scale_test_report.txt", "w", encoding='utf-8') as f:
        f.write(report)
    
    # Print report
    print(report)
    
    logger.info("Large-scale test completed successfully!")
    logger.info(f"Results saved to: {output_dir.resolve()}")

if __name__ == "__main__":
    main() 