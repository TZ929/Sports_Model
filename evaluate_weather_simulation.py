"""
Evaluate Simulated Weather Data Robustness for MLB Predictions

This script analyzes the quality, realism, and predictive value of our simulated weather data
compared to known MLB weather patterns and their impact on game outcomes.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import sys
sys.path.append('src/feature_engineering/mlb')
from weather_features_mlb import WeatherFeatureEngineer
import warnings
warnings.filterwarnings('ignore')

def create_test_dataset():
    """Create a comprehensive test dataset spanning multiple seasons and teams."""
    # Generate full season data for multiple years
    dates = pd.date_range('2020-04-01', '2024-09-30', freq='D')
    
    # All 30 MLB teams
    teams = ['NYY', 'NYM', 'BOS', 'TB', 'TOR', 'BAL', 'CWS', 'CLE', 'DET', 'KC', 
             'MIN', 'HOU', 'LAA', 'OAK', 'SEA', 'TEX', 'ATL', 'MIA', 'PHI', 'WSN',
             'CHC', 'CIN', 'MIL', 'PIT', 'STL', 'ARI', 'COL', 'LAD', 'SD', 'SF']
    
    # Create realistic game schedule (not every day, focus on season)
    games = []
    for year in range(2020, 2025):
        season_start = pd.Timestamp(f'{year}-04-01')
        season_end = pd.Timestamp(f'{year}-09-30')
        season_dates = pd.date_range(season_start, season_end, freq='D')
        
        # Sample about 60% of days (realistic game frequency)
        game_dates = np.random.choice(season_dates, size=int(len(season_dates) * 0.6), replace=False)
        
        for date in game_dates:
            # Each day, multiple games
            num_games = np.random.poisson(8)  # Average ~8 games per day
            for _ in range(num_games):
                home_team = np.random.choice(teams)
                away_team = np.random.choice([t for t in teams if t != home_team])
                games.append({
                    'game_date': date,
                    'home_team_id': home_team,
                    'away_team_id': away_team
                })
    
    return pd.DataFrame(games)

def evaluate_temperature_realism(df):
    """Evaluate temperature simulation realism."""
    print("🌡️  TEMPERATURE ANALYSIS")
    print("=" * 50)
    
    # Group by climate and month
    climate_temp = df.groupby(['ballpark_climate', 'month'])['weather_temperature'].agg(['mean', 'std', 'min', 'max'])
    
    print("\n📊 Temperature by Climate and Season:")
    print(climate_temp.round(1))
    
    # Check for realistic seasonal patterns
    monthly_temps = df.groupby('month')['weather_temperature'].mean()
    
    # Summer (June-Aug) should be warmer than early season (April-May)
    summer_avg = monthly_temps[[6, 7, 8]].mean()
    early_season_avg = monthly_temps[[4, 5]].mean() if 4 in monthly_temps.index else monthly_temps.iloc[0:2].mean()
    
    print(f"\n🌞 Summer average: {summer_avg:.1f}°C")
    print(f"🌱 Early season average: {early_season_avg:.1f}°C")
    print(f"📈 Seasonal difference: {summer_avg - early_season_avg:.1f}°C")
    
    # Check climate realism
    desert_temp = df[df['ballpark_climate'] == 'desert']['weather_temperature'].mean()
    tropical_temp = df[df['ballpark_climate'] == 'tropical']['weather_temperature'].mean()
    continental_temp = df[df['ballpark_climate'] == 'humid_continental']['weather_temperature'].mean()
    
    print(f"\n🏜️  Desert climate average: {desert_temp:.1f}°C")
    print(f"🌴 Tropical climate average: {tropical_temp:.1f}°C")
    print(f"🌨️  Continental climate average: {continental_temp:.1f}°C")
    
    # Realism score
    realism_score = 0
    if summer_avg > early_season_avg + 5:  # Realistic seasonal difference
        realism_score += 25
    if desert_temp > tropical_temp > continental_temp:  # Realistic climate ordering
        realism_score += 25
    if 15 <= summer_avg <= 50 and 10 <= early_season_avg <= 35:  # Realistic temperature ranges
        realism_score += 25
    if df['weather_temperature'].std() > 5:  # Sufficient variation
        realism_score += 25
    
    print(f"\n🎯 Temperature Realism Score: {realism_score}/100")
    return realism_score

def evaluate_humidity_patterns(df):
    """Evaluate humidity simulation patterns."""
    print("\n💧 HUMIDITY ANALYSIS")
    print("=" * 50)
    
    # Humidity by climate
    humidity_by_climate = df.groupby('ballpark_climate')['weather_humidity'].agg(['mean', 'std']).round(1)
    print("\n📊 Humidity by Climate:")
    print(humidity_by_climate)
    
    # Check realistic patterns
    desert_humidity = df[df['ballpark_climate'] == 'desert']['weather_humidity'].mean()
    tropical_humidity = df[df['ballpark_climate'] == 'tropical']['weather_humidity'].mean()
    mediterranean_humidity = df[df['ballpark_climate'] == 'mediterranean']['weather_humidity'].mean()
    
    print(f"\n🏜️  Desert humidity: {desert_humidity:.1f}%")
    print(f"🌴 Tropical humidity: {tropical_humidity:.1f}%")
    print(f"🌊 Mediterranean humidity: {mediterranean_humidity:.1f}%")
    
    # Realism score
    realism_score = 0
    if tropical_humidity > mediterranean_humidity > desert_humidity:  # Realistic ordering
        realism_score += 30
    if desert_humidity < 40:  # Desert should be dry
        realism_score += 25
    if tropical_humidity > 70:  # Tropical should be humid
        realism_score += 25
    if 20 <= df['weather_humidity'].min() <= 95 >= df['weather_humidity'].max():  # Realistic range
        realism_score += 20
    
    print(f"\n🎯 Humidity Realism Score: {realism_score}/100")
    return realism_score

def evaluate_ballpark_specific_patterns(df):
    """Evaluate ballpark-specific weather patterns."""
    print("\n🏟️  BALLPARK-SPECIFIC ANALYSIS")
    print("=" * 50)
    
    # Key ballparks with known weather characteristics
    ballpark_analysis = {}
    
    # Coors Field (COL) - High altitude, should be different
    coors_data = df[df['home_team_id'] == 'COL']
    if len(coors_data) > 0:
        ballpark_analysis['Coors Field (COL)'] = {
            'avg_temp': coors_data['weather_temperature'].mean(),
            'avg_humidity': coors_data['weather_humidity'].mean(),
            'climate': 'semi_arid'
        }
    
    # Tropicana Field (TB) - Dome
    tropicana_data = df[df['home_team_id'] == 'TB']
    if len(tropicana_data) > 0:
        ballpark_analysis['Tropicana Field (TB)'] = {
            'avg_temp': tropicana_data['weather_temperature'].mean(),
            'dome_games': tropicana_data['weather_dome_game'].sum(),
            'climate': 'humid_subtropical'
        }
    
    # Wrigley Field (CHC) - Windy City
    wrigley_data = df[df['home_team_id'] == 'CHC']
    if len(wrigley_data) > 0:
        ballpark_analysis['Wrigley Field (CHC)'] = {
            'avg_wind': wrigley_data['weather_wind_speed'].mean(),
            'climate': 'humid_continental'
        }
    
    # Oracle Park (SF) - Coastal, windy
    oracle_data = df[df['home_team_id'] == 'SF']
    if len(oracle_data) > 0:
        ballpark_analysis['Oracle Park (SF)'] = {
            'avg_wind': oracle_data['weather_wind_speed'].mean(),
            'avg_temp': oracle_data['weather_temperature'].mean(),
            'climate': 'mediterranean'
        }
    
    print("\n📊 Ballpark-Specific Weather Patterns:")
    for ballpark, stats in ballpark_analysis.items():
        print(f"\n{ballpark}:")
        for key, value in stats.items():
            print(f"  {key}: {value:.1f}" if isinstance(value, float) else f"  {key}: {value}")
    
    # Check dome games
    dome_games = df[df['weather_dome_game'] == 1]
    outdoor_games = df[df['weather_dome_game'] == 0]
    
    print(f"\n🏟️  Dome games: {len(dome_games):,}")
    print(f"🌤️  Outdoor games: {len(outdoor_games):,}")
    print(f"📊 Dome game percentage: {len(dome_games) / len(df) * 100:.1f}%")
    
    # Realism score
    realism_score = 0
    if len(dome_games) > 0:  # Dome games identified
        realism_score += 20
    if len(ballpark_analysis) >= 4:  # Multiple ballparks analyzed
        realism_score += 20
    if 'Wrigley Field (CHC)' in ballpark_analysis and ballpark_analysis['Wrigley Field (CHC)']['avg_wind'] > 5:
        realism_score += 20  # Chicago should be windy
    if 'Oracle Park (SF)' in ballpark_analysis and ballpark_analysis['Oracle Park (SF)']['avg_wind'] > 6:
        realism_score += 20  # SF should be very windy
    if 15 <= len(dome_games) / len(df) * 100 <= 25:  # Realistic dome game percentage
        realism_score += 20
    
    print(f"\n🎯 Ballpark Realism Score: {realism_score}/100")
    return realism_score

def evaluate_predictive_features(df):
    """Evaluate the predictive value of weather features."""
    print("\n🎯 PREDICTIVE VALUE ANALYSIS")
    print("=" * 50)
    
    # Check feature variation and distribution
    weather_features = [col for col in df.columns if col.startswith('weather_')]
    
    print(f"\n📊 Weather Features Available: {len(weather_features)}")
    print("Features:", weather_features)
    
    # Check for sufficient variation (important for ML)
    variation_scores = {}
    for feature in weather_features:
        if df[feature].dtype in ['int64', 'float64']:
            cv = df[feature].std() / df[feature].mean() if df[feature].mean() != 0 else 0
            variation_scores[feature] = cv
    
    print(f"\n📈 Feature Variation (Coefficient of Variation):")
    for feature, cv in sorted(variation_scores.items(), key=lambda x: x[1], reverse=True):
        print(f"  {feature}: {cv:.3f}")
    
    # Check for realistic correlations
    correlations = {}
    if 'weather_temperature' in df.columns and 'weather_humidity' in df.columns:
        temp_humidity_corr = df['weather_temperature'].corr(df['weather_humidity'])
        correlations['Temperature-Humidity'] = temp_humidity_corr
    
    if 'weather_hitting_favorability' in df.columns and 'weather_temperature' in df.columns:
        hitting_temp_corr = df['weather_hitting_favorability'].corr(df['weather_temperature'])
        correlations['Hitting Favorability-Temperature'] = hitting_temp_corr
    
    print(f"\n🔗 Feature Correlations:")
    for pair, corr in correlations.items():
        print(f"  {pair}: {corr:.3f}")
    
    # Check extreme conditions
    extreme_conditions = df['weather_extreme_conditions'].sum()
    extreme_percentage = extreme_conditions / len(df) * 100
    
    print(f"\n⚠️  Extreme weather conditions: {extreme_conditions:,} ({extreme_percentage:.1f}%)")
    
    # Predictive value score
    predictive_score = 0
    if len(weather_features) >= 15:  # Sufficient features
        predictive_score += 20
    if len([cv for cv in variation_scores.values() if cv > 0.1]) >= 5:  # Good variation
        predictive_score += 20
    if 5 <= extreme_percentage <= 15:  # Realistic extreme conditions
        predictive_score += 20
    if abs(temp_humidity_corr) < 0.7:  # Not too highly correlated
        predictive_score += 20
    if len(correlations) >= 2:  # Multiple meaningful correlations
        predictive_score += 20
    
    print(f"\n🎯 Predictive Value Score: {predictive_score}/100")
    return predictive_score

def evaluate_data_completeness(df):
    """Evaluate data completeness and quality."""
    print("\n✅ DATA COMPLETENESS ANALYSIS")
    print("=" * 50)
    
    # Check for missing values
    missing_data = df.isnull().sum()
    weather_missing = missing_data[[col for col in missing_data.index if 'weather' in col]]
    
    print(f"\n📊 Missing Values in Weather Features:")
    if weather_missing.sum() == 0:
        print("  ✅ No missing values found!")
    else:
        print(weather_missing[weather_missing > 0])
    
    # Check data types
    weather_dtypes = df.dtypes[[col for col in df.dtypes.index if 'weather' in col]]
    print(f"\n📋 Data Types:")
    for col, dtype in weather_dtypes.items():
        print(f"  {col}: {dtype}")
    
    # Check for outliers
    outlier_analysis = {}
    for col in ['weather_temperature', 'weather_humidity', 'weather_wind_speed']:
        if col in df.columns:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
            outlier_analysis[col] = len(outliers)
    
    print(f"\n📊 Outlier Analysis:")
    for col, count in outlier_analysis.items():
        percentage = count / len(df) * 100
        print(f"  {col}: {count} outliers ({percentage:.1f}%)")
    
    # Completeness score
    completeness_score = 0
    if weather_missing.sum() == 0:  # No missing values
        completeness_score += 30
    if all(dtype in ['int64', 'float64', 'object'] for dtype in weather_dtypes.values()):  # Proper data types
        completeness_score += 25
    if all(count < len(df) * 0.05 for count in outlier_analysis.values()):  # Low outlier rate
        completeness_score += 25
    if len(df) > 1000:  # Sufficient sample size
        completeness_score += 20
    
    print(f"\n🎯 Data Completeness Score: {completeness_score}/100")
    return completeness_score

def main():
    """Main evaluation function."""
    print("🔍 EVALUATING SIMULATED WEATHER DATA ROBUSTNESS")
    print("=" * 60)
    
    # Create comprehensive test dataset
    print("\n📊 Creating test dataset...")
    test_df = create_test_dataset()
    print(f"Generated {len(test_df):,} games across {test_df['game_date'].dt.year.nunique()} years")
    
    # Apply weather feature engineering
    print("\n🌤️  Applying weather feature engineering...")
    engineer = WeatherFeatureEngineer()
    weather_df = engineer.add_weather_features(test_df)
    
    print(f"Final dataset: {len(weather_df)} games with {len(weather_df.columns)} features")
    
    # Run all evaluations
    scores = {}
    scores['Temperature'] = evaluate_temperature_realism(weather_df)
    scores['Humidity'] = evaluate_humidity_patterns(weather_df)
    scores['Ballpark'] = evaluate_ballpark_specific_patterns(weather_df)
    scores['Predictive'] = evaluate_predictive_features(weather_df)
    scores['Completeness'] = evaluate_data_completeness(weather_df)
    
    # Overall assessment
    print("\n" + "=" * 60)
    print("🏆 OVERALL ASSESSMENT")
    print("=" * 60)
    
    overall_score = sum(scores.values()) / len(scores)
    
    print(f"\n📊 Individual Scores:")
    for category, score in scores.items():
        status = "✅" if score >= 70 else "⚠️" if score >= 50 else "❌"
        print(f"  {status} {category}: {score}/100")
    
    print(f"\n🎯 Overall Robustness Score: {overall_score:.1f}/100")
    
    # Recommendations
    print(f"\n💡 RECOMMENDATIONS:")
    if overall_score >= 80:
        print("  ✅ EXCELLENT - Simulated weather data is highly robust for predictions")
        print("  ✅ Safe to use for modeling and production")
        print("  ✅ Consider this a strong baseline for weather features")
    elif overall_score >= 60:
        print("  ⚠️  GOOD - Simulated weather data is adequate for predictions")
        print("  ⚠️  Suitable for initial modeling and prototyping")
        print("  ⚠️  Consider enhancing with real data for production")
    else:
        print("  ❌ POOR - Simulated weather data needs improvement")
        print("  ❌ Recommend collecting real weather data")
        print("  ❌ Current simulation may not be reliable for predictions")
    
    return overall_score, scores

if __name__ == "__main__":
    main() 