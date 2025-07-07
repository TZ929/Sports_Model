import pandas as pd
import numpy as np
import logging
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import sqlite3

# Setup logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "mlb_model_preparation.log"),
        logging.StreamHandler()
    ]
)

def load_master_dataset():
    """Load the master MLB dataset with all features."""
    logging.info("Loading MLB master dataset...")
    processed_dir = Path("data/processed")
    try:
        df = pd.read_csv(processed_dir / "real_mlb_master_dataset_complete.csv", parse_dates=['game_date'])
        logging.info(f"Master dataset loaded successfully with {len(df)} games and {len(df.columns)} features.")
        return df
    except FileNotFoundError as e:
        logging.error(f"Error loading master dataset: {e}. Please ensure master dataset has been created.")
        return None

def prepare_modeling_data(df):
    """Prepares the MLB data for modeling by defining target variables and splitting the data."""
    logging.info("--- PREPARING MLB MODELING DATA ---")
    if df is None:
        logging.error("DataFrame is None. Aborting model preparation.")
        return None, None

    logging.info("Preparing data for modeling...")

    # --- MLB-Specific Feature Engineering ---
    
    # 1. Create home field advantage features
    df['home_field_advantage'] = 1  # All games have home team
    
    # 2. Create scoring differential features
    df['total_runs'] = df['home_team_score'] + df['away_team_score']
    df['run_differential'] = df['home_team_score'] - df['away_team_score']
    
    # 3. Create team strength features (based on recent performance)
    # Use existing rolling averages as proxy for team strength
    home_strength_cols = [col for col in df.columns if 'home_team_' in col and 'roll_avg' in col]
    away_strength_cols = [col for col in df.columns if 'away_team_' in col and 'roll_avg' in col]
    
    if home_strength_cols and away_strength_cols:
        # Calculate team strength differential using specific columns to avoid linter issues
        try:
            home_strength_subset = df[home_strength_cols]
            away_strength_subset = df[away_strength_cols]
            df['team_strength_diff'] = home_strength_subset.mean(axis=1) - away_strength_subset.mean(axis=1)
        except Exception as e:
            logging.warning(f"Could not calculate team strength differential: {e}")
            df['team_strength_diff'] = 0
    
    # 4. Create weather impact features
    if 'temperature' in df.columns and 'wind_speed' in df.columns:
        df['weather_favorability'] = (
            (df['temperature'] - df['temperature'].mean()) / df['temperature'].std() +
            (df['wind_speed'].mean() - df['wind_speed']) / df['wind_speed'].std()
        ) / 2
    
    # --- Define Target Variables ---
    
    # Primary target: Home team wins
    df['home_team_wins'] = (df['home_team_score'] > df['away_team_score']).astype(int)
    
    # Secondary targets for different prediction types
    df['high_scoring_game'] = (df['total_runs'] > df['total_runs'].quantile(0.6)).astype(int)
    df['low_scoring_game'] = (df['total_runs'] < df['total_runs'].quantile(0.4)).astype(int)
    df['close_game'] = (abs(df['run_differential']) <= 2).astype(int)
    
    logging.info("Target variables created:")
    logging.info(f"  - Home team wins: {df['home_team_wins'].mean():.3f} (proportion)")
    logging.info(f"  - High scoring games: {df['high_scoring_game'].mean():.3f} (proportion)")
    logging.info(f"  - Low scoring games: {df['low_scoring_game'].mean():.3f} (proportion)")
    logging.info(f"  - Close games: {df['close_game'].mean():.3f} (proportion)")

    # --- Feature Selection ---
    
    # Base game features
    base_features = [
        'home_rest_days', 'away_rest_days', 'day_of_week', 'month',
        'home_field_advantage'
    ]
    
    # Team batting performance features
    batting_features = [col for col in df.columns if any(x in col for x in ['avg_at_bats', 'avg_hits', 'avg_home_runs', 'avg_rbi', 'avg_runs', 'avg_walks', 'avg_strikeouts', 'team_batting_avg', 'team_ops'])]
    
    # Team pitching performance features  
    pitching_features = [col for col in df.columns if any(x in col for x in ['pitching_avg_innings_pitched', 'pitching_avg_hits_allowed', 'pitching_avg_runs_allowed', 'pitching_avg_earned_runs', 'pitching_avg_walks_allowed', 'pitching_avg_strikeouts_pitched', 'pitching_avg_hrs_allowed', 'pitching_team_era'])]
    
    # Rolling average features (if any exist)
    rolling_features = [col for col in df.columns if 'roll_avg' in col]
    
    # Weather features (reduced set - only most important)
    weather_features = [col for col in df.columns if col.startswith(('temperature', 'humidity', 'wind_speed', 'weather_hitting_favorability', 'weather_dome_game', 'weather_extreme_conditions'))]
    
    # Ballpark features (reduced set)
    ballpark_features = [col for col in df.columns if 'ballpark_climate' in col or 'climate_' in col]
    
    # Engineered features
    engineered_features = ['team_strength_diff', 'weather_favorability']
    engineered_features = [f for f in engineered_features if f in df.columns]
    
    # Combine all features - prioritize baseball stats
    all_features = base_features + batting_features + pitching_features + rolling_features + weather_features + ballpark_features + engineered_features
    
    # Filter out features that don't exist in the dataframe AND remove duplicates
    available_features = []
    seen_features = set()
    for f in all_features:
        if f in df.columns and f not in seen_features:
            available_features.append(f)
            seen_features.add(f)
    
    logging.info(f"Selected {len(available_features)} features for modeling:")
    logging.info(f"  - Base features: {len([f for f in base_features if f in df.columns])}")
    logging.info(f"  - Batting features: {len([f for f in batting_features if f in df.columns])}")
    logging.info(f"  - Pitching features: {len([f for f in pitching_features if f in df.columns])}")
    logging.info(f"  - Rolling features: {len([f for f in rolling_features if f in df.columns])}")
    logging.info(f"  - Weather features: {len([f for f in weather_features if f in df.columns])}")
    logging.info(f"  - Ballpark features: {len([f for f in ballpark_features if f in df.columns])}")
    logging.info(f"  - Engineered features: {len(engineered_features)}")
    
    # Log first few features from each category for debugging
    logging.info(f"Sample batting features: {[f for f in batting_features if f in df.columns][:5]}")
    logging.info(f"Sample pitching features: {[f for f in pitching_features if f in df.columns][:5]}")

    return df, available_features

def create_model_datasets(df, features, target='home_team_wins', test_size=0.2, val_size=0.1):
    """Create train/validation/test splits with proper handling of missing values."""
    logging.info(f"Creating model datasets for target: {target}")
    
    # Handle missing values more robustly
    df_clean = df.copy()
    
    # Check for missing values in features
    missing_counts = df_clean[features].isnull().sum()
    features_with_missing = missing_counts[missing_counts > 0]
    if not features_with_missing.empty:
        logging.info(f"Features with missing values: {features_with_missing.to_dict()}")
    
    # Fill missing values with appropriate defaults
    for feature in features:
        if feature in df_clean.columns:
            if df_clean[feature].dtype in ['int64', 'float64']:
                # For numerical features, use median or 0 if all values are NaN
                if df_clean[feature].notna().any():
                    fill_value = df_clean[feature].median()
                else:
                    fill_value = 0.0
                df_clean[feature] = df_clean[feature].fillna(fill_value)
            else:
                # For categorical features, use mode or 'Unknown'
                if df_clean[feature].notna().any():
                    mode_values = df_clean[feature].mode()
                    fill_value = mode_values[0] if len(mode_values) > 0 else 'Unknown'
                else:
                    fill_value = 'Unknown'
                df_clean[feature] = df_clean[feature].fillna(fill_value)
    
    # Remove rows with missing target
    df_clean = df_clean.dropna(subset=[target])
    
    # Final check for any remaining NaN values
    remaining_nans = df_clean[features].isnull().sum().sum()
    if remaining_nans > 0:
        logging.warning(f"Still have {remaining_nans} NaN values after cleaning. Filling with 0.")
        df_clean[features] = df_clean[features].fillna(0)
    
    logging.info(f"After cleaning: {len(df_clean)} games remaining")
    
    # Handle categorical variables - encode them as dummy variables
    categorical_features = []
    for feature in features:
        if feature in df_clean.columns and df_clean[feature].dtype == 'object':
            categorical_features.append(feature)
    
    if categorical_features:
        logging.info(f"Encoding {len(categorical_features)} categorical features: {categorical_features}")
        # Create dummy variables for categorical features
        df_encoded = pd.get_dummies(df_clean, columns=categorical_features, prefix=categorical_features, drop_first=True)
        
        # Update features list to include new dummy variables
        new_features = []
        for feature in features:
            if feature in categorical_features:
                # Add all dummy variables for this categorical feature
                dummy_cols = [col for col in df_encoded.columns if col.startswith(f"{feature}_")]
                new_features.extend(dummy_cols)
            else:
                new_features.append(feature)
        
        features = new_features
        logging.info(f"After encoding: {len(features)} features")
    else:
        df_encoded = df_clean
    
    # Final check for NaN values in encoded data
    final_nans = df_encoded[features].isnull().sum().sum()
    if final_nans > 0:
        logging.warning(f"Found {final_nans} NaN values in encoded data. Filling with 0.")
        df_encoded[features] = df_encoded[features].fillna(0)
    
    # Prepare features and target
    X = df_encoded[features]
    y = df_encoded[target]
    
    # Verify no NaN values remain
    if X.isnull().sum().sum() > 0:
        raise ValueError("X still contains NaN values!")
    if y.isnull().sum() > 0:
        raise ValueError("y still contains NaN values!")
    
    # First split: train+val vs test
    X_temp, X_test, y_temp, y_test = train_test_split(
        X, y, test_size=test_size, random_state=42, stratify=y
    )
    
    # Second split: train vs val
    val_size_adjusted = val_size / (1 - test_size)
    X_train, X_val, y_train, y_val = train_test_split(
        X_temp, y_temp, test_size=val_size_adjusted, random_state=42, stratify=y_temp
    )
    
    logging.info(f"Data split completed:")
    logging.info(f"  - Training set: {len(X_train)} games ({len(X_train)/len(df_clean)*100:.1f}%)")
    logging.info(f"  - Validation set: {len(X_val)} games ({len(X_val)/len(df_clean)*100:.1f}%)")
    logging.info(f"  - Test set: {len(X_test)} games ({len(X_test)/len(df_clean)*100:.1f}%)")
    
    # Check target distribution
    logging.info(f"Target distribution:")
    logging.info(f"  - Training: {y_train.mean():.3f}")
    logging.info(f"  - Validation: {y_val.mean():.3f}")
    logging.info(f"  - Test: {y_test.mean():.3f}")
    
    return X_train, X_val, X_test, y_train, y_val, y_test, features

def scale_features(X_train, X_val, X_test, features_to_scale=None):
    """Scale numerical features using StandardScaler."""
    logging.info("Scaling features...")
    
    if features_to_scale is None:
        # Scale all numerical features
        numerical_features = X_train.select_dtypes(include=[np.number]).columns.tolist()
    else:
        numerical_features = features_to_scale
    
    scaler = StandardScaler()
    
    # Fit scaler on training data only
    X_train_scaled = X_train.copy()
    X_val_scaled = X_val.copy()
    X_test_scaled = X_test.copy()
    
    if numerical_features:
        X_train_scaled[numerical_features] = scaler.fit_transform(X_train[numerical_features])
        X_val_scaled[numerical_features] = scaler.transform(X_val[numerical_features])
        X_test_scaled[numerical_features] = scaler.transform(X_test[numerical_features])
        
        logging.info(f"Scaled {len(numerical_features)} numerical features")
    
    return X_train_scaled, X_val_scaled, X_test_scaled, scaler

def save_modeling_data(X_train, X_val, X_test, y_train, y_val, y_test, features, target, scaler=None):
    """Save the prepared datasets and metadata."""
    logging.info("Saving modeling data...")
    
    output_dir = Path("data/processed/mlb")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save datasets
    train_df = X_train.copy()
    train_df[target] = y_train
    train_df.to_csv(output_dir / f"modeling_train_{target}.csv", index=False)
    
    val_df = X_val.copy()
    val_df[target] = y_val
    val_df.to_csv(output_dir / f"modeling_val_{target}.csv", index=False)
    
    test_df = X_test.copy()
    test_df[target] = y_test
    test_df.to_csv(output_dir / f"modeling_test_{target}.csv", index=False)
    
    # Save feature list
    feature_info = {
        'features': features,
        'target': target,
        'n_features': len(features),
        'train_size': len(X_train),
        'val_size': len(X_val),
        'test_size': len(X_test)
    }
    
    import json
    with open(output_dir / f"modeling_info_{target}.json", 'w') as f:
        json.dump(feature_info, f, indent=2)
    
    # Save scaler if provided
    if scaler is not None:
        import joblib
        joblib.dump(scaler, output_dir / f"scaler_{target}.joblib")
    
    logging.info(f"Modeling data saved to {output_dir}")
    logging.info(f"  - Training: {len(train_df)} samples")
    logging.info(f"  - Validation: {len(val_df)} samples")
    logging.info(f"  - Test: {len(test_df)} samples")
    logging.info(f"  - Features: {len(features)}")

def prepare_all_targets(df, features):
    """Prepare datasets for all target variables."""
    targets = ['home_team_wins', 'high_scoring_game', 'low_scoring_game', 'close_game']
    
    for target in targets:
        if target in df.columns:
            logging.info(f"\n--- Preparing data for target: {target} ---")
            
            X_train, X_val, X_test, y_train, y_val, y_test, updated_features = create_model_datasets(
                df, features, target=target
            )
            
            # Scale features
            X_train_scaled, X_val_scaled, X_test_scaled, scaler = scale_features(
                X_train, X_val, X_test
            )
            
            # Save datasets
            save_modeling_data(
                X_train_scaled, X_val_scaled, X_test_scaled, 
                y_train, y_val, y_test, updated_features, target, scaler
            )
        else:
            logging.warning(f"Target {target} not found in dataframe")

if __name__ == '__main__':
    # Load master dataset
    master_df = load_master_dataset()
    
    if master_df is not None:
        # Prepare features and targets
        prepared_df, selected_features = prepare_modeling_data(master_df)
        
        if prepared_df is not None and selected_features:
            # Prepare datasets for all targets
            prepare_all_targets(prepared_df, selected_features)
            
            logging.info("\n--- MLB MODEL PREPARATION COMPLETE ---")
            logging.info(f"Prepared datasets for multiple prediction targets")
            logging.info(f"Features selected: {len(selected_features)}")
            logging.info(f"Total games processed: {len(prepared_df)}")
        else:
            logging.error("Failed to prepare modeling data")
    else:
        logging.error("Failed to load master dataset")
    
    logging.info("MLB model preparation process finished.") 