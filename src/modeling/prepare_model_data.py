import pandas as pd
import numpy as np
import logging
from pathlib import Path
from sklearn.model_selection import train_test_split

# Setup logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "model_preparation.log"),
        logging.StreamHandler()
    ]
)

def load_final_dataset():
    """Load the final master feature dataset."""
    logging.info("Loading final feature dataset...")
    processed_dir = Path("data/processed")
    try:
        df = pd.read_csv(processed_dir / "master_feature_dataset_v2.csv", parse_dates=['date'])
        logging.info("Final dataset loaded successfully.")
        return df
    except FileNotFoundError as e:
        logging.error(f"Error loading final dataset: {e}. Please ensure previous steps ran successfully.")
        return None

def prepare_modeling_data(df):
    """Prepares the data for modeling by defining a target variable and splitting the data."""
    if df is None:
        logging.error("DataFrame is None. Aborting model preparation.")
        return None, None

    logging.info("Preparing data for modeling...")

    # 1. Define the Target Variable
    # Predict if a player will score more points than their 5-game rolling average.
    df['points_over_avg_5g'] = (df['points'] > df['points_roll_avg_5g']).astype(int)
    logging.info("Target variable 'points_over_avg_5g' created.")

    # 2. Select Features
    # Start with a subset of features to avoid multicollinearity and simplify
    player_features = [
        'points_roll_avg_5g',
        'rebounds_roll_avg_5g',
        'assists_roll_avg_5g',
        'minutes_played',
    ]
    team_features = [
        'home_team_points_for_roll_avg_5g',
        'home_team_points_against_roll_avg_5g',
        'away_team_points_for_roll_avg_5g',
        'away_team_points_against_roll_avg_5g',
    ]
    game_features = ['home_rest_days', 'away_rest_days']
    
    features = player_features + team_features + game_features
    target = 'points_over_avg_5g'
    
    # 3. Handle Missing Values
    # We can't use rows where we don't have a recent average to compare against.
    # Also drop rows where other key features are missing.
    df_model = df.dropna(subset=[target] + features)
    logging.info(f"Dropped {len(df) - len(df_model)} rows with missing values.")
    
    X = df_model[features]
    y = df_model[target]
    
    # 4. Split Data
    # We'll do a simple random split for now. A time-based split would be better for a real model.
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    logging.info(f"Data split into training ({len(X_train)} rows) and testing ({len(X_test)} rows).")

    # Combine features and target for saving
    train_df = X_train.assign(points_over_avg_5g=y_train)
    test_df = X_test.assign(points_over_avg_5g=y_test)
    
    return train_df, test_df

def save_modeling_data(train_df, test_df):
    """Saves the training and testing dataframes to CSV files."""
    if train_df is None or test_df is None:
        logging.error("Training or testing dataframe is None. Cannot save.")
        return

    logging.info("Saving modeling data...")
    output_dir = Path("data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)

    train_df.to_csv(output_dir / "modeling_train.csv", index=False)
    test_df.to_csv(output_dir / "modeling_test.csv", index=False)
    
    logging.info(f"Modeling data saved to '{output_dir.resolve()}'")

if __name__ == '__main__':
    final_df = load_final_dataset()
    train_data, test_data = prepare_modeling_data(final_df)
    save_modeling_data(train_data, test_data)
    logging.info("Model preparation process finished.") 