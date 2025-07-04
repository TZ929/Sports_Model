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
        # Changed to load the new featured_data.csv
        df = pd.read_csv(processed_dir / "featured_data.csv", parse_dates=['date'])
        logging.info("Final dataset loaded successfully.")
        return df
    except FileNotFoundError as e:
        logging.error(f"Error loading final dataset: {e}. Please ensure previous steps ran successfully.")
        return None

def prepare_modeling_data(df):
    """Prepares the data for modeling by defining a target variable and splitting the data."""
    logging.info("--- PREPARING MODELING DATA WITH ODDS FEATURES ---")
    if df is None:
        logging.error("DataFrame is None. Aborting model preparation.")
        return None, None

    logging.info("Preparing data for modeling...")

    # --- Advanced Feature Engineering ---
    # Is the player's team home or away?
    df['is_home'] = (df['team_id'] == df['home_team_id']).astype(int)
    
    # Get opponent's defensive stats using the new, clear column names
    df['opponent_points_against_roll_avg_5g'] = np.where(
        df['is_home'] == 1,
        df['away_team_points_against_roll_avg_5g'],
        df['home_team_points_against_roll_avg_5g']
    )
    
    # Create matchup-specific feature
    df['points_vs_opp_avg'] = df['points_roll_avg_5g'] - df['opponent_points_against_roll_avg_5g']
    logging.info("Engineered new matchup feature: 'points_vs_opp_avg'.")
    # ---

    # 1. Define the Target Variable
    df['points_over_avg_5g'] = (df['points'] > df['points_roll_avg_5g']).astype(int)
    logging.info("Target variable 'points_over_avg_5g' created.")

    # 2. Select Features
    player_features = [
        'points_roll_avg_5g',
        'rebounds_roll_avg_5g',
        'assists_roll_avg_5g',
        'game_score_roll_avg_5g', # Added new feature
        'minutes_played',
    ]
    game_features = ['home_rest_days', 'away_rest_days', 'points_vs_opp_avg']
    odds_features = [
        'fanduel_points_line',
        'fanduel_points_over_odds',
        'fanduel_points_under_odds'
    ]
    
    features = player_features + game_features + odds_features
    target = 'points_over_avg_5g'
    
    # 3. Handle Missing Values
    # Fill missing odds with a neutral value
    df[odds_features] = df[odds_features].fillna({
        'fanduel_points_line': 0,
        'fanduel_points_over_odds': -110,
        'fanduel_points_under_odds': -110
    })
    
    # Fill missing player and game features with 0
    df[player_features + game_features] = df[player_features + game_features].fillna(0)

    # Drop rows only if the target variable is missing
    df_model = df.dropna(subset=[target])
    logging.info(f"Dropped {len(df) - len(df_model)} rows with missing target values.")
    
    X = df_model[features]
    y = df_model[target]
    
    # 4. Split Data
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