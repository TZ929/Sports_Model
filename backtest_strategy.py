import pandas as pd
import numpy as np
import logging
from pathlib import Path

# Use the new, more robust model finder
from run_prediction import find_latest_model, load_model

# Setup logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "backtesting.log"),
        logging.StreamHandler()
    ]
)

def convert_odds_to_probability(american_odds):
    """Converts American odds to implied probability."""
    if american_odds > 0:
        return 100 / (american_odds + 100)
    else:
        return abs(american_odds) / (abs(american_odds) + 100)

def calculate_payout(american_odds, stake=1):
    """Calculates the payout for a given stake and American odds."""
    if american_odds > 0:
        return stake * (american_odds / 100)
    else:
        return stake * (100 / abs(american_odds))

def run_backtest(value_threshold=0.05, stake=1):
    """
    Runs a backtesting simulation on the test dataset.
    """
    logging.info("--- STARTING BACKTESTING SIMULATION ---")

    # 1. Load the latest model
    model_path, season = find_latest_model()
    if not model_path:
        logging.error("No model found. Aborting backtest.")
        return
    
    model = load_model(model_path)
    if not model:
        logging.error(f"Failed to load model from {model_path}. Aborting backtest.")
        return
    logging.info(f"Loaded model from {model_path} for season {season}")

    # 2. Load the test data
    if not season:
        # This case should ideally not be hit due to the check above, but it satisfies the linter
        logging.error("Season name is missing. Aborting backtest.")
        return

    if season == "latest":
        # Handle the case of a non-versioned model
        test_data_path = Path("data") / "processed" / "modeling_test.csv"
    else:
        test_data_path = Path("data") / "processed" / season / "modeling_test.csv"
    
    if not test_data_path.exists():
        logging.error(f"Test data not found at {test_data_path}. Aborting.")
        return
    test_df = pd.read_csv(test_data_path)
    logging.info(f"Loaded test data with {len(test_df)} records for season {season}.")

    # 3. Simulate betting
    bets_placed = 0
    total_wagered = 0
    total_profit = 0
    wins = 0

    X_test = test_df.drop(columns=['points_over_avg_5g'])
    y_test = test_df['points_over_avg_5g']
    
    # Get model probabilities for the 'Over' class (class 1)
    model_probabilities = model.predict_proba(X_test)[:, 1]

    for i, model_prob in enumerate(model_probabilities):
        row = test_df.iloc[i]
        implied_prob = convert_odds_to_probability(row['fanduel_points_over_odds'])
        
        # Identify value bet
        if model_prob > implied_prob + value_threshold:
            bets_placed += 1
            total_wagered += stake
            
            # Check outcome
            actual_outcome = y_test.iloc[i]
            if actual_outcome == 1: # Bet on Over won
                wins += 1
                profit = calculate_payout(row['fanduel_points_over_odds'], stake)
                total_profit += profit
            else: # Bet on Over lost
                total_profit -= stake

    # 4. Summarize results
    logging.info("--- BACKTESTING FINISHED ---")
    if bets_placed > 0:
        win_rate = (wins / bets_placed) * 100
        roi = (total_profit / total_wagered) * 100
        
        logging.info(f"Total Bets Placed: {bets_placed}")
        logging.info(f"Wins: {wins}")
        logging.info(f"Win Rate: {win_rate:.2f}%")
        logging.info(f"Total Wagered: {total_wagered:.2f} units")
        logging.info(f"Total Profit: {total_profit:.2f} units")
        logging.info(f"Return on Investment (ROI): {roi:.2f}%")
    else:
        logging.info("No value bets were identified.")


if __name__ == '__main__':
    run_backtest() 