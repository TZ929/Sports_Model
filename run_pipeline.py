import subprocess
import sys
import logging
from pathlib import Path

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

def run_script(script_path: str):
    """
    Runs a Python script as a subprocess and logs its output.

    Args:
        script_path: The relative path to the Python script to run.
    """
    script_full_path = Path(__file__).parent / script_path
    if not script_full_path.exists():
        logging.error(f"Script not found at: {script_full_path}")
        return False
        
    logging.info(f"--- Running script: {script_path} ---")
    
    try:
        # Using sys.executable to ensure the same Python interpreter is used
        result = subprocess.run(
            [sys.executable, str(script_full_path)],
            capture_output=True,
            text=True,
            check=True  # Raises CalledProcessError if the script returns a non-zero exit code
        )
        
        # Log stdout and stderr from the script
        if result.stdout:
            logging.info(f"Output from {script_path}:\n{result.stdout}")
        if result.stderr:
            logging.warning(f"Errors from {script_path}:\n{result.stderr}")
            
        logging.info(f"--- Finished script: {script_path} ---")
        return True
        
    except subprocess.CalledProcessError as e:
        logging.error(f"An error occurred while running {script_path}.")
        logging.error(f"Return code: {e.returncode}")
        logging.error(f"Output:\n{e.stdout}")
        logging.error(f"Errors:\n{e.stderr}")
        return False
    except FileNotFoundError:
        logging.error(f"Error: The script at {script_full_path} was not found.")
        return False


def main():
    """
    Main function to orchestrate the daily betting pipeline.
    
    This pipeline runs the following steps in order:
    1. Scrape daily odds to get the latest prop bets.
    2. Run the prediction model against the scraped odds to find value.
    """
    logging.info("==============================================")
    logging.info("  Starting Daily Sports Prediction Pipeline   ")
    logging.info("==============================================")
    
    # --- Step 1: Scrape Daily Odds ---
    # This script currently uses placeholder data. In a real-world scenario,
    # it would be scraping live sportsbook data.
    success = run_script("src/data_collection/scrape_daily_odds.py")
    if not success:
        logging.critical("Failed to scrape daily odds. Halting pipeline.")
        return

    # --- Step 2: Run Prediction and Find Value ---
    # This script takes the scraped odds, runs them through the model,
    # and logs any identified value bets to logs/predictions.log.
    success = run_script("run_prediction.py")
    if not success:
        logging.critical("Failed to run the prediction pipeline.")
        return

    logging.info("==============================================")
    logging.info("   Daily Sports Prediction Pipeline Finished  ")
    logging.info("==============================================")
    logging.info("Check 'logs/predictions.log' for value bet opportunities.")


if __name__ == "__main__":
    main() 