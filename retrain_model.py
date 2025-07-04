import logging
import sys
from pathlib import Path

# Add project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[0]))

from src.modeling.prepare_model_data import prepare_modeling_data, load_final_dataset, save_modeling_data
from src.modeling.advanced_model import train_advanced_model, evaluate_model

# Setup logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "retraining.log"),
        logging.StreamHandler()
    ]
)

def retrain():
    """
    Runs the full model retraining pipeline.
    """
    logging.info("--- STARTING FULL MODEL RETRAINING ---")

    # 1. Prepare data
    final_df = load_final_dataset()
    train_data, test_data = prepare_modeling_data(final_df)
    save_modeling_data(train_data, test_data)
    logging.info("Modeling data has been prepared and saved.")

    # 2. Train advanced model
    if train_data is not None and test_data is not None:
        advanced_model = train_advanced_model(train_data)
        if advanced_model:
            evaluate_model(advanced_model, test_data)
    
    logging.info("--- MODEL RETRAINING FINISHED ---")

if __name__ == '__main__':
    retrain() 