import logging
import argparse
from pathlib import Path

from src.modeling.prepare_model_data import prepare_modeling_data, load_final_dataset, save_modeling_data
from src.modeling.advanced_model import train_advanced_model, evaluate_model

# Add project root to the Python path - this is no longer needed if run as a module
# import sys
# sys.path.append(str(Path(__file__).resolve().parents[0]))


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

def retrain(args):
    """
    Runs the full model retraining pipeline based on provided arguments.
    """
    logging.info("--- STARTING FULL MODEL RETRAINING ---")

    # Define paths
    output_dir = Path(args.output_dir)
    season_slug = args.season if args.season else "latest"
    
    data_dir = output_dir / "data" / "processed" / season_slug
    models_dir = output_dir / "data" / "models" / season_slug
    analysis_dir = output_dir / "analysis_results" / season_slug

    # Create directories if they don't exist
    data_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)
    analysis_dir.mkdir(parents=True, exist_ok=True)

    # Define filenames
    model_filename = f"advanced_model_{season_slug}.joblib"
    confusion_matrix_filename = f"advanced_model_confusion_matrix_{season_slug}.png"
    feature_importance_filename = f"advanced_model_feature_importance_{season_slug}.png"

    # 1. Prepare data
    final_df = load_final_dataset(Path(args.input_file))
    if final_df is None:
        logging.error("Failed to load dataset. Aborting.")
        return

    train_data, test_data = prepare_modeling_data(final_df)
    save_modeling_data(train_data, test_data, output_dir=data_dir) # Use default filenames for simplicity
    logging.info(f"Modeling data has been prepared and saved to {data_dir}")

    # 2. Train advanced model
    if train_data is not None and test_data is not None:
        advanced_model = train_advanced_model(
            train_data, 
            models_dir=models_dir, 
            model_filename=model_filename
        )
        if advanced_model:
            evaluate_model(
                advanced_model, 
                test_data, 
                analysis_dir=analysis_dir,
                confusion_matrix_filename=confusion_matrix_filename,
                feature_importance_filename=feature_importance_filename
            )
    
    logging.info("--- MODEL RETRAINING FINISHED ---")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Retrain the sports prediction model.")
    parser.add_argument(
        "--input-file", 
        type=str, 
        default="data/processed/featured_data.csv",
        help="Path to the input dataset (featured_data.csv)."
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=".",
        help="The root directory where output data, models, and analysis results will be saved."
    )
    parser.add_argument(
        "--season",
        type=str,
        default=None,
        help="Optional season identifier (e.g., '2023-2024') to version the outputs."
    )
    
    args = parser.parse_args()
    retrain(args) 