import argparse
import sys
from pathlib import Path

# Add project root to the Python path
sys.path.append(str(Path(__file__).resolve().parent))

from src.prediction.predict import load_model, fetch_prediction_data, engineer_features, make_prediction

def main():
    """Main function to run the prediction CLI."""
    parser = argparse.ArgumentParser(description="Make predictions for player performance in a given game.")
    parser.add_argument("game_id", type=str, help="The ID of the game to predict.")
    parser.add_argument("player_id", type=str, help="The ID of the player to predict for.")
    parser.add_argument("--model_path", type=str, default="data/models/advanced_model.joblib", help="Path to the trained model file.")
    
    args = parser.parse_args()

    print(f"--- Running Prediction for game: {args.game_id}, player: {args.player_id} ---")

    # 1. Load Model
    model = load_model(args.model_path)
    if model is None:
        print("❌ Prediction failed: Could not load the model.")
        return

    # 2. Fetch Data
    raw_data = fetch_prediction_data(args.game_id, args.player_id)
    if raw_data is None:
        print("❌ Prediction failed: Could not fetch the required data.")
        return

    # 3. Engineer Features
    features = engineer_features(raw_data)
    if features.empty:
        print("❌ Prediction failed: Could not engineer features from the data.")
        return
        
    # 4. Make Prediction
    result = make_prediction(model, features)

    if result:
        prediction, proba = result
        print("\n--- Prediction Result ---")
        print(f"Outcome: {'OVER' if prediction[0] == 1 else 'UNDER'} the points average")
        print(f"Confidence (probability of OVER): {proba[0][1]:.2%}")
        print(f"Confidence (probability of UNDER): {proba[0][0]:.2%}")
        print("-------------------------\n")
    else:
        print("❌ Prediction failed during the final step.")

if __name__ == "__main__":
    main() 