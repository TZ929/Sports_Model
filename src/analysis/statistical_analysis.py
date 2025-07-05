import pandas as pd
import logging
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

# Setup logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "statistical_analysis.log"),
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

def analyze_features(df):
    """Perform statistical analysis on the feature dataset."""
    if df is None:
        logging.error("DataFrame is None. Aborting analysis.")
        return

    logging.info("Starting statistical analysis...")

    # Define features to analyze
    player_rolling_features = [col for col in df.columns if 'roll_avg' in col and 'player' in col]
    team_rolling_features = [col for col in df.columns if 'roll_avg' in col and 'team' in col]
    game_context_features = ['home_rest_days', 'away_rest_days']
    key_features = player_rolling_features + team_rolling_features + game_context_features

    # 1. Descriptive Statistics
    logging.info("--- Descriptive Statistics for Key Features ---")
    desc_stats = df[key_features].describe()
    logging.info(f"\n{desc_stats.to_string()}")

    # Create directory for analysis output
    analysis_dir = Path("analysis_results")
    analysis_dir.mkdir(exist_ok=True)

    # 2. Distribution Plots (Histograms)
    logging.info("--- Generating Distribution Plots ---")
    for feature in key_features:
        plt.figure(figsize=(10, 6))
        sns.histplot(df[feature].dropna(), kde=True)
        plt.title(f'Distribution of {feature}')
        plt.xlabel(feature)
        plt.ylabel('Frequency')
        plt.grid(True)
        plt.savefig(analysis_dir / f"{feature}_distribution.png")
        plt.close()
    logging.info(f"Distribution plots saved to '{analysis_dir.resolve()}'")

    # 3. Correlation Matrix
    logging.info("--- Generating Correlation Matrix ---")
    correlation_matrix = df[key_features].corr()
    
    plt.figure(figsize=(20, 18))
    sns.heatmap(correlation_matrix, annot=False, cmap='coolwarm')
    plt.title('Correlation Matrix of Engineered Features')
    plt.savefig(analysis_dir / "feature_correlation_matrix.png")
    plt.close()
    logging.info(f"Correlation matrix heatmap saved to '{analysis_dir.resolve()}'")
    
    # Log highly correlated features
    highly_correlated = correlation_matrix[correlation_matrix.abs() > 0.8]
    highly_correlated = highly_correlated[highly_correlated < 1.0].stack().reset_index()
    highly_correlated.columns = ['Feature 1', 'Feature 2', 'Correlation']
    # Remove duplicate pairs
    highly_correlated['sorted_features'] = highly_correlated.apply(lambda row: tuple(sorted((row['Feature 1'], row['Feature 2']))), axis=1)
    highly_correlated = highly_correlated.drop_duplicates(subset='sorted_features').drop(columns='sorted_features')
    
    if not highly_correlated.empty:
        logging.info("--- Highly Correlated Feature Pairs (>.8) ---")
        logging.info(f"\n{highly_correlated.to_string()}")

    logging.info("Statistical analysis complete.")


if __name__ == '__main__':
    final_df = load_final_dataset()
    analyze_features(final_df) 