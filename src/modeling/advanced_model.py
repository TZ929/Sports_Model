import pandas as pd
import logging
from pathlib import Path
import lightgbm as lgb
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score, confusion_matrix
import seaborn as sns
import matplotlib.pyplot as plt

# Setup logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "modeling.log"),
        logging.StreamHandler()
    ]
)

def load_modeling_data():
    """Load the training and testing datasets."""
    logging.info("Loading modeling data...")
    processed_dir = Path("data/processed")
    try:
        train_df = pd.read_csv(processed_dir / "modeling_train.csv")
        test_df = pd.read_csv(processed_dir / "modeling_test.csv")
        logging.info("Modeling data loaded successfully.")
        return train_df, test_df
    except FileNotFoundError as e:
        logging.error(f"Error loading modeling data: {e}. Please run the data preparation script first.")
        return None, None

def train_advanced_model(train_df):
    """Trains an advanced LightGBM model with optimized hyperparameters."""
    if train_df is None:
        logging.error("Training data is None. Aborting training.")
        return None

    logging.info("Training advanced model (LightGBM) with tuned parameters...")
    
    X_train = train_df.drop(columns=['points_over_avg_5g'])
    y_train = train_df['points_over_avg_5g']
    
    # Best parameters from Optuna tuning
    best_params = {
        'n_estimators': 992,
        'learning_rate': 0.00860483369995987,
        'num_leaves': 277,
        'max_depth': 3,
        'min_child_samples': 5,
        'subsample': 0.7799991055981887,
        'colsample_bytree': 0.8467993189633719,
        'reg_alpha': 0.0006342386013819185,
        'reg_lambda': 1.4588353193105266e-05,
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'boosting_type': 'gbdt',
        'random_state': 42
    }
    
    model = lgb.LGBMClassifier(**best_params)
    model.fit(X_train, y_train)
    
    logging.info("Tuned advanced model trained successfully.")
    return model

def evaluate_model(model, test_df):
    """Evaluates the model on the test set and analyzes feature importance."""
    if model is None or test_df is None:
        logging.error("Model or test data is None. Aborting evaluation.")
        return

    logging.info("Evaluating model performance...")
    
    X_test = test_df.drop(columns=['points_over_avg_5g'])
    y_test = test_df['points_over_avg_5g']
    
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    
    # Calculate metrics
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_pred_proba)
    
    logging.info("--- Advanced Model Performance Metrics ---")
    logging.info(f"Accuracy: {accuracy:.4f}")
    logging.info(f"Precision: {precision:.4f}")
    logging.info(f"Recall: {recall:.4f}")
    logging.info(f"ROC AUC: {roc_auc:.4f}")
    
    # Confusion Matrix
    cm = confusion_matrix(y_test, y_pred)
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Greens', xticklabels=['Under', 'Over'], yticklabels=['Under', 'Over'])
    plt.title('Advanced Model Confusion Matrix')
    plt.xlabel('Predicted')
    plt.ylabel('Actual')
    
    analysis_dir = Path("analysis_results")
    analysis_dir.mkdir(exist_ok=True)
    plt.savefig(analysis_dir / "advanced_model_confusion_matrix.png")
    plt.close()
    logging.info(f"Confusion matrix saved to '{analysis_dir.resolve()}'")

    # Feature Importance
    logging.info("--- Feature Importance Analysis ---")
    feature_importances = pd.DataFrame({'feature': X_test.columns, 'importance': model.feature_importances_})
    feature_importances = feature_importances.sort_values(by='importance', ascending=False)
    
    logging.info(f"\n{feature_importances.to_string()}")
    
    plt.figure(figsize=(10, 8))
    sns.barplot(x='importance', y='feature', data=feature_importances)
    plt.title('Feature Importance')
    plt.tight_layout()
    plt.savefig(analysis_dir / "advanced_model_feature_importance.png")
    plt.close()
    logging.info(f"Feature importance plot saved to '{analysis_dir.resolve()}'")


if __name__ == '__main__':
    train_data, test_data = load_modeling_data()
    advanced_model = train_advanced_model(train_data)
    evaluate_model(advanced_model, test_data)
    logging.info("Advanced modeling process finished.") 