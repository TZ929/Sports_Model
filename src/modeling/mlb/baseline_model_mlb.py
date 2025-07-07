import pandas as pd
import numpy as np
import logging
from pathlib import Path
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score, confusion_matrix, classification_report
import seaborn as sns
import matplotlib.pyplot as plt
import joblib
import json

# Setup logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "mlb_baseline_modeling.log"),
        logging.StreamHandler()
    ]
)

def load_modeling_data(target='home_team_wins'):
    """Load the training, validation, and testing datasets for a specific target."""
    logging.info(f"Loading modeling data for target: {target}")
    processed_dir = Path("data/processed/mlb")
    
    try:
        train_df = pd.read_csv(processed_dir / f"modeling_train_{target}.csv")
        val_df = pd.read_csv(processed_dir / f"modeling_val_{target}.csv")
        test_df = pd.read_csv(processed_dir / f"modeling_test_{target}.csv")
        
        # Load feature info
        with open(processed_dir / f"modeling_info_{target}.json", 'r') as f:
            feature_info = json.load(f)
        
        logging.info(f"Modeling data loaded successfully for {target}")
        logging.info(f"  - Training: {len(train_df)} samples")
        logging.info(f"  - Validation: {len(val_df)} samples") 
        logging.info(f"  - Test: {len(test_df)} samples")
        logging.info(f"  - Features: {feature_info['n_features']}")
        
        return train_df, val_df, test_df, feature_info
    except FileNotFoundError as e:
        logging.error(f"Error loading modeling data: {e}. Please run data preparation first.")
        return None, None, None, None

def train_logistic_regression(train_df, val_df, target, features):
    """Train a Logistic Regression baseline model."""
    logging.info("Training Logistic Regression baseline model...")
    
    X_train = train_df[features]
    y_train = train_df[target]
    X_val = val_df[features]
    y_val = val_df[target]
    
    # Train model with different regularization strengths
    best_score = 0
    best_model = None
    best_c = None
    
    c_values = [0.01, 0.1, 1.0, 10.0, 100.0]
    
    for c in c_values:
        model = LogisticRegression(
            C=c, 
            random_state=42, 
            max_iter=1000,
            solver='liblinear'
        )
        model.fit(X_train, y_train)
        
        # Evaluate on validation set
        y_val_pred_proba = model.predict_proba(X_val)[:, 1]
        val_auc = roc_auc_score(y_val, y_val_pred_proba)
        
        logging.info(f"  C={c}: Validation AUC = {val_auc:.4f}")
        
        if val_auc > best_score:
            best_score = val_auc
            best_model = model
            best_c = c
    
    logging.info(f"Best Logistic Regression: C={best_c}, Validation AUC = {best_score:.4f}")
    return best_model

def train_random_forest(train_df, val_df, target, features):
    """Train a Random Forest baseline model."""
    logging.info("Training Random Forest baseline model...")
    
    X_train = train_df[features]
    y_train = train_df[target]
    X_val = val_df[features]
    y_val = val_df[target]
    
    # Train model with different parameters
    best_score = 0
    best_model = None
    best_params = None
    
    param_combinations = [
        {'n_estimators': 100, 'max_depth': 10, 'min_samples_split': 5},
        {'n_estimators': 200, 'max_depth': 15, 'min_samples_split': 10},
        {'n_estimators': 300, 'max_depth': 20, 'min_samples_split': 5},
        {'n_estimators': 100, 'max_depth': None, 'min_samples_split': 2},
    ]
    
    for params in param_combinations:
        model = RandomForestClassifier(
            random_state=42,
            **params
        )
        model.fit(X_train, y_train)
        
        # Evaluate on validation set
        y_val_pred_proba = model.predict_proba(X_val)[:, 1]
        val_auc = roc_auc_score(y_val, y_val_pred_proba)
        
        logging.info(f"  RF {params}: Validation AUC = {val_auc:.4f}")
        
        if val_auc > best_score:
            best_score = val_auc
            best_model = model
            best_params = params
    
    logging.info(f"Best Random Forest: {best_params}, Validation AUC = {best_score:.4f}")
    return best_model

def evaluate_model(model, test_df, target, features, model_name="Model"):
    """Evaluate the model on the test set."""
    logging.info(f"Evaluating {model_name} performance...")
    
    X_test = test_df[features]
    y_test = test_df[target]
    
    # Make predictions
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    
    # Calculate metrics
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred, average='binary')
    recall = recall_score(y_test, y_pred, average='binary')
    roc_auc = roc_auc_score(y_test, y_pred_proba)
    
    logging.info(f"--- {model_name} Performance Metrics ---")
    logging.info(f"Accuracy: {accuracy:.4f}")
    logging.info(f"Precision: {precision:.4f}")
    logging.info(f"Recall: {recall:.4f}")
    logging.info(f"ROC AUC: {roc_auc:.4f}")
    
    # Classification report
    logging.info(f"\nClassification Report:\n{classification_report(y_test, y_pred)}")
    
    # Confusion Matrix
    cm = confusion_matrix(y_test, y_pred)
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', 
                xticklabels=['Negative', 'Positive'], 
                yticklabels=['Negative', 'Positive'])
    plt.title(f'{model_name} Confusion Matrix - {target}')
    plt.xlabel('Predicted')
    plt.ylabel('Actual')
    
    # Save the plot
    analysis_dir = Path("analysis_results")
    analysis_dir.mkdir(exist_ok=True)
    plt.savefig(analysis_dir / f"mlb_{model_name.lower().replace(' ', '_')}_confusion_matrix_{target}.png")
    plt.close()
    logging.info(f"Confusion matrix saved to analysis_results/")
    
    return {
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'roc_auc': roc_auc,
        'model_name': model_name,
        'target': target
    }

def analyze_feature_importance(model, features, target, model_name="Model"):
    """Analyze and visualize feature importance."""
    if hasattr(model, 'feature_importances_'):
        logging.info(f"--- {model_name} Feature Importance Analysis ---")
        
        feature_importances = pd.DataFrame({
            'feature': features,
            'importance': model.feature_importances_
        }).sort_values(by='importance', ascending=False)
        
        logging.info(f"\nTop 10 Features:\n{feature_importances.head(10).to_string()}")
        
        # Plot feature importance
        plt.figure(figsize=(12, 8))
        top_features = feature_importances.head(15)
        sns.barplot(x='importance', y='feature', data=top_features)
        plt.title(f'{model_name} Feature Importance - {target}')
        plt.xlabel('Importance')
        plt.tight_layout()
        
        analysis_dir = Path("analysis_results")
        analysis_dir.mkdir(exist_ok=True)
        plt.savefig(analysis_dir / f"mlb_{model_name.lower().replace(' ', '_')}_feature_importance_{target}.png")
        plt.close()
        logging.info(f"Feature importance plot saved to analysis_results/")
        
        return feature_importances
    else:
        logging.info(f"{model_name} does not have feature importance attribute")
        return None

def save_model(model, model_name, target):
    """Save the trained model."""
    models_dir = Path("data/models/mlb")
    models_dir.mkdir(parents=True, exist_ok=True)
    
    model_filename = f"{model_name.lower().replace(' ', '_')}_{target}.joblib"
    model_path = models_dir / model_filename
    
    joblib.dump(model, model_path)
    logging.info(f"Model saved to {model_path}")

def train_and_evaluate_baseline_models(target='home_team_wins'):
    """Train and evaluate all baseline models for a specific target."""
    logging.info(f"\n{'='*60}")
    logging.info(f"TRAINING BASELINE MODELS FOR TARGET: {target.upper()}")
    logging.info(f"{'='*60}")
    
    # Load data
    train_df, val_df, test_df, feature_info = load_modeling_data(target)
    
    if train_df is None:
        logging.error("Failed to load modeling data")
        return None
    
    features = feature_info['features']
    
    # Train models
    logging.info("\n--- TRAINING PHASE ---")
    lr_model = train_logistic_regression(train_df, val_df, target, features)
    rf_model = train_random_forest(train_df, val_df, target, features)
    
    # Evaluate models
    logging.info("\n--- EVALUATION PHASE ---")
    lr_results = evaluate_model(lr_model, test_df, target, features, "Logistic Regression")
    rf_results = evaluate_model(rf_model, test_df, target, features, "Random Forest")
    
    # Feature importance analysis
    logging.info("\n--- FEATURE IMPORTANCE ANALYSIS ---")
    rf_importance = analyze_feature_importance(rf_model, features, target, "Random Forest")
    
    # Save models
    logging.info("\n--- SAVING MODELS ---")
    save_model(lr_model, "logistic_regression", target)
    save_model(rf_model, "random_forest", target)
    
    # Compare models
    logging.info("\n--- MODEL COMPARISON ---")
    comparison = pd.DataFrame([lr_results, rf_results])
    logging.info(f"\nModel Comparison:\n{comparison.to_string()}")
    
    # Determine best model
    best_model_idx = comparison['roc_auc'].idxmax()
    best_model_name = comparison.loc[best_model_idx, 'model_name']
    best_auc = comparison.loc[best_model_idx, 'roc_auc']
    
    logging.info(f"\nBest Model: {best_model_name} (AUC: {best_auc:.4f})")
    
    return {
        'target': target,
        'logistic_regression': lr_results,
        'random_forest': rf_results,
        'best_model': best_model_name,
        'best_auc': best_auc,
        'feature_importance': rf_importance
    }

def main():
    """Main function to train baseline models for all targets."""
    targets = ['home_team_wins', 'high_scoring_game', 'low_scoring_game', 'close_game']
    
    all_results = {}
    
    for target in targets:
        try:
            results = train_and_evaluate_baseline_models(target)
            if results:
                all_results[target] = results
        except Exception as e:
            logging.error(f"Error training models for {target}: {e}")
            continue
    
    # Summary report
    logging.info(f"\n{'='*80}")
    logging.info("BASELINE MODELS TRAINING SUMMARY")
    logging.info(f"{'='*80}")
    
    for target, results in all_results.items():
        logging.info(f"\n{target.upper()}:")
        logging.info(f"  Best Model: {results['best_model']}")
        logging.info(f"  Best AUC: {results['best_auc']:.4f}")
        logging.info(f"  LR AUC: {results['logistic_regression']['roc_auc']:.4f}")
        logging.info(f"  RF AUC: {results['random_forest']['roc_auc']:.4f}")
    
    logging.info(f"\n{'='*80}")
    logging.info("MLB BASELINE MODELING COMPLETE")
    logging.info(f"{'='*80}")

if __name__ == '__main__':
    main() 