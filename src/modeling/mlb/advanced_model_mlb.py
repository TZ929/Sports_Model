import pandas as pd
import numpy as np
import logging
from pathlib import Path
import lightgbm as lgb
import xgboost as xgb
import optuna
import joblib
import json
from sklearn.metrics import accuracy_score, precision_score, recall_score, roc_auc_score, confusion_matrix, classification_report
from sklearn.model_selection import cross_val_score
import seaborn as sns
import matplotlib.pyplot as plt

# Setup logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "mlb_advanced_modeling.log"),
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

def lightgbm_objective(trial, X_train, y_train, X_val, y_val):
    """Objective function for LightGBM hyperparameter optimization."""
    
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'boosting_type': 'gbdt',
        'random_state': 42,
        'n_estimators': trial.suggest_int('n_estimators', 100, 1000),
        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
        'num_leaves': trial.suggest_int('num_leaves', 20, 300),
        'max_depth': trial.suggest_int('max_depth', 3, 15),
        'min_child_samples': trial.suggest_int('min_child_samples', 5, 100),
        'subsample': trial.suggest_float('subsample', 0.6, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
        'reg_alpha': trial.suggest_float('reg_alpha', 1e-8, 10.0, log=True),
        'reg_lambda': trial.suggest_float('reg_lambda', 1e-8, 10.0, log=True),
    }
    
    model = lgb.LGBMClassifier(**params)
    model.fit(X_train, y_train)
    
    # Evaluate on validation set
    y_val_pred_proba = model.predict_proba(X_val)[:, 1]
    val_auc = roc_auc_score(y_val, y_val_pred_proba)
    
    return val_auc

def xgboost_objective(trial, X_train, y_train, X_val, y_val):
    """Objective function for XGBoost hyperparameter optimization."""
    
    params = {
        'objective': 'binary:logistic',
        'eval_metric': 'auc',
        'random_state': 42,
        'n_estimators': trial.suggest_int('n_estimators', 100, 1000),
        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
        'max_depth': trial.suggest_int('max_depth', 3, 15),
        'min_child_weight': trial.suggest_int('min_child_weight', 1, 10),
        'subsample': trial.suggest_float('subsample', 0.6, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
        'reg_alpha': trial.suggest_float('reg_alpha', 1e-8, 10.0, log=True),
        'reg_lambda': trial.suggest_float('reg_lambda', 1e-8, 10.0, log=True),
    }
    
    model = xgb.XGBClassifier(**params)
    model.fit(X_train, y_train)
    
    # Evaluate on validation set
    y_val_pred_proba = model.predict_proba(X_val)[:, 1]
    val_auc = roc_auc_score(y_val, y_val_pred_proba)
    
    return val_auc

def tune_lightgbm(train_df, val_df, target, features, n_trials=50):
    """Tune LightGBM hyperparameters using Optuna."""
    logging.info(f"Tuning LightGBM hyperparameters with {n_trials} trials...")
    
    X_train = train_df[features]
    y_train = train_df[target]
    X_val = val_df[features]
    y_val = val_df[target]
    
    study = optuna.create_study(direction='maximize', study_name=f'lightgbm_{target}')
    study.optimize(lambda trial: lightgbm_objective(trial, X_train, y_train, X_val, y_val), n_trials=n_trials)
    
    logging.info(f"LightGBM tuning completed. Best AUC: {study.best_value:.4f}")
    logging.info(f"Best parameters: {study.best_params}")
    
    # Train final model with best parameters
    best_params = study.best_params.copy()
    best_params.update({
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'boosting_type': 'gbdt',
        'random_state': 42
    })
    
    best_model = lgb.LGBMClassifier(**best_params)
    best_model.fit(X_train, y_train)
    
    return best_model, study.best_params, study.best_value

def tune_xgboost(train_df, val_df, target, features, n_trials=50):
    """Tune XGBoost hyperparameters using Optuna."""
    logging.info(f"Tuning XGBoost hyperparameters with {n_trials} trials...")
    
    X_train = train_df[features]
    y_train = train_df[target]
    X_val = val_df[features]
    y_val = val_df[target]
    
    study = optuna.create_study(direction='maximize', study_name=f'xgboost_{target}')
    study.optimize(lambda trial: xgboost_objective(trial, X_train, y_train, X_val, y_val), n_trials=n_trials)
    
    logging.info(f"XGBoost tuning completed. Best AUC: {study.best_value:.4f}")
    logging.info(f"Best parameters: {study.best_params}")
    
    # Train final model with best parameters
    best_params = study.best_params.copy()
    best_params.update({
        'objective': 'binary:logistic',
        'eval_metric': 'auc',
        'random_state': 42
    })
    
    best_model = xgb.XGBClassifier(**best_params)
    best_model.fit(X_train, y_train)
    
    return best_model, study.best_params, study.best_value

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
    precision = precision_score(y_test, y_pred, average='binary', zero_division=0)
    recall = recall_score(y_test, y_pred, average='binary')
    roc_auc = roc_auc_score(y_test, y_pred_proba)
    
    logging.info(f"--- {model_name} Performance Metrics ---")
    logging.info(f"Accuracy: {accuracy:.4f}")
    logging.info(f"Precision: {precision:.4f}")
    logging.info(f"Recall: {recall:.4f}")
    logging.info(f"ROC AUC: {roc_auc:.4f}")
    
    # Classification report
    logging.info(f"\nClassification Report:\n{classification_report(y_test, y_pred, zero_division=0)}")
    
    # Confusion Matrix
    cm = confusion_matrix(y_test, y_pred)
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Greens', 
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
        
        logging.info(f"\nTop 15 Features:\n{feature_importances.head(15).to_string()}")
        
        # Plot feature importance
        plt.figure(figsize=(12, 10))
        top_features = feature_importances.head(20)
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

def save_model_and_params(model, model_name, target, best_params=None, best_score=None):
    """Save the trained model and its parameters."""
    models_dir = Path("data/models/mlb")
    models_dir.mkdir(parents=True, exist_ok=True)
    
    # Save model
    model_filename = f"{model_name.lower().replace(' ', '_')}_{target}.joblib"
    model_path = models_dir / model_filename
    joblib.dump(model, model_path)
    logging.info(f"Model saved to {model_path}")
    
    # Save parameters and score
    if best_params is not None:
        params_filename = f"{model_name.lower().replace(' ', '_')}_{target}_params.json"
        params_path = models_dir / params_filename
        
        params_data = {
            'best_params': best_params,
            'best_validation_score': best_score,
            'model_type': model_name,
            'target': target
        }
        
        with open(params_path, 'w') as f:
            json.dump(params_data, f, indent=2)
        logging.info(f"Parameters saved to {params_path}")

def train_and_evaluate_advanced_models(target='home_team_wins', n_trials=30):
    """Train and evaluate all advanced models for a specific target."""
    logging.info(f"\n{'='*70}")
    logging.info(f"TRAINING ADVANCED MODELS FOR TARGET: {target.upper()}")
    logging.info(f"{'='*70}")
    
    # Load data
    train_df, val_df, test_df, feature_info = load_modeling_data(target)
    
    if train_df is None:
        logging.error("Failed to load modeling data")
        return None
    
    features = feature_info['features']
    
    # Train and tune models
    logging.info("\n--- HYPERPARAMETER TUNING PHASE ---")
    
    # Tune LightGBM
    lgb_model, lgb_params, lgb_score = tune_lightgbm(train_df, val_df, target, features, n_trials)
    
    # Tune XGBoost
    xgb_model, xgb_params, xgb_score = tune_xgboost(train_df, val_df, target, features, n_trials)
    
    # Evaluate models
    logging.info("\n--- EVALUATION PHASE ---")
    lgb_results = evaluate_model(lgb_model, test_df, target, features, "LightGBM")
    xgb_results = evaluate_model(xgb_model, test_df, target, features, "XGBoost")
    
    # Feature importance analysis
    logging.info("\n--- FEATURE IMPORTANCE ANALYSIS ---")
    lgb_importance = analyze_feature_importance(lgb_model, features, target, "LightGBM")
    xgb_importance = analyze_feature_importance(xgb_model, features, target, "XGBoost")
    
    # Save models
    logging.info("\n--- SAVING MODELS ---")
    save_model_and_params(lgb_model, "LightGBM", target, lgb_params, lgb_score)
    save_model_and_params(xgb_model, "XGBoost", target, xgb_params, xgb_score)
    
    # Compare models
    logging.info("\n--- MODEL COMPARISON ---")
    comparison = pd.DataFrame([lgb_results, xgb_results])
    logging.info(f"\nAdvanced Model Comparison:\n{comparison.to_string()}")
    
    # Determine best model
    best_model_idx = comparison['roc_auc'].idxmax()
    best_model_name = comparison.loc[best_model_idx, 'model_name']
    best_auc = comparison.loc[best_model_idx, 'roc_auc']
    
    logging.info(f"\nBest Advanced Model: {best_model_name} (AUC: {best_auc:.4f})")
    
    return {
        'target': target,
        'lightgbm': lgb_results,
        'xgboost': xgb_results,
        'best_model': best_model_name,
        'best_auc': best_auc,
        'lgb_feature_importance': lgb_importance,
        'xgb_feature_importance': xgb_importance,
        'lgb_params': lgb_params,
        'xgb_params': xgb_params
    }

def main():
    """Main function to train advanced models for all targets."""
    targets = ['home_team_wins', 'high_scoring_game', 'low_scoring_game', 'close_game']
    
    all_results = {}
    
    for target in targets:
        try:
            results = train_and_evaluate_advanced_models(target, n_trials=30)
            if results:
                all_results[target] = results
        except Exception as e:
            logging.error(f"Error training advanced models for {target}: {e}")
            continue
    
    # Summary report
    logging.info(f"\n{'='*80}")
    logging.info("ADVANCED MODELS TRAINING SUMMARY")
    logging.info(f"{'='*80}")
    
    for target, results in all_results.items():
        logging.info(f"\n{target.upper()}:")
        logging.info(f"  Best Model: {results['best_model']}")
        logging.info(f"  Best AUC: {results['best_auc']:.4f}")
        logging.info(f"  LightGBM AUC: {results['lightgbm']['roc_auc']:.4f}")
        logging.info(f"  XGBoost AUC: {results['xgboost']['roc_auc']:.4f}")
    
    # Create comparison with baseline models
    logging.info(f"\n{'='*80}")
    logging.info("ADVANCED VS BASELINE COMPARISON")
    logging.info(f"{'='*80}")
    logging.info("(Note: Load baseline results separately for detailed comparison)")
    
    logging.info(f"\n{'='*80}")
    logging.info("MLB ADVANCED MODELING COMPLETE")
    logging.info(f"{'='*80}")

if __name__ == '__main__':
    main() 