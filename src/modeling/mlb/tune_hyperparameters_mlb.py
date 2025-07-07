import pandas as pd
import numpy as np
import logging
from pathlib import Path
import optuna
import lightgbm as lgb
import xgboost as xgb
from sklearn.model_selection import cross_val_score
from sklearn.metrics import roc_auc_score
import json

# Setup logging
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "mlb_hyperparameter_tuning.log"),
        logging.StreamHandler()
    ]
)

def load_modeling_data(target='home_team_wins'):
    """Load the training dataset for hyperparameter tuning."""
    logging.info(f"Loading modeling data for tuning: {target}")
    processed_dir = Path("data/processed/mlb")
    
    try:
        train_df = pd.read_csv(processed_dir / f"modeling_train_{target}.csv")
        val_df = pd.read_csv(processed_dir / f"modeling_val_{target}.csv")
        
        # Load feature info
        with open(processed_dir / f"modeling_info_{target}.json", 'r') as f:
            feature_info = json.load(f)
        
        logging.info(f"Training data loaded successfully for {target}")
        logging.info(f"  - Training: {len(train_df)} samples")
        logging.info(f"  - Validation: {len(val_df)} samples")
        logging.info(f"  - Features: {feature_info['n_features']}")
        
        return train_df, val_df, feature_info
    except FileNotFoundError as e:
        logging.error(f"Error loading training data: {e}. Please run data preparation first.")
        return None, None, None

def lightgbm_objective(trial, X_train, y_train, cv_folds=3):
    """The objective function for LightGBM hyperparameter optimization."""
    
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
    
    # Using cross-validation to get a more robust estimate of performance
    cv_scores = cross_val_score(model, X_train, y_train, cv=cv_folds, scoring='roc_auc', n_jobs=-1)
    
    return cv_scores.mean()

def xgboost_objective(trial, X_train, y_train, cv_folds=3):
    """The objective function for XGBoost hyperparameter optimization."""
    
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
    
    # Using cross-validation to get a more robust estimate of performance
    cv_scores = cross_val_score(model, X_train, y_train, cv=cv_folds, scoring='roc_auc', n_jobs=-1)
    
    return cv_scores.mean()

def tune_model(model_type, train_df, target, features, n_trials=50, cv_folds=3):
    """Tune hyperparameters for a specific model type."""
    logging.info(f"Tuning {model_type} hyperparameters for {target}")
    logging.info(f"Using {n_trials} trials with {cv_folds}-fold cross-validation")
    
    X_train = train_df[features]
    y_train = train_df[target]
    
    # Create study
    study_name = f"{model_type.lower()}_{target}"
    study = optuna.create_study(direction='maximize', study_name=study_name)
    
    # Define objective function based on model type
    if model_type.lower() == 'lightgbm':
        objective_func = lambda trial: lightgbm_objective(trial, X_train, y_train, cv_folds)
    elif model_type.lower() == 'xgboost':
        objective_func = lambda trial: xgboost_objective(trial, X_train, y_train, cv_folds)
    else:
        raise ValueError(f"Unsupported model type: {model_type}")
    
    # Optimize
    study.optimize(objective_func, n_trials=n_trials)
    
    logging.info(f"--- {model_type} Hyperparameter Tuning Complete ---")
    logging.info(f"Number of finished trials: {len(study.trials)}")
    logging.info(f"Best trial AUC: {study.best_value:.4f}")
    logging.info(f"Best parameters:")
    for key, value in study.best_params.items():
        logging.info(f"  {key}: {value}")
    
    return study.best_params, study.best_value, study

def save_tuning_results(model_type, target, best_params, best_score, study):
    """Save hyperparameter tuning results."""
    results_dir = Path("data/models/mlb/tuning_results")
    results_dir.mkdir(parents=True, exist_ok=True)
    
    # Save best parameters
    params_file = results_dir / f"{model_type.lower()}_{target}_best_params.json"
    params_data = {
        'model_type': model_type,
        'target': target,
        'best_params': best_params,
        'best_cv_score': best_score,
        'n_trials': len(study.trials)
    }
    
    with open(params_file, 'w') as f:
        json.dump(params_data, f, indent=2)
    
    logging.info(f"Tuning results saved to {params_file}")
    
    # Save study details
    study_file = results_dir / f"{model_type.lower()}_{target}_study.json"
    study_data = {
        'trials': [
            {
                'number': trial.number,
                'value': trial.value,
                'params': trial.params,
                'state': str(trial.state)
            }
            for trial in study.trials
        ]
    }
    
    with open(study_file, 'w') as f:
        json.dump(study_data, f, indent=2)
    
    logging.info(f"Study details saved to {study_file}")

def tune_all_models_for_target(target='home_team_wins', n_trials=50):
    """Tune hyperparameters for all models for a specific target."""
    logging.info(f"\n{'='*60}")
    logging.info(f"HYPERPARAMETER TUNING FOR TARGET: {target.upper()}")
    logging.info(f"{'='*60}")
    
    # Load data
    train_df, val_df, feature_info = load_modeling_data(target)
    
    if train_df is None:
        logging.error("Failed to load modeling data")
        return None
    
    features = feature_info['features']
    
    # Combine train and validation for cross-validation
    combined_df = pd.concat([train_df, val_df], ignore_index=True)
    
    results = {}
    
    # Tune LightGBM
    try:
        lgb_params, lgb_score, lgb_study = tune_model('LightGBM', combined_df, target, features, n_trials)
        save_tuning_results('LightGBM', target, lgb_params, lgb_score, lgb_study)
        results['lightgbm'] = {
            'best_params': lgb_params,
            'best_score': lgb_score
        }
    except Exception as e:
        logging.error(f"Error tuning LightGBM: {e}")
    
    # Tune XGBoost
    try:
        xgb_params, xgb_score, xgb_study = tune_model('XGBoost', combined_df, target, features, n_trials)
        save_tuning_results('XGBoost', target, xgb_params, xgb_score, xgb_study)
        results['xgboost'] = {
            'best_params': xgb_params,
            'best_score': xgb_score
        }
    except Exception as e:
        logging.error(f"Error tuning XGBoost: {e}")
    
    # Summary
    logging.info(f"\n--- TUNING SUMMARY FOR {target.upper()} ---")
    for model_name, model_results in results.items():
        logging.info(f"{model_name.upper()}:")
        logging.info(f"  Best CV AUC: {model_results['best_score']:.4f}")
        logging.info(f"  Best params: {model_results['best_params']}")
    
    return results

def main():
    """Main function to tune hyperparameters for all targets."""
    targets = ['home_team_wins', 'high_scoring_game', 'low_scoring_game', 'close_game']
    
    all_results = {}
    
    for target in targets:
        try:
            results = tune_all_models_for_target(target, n_trials=30)  # Reduced for faster execution
            if results:
                all_results[target] = results
        except Exception as e:
            logging.error(f"Error tuning models for {target}: {e}")
            continue
    
    # Overall summary
    logging.info(f"\n{'='*80}")
    logging.info("HYPERPARAMETER TUNING SUMMARY - ALL TARGETS")
    logging.info(f"{'='*80}")
    
    for target, target_results in all_results.items():
        logging.info(f"\n{target.upper()}:")
        for model_name, model_results in target_results.items():
            logging.info(f"  {model_name.upper()}: {model_results['best_score']:.4f} AUC")
    
    logging.info(f"\n{'='*80}")
    logging.info("MLB HYPERPARAMETER TUNING COMPLETE")
    logging.info(f"{'='*80}")

if __name__ == '__main__':
    main() 