import pandas as pd
import logging
from pathlib import Path
import optuna
import lightgbm as lgb
from sklearn.model_selection import cross_val_score

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
    """Load the training dataset."""
    logging.info("Loading modeling data for tuning...")
    processed_dir = Path("data/processed")
    try:
        train_df = pd.read_csv(processed_dir / "modeling_train.csv")
        logging.info("Training data loaded successfully.")
        return train_df
    except FileNotFoundError as e:
        logging.error(f"Error loading training data: {e}. Please run the data preparation script first.")
        return None

def objective(trial, X, y):
    """The objective function for Optuna to optimize."""
    
    param = {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'boosting_type': 'gbdt',
        'n_estimators': trial.suggest_int('n_estimators', 100, 1000),
        'learning_rate': trial.suggest_float('learning_rate', 1e-3, 0.1, log=True),
        'num_leaves': trial.suggest_int('num_leaves', 20, 300),
        'max_depth': trial.suggest_int('max_depth', 3, 12),
        'min_child_samples': trial.suggest_int('min_child_samples', 5, 100),
        'subsample': trial.suggest_float('subsample', 0.6, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
        'reg_alpha': trial.suggest_float('reg_alpha', 1e-8, 10.0, log=True),
        'reg_lambda': trial.suggest_float('reg_lambda', 1e-8, 10.0, log=True),
    }

    model = lgb.LGBMClassifier(**param, random_state=42)
    
    # Using cross-validation to get a more robust estimate of performance
    score = cross_val_score(model, X, y, n_jobs=-1, cv=3, scoring='roc_auc').mean()
    
    return score


if __name__ == '__main__':
    train_data = load_modeling_data()
    
    if train_data is not None:
        X_train = train_data.drop(columns=['points_over_avg_5g'])
        y_train = train_data['points_over_avg_5g']
        
        study = optuna.create_study(direction='maximize')
        study.optimize(lambda trial: objective(trial, X_train, y_train), n_trials=50) # Run 50 trials
        
        logging.info("--- Hyperparameter Tuning Complete ---")
        logging.info(f"Number of finished trials: {len(study.trials)}")
        logging.info("Best trial:")
        best_trial = study.best_trial
        
        logging.info(f"  Value (ROC AUC): {best_trial.value}")
        logging.info("  Params: ")
        for key, value in best_trial.params.items():
            logging.info(f"    {key}: {value}") 