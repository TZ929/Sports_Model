import sys
import os
import pandas as pd
import numpy as np
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.modeling.prepare_model_data import prepare_modeling_data

@pytest.fixture
def sample_featured_data():
    """Provides a realistic sample DataFrame similar to featured_data.csv."""
    data = {
        'team_id': ['T1', 'T2', 'T1', 'T2', 'T3', 'T4', 'T1', 'T2', 'T3', 'T4'],
        'home_team_id': ['T1', 'T1', 'T3', 'T3', 'T4', 'T1', 'T2', 'T3', 'T4', 'T2'],
        'away_team_id': ['T2', 'T2', 'T1', 'T1', 'T3', 'T4', 'T1', 'T2', 'T3', 'T1'],
        'points': [10, 20, 15, 25, 30, 12, 18, 22, 28, 35],
        'points_roll_avg_5g': [8, 22, 12, 28, 25, 10, 20, 25, 26, 30],
        'rebounds_roll_avg_5g': [5, 8, 6, 9, 7, 4, 7, 8, 9, 10],
        'assists_roll_avg_5g': [2, 6, 3, 7, 5, 3, 5, 6, 7, 8],
        'minutes_played': [30, 32, 28, 35, 33, 29, 31, 34, 36, 38],
        'home_rest_days': [2, 2, 3, 3, 1, 2, 3, 1, 2, 3],
        'away_rest_days': [3, 3, 2, 2, 1, 3, 2, 1, 3, 2],
        'home_team_points_against_roll_avg_5g': [100, 100, 105, 105, 98, 101, 102, 103, 104, 105],
        'away_team_points_against_roll_avg_5g': [102, 102, 110, 110, 95, 103, 104, 106, 107, 108],
        'fanduel_points_line': [10.5, 21.5, np.nan, 26.5, 28.5, 11.5, 19.5, 23.5, 27.5, 33.5],
        'fanduel_points_over_odds': [-110, -115, np.nan, -105, -110, -110, -115, -105, -110, -110],
        'fanduel_points_under_odds': [-110, -105, np.nan, -115, -110, -110, -105, -115, -110, -110],
    }
    return pd.DataFrame(data)

def test_prepare_data_handles_none():
    """Tests that the function returns None when the input is None."""
    train_df, test_df = prepare_modeling_data(None)
    assert train_df is None
    assert test_df is None

def test_prepare_modeling_data_full_process(sample_featured_data):
    """
    Tests the full data preparation process from feature engineering to splitting.
    """
    # Make a copy to avoid modifying the fixture
    df = sample_featured_data.copy()
    
    # Run the preparation function
    train_df, test_df = prepare_modeling_data(df)

    # Add assertions to satisfy the linter
    assert train_df is not None
    assert test_df is not None

    # 1. Test Feature Engineering
    # The original df is modified in place before splitting, so we can check it
    assert 'is_home' in df.columns
    assert df['is_home'].iloc[0] == 1 # T1 is home
    assert df['is_home'].iloc[1] == 0 # T2 is away

    assert 'opponent_points_against_roll_avg_5g' in df.columns
    assert df['opponent_points_against_roll_avg_5g'].iloc[0] == df['away_team_points_against_roll_avg_5g'].iloc[0]
    
    assert 'points_vs_opp_avg' in df.columns
    assert df['points_vs_opp_avg'].iloc[0] == df['points_roll_avg_5g'].iloc[0] - df['opponent_points_against_roll_avg_5g'].iloc[0]

    # 2. Test Target Creation
    assert 'points_over_avg_5g' in df.columns
    assert df['points_over_avg_5g'].iloc[0] == 1 # 10 > 8
    assert df['points_over_avg_5g'].iloc[1] == 0 # 20 < 22

    # 3. Test Imputation (on the final split data)
    # The row with NaNs (index 2) should have been imputed before splitting
    imputed_row_in_train = train_df[train_df['fanduel_points_line'] == 0]
    imputed_row_in_test = test_df[test_df['fanduel_points_line'] == 0]
    assert len(imputed_row_in_train) + len(imputed_row_in_test) == 1

    # 4. Test Data Splitting and Final Shape
    assert isinstance(train_df, pd.DataFrame)
    assert isinstance(test_df, pd.DataFrame)
    assert len(train_df) + len(test_df) == len(sample_featured_data) # All rows should be present
    
    expected_cols = [
        'points_roll_avg_5g', 'rebounds_roll_avg_5g', 'assists_roll_avg_5g',
        'minutes_played', 'home_rest_days', 'away_rest_days', 'points_vs_opp_avg',
        'fanduel_points_line', 'fanduel_points_over_odds', 'fanduel_points_under_odds',
        'points_over_avg_5g'
    ]
    assert all(col in train_df.columns for col in expected_cols)
    assert all(col in test_df.columns for col in expected_cols) 