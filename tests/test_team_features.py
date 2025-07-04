import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import pytest
from src.feature_engineering.team_features import TeamFeatures

@pytest.fixture
def sample_game_data():
    """Provides a sample DataFrame of game data for testing."""
    data = {
        'game_id': [1, 2, 3, 4, 5, 6],
        'date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05', '2023-01-06']),
        'home_team_id': [101, 102, 101, 103, 102, 101],
        'away_team_id': [102, 101, 103, 102, 101, 103],
        'home_score': [110, 105, 120, 95, 100, 115],
        'away_score': [100, 115, 110, 90, 105, 125]
    }
    return pd.DataFrame(data)

def test_team_features_creation(sample_game_data):
    """
    Tests that the TeamFeatures class correctly creates rolling average features.
    """
    config = {
        'stats_to_average': ['points_for', 'points_against'],
        'rolling_windows': [3]
    }
    team_features = TeamFeatures(config=config)
    featured_data = team_features.create_team_strength_features(sample_game_data)

    # Check that new columns have been added
    expected_new_cols = [
        'home_team_points_for_roll_avg_3g', 'home_team_points_against_roll_avg_3g',
        'away_team_points_for_roll_avg_3g', 'away_team_points_against_roll_avg_3g'
    ]
    for col in expected_new_cols:
        assert col in featured_data.columns, f"Column '{col}' not found in output"

    # Check the calculation for a specific data point: game_id 5
    game_5_data = featured_data[featured_data['game_id'] == 5].iloc[0]

    # Expected values are the rolling averages *before* this game (using shift(1))
    
    # Home Team (102) previous games: 1, 2, 4
    # Pts For: 100 (vs 101), 105 (vs 101), 90 (vs 103) -> Mistake in logic.
    # Let's trace team 102 scores:
    # Game 1 (away): score=100, against=110
    # Game 2 (home): score=105, against=115
    # Game 4 (away): score=90,  against=95
    # For game 5, avg pts for: (100 + 105 + 90) / 3 = 98.33
    # For game 5, avg pts against: (110 + 115 + 95) / 3 = 106.67
    expected_home_pts_for_avg = (100 + 105 + 90) / 3
    expected_home_pts_against_avg = (110 + 115 + 95) / 3
    
    assert abs(game_5_data['home_team_points_for_roll_avg_3g'] - expected_home_pts_for_avg) < 0.01
    assert abs(game_5_data['home_team_points_against_roll_avg_3g'] - expected_home_pts_against_avg) < 0.01

    # Away Team (101) previous games: 1, 2, 3
    # Game 1 (home): score=110, against=100
    # Game 2 (away): score=115, against=105
    # Game 3 (home): score=120, against=110
    # For game 5, avg pts for: (110 + 115 + 120) / 3 = 115.0
    # For game 5, avg pts against: (100 + 105 + 110) / 3 = 105.0
    expected_away_pts_for_avg = (110 + 115 + 120) / 3
    expected_away_pts_against_avg = (100 + 105 + 110) / 3
    
    assert abs(game_5_data['away_team_points_for_roll_avg_3g'] - expected_away_pts_for_avg) < 0.01
    assert abs(game_5_data['away_team_points_against_roll_avg_3g'] - expected_away_pts_against_avg) < 0.01

def test_team_features_fewer_games_than_window(sample_game_data):
    """
    Tests that rolling averages are calculated correctly when a team has played fewer games than the window size.
    """
    config = {
        'stats_to_average': ['points_for', 'points_against'],
        'rolling_windows': [5]  # Use a larger window
    }
    team_features = TeamFeatures(config=config)
    
    # Use only the first 3 games for this test
    featured_data = team_features.create_team_strength_features(sample_game_data.head(3))

    # For game_id 3 (home_team_id=101), it has two prior games (game_id 1 and 2)
    game_3_data = featured_data[featured_data['game_id'] == 3].iloc[0]
    
    # Team 101's scores:
    # Game 1 (home): 110 for, 100 against
    # Game 2 (away): 115 for, 105 against
    # Avg pts for: (110 + 115) / 2 = 112.5
    # Avg pts against: (100 + 105) / 2 = 102.5
    expected_home_pts_for_avg = 112.5
    expected_home_pts_against_avg = 102.5
    
    assert abs(game_3_data['home_team_points_for_roll_avg_5g'] - expected_home_pts_for_avg) < 0.01
    assert abs(game_3_data['home_team_points_against_roll_avg_5g'] - expected_home_pts_against_avg) < 0.01

def test_team_features_empty_input():
    """
    Tests that the function handles an empty DataFrame without errors.
    """
    config = {
        'stats_to_average': ['points_for', 'points_against'],
        'rolling_windows': [3, 5, 10]
    }
    team_features = TeamFeatures(config=config)
    empty_df_data = {
        'game_id': [], 'date': [], 'home_team_id': [], 'away_team_id': [], 'home_score': [], 'away_score': []
    }
    empty_df = pd.DataFrame(empty_df_data)
    empty_df['date'] = pd.to_datetime(empty_df['date'])
    
    featured_data = team_features.create_team_strength_features(empty_df)
    
    assert featured_data.empty
    assert 'home_team_points_for_roll_avg_3g' not in featured_data.columns

def test_multiple_rolling_windows(sample_game_data):
    """
    Tests that features for multiple rolling windows are created correctly.
    """
    config = {
        'stats_to_average': ['points_for'],
        'rolling_windows': [3, 5]
    }
    team_features = TeamFeatures(config=config)
    featured_data = team_features.create_team_strength_features(sample_game_data)
    
    expected_cols = ['home_team_points_for_roll_avg_3g', 'home_team_points_for_roll_avg_5g']
    for col in expected_cols:
        assert col in featured_data.columns 