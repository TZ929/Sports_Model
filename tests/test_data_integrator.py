import sys
import os
import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.preprocessing.data_integrator import DataIntegrator

@pytest.fixture
def data_integrator_instance():
    """Provides a DataIntegrator instance with a default 'left' merge strategy."""
    return DataIntegrator(config={'merge_strategy': 'left'})

@pytest.fixture
def sample_player_stats():
    """Provides a sample DataFrame of player stats."""
    data = {
        'player_id': [1, 2, 1, 3],
        'game_id': ['g1', 'g1', 'g2', 'g3'], # g3 has no corresponding game
        'points': [10, 15, 5, 20]
    }
    return pd.DataFrame(data)

@pytest.fixture
def sample_games():
    """Provides a sample DataFrame of games."""
    data = {
        'game_id': ['g1', 'g2'],
        'date': ['2023-10-25', '2023-10-26'],
        'home_team': ['Team A', 'Team C']
    }
    return pd.DataFrame(data)

def test_successful_left_merge(data_integrator_instance, sample_player_stats, sample_games):
    """Tests a standard left merge."""
    merged_df = data_integrator_instance.integrate_game_data(sample_player_stats, sample_games)
    
    assert len(merged_df) == 4 # Should keep all rows from player_stats
    assert 'date' in merged_df.columns
    assert merged_df[merged_df['game_id'] == 'g1']['home_team'].iloc[0] == 'Team A'
    # Check that game 'g3' has NaN for game-specific columns
    assert pd.isna(merged_df[merged_df['game_id'] == 'g3']['date'].iloc[0]) 

def test_inner_merge(sample_player_stats, sample_games):
    """Tests an inner merge, which should drop non-matching rows."""
    integrator = DataIntegrator(config={'merge_strategy': 'inner'})
    merged_df = integrator.integrate_game_data(sample_player_stats, sample_games)
    
    assert len(merged_df) == 3 # Should only keep game_id 'g1' and 'g2'
    assert 'g3' not in merged_df['game_id'].values

def test_missing_game_id_column(data_integrator_instance, sample_player_stats):
    """Tests that a ValueError is raised if 'game_id' is missing."""
    games_no_id = pd.DataFrame({'id': ['g1'], 'date': ['2023-10-25']})
    
    with pytest.raises(ValueError, match="Both DataFrames must contain a 'game_id' column."):
        data_integrator_instance.integrate_game_data(sample_player_stats, games_no_id) 