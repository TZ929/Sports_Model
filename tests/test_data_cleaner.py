import sys
import os
import pandas as pd
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.preprocessing.data_cleaner import DataCleaner

@pytest.fixture
def data_cleaner_instance():
    """Provides an instance of DataCleaner."""
    return DataCleaner(config={})

@pytest.fixture
def sample_teams_data():
    """Provides a sample teams DataFrame."""
    data = {
        'team_id': [1, 2],
        'team_name': ['Team A*', 'Team B'],
        'created_at': ['2023-01-01', '2023-01-01'],
        'updated_at': ['2023-01-01', '2023-01-01']
    }
    return pd.DataFrame(data)

@pytest.fixture
def sample_players_data():
    """Provides a sample players DataFrame."""
    data = {
        'player_id': [101, 102, 103],
        'full_name': ['Player One', 'Celtics Player', 'Player Three'],
        'team_name': ['Team A', 'Celtics', 'Team B'],
        'height': [200, 201, 202],
        'weight': [100, 101, 102],
        'birth_date': ['2000-01-01', '2000-01-01', '2000-01-01'],
        'created_at': ['2023-01-01', '2023-01-01', '2023-01-01'],
        'updated_at': ['2023-01-01', '2023-01-01', '2023-01-01']
    }
    return pd.DataFrame(data)

@pytest.fixture
def sample_player_stats_data():
    """Provides a sample player stats DataFrame."""
    data = {
        'player_id': [101, 103],
        'rebounds': [10, 8],
        'offensive_rebounds': [3, 2],
        'created_at': ['2023-01-01', '2023-01-01'],
        'updated_at': ['2023-01-01', '2023-01-01']
    }
    return pd.DataFrame(data)

@pytest.fixture
def sample_games_data():
    """Provides a sample games DataFrame."""
    data = {
        'game_id': [1001],
        'created_at': ['2023-01-01'],
        'updated_at': ['2023-01-01']
    }
    return pd.DataFrame(data)

def test_clean_player_game_stats(data_cleaner_instance, sample_player_stats_data):
    """Tests the cleaning of player game stats."""
    cleaned_df = data_cleaner_instance.clean_player_game_stats(sample_player_stats_data)
    
    assert 'defensive_rebounds' in cleaned_df.columns
    assert cleaned_df['defensive_rebounds'].iloc[0] == 7 # 10 - 3
    assert 'created_at' not in cleaned_df.columns
    assert 'updated_at' not in cleaned_df.columns

def test_clean_all_data(data_cleaner_instance, sample_teams_data, sample_players_data, sample_games_data, sample_player_stats_data):
    """Tests the comprehensive clean_all_data method."""
    teams_df, players_df, games_df, player_stats_df = data_cleaner_instance.clean_all_data(
        sample_teams_data, sample_players_data, sample_games_data, sample_player_stats_data
    )

    # Test teams cleaning
    assert teams_df['team_name'].iloc[0] == 'Team A'
    assert 'created_at' not in teams_df.columns

    # Test players cleaning
    assert 'Celtics Player' not in players_df['full_name'].values
    assert len(players_df) == 2
    assert 'team_id' in players_df.columns
    assert players_df['team_id'].iloc[0] == 1 # Mapped from 'Team A'
    assert 'team_name' not in players_df.columns
    
    # Test games cleaning
    assert 'created_at' not in games_df.columns

    # Test player_stats cleaning
    assert 'defensive_rebounds' in player_stats_df.columns
    assert 'created_at' not in player_stats_df.columns 