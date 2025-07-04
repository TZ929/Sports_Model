import sys
import os
import pandas as pd
import pytest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Since predict.py has a lot of imports, we import the functions we need to test directly
from src.prediction.predict import load_model, fetch_prediction_data, make_prediction, engineer_features

@pytest.fixture
def mock_model():
    """Provides a mock model object."""
    model = MagicMock()
    model.feature_name_ = ['feature1', 'feature2']
    model.predict.return_value = [1]
    model.predict_proba.return_value = [[0.1, 0.9]]
    return model

def test_load_model_success(mocker, mock_model):
    """Tests successful model loading."""
    mocker.patch('joblib.load', return_value=mock_model)
    loaded_model = load_model('dummy_path')
    assert loaded_model is not None
    assert loaded_model.predict([1,2]) == [1]

def test_load_model_file_not_found(mocker):
    """Tests model loading when the file does not exist."""
    mocker.patch('joblib.load', side_effect=FileNotFoundError)
    loaded_model = load_model('non_existent_path')
    assert loaded_model is None

def test_make_prediction_success(mock_model):
    """Tests a successful prediction call."""
    data = pd.DataFrame({'feature1': [1], 'feature2': [2]})
    result = make_prediction(mock_model, data)

    assert result is not None
    prediction, proba = result
    
    mock_model.predict.assert_called_once()
    mock_model.predict_proba.assert_called_once()
    assert prediction == [1]
    assert proba is not None
    assert proba[0][1] == 0.9

def test_make_prediction_model_is_none():
    """Tests that no prediction is made if the model is None."""
    data = pd.DataFrame({'feature1': [1], 'feature2': [2]})
    result = make_prediction(None, data)
    assert result is None

def test_make_prediction_reorders_and_adds_columns(mock_model):
    """Tests that the function correctly reorders and adds missing columns."""
    # Input data has columns in wrong order and one missing
    mock_model.feature_name_ = ['feature1', 'feature2', 'feature3']
    data = pd.DataFrame({'feature2': [1], 'feature1': [2]})
    
    make_prediction(mock_model, data)
    
    # Check that predict was called with a DataFrame that has the correct columns in the correct order
    # and the missing one added with a value of 0.
    called_with_df = mock_model.predict.call_args[0][0]
    assert list(called_with_df.columns) == ['feature1', 'feature2', 'feature3']
    assert called_with_df['feature3'].iloc[0] == 0 

@pytest.fixture
def sample_db_game_info():
    """Provides a sample game info DataFrame as returned from the DB."""
    return pd.DataFrame([{'game_id': 'g1', 'date': '2023-11-01', 'team_id': 'T1'}])

@pytest.fixture
def sample_db_player_log():
    """Provides a sample player log DataFrame as returned from the DB."""
    return pd.DataFrame([{'player_id': 'p1', 'game_id': 'g0', 'points': 10}])

@pytest.fixture
def sample_db_player_info():
    """Provides a sample player info DataFrame as returned from the DB."""
    return pd.DataFrame([{'player_id': 'p1', 'team_id': 'T1'}])

def test_fetch_prediction_data(mocker, sample_db_game_info, sample_db_player_log, sample_db_player_info):
    """Tests the data fetching and preparation logic."""
    # Mock all the database-interacting functions
    mocker.patch('src.prediction.predict.get_game_info', return_value=sample_db_game_info)
    mocker.patch('src.prediction.predict.get_player_game_log', return_value=sample_db_player_log)
    mocker.patch('src.prediction.predict.get_player_info', return_value=sample_db_player_info)
    mock_session = MagicMock()
    mocker.patch('src.utils.database.db_manager.get_session', return_value=mock_session)

    # Call the function
    result_df = fetch_prediction_data('g1', 'p1')

    assert result_df is not None
    assert len(result_df) == 2 # player log + current game
    assert result_df.iloc[-1]['game_id'] == 'g1'

def test_engineer_features(mocker):
    """Tests that the feature engineering pipeline is called correctly."""
    # Mock the feature engineering classes
    mock_player_features = mocker.patch('src.prediction.predict.PlayerFeatures')
    mock_game_features = mocker.patch('src.prediction.predict.GameFeatures')
    mock_team_features = mocker.patch('src.prediction.predict.TeamFeatures')

    # Create a dummy DataFrame to pass to the function
    dummy_df = pd.DataFrame([{'game_id': 'g1'}])
    
    engineer_features(dummy_df)

    # Assert that the create methods of our mocked classes were called
    mock_player_features.return_value.create_rolling_averages.assert_called_once()
    mock_game_features.return_value.create_game_context_features.assert_called_once()
    mock_team_features.return_value.create_team_strength_features.assert_called_once() 