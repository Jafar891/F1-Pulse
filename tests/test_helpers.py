import pytest
from modules.f1_helpers import get_latest_race_session

def test_get_latest_race_session_success():
    # 1. Fake data
    mock_sessions = [
        {"session_key": 101, "session_type": "Practice 1"},
        {"session_key": 102, "session_type": "Race"},
        {"session_key": 103, "session_type": "Practice 2"}
    ]
    
    # 2. Run the function
    result = get_latest_race_session(mock_sessions)
    
    # 3. Assert it grabbed the correct one
    assert result["session_key"] == 102
    assert result["session_type"] == "Race"

def test_get_latest_race_session_empty():
    assert get_latest_race_session([]) is None