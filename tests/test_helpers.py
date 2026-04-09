# test_helpers.py
import sys
import os

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pytest
import pandas as pd
from unittest import mock
from modules.f1_helpers import (
    get_latest_race_session,
    safe_cast_pdf,
    pdf_to_spark
)


# -------------------------------------------------------------------
# Mock SESSION_TYPE
# -------------------------------------------------------------------
@pytest.fixture(autouse=True)
def patch_session_type():
    with mock.patch("modules.f1_helpers.SESSION_TYPE", "Race"):
        yield


# -------------------------------------------------------------------
# get_latest_race_session tests
# -------------------------------------------------------------------
def test_get_latest_race_session_success():
    mock_sessions = [
        {"session_key": 101, "session_type": "Practice 1"},
        {"session_key": 102, "session_type": "Race"},
        {"session_key": 103, "session_type": "Practice 2"}
    ]
    result = get_latest_race_session(mock_sessions)
    assert result["session_key"] == 102
    assert result["session_type"] == "Race"


def test_get_latest_race_session_multiple_same_type():
    mock_sessions = [
        {"session_key": 201, "session_type": "Race"},
        {"session_key": 202, "session_type": "Practice 1"},
        {"session_key": 203, "session_type": "Race"}
    ]
    result = get_latest_race_session(mock_sessions)
    assert result["session_key"] == 203  # latest Race session


def test_get_latest_race_session_fallback(caplog):
    mock_sessions = [
        {"session_key": 301, "session_type": "Practice 1"},
        {"session_key": 302, "session_type": "Practice 2"}
    ]
    with caplog.at_level("WARNING"):
        result = get_latest_race_session(mock_sessions)
        assert "No session_type='Race' found" in caplog.text
    assert result["session_key"] == 302  # fallback to last


def test_get_latest_race_session_empty():
    assert get_latest_race_session([]) is None


# -------------------------------------------------------------------
# safe_cast_pdf tests
# -------------------------------------------------------------------
def test_safe_cast_pdf_casts_objects_to_str_and_none():
    data = {
        "col_str": ["a", "b", None, "None"],
        "col_int": [1, 2, 3, 4],
        "col_float": [1.1, 2.2, 3.3, 4.4]
    }
    pdf = pd.DataFrame(data)
    casted = safe_cast_pdf(pdf.copy())

    # Object column converted to str
    assert casted["col_str"].dtype == object
    # 'None' string replaced with actual None
    assert casted["col_str"].iloc[3] is None
    # Numeric columns unchanged
    assert all(casted["col_int"] == data["col_int"])
    assert all(casted["col_float"] == data["col_float"])


# -------------------------------------------------------------------
# pdf_to_spark tests (mock Spark)
# -------------------------------------------------------------------
@pytest.fixture
def mock_spark():
    mock_spark = mock.MagicMock()
    # Mock createDataFrame to just return input for testing
    def fake_create_df(pdf):
        df_mock = mock.MagicMock()
        # Mock chaining with withColumn
        df_mock.withColumn.return_value = df_mock
        return df_mock
    mock_spark.createDataFrame.side_effect = fake_create_df
    return mock_spark


def test_pdf_to_spark_empty_dataset(mock_spark, caplog):
    with caplog.at_level("WARNING"):
        result = pdf_to_spark(mock_spark, [], "http://fake.url")
        assert result is None
        assert "Empty dataset" in caplog.text


def test_pdf_to_spark_success(mock_spark):
    data = [
        {"name": "Alice", "score": 10},
        {"name": "Bob", "score": 20}
    ]
    result = pdf_to_spark(mock_spark, data, "http://fake.url")

    # Ensure createDataFrame was called
    assert mock_spark.createDataFrame.called
    # Ensure audit columns were added via withColumn
    result.withColumn.assert_any_call("ingested_at", mock.ANY)
    result.withColumn.assert_any_call("source_url", mock.ANY)


def test_pdf_to_spark_conversion_exception(mock_spark, caplog):
    # Force createDataFrame to raise an exception
    mock_spark.createDataFrame.side_effect = Exception("Boom!")
    with caplog.at_level("ERROR"):
        result = pdf_to_spark(mock_spark, [{"x": 1}], "url")
        assert result is None
        assert "DataFrame conversion failed: Boom!" in caplog.text