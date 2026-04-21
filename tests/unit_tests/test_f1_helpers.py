# =============================================================================
# F1-Pulse | Unit Tests — f1_helpers.py
# File:     tests/unit_tests/test_f1_helpers.py
# Author:   Jafar891
# Updated:  2026
#
# Tests for:
#   get_latest_race_session — success, multiple matches, fallback, empty input
#   safe_cast_pdf           — object casting, None replacement, numeric preservation
#   pdf_to_spark            — empty input, success path, conversion exception
# =============================================================================

import path_setup  # noqa: F401  — inserts project root into sys.path

import pytest
import pandas as pd
from unittest import mock
from modules.f1_helpers import (
    get_latest_race_session,
    safe_cast_pdf,
    pdf_to_spark,
)


# ---------------------------------------------------------------------------
# Mock SESSION_TYPE so tests are independent of config values
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def patch_session_type():
    with mock.patch("modules.f1_helpers.SESSION_TYPE", "Race"):
        yield


# ---------------------------------------------------------------------------
# get_latest_race_session
# ---------------------------------------------------------------------------

def test_get_latest_race_session_success():
    sessions = [
        {"session_key": 101, "session_type": "Practice 1"},
        {"session_key": 102, "session_type": "Race"},
        {"session_key": 103, "session_type": "Practice 2"},
    ]
    result = get_latest_race_session(sessions)
    assert result["session_key"] == 102
    assert result["session_type"] == "Race"


def test_get_latest_race_session_multiple_same_type():
    """When multiple Race sessions exist, the last one should be returned."""
    sessions = [
        {"session_key": 201, "session_type": "Race"},
        {"session_key": 202, "session_type": "Practice 1"},
        {"session_key": 203, "session_type": "Race"},
    ]
    result = get_latest_race_session(sessions)
    assert result["session_key"] == 203


def test_get_latest_race_session_fallback(caplog):
    """No Race session — should warn and fall back to the last record."""
    sessions = [
        {"session_key": 301, "session_type": "Practice 1"},
        {"session_key": 302, "session_type": "Practice 2"},
    ]
    with caplog.at_level("WARNING"):
        result = get_latest_race_session(sessions)
        assert "No session_type='Race' found" in caplog.text

    assert result["session_key"] == 302


def test_get_latest_race_session_empty():
    """Empty list should return None without raising."""
    assert get_latest_race_session([]) is None


# ---------------------------------------------------------------------------
# safe_cast_pdf
# ---------------------------------------------------------------------------

def test_safe_cast_pdf_casts_object_columns_to_str():
    pdf = pd.DataFrame({"col_str": ["a", "b", None, "None"]})
    result = safe_cast_pdf(pdf.copy())
    assert result["col_str"].dtype == object


def test_safe_cast_pdf_replaces_none_string_with_none():
    pdf = pd.DataFrame({"col_str": ["a", "b", None, "None"]})
    result = safe_cast_pdf(pdf.copy())
    assert result["col_str"].iloc[3] is None


def test_safe_cast_pdf_preserves_integer_columns():
    pdf = pd.DataFrame({"col_int": [1, 2, 3, 4]})
    result = safe_cast_pdf(pdf.copy())
    assert list(result["col_int"]) == [1, 2, 3, 4]


def test_safe_cast_pdf_preserves_float_columns():
    pdf = pd.DataFrame({"col_float": [1.1, 2.2, 3.3, 4.4]})
    result = safe_cast_pdf(pdf.copy())
    assert list(result["col_float"]) == [1.1, 2.2, 3.3, 4.4]


def test_safe_cast_pdf_mixed_columns():
    """Object columns cast, numeric columns unchanged — both in same DataFrame."""
    pdf = pd.DataFrame({
        "col_str":   ["a", "b", None, "None"],
        "col_int":   [1, 2, 3, 4],
        "col_float": [1.1, 2.2, 3.3, 4.4],
    })
    result = safe_cast_pdf(pdf.copy())

    assert result["col_str"].iloc[3] is None          # "None" → None
    assert list(result["col_int"])   == [1, 2, 3, 4]  # unchanged
    assert list(result["col_float"]) == [1.1, 2.2, 3.3, 4.4]  # unchanged


# ---------------------------------------------------------------------------
# pdf_to_spark
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_spark():
    spark = mock.MagicMock()

    def fake_create_df(pdf):
        df_mock = mock.MagicMock()
        df_mock.withColumn.return_value = df_mock
        return df_mock

    spark.createDataFrame.side_effect = fake_create_df
    return spark


def test_pdf_to_spark_returns_none_on_empty_data(mock_spark, caplog):
    with caplog.at_level("WARNING"):
        result = pdf_to_spark(mock_spark, [], "http://fake.url")

    assert result is None
    assert "Empty dataset" in caplog.text


def test_pdf_to_spark_calls_create_dataframe(mock_spark):
    data = [{"name": "Alice", "score": 10}, {"name": "Bob", "score": 20}]
    pdf_to_spark(mock_spark, data, "http://fake.url")
    assert mock_spark.createDataFrame.called


def test_pdf_to_spark_adds_ingested_at_audit_column(mock_spark):
    data = [{"name": "Alice", "score": 10}]
    result = pdf_to_spark(mock_spark, data, "http://fake.url")
    result.withColumn.assert_any_call("ingested_at", mock.ANY)


def test_pdf_to_spark_adds_source_url_audit_column(mock_spark):
    data = [{"name": "Alice", "score": 10}]
    result = pdf_to_spark(mock_spark, data, "http://fake.url")
    result.withColumn.assert_any_call("source_url", mock.ANY)


def test_pdf_to_spark_returns_none_on_exception(mock_spark, caplog):
    mock_spark.createDataFrame.side_effect = Exception("Boom!")

    with caplog.at_level("ERROR"):
        result = pdf_to_spark(mock_spark, [{"x": 1}], "http://fake.url")

    assert result is None
    assert "DataFrame conversion failed: Boom!" in caplog.text