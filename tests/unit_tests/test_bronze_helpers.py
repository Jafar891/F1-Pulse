# =============================================================================
# F1-Pulse | Unit Tests — bronze_helpers.py
# File:     tests/unit_tests/test_bronze_helpers.py
# Author:   Jafar891
# Updated:  2026
# =============================================================================

import path_setup  # noqa: F401  — inserts project root into sys.path

import pytest
from unittest import mock
from modules.bronze_helpers import write_bronze


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_df():
    """Minimal Spark DataFrame mock with a fluent write chain."""
    df = mock.MagicMock()
    df.count.return_value = 42

    writer = mock.MagicMock()
    writer.format.return_value = writer
    writer.mode.return_value = writer
    writer.option.return_value = writer
    writer.saveAsTable.return_value = None
    df.write = writer

    return df


# ---------------------------------------------------------------------------
# Success path
# ---------------------------------------------------------------------------

def test_write_bronze_calls_save_as_table(mock_df, caplog):
    with caplog.at_level("INFO"):
        write_bronze(mock_df, "f1_pulse", "bronze", "raw_laps_2026")

    mock_df.write.format.assert_called_once_with("delta")
    mock_df.write.format().mode.assert_called_once_with("overwrite")
    mock_df.write.format().mode().option.assert_called_once_with("overwriteSchema", "true")
    mock_df.write.format().mode().option().saveAsTable.assert_called_once_with(
        "f1_pulse.bronze.raw_laps_2026"
    )


def test_write_bronze_logs_full_table_name(mock_df, caplog):
    with caplog.at_level("INFO"):
        write_bronze(mock_df, "f1_pulse", "bronze", "raw_sessions_2026")

    assert "f1_pulse.bronze.raw_sessions_2026" in caplog.text


def test_write_bronze_logs_row_count(mock_df, caplog):
    mock_df.count.return_value = 99
    with caplog.at_level("INFO"):
        write_bronze(mock_df, "f1_pulse", "bronze", "raw_laps_2026")

    assert "99" in caplog.text


# ---------------------------------------------------------------------------
# Failure path
# ---------------------------------------------------------------------------

def test_write_bronze_raises_runtime_error_on_failure(caplog):
    df = mock.MagicMock()
    writer = mock.MagicMock()
    writer.format.return_value = writer
    writer.mode.return_value = writer
    writer.option.return_value = writer
    writer.saveAsTable.side_effect = Exception("Delta write failed")
    df.write = writer

    with caplog.at_level("ERROR"):
        with pytest.raises(RuntimeError, match="Cannot write Bronze table"):
            write_bronze(df, "f1_pulse", "bronze", "raw_laps_2026")

    assert "Failed to write" in caplog.text