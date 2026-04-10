# =============================================================================
# F1-Pulse | Unit Tests — silver_helpers.py
# File:     tests/test_silver_helpers.py
# Author:   Jafar891
# Updated:  2026
#
# Tests for:
#   read_bronze  — success, read failure, empty table warning
#   write_silver — success, write failure
#   assert_columns — passes clean, raises on missing columns
#   log_quality_check — correct drop % calculation, zero-row edge case
# =============================================================================

import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pytest
from unittest import mock
from modules.silver_helpers import (
    read_bronze,
    write_silver,
    assert_columns,
    log_quality_check,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_spark():
    spark = mock.MagicMock()
    df = mock.MagicMock()
    df.count.return_value = 100
    spark.read.table.return_value = df
    return spark, df


@pytest.fixture
def mock_writer_df():
    """DataFrame mock with a fluent write chain."""
    df = mock.MagicMock()
    df.count.return_value = 50

    writer = mock.MagicMock()
    writer.format.return_value = writer
    writer.mode.return_value = writer
    writer.option.return_value = writer
    writer.saveAsTable.return_value = None
    df.write = writer

    return df


# ---------------------------------------------------------------------------
# read_bronze
# ---------------------------------------------------------------------------

def test_read_bronze_returns_dataframe(mock_spark):
    spark, df = mock_spark
    result = read_bronze(spark, "f1_pulse", "bronze", "raw_laps_2026")
    spark.read.table.assert_called_once_with("f1_pulse.bronze.raw_laps_2026")
    assert result is df


def test_read_bronze_warns_on_empty_table(mock_spark, caplog):
    spark, df = mock_spark
    df.count.return_value = 0

    with caplog.at_level("WARNING"):
        read_bronze(spark, "f1_pulse", "bronze", "raw_laps_2026")

    assert "empty" in caplog.text.lower()


def test_read_bronze_raises_on_failure(caplog):
    spark = mock.MagicMock()
    spark.read.table.side_effect = Exception("Table not found")

    with caplog.at_level("ERROR"):
        with pytest.raises(RuntimeError, match="Cannot read Bronze table"):
            read_bronze(spark, "f1_pulse", "bronze", "raw_laps_2026")


# ---------------------------------------------------------------------------
# write_silver
# ---------------------------------------------------------------------------

def test_write_silver_saves_to_correct_table(mock_writer_df, caplog):
    with caplog.at_level("INFO"):
        write_silver(mock_writer_df, "f1_pulse", "silver", "enriched_laps")

    mock_writer_df.write.format().mode().option().saveAsTable.assert_called_once_with(
        "f1_pulse.silver.enriched_laps"
    )


def test_write_silver_logs_row_count(mock_writer_df, caplog):
    mock_writer_df.count.return_value = 777
    with caplog.at_level("INFO"):
        write_silver(mock_writer_df, "f1_pulse", "silver", "enriched_laps")

    assert "777" in caplog.text


def test_write_silver_raises_on_failure(caplog):
    df = mock.MagicMock()
    writer = mock.MagicMock()
    writer.format.return_value = writer
    writer.mode.return_value = writer
    writer.option.return_value = writer
    writer.saveAsTable.side_effect = Exception("Delta write error")
    df.write = writer

    with caplog.at_level("ERROR"):
        with pytest.raises(RuntimeError, match="Cannot write Silver table"):
            write_silver(df, "f1_pulse", "silver", "enriched_laps")


# ---------------------------------------------------------------------------
# assert_columns
# ---------------------------------------------------------------------------

def test_assert_columns_passes_when_all_present():
    df = mock.MagicMock()
    df.columns = ["driver_number", "lap_duration", "is_pit_out_lap"]

    # Should not raise
    assert_columns(df, ["driver_number", "lap_duration"], "test_table")


def test_assert_columns_raises_on_missing_columns():
    df = mock.MagicMock()
    df.columns = ["driver_number"]

    with pytest.raises(ValueError, match="Schema drift detected"):
        assert_columns(df, ["driver_number", "lap_duration", "team_name"], "test_table")


def test_assert_columns_error_lists_all_missing():
    df = mock.MagicMock()
    df.columns = ["driver_number"]

    with pytest.raises(ValueError) as exc_info:
        assert_columns(df, ["driver_number", "lap_duration", "team_name"], "test_table")

    error_msg = str(exc_info.value)
    assert "lap_duration" in error_msg
    assert "team_name" in error_msg


def test_assert_columns_raises_on_empty_df_columns():
    df = mock.MagicMock()
    df.columns = []

    with pytest.raises(ValueError, match="Schema drift detected"):
        assert_columns(df, ["session_key"], "empty_table")


# ---------------------------------------------------------------------------
# log_quality_check
# ---------------------------------------------------------------------------

def test_log_quality_check_correct_percentage(caplog):
    with caplog.at_level("INFO"):
        log_quality_check("Laps", total=1000, kept=800)

    assert "1,000" in caplog.text
    assert "800" in caplog.text
    assert "200" in caplog.text   # dropped
    assert "20.0%" in caplog.text


def test_log_quality_check_zero_total(caplog):
    """Should not raise ZeroDivisionError."""
    with caplog.at_level("INFO"):
        log_quality_check("Sessions", total=0, kept=0)

    assert "0.0%" in caplog.text


def test_log_quality_check_all_kept(caplog):
    with caplog.at_level("INFO"):
        log_quality_check("Drivers", total=20, kept=20)

    assert "0.0%" in caplog.text