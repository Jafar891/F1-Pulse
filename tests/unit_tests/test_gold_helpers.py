# =============================================================================
# F1-Pulse | Unit Tests — gold_helpers.py
# File:     tests/unit_tests/test_gold_helpers.py
# Author:   Jafar891
# Updated:  2026
# =============================================================================

import path_setup  # noqa: F401  — inserts project root into sys.path

import pytest
from unittest import mock
from modules.gold_helpers import read_silver, write_gold, log_validity_summary


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_spark():
    spark = mock.MagicMock()
    df = mock.MagicMock()
    df.count.return_value = 500
    spark.read.table.return_value = df
    return spark, df


@pytest.fixture
def mock_writer_df():
    df = mock.MagicMock()
    df.count.return_value = 20

    writer = mock.MagicMock()
    writer.format.return_value = writer
    writer.mode.return_value = writer
    writer.option.return_value = writer
    writer.saveAsTable.return_value = None
    df.write = writer

    return df


# ---------------------------------------------------------------------------
# read_silver
# ---------------------------------------------------------------------------

def test_read_silver_returns_dataframe(mock_spark):
    spark, df = mock_spark
    result = read_silver(spark, "f1_pulse", "silver", "enriched_laps")
    spark.read.table.assert_called_once_with("f1_pulse.silver.enriched_laps")
    assert result is df


def test_read_silver_raises_value_error_on_empty_table(mock_spark):
    spark, df = mock_spark
    df.count.return_value = 0

    with pytest.raises(ValueError, match="is empty"):
        read_silver(spark, "f1_pulse", "silver", "enriched_laps")


def test_read_silver_raises_runtime_error_on_read_failure(caplog):
    spark = mock.MagicMock()
    spark.read.table.side_effect = Exception("Table does not exist")

    with caplog.at_level("ERROR"):
        with pytest.raises(RuntimeError, match="Cannot read Silver table"):
            read_silver(spark, "f1_pulse", "silver", "enriched_laps")


def test_read_silver_logs_row_count(mock_spark, caplog):
    spark, df = mock_spark
    df.count.return_value = 1234

    with caplog.at_level("INFO"):
        read_silver(spark, "f1_pulse", "silver", "enriched_laps")

    assert "1,234" in caplog.text


# ---------------------------------------------------------------------------
# write_gold
# ---------------------------------------------------------------------------

def test_write_gold_saves_to_correct_table(mock_writer_df):
    write_gold(mock_writer_df, "f1_pulse", "gold", "driver_performance_metrics")
    mock_writer_df.write.format().mode().option().saveAsTable.assert_called_once_with(
        "f1_pulse.gold.driver_performance_metrics"
    )


def test_write_gold_logs_table_name(mock_writer_df, caplog):
    with caplog.at_level("INFO"):
        write_gold(mock_writer_df, "f1_pulse", "gold", "constructor_standings")

    assert "f1_pulse.gold.constructor_standings" in caplog.text


def test_write_gold_raises_runtime_error_on_failure(caplog):
    df = mock.MagicMock()
    writer = mock.MagicMock()
    writer.format.return_value = writer
    writer.mode.return_value = writer
    writer.option.return_value = writer
    writer.saveAsTable.side_effect = Exception("Write failed")
    df.write = writer

    with caplog.at_level("ERROR"):
        with pytest.raises(RuntimeError, match="Cannot write Gold table"):
            write_gold(df, "f1_pulse", "gold", "lap_progression")


# ---------------------------------------------------------------------------
# log_validity_summary
# ---------------------------------------------------------------------------

def test_log_validity_summary_emits_valid_and_flagged_counts(caplog):
    df = mock.MagicMock()

    valid_df   = mock.MagicMock()
    flagged_df = mock.MagicMock()
    valid_df.count.return_value   = 850
    flagged_df.count.return_value = 150
    df.filter.side_effect = [valid_df, flagged_df]

    with caplog.at_level("INFO"):
        log_validity_summary(df)

    assert "850" in caplog.text
    assert "150" in caplog.text