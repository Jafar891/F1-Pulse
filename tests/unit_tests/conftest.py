# =============================================================================
# F1-Pulse | Test Configuration
# File:     tests/conftest.py
# Author:   Jafar891
# Updated:  2026
#
# Shared pytest fixtures available to all test files.
# Provides a local SparkSession scoped to the entire test session —
# creating Spark once is expensive; reusing it keeps the suite fast.
# =============================================================================

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Local SparkSession for unit and integration tests.

    Scoped to 'session' so Spark starts once per pytest run.
    Uses local[*] mode — no cluster required.
    Delta Lake extensions are NOT loaded here; tests operate on
    plain DataFrames without Delta writes.
    """
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("f1_pulse_tests")
        .config("spark.sql.shuffle.partitions", "2")   # fast local shuffle
        .config("spark.ui.enabled", "false")           # suppress Spark UI
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")           # suppress Spark noise
    yield session
    session.stop()