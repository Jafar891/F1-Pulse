# =============================================================================
# F1-Pulse | Integration Tests — Bronze → Silver
# File:     tests/integration_tests/test_integration_bronze_silver.py
# Author:   Jafar891
# Updated:  2026
# =============================================================================

import sys
import os

# tests/integration_tests/ -> tests/ -> project root (two levels up)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pytest
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField, IntegerType, FloatType, 
    BooleanType, StringType, TimestampType
)
from modules.silver_transforms import transform_sessions, transform_laps


# ---------------------------------------------------------------------------
# Fixtures — Bronze-shaped data (Explicit Schemas)
# ---------------------------------------------------------------------------

@pytest.fixture
def bronze_sessions(spark):
    """Bronze sessions with EXPLICIT schemas to prevent inference errors."""
    schema = StructType([
        StructField("session_key", IntegerType(), True),
        StructField("session_name", StringType(), True),
        StructField("session_type", StringType(), True),
        StructField("location", StringType(), True),
        StructField("country_name", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("date_start", TimestampType(), True),
        StructField("date_end", TimestampType(), True),
        StructField("ingested_at", StringType(), True),
        StructField("source_url", StringType(), True)
    ])
    
    data = [
        (9158, "Abu Dhabi Grand Prix", "Race", "Yas Marina Circuit", " United Arab Emirates ", 2026, datetime(2026, 11, 24, 13, 0, 0), datetime(2026, 11, 24, 15, 10, 0), None, "http://openf1.org/v1/sessions"),
        (9157, "Abu Dhabi Qualifying", "Qualifying", "Yas Marina Circuit", "United Arab Emirates", 2026, datetime(2026, 11, 23, 13, 0, 0), datetime(2026, 11, 23, 14, 0, 0), None, "http://openf1.org/v1/sessions"),
        (9155, "Abu Dhabi Practice 1", "Practice 1", "Yas Marina Circuit", "United Arab Emirates", 2026, datetime(2026, 11, 22, 9, 30, 0), datetime(2026, 11, 22, 10, 30, 0), None, "http://openf1.org/v1/sessions"),
        (None, "Ghost Session", "Race", "Unknown", "Unknown", 2026, datetime(2026, 1, 1, 0, 0, 0), datetime(2026, 1, 1, 1, 0, 0), None, "http://openf1.org/v1/sessions")
    ]
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def bronze_laps(spark):
    schema = StructType([
        StructField("driver_number", StringType(), True),
        StructField("lap_number", IntegerType(), True),
        StructField("lap_duration", FloatType(), True),
        StructField("is_pit_out_lap", BooleanType(), True)
    ])
    
    data = [
        ("1",  1, 88.512, False),
        ("1",  2, 89.034, False),
        ("1",  3, None,   False),
        ("1",  4, 88.900, True),
        ("1",  5, 25.001, False),
        ("44", 1, 91.200, False),
        ("44", 2, 90.800, False),
        ("99", 1, 88.000, False)
    ]
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def bronze_drivers(spark):
    schema = StructType([
        StructField("driver_number", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("team_name", StringType(), True),
        StructField("country_code", StringType(), True),
        StructField("headshot_url", StringType(), True)
    ])
    
    data = [
        ("1",  "Max Verstappen", "Red Bull Racing", "NLD", "http://img/ver"),
        ("1",  "Max Verstappen", "Red Bull Racing", "NLD", "http://img/ver"),   # intentional duplicate
        ("44", "Lewis Hamilton", "Ferrari", "GBR", "http://img/ham")
    ]
    return spark.createDataFrame(data, schema=schema)


# ---------------------------------------------------------------------------
# Sessions: Bronze → Silver
# ---------------------------------------------------------------------------

class TestBronzeToSilverSessions:

    def test_only_race_and_qualifying_pass_through(self, spark, bronze_sessions):
        result = transform_sessions(bronze_sessions, pipeline_year=2026)
        types = {r["session_type"] for r in result.collect()}
        assert types == {"RACE", "QUALIFYING"}

    def test_practice_rows_excluded(self, spark, bronze_sessions):
        result = transform_sessions(bronze_sessions, pipeline_year=2026)
        assert result.count() == 2

    def test_null_session_key_excluded(self, spark, bronze_sessions):
        result = transform_sessions(bronze_sessions, pipeline_year=2026)
        assert result.filter(result.session_id.isNull()).count() == 0

    def test_country_name_whitespace_stripped(self, spark, bronze_sessions):
        result = transform_sessions(bronze_sessions, pipeline_year=2026)
        race_row = result.filter(result.session_id == 9158).first()
        assert race_row["country_name"] == "United Arab Emirates"

    def test_silver_session_schema_types(self, spark, bronze_sessions):
        result = transform_sessions(bronze_sessions, pipeline_year=2026)
        schema_dict = {f.name: f.dataType for f in result.schema.fields}
        assert isinstance(schema_dict["session_id"],    IntegerType)
        assert isinstance(schema_dict["year"],          IntegerType)
        assert isinstance(schema_dict["pipeline_year"], IntegerType)
        assert isinstance(schema_dict["start_time"],    TimestampType)

    def test_pipeline_year_set_correctly(self, spark, bronze_sessions):
        result = transform_sessions(bronze_sessions, pipeline_year=2026)
        assert all(r["pipeline_year"] == 2026 for r in result.collect())

    def test_processed_at_not_null(self, spark, bronze_sessions):
        result = transform_sessions(bronze_sessions, pipeline_year=2026)
        assert result.filter(result.processed_at.isNull()).count() == 0


# ---------------------------------------------------------------------------
# Laps: Bronze → Silver
# ---------------------------------------------------------------------------

class TestBronzeToSilverLaps:

    def test_unmatched_driver_excluded(self, spark, bronze_laps, bronze_drivers):
        result = transform_laps(bronze_laps, bronze_drivers, min_lap_duration_s=60.0)
        drivers_present = {r["driver_number"] for r in result.collect()}
        assert "99" not in drivers_present

    def test_total_rows_correct(self, spark, bronze_laps, bronze_drivers):
        result = transform_laps(bronze_laps, bronze_drivers, min_lap_duration_s=60.0)
        assert result.count() == 7   # 5 VER + 2 HAM (driver 99 excluded)

    def test_valid_lap_count_correct(self, spark, bronze_laps, bronze_drivers):
        result = transform_laps(bronze_laps, bronze_drivers, min_lap_duration_s=60.0)
        assert result.filter(result.is_valid_lap == True).count() == 4

    def test_flagged_lap_count_correct(self, spark, bronze_laps, bronze_drivers):
        result = transform_laps(bronze_laps, bronze_drivers, min_lap_duration_s=60.0)
        assert result.filter(result.is_valid_lap == False).count() == 3

    def test_driver_enrichment_correct(self, spark, bronze_laps, bronze_drivers):
        result = transform_laps(bronze_laps, bronze_drivers, min_lap_duration_s=60.0)
        ver_row = result.filter(result.driver_number == "1").first()
        assert ver_row["full_name"] == "Max Verstappen"
        assert ver_row["team_name"] == "Red Bull Racing"

    def test_duplicate_drivers_do_not_multiply_laps(
        self, spark, bronze_laps, bronze_drivers
    ):
        result = transform_laps(bronze_laps, bronze_drivers, min_lap_duration_s=60.0)
        assert result.filter(result.driver_number == "1").count() == 5

    def test_silver_lap_schema_types(self, spark, bronze_laps, bronze_drivers):
        result = transform_laps(bronze_laps, bronze_drivers, min_lap_duration_s=60.0)
        schema_dict = {f.name: f.dataType for f in result.schema.fields}
        assert isinstance(schema_dict["lap_number"],     IntegerType)
        assert isinstance(schema_dict["lap_duration"],   FloatType)
        assert isinstance(schema_dict["is_pit_out_lap"], BooleanType)
        assert isinstance(schema_dict["is_valid_lap"],   BooleanType)

    def test_processed_at_not_null(self, spark, bronze_laps, bronze_drivers):
        result = transform_laps(bronze_laps, bronze_drivers, min_lap_duration_s=60.0)
        assert result.filter(result.processed_at.isNull()).count() == 0