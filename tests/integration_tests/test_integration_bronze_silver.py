# =============================================================================
# F1-Pulse | Integration Tests — Bronze → Silver
# File:     tests/test_integration_bronze_silver.py
# Author:   Jafar891
# Updated:  2026
#
# Validates that Bronze-shaped data flows cleanly through Silver transforms.
# Simulates what the real pipeline does without any Delta I/O:
#   - Bronze sessions payload → transform_sessions → assert Silver contract
#   - Bronze laps + drivers   → transform_laps    → assert Silver contract
#
# Uses a real local SparkSession (via conftest.py).
# =============================================================================

import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pytest
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, FloatType, BooleanType, TimestampType
from modules.silver_transforms import transform_sessions, transform_laps


# ---------------------------------------------------------------------------
# Fixtures — Bronze-shaped data (as produced by pdf_to_spark)
# ---------------------------------------------------------------------------

@pytest.fixture
def bronze_sessions(spark):
    """
    Realistic Bronze sessions payload — includes noise rows (FP1, Sprint)
    that Silver must filter out, plus whitespace and mixed-case types.
    """
    return spark.createDataFrame([
        Row(
            session_key=9158, session_name="Abu Dhabi Grand Prix",
            session_type="Race", location="Yas Marina Circuit",
            country_name=" United Arab Emirates ", year=2026,
            date_start="2026-11-24T13:00:00", date_end="2026-11-24T15:10:00",
            ingested_at=None, source_url="http://openf1.org/v1/sessions"
        ),
        Row(
            session_key=9157, session_name="Abu Dhabi Qualifying",
            session_type="Qualifying", location="Yas Marina Circuit",
            country_name="United Arab Emirates", year=2026,
            date_start="2026-11-23T13:00:00", date_end="2026-11-23T14:00:00",
            ingested_at=None, source_url="http://openf1.org/v1/sessions"
        ),
        Row(
            session_key=9155, session_name="Abu Dhabi Practice 1",
            session_type="Practice 1", location="Yas Marina Circuit",
            country_name="United Arab Emirates", year=2026,
            date_start="2026-11-22T09:30:00", date_end="2026-11-22T10:30:00",
            ingested_at=None, source_url="http://openf1.org/v1/sessions"
        ),
        Row(
            session_key=None, session_name="Ghost Session",
            session_type="Race", location="Unknown",
            country_name="Unknown", year=2026,
            date_start="2026-01-01T00:00:00", date_end="2026-01-01T01:00:00",
            ingested_at=None, source_url="http://openf1.org/v1/sessions"
        ),
    ])


@pytest.fixture
def bronze_laps(spark):
    """
    Realistic Bronze laps payload — includes pit-out laps, anomalous durations,
    null lap times, and a driver not in the drivers table (no match).
    """
    return spark.createDataFrame([
        Row(driver_number="1",  lap_number=1,  lap_duration=88.512, is_pit_out_lap=False),
        Row(driver_number="1",  lap_number=2,  lap_duration=89.034, is_pit_out_lap=False),
        Row(driver_number="1",  lap_number=3,  lap_duration=None,   is_pit_out_lap=False),
        Row(driver_number="1",  lap_number=4,  lap_duration=88.900, is_pit_out_lap=True),
        Row(driver_number="1",  lap_number=5,  lap_duration=25.001, is_pit_out_lap=False),
        Row(driver_number="44", lap_number=1,  lap_duration=91.200, is_pit_out_lap=False),
        Row(driver_number="44", lap_number=2,  lap_duration=90.800, is_pit_out_lap=False),
        Row(driver_number="99", lap_number=1,  lap_duration=88.000, is_pit_out_lap=False),
    ])


@pytest.fixture
def bronze_drivers(spark):
    """
    Realistic Bronze drivers payload — includes a duplicate row to validate dedup.
    """
    return spark.createDataFrame([
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull Racing",
            country_code="NLD", headshot_url="http://img/ver"),
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull Racing",
            country_code="NLD", headshot_url="http://img/ver"),   # duplicate
        Row(driver_number="44", full_name="Lewis Hamilton", team_name="Ferrari",
            country_code="GBR", headshot_url="http://img/ham"),
    ])


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
        assert result.count() == 2   # Race + Qualifying only

    def test_null_session_key_excluded(self, spark, bronze_sessions):
        result = transform_sessions(bronze_sessions, pipeline_year=2026)
        nulls = result.filter(result.session_id.isNull()).count()
        assert nulls == 0

    def test_country_name_whitespace_stripped(self, spark, bronze_sessions):
        result = transform_sessions(bronze_sessions, pipeline_year=2026)
        race_row = result.filter(result.session_id == 9158).first()
        assert race_row["country_name"] == "United Arab Emirates"

    def test_silver_session_schema_types(self, spark, bronze_sessions):
        result = transform_sessions(bronze_sessions, pipeline_year=2026)
        schema = {f.name: f.dataType for f in result.schema.fields}
        assert isinstance(schema["session_id"],    IntegerType)
        assert isinstance(schema["year"],          IntegerType)
        assert isinstance(schema["pipeline_year"], IntegerType)
        assert isinstance(schema["start_time"],    TimestampType)

    def test_pipeline_year_set_correctly(self, spark, bronze_sessions):
        result = transform_sessions(bronze_sessions, pipeline_year=2026)
        assert all(r["pipeline_year"] == 2026 for r in result.collect())

    def test_processed_at_not_null(self, spark, bronze_sessions):
        result = transform_sessions(bronze_sessions, pipeline_year=2026)
        nulls = result.filter(result.processed_at.isNull()).count()
        assert nulls == 0


# ---------------------------------------------------------------------------
# Laps: Bronze → Silver
# ---------------------------------------------------------------------------

class TestBronzeToSilverLaps:

    def test_row_count_excludes_unmatched_driver(
        self, spark, bronze_laps, bronze_drivers
    ):
        """Driver 99 has no entry in drivers — inner join drops their laps."""
        result = transform_laps(bronze_laps, bronze_drivers, min_lap_duration_s=60.0)
        drivers_present = {r["driver_number"] for r in result.collect()}
        assert "99" not in drivers_present

    def test_total_rows_correct(self, spark, bronze_laps, bronze_drivers):
        # 5 VER laps + 2 HAM laps = 7 (driver 99 excluded)
        result = transform_laps(bronze_laps, bronze_drivers, min_lap_duration_s=60.0)
        assert result.count() == 7

    def test_valid_lap_count_correct(self, spark, bronze_laps, bronze_drivers):
        # VER valid: laps 1 & 2 (lap 3=null, lap 4=pit-out, lap 5=too short)
        # HAM valid: laps 1 & 2
        result = transform_laps(bronze_laps, bronze_drivers, min_lap_duration_s=60.0)
        valid = result.filter(result.is_valid_lap == True).count()
        assert valid == 4

    def test_flagged_lap_count_correct(self, spark, bronze_laps, bronze_drivers):
        result = transform_laps(bronze_laps, bronze_drivers, min_lap_duration_s=60.0)
        flagged = result.filter(result.is_valid_lap == False).count()
        assert flagged == 3   # null + pit-out + too short

    def test_driver_enrichment_correct(self, spark, bronze_laps, bronze_drivers):
        result = transform_laps(bronze_laps, bronze_drivers, min_lap_duration_s=60.0)
        ver_row = result.filter(result.driver_number == "1").first()
        assert ver_row["full_name"] == "Max Verstappen"
        assert ver_row["team_name"] == "Red Bull Racing"

    def test_duplicate_drivers_do_not_multiply_laps(
        self, spark, bronze_laps, bronze_drivers
    ):
        """Bronze driver 1 has a duplicate row — dedup must prevent lap fan-out."""
        result = transform_laps(bronze_laps, bronze_drivers, min_lap_duration_s=60.0)
        ver_laps = result.filter(result.driver_number == "1").count()
        assert ver_laps == 5   # exactly 5, not 10

    def test_silver_lap_schema_types(self, spark, bronze_laps, bronze_drivers):
        result = transform_laps(bronze_laps, bronze_drivers, min_lap_duration_s=60.0)
        schema = {f.name: f.dataType for f in result.schema.fields}
        assert isinstance(schema["lap_number"],    IntegerType)
        assert isinstance(schema["lap_duration"],  FloatType)
        assert isinstance(schema["is_pit_out_lap"], BooleanType)
        assert isinstance(schema["is_valid_lap"],  BooleanType)

    def test_processed_at_not_null(self, spark, bronze_laps, bronze_drivers):
        result = transform_laps(bronze_laps, bronze_drivers, min_lap_duration_s=60.0)
        nulls = result.filter(result.processed_at.isNull()).count()
        assert nulls == 0