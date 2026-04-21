# =============================================================================
# F1-Pulse | Unit Tests — silver_transforms.py
# File:     tests/unit_tests/test_silver_transforms.py
# Author:   Jafar891
# Updated:  2026
# =============================================================================

import path_setup  # noqa: F401  — inserts project root into sys.path

import pytest
from pyspark.sql import Row
from modules.silver_transforms import transform_sessions, transform_laps


# ---------------------------------------------------------------------------
# transform_sessions
# ---------------------------------------------------------------------------

class TestTransformSessions:

    @pytest.fixture
    def valid_sessions(self, spark):
        return spark.createDataFrame([
            Row(
                session_key=1, session_name="Abu Dhabi Grand Prix",
                session_type="Race", location="Yas Marina",
                country_name=" UAE ", date_start="2026-11-23T13:00:00",
                date_end="2026-11-23T15:00:00", year=2026,
                ingested_at="2026-01-01T00:00:00", source_url="http://fake"
            ),
            Row(
                session_key=2, session_name="Abu Dhabi Qualifying",
                session_type="Qualifying", location="Yas Marina",
                country_name="UAE", date_start="2026-11-22T13:00:00",
                date_end="2026-11-22T14:00:00", year=2026,
                ingested_at="2026-01-01T00:00:00", source_url="http://fake"
            ),
        ])

    def test_output_contains_expected_columns(self, spark, valid_sessions):
        result = transform_sessions(valid_sessions, pipeline_year=2026)
        expected = {
            "session_id", "session_name", "session_type", "location",
            "country_name", "start_time", "end_time", "year",
            "processed_at", "pipeline_year",
        }
        assert expected.issubset(set(result.columns))

    def test_filters_out_non_race_qualifying(self, spark):
        df = spark.createDataFrame([
            Row(
                session_key=10, session_name="FP1", session_type="Practice 1",
                location="Monaco", country_name="Monaco",
                date_start="2026-05-22T10:00:00", date_end="2026-05-22T11:00:00",
                year=2026, ingested_at="2026-01-01T00:00:00", source_url="http://fake"
            ),
            Row(
                session_key=11, session_name="Race", session_type="Race",
                location="Monaco", country_name="Monaco",
                date_start="2026-05-24T13:00:00", date_end="2026-05-24T15:00:00",
                year=2026, ingested_at="2026-01-01T00:00:00", source_url="http://fake"
            ),
        ])
        result = transform_sessions(df, pipeline_year=2026)
        assert result.count() == 1
        assert result.first()["session_id"] == 11

    def test_trims_country_name_whitespace(self, spark, valid_sessions):
        result = transform_sessions(valid_sessions, pipeline_year=2026)
        race_row = result.filter(result.session_id == 1).first()
        assert race_row["country_name"] == "UAE"

    def test_session_type_uppercased(self, spark):
        df = spark.createDataFrame([
            Row(
                session_key=20, session_name="Race", session_type="race",
                location="Spa", country_name="Belgium",
                date_start="2026-07-30T13:00:00", date_end="2026-07-30T15:00:00",
                year=2026, ingested_at="2026-01-01T00:00:00", source_url="http://fake"
            ),
        ])
        result = transform_sessions(df, pipeline_year=2026)
        assert result.first()["session_type"] == "RACE"

    def test_deduplicates_on_session_id(self, spark):
        df = spark.createDataFrame([
            Row(
                session_key=99, session_name="Race", session_type="Race",
                location="Monza", country_name="Italy",
                date_start="2026-09-06T13:00:00", date_end="2026-09-06T15:00:00",
                year=2026, ingested_at="2026-01-01T00:00:00", source_url="http://fake"
            ),
            Row(
                session_key=99, session_name="Race duplicate", session_type="Race",
                location="Monza", country_name="Italy",
                date_start="2026-09-06T13:00:00", date_end="2026-09-06T15:00:00",
                year=2026, ingested_at="2026-01-01T00:00:00", source_url="http://fake"
            ),
        ])
        result = transform_sessions(df, pipeline_year=2026)
        assert result.count() == 1

    def test_pipeline_year_audit_column(self, spark, valid_sessions):
        result = transform_sessions(valid_sessions, pipeline_year=2026)
        assert result.first()["pipeline_year"] == 2026

    def test_filters_null_session_key(self, spark):
        df = spark.createDataFrame([
            # Bad row with null key
            Row(
                session_key=None, session_name="Race", session_type="Race",
                location="Spa", country_name="Belgium",
                date_start="2026-07-30T13:00:00", date_end="2026-07-30T15:00:00",
                year=2026, ingested_at="2026-01-01T00:00:00", source_url="http://fake"
            ),
            # Valid row added so Spark knows session_key is an integer!
            Row(
                session_key=100, session_name="Valid", session_type="Race",
                location="Spa", country_name="Belgium",
                date_start="2026-07-30T13:00:00", date_end="2026-07-30T15:00:00",
                year=2026, ingested_at="2026-01-01T00:00:00", source_url="http://fake"
            )
        ])
        result = transform_sessions(df, pipeline_year=2026)
        assert result.count() == 1  # 1 valid row left

    def test_schema_drift_raises_value_error(self, spark):
        df = spark.createDataFrame([Row(session_key=1, session_name="x")])
        with pytest.raises(ValueError, match="Schema drift detected"):
            transform_sessions(df, pipeline_year=2026)


# ---------------------------------------------------------------------------
# transform_laps
# ---------------------------------------------------------------------------

class TestTransformLaps:

    @pytest.fixture
    def laps_df(self, spark):
        return spark.createDataFrame([
            Row(driver_number="1",  lap_number=1, lap_duration=90.5,  is_pit_out_lap=False),
            Row(driver_number="1",  lap_number=2, lap_duration=91.2,  is_pit_out_lap=False),
            Row(driver_number="11", lap_number=1, lap_duration=89.9,  is_pit_out_lap=False),
            Row(driver_number="11", lap_number=2, lap_duration=None,  is_pit_out_lap=False),
            Row(driver_number="1",  lap_number=3, lap_duration=30.0,  is_pit_out_lap=False),
            Row(driver_number="1",  lap_number=4, lap_duration=92.0,  is_pit_out_lap=True),
        ])

    @pytest.fixture
    def drivers_df(self, spark):
        return spark.createDataFrame([
            Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull",
                country_code="NLD", headshot_url="http://img/max"),
            Row(driver_number="11", full_name="Sergio Perez",   team_name="Red Bull",
                country_code="MEX", headshot_url="http://img/checo"),
        ])

    def test_output_contains_expected_columns(self, spark, laps_df, drivers_df):
        result = transform_laps(laps_df, drivers_df, min_lap_duration_s=60.0)
        expected = {
            "driver_number", "full_name", "team_name", "country_code",
            "headshot_url", "lap_number", "lap_duration", "is_pit_out_lap",
            "is_valid_lap", "processed_at",
        }
        assert expected.issubset(set(result.columns))

    def test_invalid_flag_null_lap_duration(self, spark, laps_df, drivers_df):
        result = transform_laps(laps_df, drivers_df, min_lap_duration_s=60.0)
        null_lap = result.filter(
            (result.driver_number == "11") & (result.lap_number == 2)
        ).first()
        assert null_lap["is_valid_lap"] is False

    def test_invalid_flag_below_min_duration(self, spark, laps_df, drivers_df):
        result = transform_laps(laps_df, drivers_df, min_lap_duration_s=60.0)
        short_lap = result.filter(
            (result.driver_number == "1") & (result.lap_number == 3)
        ).first()
        assert short_lap["is_valid_lap"] is False

    def test_invalid_flag_pit_out_lap(self, spark, laps_df, drivers_df):
        result = transform_laps(laps_df, drivers_df, min_lap_duration_s=60.0)
        pit_lap = result.filter(
            (result.driver_number == "1") & (result.lap_number == 4)
        ).first()
        assert pit_lap["is_valid_lap"] is False

    def test_valid_flag_normal_lap(self, spark, laps_df, drivers_df):
        result = transform_laps(laps_df, drivers_df, min_lap_duration_s=60.0)
        normal_lap = result.filter(
            (result.driver_number == "1") & (result.lap_number == 1)
        ).first()
        assert normal_lap["is_valid_lap"] is True

    def test_driver_enrichment_joined_correctly(self, spark, laps_df, drivers_df):
        result = transform_laps(laps_df, drivers_df, min_lap_duration_s=60.0)
        row = result.filter(result.driver_number == "1").first()
        assert row["full_name"] == "Max Verstappen"
        assert row["team_name"] == "Red Bull"

    def test_unmatched_driver_excluded_by_inner_join(self, spark, drivers_df):
        laps = spark.createDataFrame([
            Row(driver_number="99", lap_number=1, lap_duration=90.0, is_pit_out_lap=False),
        ])
        result = transform_laps(laps, drivers_df, min_lap_duration_s=60.0)
        assert result.count() == 0

    def test_laps_schema_drift_raises(self, spark, drivers_df):
        bad_laps = spark.createDataFrame([Row(driver_number="1")])
        with pytest.raises(ValueError, match="Schema drift detected"):
            transform_laps(bad_laps, drivers_df, min_lap_duration_s=60.0)

    def test_drivers_schema_drift_raises(self, spark, laps_df):
        bad_drivers = spark.createDataFrame([Row(driver_number="1")])
        with pytest.raises(ValueError, match="Schema drift detected"):
            transform_laps(laps_df, bad_drivers, min_lap_duration_s=60.0)