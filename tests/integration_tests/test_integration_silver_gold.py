# =============================================================================
# F1-Pulse | Integration Tests — Silver → Gold
# File:     tests/integration_tests/test_integration_silver_gold.py
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
from pyspark.sql.types import (
    StructType, StructField, IntegerType, FloatType, DoubleType,
    BooleanType, StringType, TimestampType
)
from modules.gold_transforms import (
    build_driver_leaderboard,
    build_constructor_standings,
    build_lap_progression,
)


# ---------------------------------------------------------------------------
# Shared Silver-shaped fixture (Explicit Schema)
# ---------------------------------------------------------------------------

@pytest.fixture
def silver_laps(spark):
    """Silver laps with EXPLICIT schema to prevent PySpark inference failures."""
    schema = StructType([
        StructField("driver_number", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("team_name", StringType(), True),
        StructField("lap_number", IntegerType(), True),
        StructField("lap_duration", FloatType(), True),
        StructField("is_valid_lap", BooleanType(), True)
    ])
    
    data = [
        # VER — Red Bull — 5 valid + 1 flagged
        ("1",  "Max Verstappen", "Red Bull Racing", 1, 88.5, True),
        ("1",  "Max Verstappen", "Red Bull Racing", 2, 89.0, True),
        ("1",  "Max Verstappen", "Red Bull Racing", 3, 89.5, True),
        ("1",  "Max Verstappen", "Red Bull Racing", 4, 90.0, True),
        ("1",  "Max Verstappen", "Red Bull Racing", 5, 91.0, True),
        ("1",  "Max Verstappen", "Red Bull Racing", 6, 25.0, False),  # flagged
        
        # PER — Red Bull — 3 valid
        ("11", "Sergio Perez", "Red Bull Racing", 1, 91.0, True),
        ("11", "Sergio Perez", "Red Bull Racing", 2, 92.0, True),
        ("11", "Sergio Perez", "Red Bull Racing", 3, 93.0, True),
        
        # HAM — Ferrari — 3 valid
        ("44", "Lewis Hamilton", "Ferrari", 1, 94.0, True),
        ("44", "Lewis Hamilton", "Ferrari", 2, 95.0, True),
        ("44", "Lewis Hamilton", "Ferrari", 3, 96.0, True),
    ]
    return spark.createDataFrame(data, schema=schema)


# ---------------------------------------------------------------------------
# Driver Leaderboard
# ---------------------------------------------------------------------------

class TestSilverToGoldLeaderboard:

    def test_three_drivers_in_output(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        assert result.count() == 3

    def test_ver_is_rank_1(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        assert result.filter(result.position_rank == 1).first()["driver_number"] == "1"

    def test_ham_is_rank_3(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        assert result.filter(result.position_rank == 3).first()["driver_number"] == "44"

    def test_ranks_are_unique(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        ranks = [r["position_rank"] for r in result.collect()]
        assert len(ranks) == len(set(ranks))

    def test_flagged_lap_excluded_from_fastest(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        ver_row = result.filter(result.driver_number == "1").first()
        assert ver_row["fastest_lap_s"] == 88.5   # not 25.0

    def test_ver_total_valid_laps(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        assert result.filter(result.driver_number == "1").first()["total_valid_laps"] == 5

    def test_avg_lap_time_correct_for_ver(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        ver_row = result.filter(result.driver_number == "1").first()
        assert ver_row["avg_lap_time_s"] == 89.6   # mean of [88.5,89,89.5,90,91]

    def test_lap_consistency_not_null(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        assert result.filter(result.lap_consistency_s.isNull()).count() == 0

    def test_season_year_correct(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        assert all(r["season_year"] == 2026 for r in result.collect())

    def test_schema_types(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        schema = {f.name: f.dataType for f in result.schema.fields}
        assert isinstance(schema["position_rank"],    IntegerType)
        assert isinstance(schema["fastest_lap_s"],    (FloatType, DoubleType))
        assert isinstance(schema["total_valid_laps"], IntegerType)
        # Assuming generated_at is correctly implemented as a TimestampType
        assert isinstance(schema["generated_at"],     TimestampType)


# ---------------------------------------------------------------------------
# Constructor Standings
# ---------------------------------------------------------------------------

class TestSilverToGoldConstructors:

    def test_two_teams_in_output(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        assert result.count() == 2

    def test_red_bull_is_rank_1(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        assert result.filter(result.constructor_rank == 1).first()["team_name"] == "Red Bull Racing"

    def test_ferrari_is_rank_2(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        assert result.filter(result.constructor_rank == 2).first()["team_name"] == "Ferrari"

    def test_red_bull_best_lap_excludes_flagged(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        rb_row = result.filter(result.team_name == "Red Bull Racing").first()
        assert rb_row["best_lap_s"] == 88.5   # not 25.0

    def test_red_bull_total_valid_laps_both_drivers(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        rb_row = result.filter(result.team_name == "Red Bull Racing").first()
        assert rb_row["total_valid_laps"] == 8   # 5 VER + 3 PER

    def test_season_year_correct(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        assert all(r["season_year"] == 2026 for r in result.collect())


# ---------------------------------------------------------------------------
# Lap Progression
# ---------------------------------------------------------------------------

class TestSilverToGoldLapProgression:

    def test_valid_row_count(self, spark, silver_laps):
        result = build_lap_progression(silver_laps, season_year=2026)
        assert result.count() == 11   # 5 VER + 3 PER + 3 HAM

    def test_flagged_lap_not_in_output(self, spark, silver_laps):
        result = build_lap_progression(silver_laps, season_year=2026)
        assert result.filter(
            (result.driver_number == "1") & (result.lap_number == 6)
        ).count() == 0

    def test_rolling_avg_lap1_equals_lap_duration(self, spark, silver_laps):
        result = build_lap_progression(silver_laps, season_year=2026)
        ver_lap1 = result.filter(
            (result.driver_number == "1") & (result.lap_number == 1)
        ).first()
        assert ver_lap1["rolling_avg_5lap_s"] == ver_lap1["lap_duration_s"]

    def test_rolling_avg_lap5_window_of_5(self, spark, silver_laps):
        """VER laps 1–5: mean of [88.5, 89.0, 89.5, 90.0, 91.0] = 89.6"""
        result = build_lap_progression(silver_laps, season_year=2026)
        ver_lap5 = result.filter(
            (result.driver_number == "1") & (result.lap_number == 5)
        ).first()
        assert ver_lap5["rolling_avg_5lap_s"] == 89.6

    def test_rolling_avg_not_cross_driver(self, spark, silver_laps):
        result = build_lap_progression(silver_laps, season_year=2026)
        ham_lap1 = result.filter(
            (result.driver_number == "44") & (result.lap_number == 1)
        ).first()
        assert ham_lap1["rolling_avg_5lap_s"] == 94.0

    def test_output_ordered_by_name_and_lap(self, spark, silver_laps):
        result = build_lap_progression(silver_laps, season_year=2026)
        rows = [(r["full_name"], r["lap_number"]) for r in result.collect()]
        assert rows == sorted(rows, key=lambda x: (x[0], x[1]))

    def test_season_year_correct(self, spark, silver_laps):
        result = build_lap_progression(silver_laps, season_year=2026)
        assert all(r["season_year"] == 2026 for r in result.collect())