# =============================================================================
# F1-Pulse | Integration Tests — Silver → Gold
# File:     tests/test_integration_silver_gold.py
# Author:   Jafar891
# Updated:  2026
#
# Validates that Silver-shaped enriched_laps flows correctly through all
# three Gold builders.  No Delta I/O — pure DataFrame-in, DataFrame-out.
#
# Covers:
#   - Aggregation correctness (fastest lap, avg, median, std-dev)
#   - Ranking correctness and uniqueness
#   - Rolling window isolation per driver
#   - Flagged laps excluded from all Gold outputs
#   - Audit columns present and correct
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
from pyspark.sql.types import IntegerType, FloatType, TimestampType
from modules.gold_transforms import (
    build_driver_leaderboard,
    build_constructor_standings,
    build_lap_progression,
)


# ---------------------------------------------------------------------------
# Shared Silver-shaped fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def silver_laps(spark):
    """
    Realistic Silver enriched_laps payload with 3 drivers across 2 teams.
    Includes flagged laps to validate Gold exclusion logic.
    """
    return spark.createDataFrame([
        # VER — Red Bull — 5 valid laps + 1 flagged
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull Racing",
            lap_number=1, lap_duration=88.5, is_valid_lap=True),
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull Racing",
            lap_number=2, lap_duration=89.0, is_valid_lap=True),
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull Racing",
            lap_number=3, lap_duration=89.5, is_valid_lap=True),
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull Racing",
            lap_number=4, lap_duration=90.0, is_valid_lap=True),
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull Racing",
            lap_number=5, lap_duration=91.0, is_valid_lap=True),
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull Racing",
            lap_number=6, lap_duration=25.0, is_valid_lap=False),  # flagged
        # PER — Red Bull — 3 valid laps (slower than VER)
        Row(driver_number="11", full_name="Sergio Perez", team_name="Red Bull Racing",
            lap_number=1, lap_duration=91.0, is_valid_lap=True),
        Row(driver_number="11", full_name="Sergio Perez", team_name="Red Bull Racing",
            lap_number=2, lap_duration=92.0, is_valid_lap=True),
        Row(driver_number="11", full_name="Sergio Perez", team_name="Red Bull Racing",
            lap_number=3, lap_duration=93.0, is_valid_lap=True),
        # HAM — Ferrari — 3 valid laps (slowest)
        Row(driver_number="44", full_name="Lewis Hamilton", team_name="Ferrari",
            lap_number=1, lap_duration=94.0, is_valid_lap=True),
        Row(driver_number="44", full_name="Lewis Hamilton", team_name="Ferrari",
            lap_number=2, lap_duration=95.0, is_valid_lap=True),
        Row(driver_number="44", full_name="Lewis Hamilton", team_name="Ferrari",
            lap_number=3, lap_duration=96.0, is_valid_lap=True),
    ])


# ---------------------------------------------------------------------------
# Driver Leaderboard
# ---------------------------------------------------------------------------

class TestSilverToGoldLeaderboard:

    def test_three_drivers_in_output(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        assert result.count() == 3

    def test_ver_is_rank_1(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        rank1 = result.filter(result.position_rank == 1).first()
        assert rank1["driver_number"] == "1"

    def test_ham_is_rank_3(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        rank3 = result.filter(result.position_rank == 3).first()
        assert rank3["driver_number"] == "44"

    def test_ranks_are_unique(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        ranks = [r["position_rank"] for r in result.collect()]
        assert len(ranks) == len(set(ranks))

    def test_flagged_lap_excluded_from_fastest(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        ver_row = result.filter(result.driver_number == "1").first()
        assert ver_row["fastest_lap_s"] == 88.5  # not 25.0

    def test_ver_total_valid_laps(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        ver_row = result.filter(result.driver_number == "1").first()
        assert ver_row["total_valid_laps"] == 5  # flagged lap excluded

    def test_avg_lap_time_correct_for_ver(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        ver_row = result.filter(result.driver_number == "1").first()
        # Mean of [88.5, 89.0, 89.5, 90.0, 91.0] = 89.6
        assert ver_row["avg_lap_time_s"] == 89.6

    def test_lap_consistency_not_null(self, spark, silver_laps):
        """Std-dev requires >1 lap — all drivers here have multiple."""
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        nulls = result.filter(result.lap_consistency_s.isNull()).count()
        assert nulls == 0

    def test_season_year_correct(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        assert all(r["season_year"] == 2026 for r in result.collect())

    def test_schema_types(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        schema = {f.name: f.dataType for f in result.schema.fields}
        assert isinstance(schema["position_rank"],     IntegerType)
        assert isinstance(schema["fastest_lap_s"],     FloatType)
        assert isinstance(schema["total_valid_laps"],  IntegerType)
        assert isinstance(schema["generated_at"],      TimestampType)


# ---------------------------------------------------------------------------
# Constructor Standings
# ---------------------------------------------------------------------------

class TestSilverToGoldConstructors:

    def test_two_teams_in_output(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        assert result.count() == 2

    def test_red_bull_is_rank_1(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        rank1 = result.filter(result.constructor_rank == 1).first()
        assert rank1["team_name"] == "Red Bull Racing"

    def test_ferrari_is_rank_2(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        rank2 = result.filter(result.constructor_rank == 2).first()
        assert rank2["team_name"] == "Ferrari"

    def test_red_bull_best_lap_excludes_flagged(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        rb_row = result.filter(result.team_name == "Red Bull Racing").first()
        assert rb_row["best_lap_s"] == 88.5  # not 25.0

    def test_red_bull_total_valid_laps_both_drivers(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        rb_row = result.filter(result.team_name == "Red Bull Racing").first()
        assert rb_row["total_valid_laps"] == 8  # 5 VER + 3 PER

    def test_season_year_correct(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        assert all(r["season_year"] == 2026 for r in result.collect())


# ---------------------------------------------------------------------------
# Lap Progression
# ---------------------------------------------------------------------------

class TestSilverToGoldLapProgression:

    def test_valid_row_count(self, spark, silver_laps):
        # 5 VER + 3 PER + 3 HAM = 11 valid (1 VER flagged excluded)
        result = build_lap_progression(silver_laps, season_year=2026)
        assert result.count() == 11

    def test_flagged_lap_not_in_output(self, spark, silver_laps):
        result = build_lap_progression(silver_laps, season_year=2026)
        flagged = result.filter(
            (result.driver_number == "1") & (result.lap_number == 6)
        ).count()
        assert flagged == 0

    def test_rolling_avg_lap1_equals_lap_duration(self, spark, silver_laps):
        result = build_lap_progression(silver_laps, season_year=2026)
        ver_lap1 = result.filter(
            (result.driver_number == "1") & (result.lap_number == 1)
        ).first()
        assert ver_lap1["rolling_avg_5lap_s"] == ver_lap1["lap_duration_s"]

    def test_rolling_avg_lap5_window_of_5(self, spark, silver_laps):
        """VER laps 1–5: mean of [88.5,89.0,89.5,90.0,91.0] = 89.6"""
        result = build_lap_progression(silver_laps, season_year=2026)
        ver_lap5 = result.filter(
            (result.driver_number == "1") & (result.lap_number == 5)
        ).first()
        assert ver_lap5["rolling_avg_5lap_s"] == 89.6

    def test_rolling_avg_not_cross_driver(self, spark, silver_laps):
        """HAM lap 1 rolling avg must only reflect HAM data, not VER/PER."""
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