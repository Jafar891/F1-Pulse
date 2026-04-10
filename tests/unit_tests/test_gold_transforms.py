# =============================================================================
# F1-Pulse | Unit Tests — gold_transforms.py
# File:     tests/test_gold_transforms.py
# Author:   Jafar891
# Updated:  2026
#
# Tests for:
#   build_driver_leaderboard  — aggregations, ranking, valid-only filter
#   build_constructor_standings — team aggregation, ranking
#   build_lap_progression     — rolling window, lap ordering, valid-only filter
#
# Uses a real local SparkSession (via conftest.py) — no Delta writes.
# =============================================================================

import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pytest
from pyspark.sql import Row
from pyspark.sql.functions import col
from modules.gold_transforms import (
    build_driver_leaderboard,
    build_constructor_standings,
    build_lap_progression,
)


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def silver_laps(spark):
    """
    Minimal enriched_laps Silver DataFrame.
    Includes valid and flagged laps to verify Gold filters correctly.
    """
    return spark.createDataFrame([
        # VER — 3 valid laps
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull",
            lap_number=1, lap_duration=88.5, is_valid_lap=True),
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull",
            lap_number=2, lap_duration=89.1, is_valid_lap=True),
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull",
            lap_number=3, lap_duration=90.0, is_valid_lap=True),
        # HAM — 2 valid laps, slower
        Row(driver_number="44", full_name="Lewis Hamilton", team_name="Ferrari",
            lap_number=1, lap_duration=91.0, is_valid_lap=True),
        Row(driver_number="44", full_name="Lewis Hamilton", team_name="Ferrari",
            lap_number=2, lap_duration=92.0, is_valid_lap=True),
        # Flagged lap — must be excluded from all Gold tables
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull",
            lap_number=4, lap_duration=30.0, is_valid_lap=False),
    ])


# ---------------------------------------------------------------------------
# build_driver_leaderboard
# ---------------------------------------------------------------------------

class TestDriverLeaderboard:

    def test_output_columns_present(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        expected = {
            "position_rank", "full_name", "team_name", "driver_number",
            "fastest_lap_s", "avg_lap_time_s", "median_lap_time_s",
            "lap_consistency_s", "total_valid_laps", "generated_at", "season_year",
        }
        assert expected.issubset(set(result.columns))

    def test_excludes_flagged_laps(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        ver_row = result.filter(result.driver_number == "1").first()
        # Flagged lap (30.0s) must not pollute fastest_lap
        assert ver_row["fastest_lap_s"] == 88.5
        assert ver_row["total_valid_laps"] == 3

    def test_ranking_fastest_driver_is_rank_1(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        rank1 = result.filter(result.position_rank == 1).first()
        assert rank1["driver_number"] == "1"   # VER fastest

    def test_ranking_slower_driver_is_rank_2(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        rank2 = result.filter(result.position_rank == 2).first()
        assert rank2["driver_number"] == "44"

    def test_season_year_audit_column(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        assert result.first()["season_year"] == 2026

    def test_total_valid_laps_correct(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        ham_row = result.filter(result.driver_number == "44").first()
        assert ham_row["total_valid_laps"] == 2

    def test_row_count_equals_driver_count(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        assert result.count() == 2   # VER + HAM


# ---------------------------------------------------------------------------
# build_constructor_standings
# ---------------------------------------------------------------------------

class TestConstructorStandings:

    def test_output_columns_present(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        expected = {
            "constructor_rank", "team_name", "best_lap_s",
            "avg_team_pace_s", "total_valid_laps", "generated_at", "season_year",
        }
        assert expected.issubset(set(result.columns))

    def test_row_count_equals_team_count(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        assert result.count() == 2   # Red Bull + Ferrari

    def test_fastest_team_is_rank_1(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        rank1 = result.filter(result.constructor_rank == 1).first()
        assert rank1["team_name"] == "Red Bull"

    def test_excludes_flagged_laps_from_best_lap(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        rb_row = result.filter(result.team_name == "Red Bull").first()
        assert rb_row["best_lap_s"] == 88.5   # not 30.0 (flagged)

    def test_total_valid_laps_sums_all_drivers(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        rb_row = result.filter(result.team_name == "Red Bull").first()
        assert rb_row["total_valid_laps"] == 3


# ---------------------------------------------------------------------------
# build_lap_progression
# ---------------------------------------------------------------------------

class TestLapProgression:

    def test_output_columns_present(self, spark, silver_laps):
        result = build_lap_progression(silver_laps, season_year=2026)
        expected = {
            "full_name", "team_name", "driver_number", "lap_number",
            "lap_duration_s", "rolling_avg_5lap_s", "generated_at", "season_year",
        }
        assert expected.issubset(set(result.columns))

    def test_excludes_flagged_laps(self, spark, silver_laps):
        result = build_lap_progression(silver_laps, season_year=2026)
        # Lap 4 for VER is flagged — must not appear
        flagged = result.filter(
            (result.driver_number == "1") & (result.lap_number == 4)
        )
        assert flagged.count() == 0

    def test_total_row_count_valid_laps_only(self, spark, silver_laps):
        result = build_lap_progression(silver_laps, season_year=2026)
        assert result.count() == 5   # 3 VER valid + 2 HAM valid

    def test_rolling_avg_first_lap_equals_lap_duration(self, spark, silver_laps):
        """On lap 1 there are no prior laps — rolling avg == lap duration."""
        result = build_lap_progression(silver_laps, season_year=2026)
        ver_lap1 = result.filter(
            (result.driver_number == "1") & (result.lap_number == 1)
        ).first()
        assert ver_lap1["rolling_avg_5lap_s"] == ver_lap1["lap_duration_s"]

    def test_rolling_avg_uses_correct_window(self, spark):
        """
        With 3 laps of known values, lap 3's rolling avg should be
        the mean of laps 1–3 (all within 5-lap window).
        """
        laps = spark.createDataFrame([
            Row(driver_number="1", full_name="VER", team_name="RB",
                lap_number=1, lap_duration=90.0, is_valid_lap=True),
            Row(driver_number="1", full_name="VER", team_name="RB",
                lap_number=2, lap_duration=92.0, is_valid_lap=True),
            Row(driver_number="1", full_name="VER", team_name="RB",
                lap_number=3, lap_duration=94.0, is_valid_lap=True),
        ])
        result = build_lap_progression(laps, season_year=2026)
        lap3 = result.filter(result.lap_number == 3).first()
        # Mean of [90.0, 92.0, 94.0] = 92.0
        assert lap3["rolling_avg_5lap_s"] == 92.0

    def test_rolling_avg_partitioned_by_driver(self, spark, silver_laps):
        """Rolling avg must NOT bleed across drivers."""
        result = build_lap_progression(silver_laps, season_year=2026)
        ham_lap1 = result.filter(
            (result.driver_number == "44") & (result.lap_number == 1)
        ).first()
        # HAM lap 1 rolling avg = only HAM lap 1 (not mixed with VER laps)
        assert ham_lap1["rolling_avg_5lap_s"] == 91.0

    def test_ordered_by_full_name_and_lap_number(self, spark, silver_laps):
        result = build_lap_progression(silver_laps, season_year=2026)
        rows = [(r["full_name"], r["lap_number"]) for r in result.collect()]
        assert rows == sorted(rows, key=lambda x: (x[0], x[1]))