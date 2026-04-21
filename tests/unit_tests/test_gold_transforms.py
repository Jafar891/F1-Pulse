# =============================================================================
# F1-Pulse | Unit Tests — gold_transforms.py
# File:     tests/unit_tests/test_gold_transforms.py
# Author:   Jafar891
# Updated:  2026
# =============================================================================

import path_setup  # noqa: F401  — inserts project root into sys.path

import pytest
from pyspark.sql import Row
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
    return spark.createDataFrame([
        # VER — 3 valid laps + 1 flagged
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull",
            lap_number=1, lap_duration=88.5, is_valid_lap=True),
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull",
            lap_number=2, lap_duration=89.1, is_valid_lap=True),
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull",
            lap_number=3, lap_duration=90.0, is_valid_lap=True),
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull",
            lap_number=4, lap_duration=30.0, is_valid_lap=False),   # flagged
        # HAM — 2 valid laps, slower
        Row(driver_number="44", full_name="Lewis Hamilton", team_name="Ferrari",
            lap_number=1, lap_duration=91.0, is_valid_lap=True),
        Row(driver_number="44", full_name="Lewis Hamilton", team_name="Ferrari",
            lap_number=2, lap_duration=92.0, is_valid_lap=True),
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
        assert ver_row["fastest_lap_s"] == 88.5   # not 30.0
        assert ver_row["total_valid_laps"] == 3

    def test_ranking_fastest_driver_is_rank_1(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        rank1 = result.filter(result.position_rank == 1).first()
        assert rank1["driver_number"] == "1"

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
        assert result.count() == 2


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
        assert result.count() == 2

    def test_fastest_team_is_rank_1(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        rank1 = result.filter(result.constructor_rank == 1).first()
        assert rank1["team_name"] == "Red Bull"

    def test_excludes_flagged_laps_from_best_lap(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        rb_row = result.filter(result.team_name == "Red Bull").first()
        assert rb_row["best_lap_s"] == 88.5   # not 30.0

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
        flagged = result.filter(
            (result.driver_number == "1") & (result.lap_number == 4)
        )
        assert flagged.count() == 0

    def test_total_row_count_valid_laps_only(self, spark, silver_laps):
        result = build_lap_progression(silver_laps, season_year=2026)
        assert result.count() == 5   # 3 VER + 2 HAM

    def test_rolling_avg_first_lap_equals_lap_duration(self, spark, silver_laps):
        result = build_lap_progression(silver_laps, season_year=2026)
        ver_lap1 = result.filter(
            (result.driver_number == "1") & (result.lap_number == 1)
        ).first()
        assert ver_lap1["rolling_avg_5lap_s"] == ver_lap1["lap_duration_s"]

    def test_rolling_avg_uses_correct_window(self, spark):
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
        assert lap3["rolling_avg_5lap_s"] == 92.0   # mean of [90, 92, 94]

    def test_rolling_avg_partitioned_by_driver(self, spark, silver_laps):
        result = build_lap_progression(silver_laps, season_year=2026)
        ham_lap1 = result.filter(
            (result.driver_number == "44") & (result.lap_number == 1)
        ).first()
        assert ham_lap1["rolling_avg_5lap_s"] == 91.0

    def test_ordered_by_full_name_and_lap_number(self, spark, silver_laps):
        result = build_lap_progression(silver_laps, season_year=2026)
        rows = [(r["full_name"], r["lap_number"]) for r in result.collect()]
        assert rows == sorted(rows, key=lambda x: (x[0], x[1]))