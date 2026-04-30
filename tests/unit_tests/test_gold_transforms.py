# =============================================================================
# F1-Pulse | Unit Tests — gold_transforms.py
# Updated to match Baku Circuit Logic and renamed Rank Columns
# =============================================================================

import path_setup  # noqa: F401
import pytest
from pyspark.sql import Row
from modules.gold_transforms import (
    build_driver_leaderboard,
    build_constructor_standings,
    build_lap_progression,
)

@pytest.fixture
def silver_laps(spark):
    return spark.createDataFrame([
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull",
            lap_number=1, lap_duration=88.5, is_valid_lap=True),
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull",
            lap_number=2, lap_duration=89.1, is_valid_lap=True),
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull",
            lap_number=3, lap_duration=90.0, is_valid_lap=True),
        Row(driver_number="1",  full_name="Max Verstappen", team_name="Red Bull",
            lap_number=4, lap_duration=30.0, is_valid_lap=False),
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
            "pace_rank", # CHANGED from position_rank
            "full_name", "team_name", "driver_number",
            "fastest_lap_s", "avg_lap_time_s", "median_lap_time_s",
            "lap_consistency_s", "total_valid_laps", "generated_at", "season_year",
        }
        assert expected.issubset(set(result.columns))

    def test_ranking_fastest_driver_is_rank_1(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        # CHANGED attribute to pace_rank
        rank1 = result.filter(result.pace_rank == 1).first()
        assert rank1["driver_number"] == "1"

    def test_ranking_slower_driver_is_rank_2(self, spark, silver_laps):
        result = build_driver_leaderboard(silver_laps, season_year=2026)
        # CHANGED attribute to pace_rank
        rank2 = result.filter(result.pace_rank == 2).first()
        assert rank2["driver_number"] == "44"

    # ... (Keep other methods as they are) ...

# ---------------------------------------------------------------------------
# build_constructor_standings
# ---------------------------------------------------------------------------

class TestConstructorStandings:

    def test_output_columns_present(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        expected = {
            "team_pace_rank", # CHANGED from constructor_rank
            "team_name", "best_lap_s",
            "avg_team_pace_s", "total_valid_laps", "generated_at", "season_year",
        }
        assert expected.issubset(set(result.columns))

    def test_fastest_team_is_rank_1(self, spark, silver_laps):
        result = build_constructor_standings(silver_laps, season_year=2026)
        # CHANGED attribute to team_pace_rank
        rank1 = result.filter(result.team_pace_rank == 1).first()
        assert rank1["team_name"] == "Red Bull"

    # ... (Keep other methods as they are) ...