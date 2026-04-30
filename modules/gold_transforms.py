# =============================================================================
# F1-Pulse | Gold Layer — Transformation Logic
# Module:   modules/gold_transforms.py
# Author:   Jafar891
# Updated:  2026
#
# Pure aggregation functions.  No Spark I/O — receives DataFrames,
# returns DataFrames.  Call read_silver / write_gold from gold_helpers.py
# in the orchestrating notebook.
# =============================================================================

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg, col, count, current_timestamp, dense_rank,
    lit, min, percentile_approx, round as spark_round, stddev, coalesce,
)
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

log = logging.getLogger("f1_pulse.gold")


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def _valid_laps(laps: DataFrame) -> DataFrame:
    """
    Return only rows where is_valid_lap is True.

    Internal helper — not intended to be called directly from notebooks.
    """
    return laps.filter(col("is_valid_lap"))


# ---------------------------------------------------------------------------
# Gold Table 1 — Driver Pace Metrics
# ---------------------------------------------------------------------------

def build_driver_leaderboard(laps: DataFrame, season_year: int) -> DataFrame:
    """
    Aggregated per-driver pace metrics across valid race laps.

    Output columns
    --------------
    pace_rank          int     Rank by avg pace + consistency (1 = best).
    full_name          str     Driver full name.
    team_name          str     Constructor name.
    driver_number      str     Driver number.
    fastest_lap_s      float   Minimum valid lap duration in seconds.
    avg_lap_time_s     float   Mean valid lap duration in seconds.
    median_lap_time_s  float   Median valid lap duration in seconds.
    lap_consistency_s  float   Std-dev of valid lap durations (0 if single lap).
    total_valid_laps   int     Count of valid laps used in aggregation.
    generated_at       ts      Row generation timestamp.
    season_year        int     Season year from config (audit column).

    Args:
        laps:        Silver enriched_laps DataFrame.
        season_year: Season year written as an audit column.

    Returns:
        Aggregated DataFrame ordered by pace_rank.
    """
    log.info("  Building driver pace metrics …")

    agg = (
        _valid_laps(laps)
        .groupBy("driver_number", "full_name", "team_name")
        .agg(
            spark_round(min("lap_duration"), 3).alias("fastest_lap_s"),
            spark_round(avg("lap_duration"), 3).alias("avg_lap_time_s"),
            spark_round(percentile_approx("lap_duration", 0.5, 10000), 3).alias("median_lap_time_s"),
            spark_round(coalesce(stddev("lap_duration"), lit(0)), 3).alias("lap_consistency_s"),
            count("lap_number").cast(IntegerType()).alias("total_valid_laps"),
        )
    )

    rank_window = Window.orderBy(
        col("avg_lap_time_s").asc(),
        col("lap_consistency_s").asc(),
    )

    return (
        agg
        .withColumn("pace_rank", dense_rank().over(rank_window))
        .select(
            "pace_rank",
            "full_name",
            "team_name",
            "driver_number",
            "fastest_lap_s",
            "avg_lap_time_s",
            "median_lap_time_s",
            "lap_consistency_s",
            "total_valid_laps",
            current_timestamp().alias("generated_at"),
            lit(season_year).cast(IntegerType()).alias("season_year"),
        )
        .orderBy("pace_rank")
    )


# ---------------------------------------------------------------------------
# Gold Table 2 — Constructor Pace Metrics
# ---------------------------------------------------------------------------

def build_constructor_standings(laps: DataFrame, season_year: int) -> DataFrame:
    """
    Team-level pace metrics aggregated across all drivers.

    Output columns
    --------------
    team_pace_rank   int     Rank by avg team pace (1 = best).
    team_name        str     Constructor name.
    best_lap_s       float   Fastest individual lap across both drivers.
    avg_team_pace_s  float   Mean valid lap duration across both drivers.
    total_valid_laps int     Count of valid laps used in aggregation.
    generated_at     ts      Row generation timestamp.
    season_year      int     Season year from config (audit column).

    Args:
        laps:        Silver enriched_laps DataFrame.
        season_year: Season year written as an audit column.

    Returns:
        Aggregated DataFrame ordered by team_pace_rank.
    """
    log.info("  Building constructor pace metrics …")

    agg = (
        _valid_laps(laps)
        .groupBy("team_name")
        .agg(
            spark_round(min("lap_duration"), 3).alias("best_lap_s"),
            spark_round(avg("lap_duration"), 3).alias("avg_team_pace_s"),
            count("lap_number").cast(IntegerType()).alias("total_valid_laps"),
        )
    )

    rank_window = Window.orderBy(
        col("avg_team_pace_s").asc(),
        col("total_valid_laps").desc(),
    )

    return (
        agg
        .withColumn("team_pace_rank", dense_rank().over(rank_window))
        .select(
            "team_pace_rank",
            "team_name",
            "best_lap_s",
            "avg_team_pace_s",
            "total_valid_laps",
            current_timestamp().alias("generated_at"),
            lit(season_year).cast(IntegerType()).alias("season_year"),
        )
        .orderBy("team_pace_rank")
    )


# ---------------------------------------------------------------------------
# Gold Table 3 — Lap-by-Lap Progression
# ---------------------------------------------------------------------------

def build_lap_progression(laps: DataFrame, season_year: int) -> DataFrame:
    """
    Lap-level pace evolution per driver with a rolling 5-lap average.

    Output columns
    --------------
    full_name            str    Driver full name.
    team_name            str    Constructor name.
    driver_number        str    Driver number.
    lap_number           int    Lap number within the session.
    lap_duration_s       float  Valid lap duration in seconds.
    rolling_avg_5lap_s   float  Rolling mean over the previous 5 laps.
    generated_at         ts     Row generation timestamp.
    season_year          int    Season year from config (audit column).

    Args:
        laps:        Silver enriched_laps DataFrame.
        season_year: Season year written as an audit column.

    Returns:
        Lap-level DataFrame ordered by driver and lap number.
    """
    log.info("  Building lap progression (time-series) …")

    lap_window = (
        Window
        .partitionBy("driver_number")
        .orderBy("lap_number")
        .rowsBetween(-4, 0)
    )

    return (
        _valid_laps(laps)
        .withColumn(
            "rolling_avg_5lap_s",
            spark_round(avg("lap_duration").over(lap_window), 3),
        )
        .select(
            "full_name",
            "team_name",
            "driver_number",
            col("lap_number").cast(IntegerType()),
            spark_round(col("lap_duration"), 3).alias("lap_duration_s"),
            "rolling_avg_5lap_s",
            current_timestamp().alias("generated_at"),
            lit(season_year).cast(IntegerType()).alias("season_year"),
        )
        .orderBy("full_name", "lap_number")
    )