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
    lit, min, percentile_approx, round as spark_round, stddev,
)
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

log = logging.getLogger("f1_pulse.gold")


# ---------------------------------------------------------------------------
# Shared filter
# ---------------------------------------------------------------------------

def _valid_laps(laps: DataFrame) -> DataFrame:
    """
    Return only rows where ``is_valid_lap`` is True.
    Centralised so all three builders use identical filter logic.
    """
    return laps.filter(col("is_valid_lap") == True)  # noqa: E712


# ---------------------------------------------------------------------------
# Gold Table 1 — Driver Performance Leaderboard
# ---------------------------------------------------------------------------

def build_driver_leaderboard(laps: DataFrame, season_year: int) -> DataFrame:
    """
    Aggregated per-driver metrics across all valid race laps.

    Output schema
    -------------
    position_rank       int      Dense rank by fastest lap (1 = fastest).
    full_name           str
    team_name           str
    driver_number       str
    fastest_lap_s       float    Best single lap time   (3 dp).
    avg_lap_time_s      float    Mean lap time           (3 dp).
    median_lap_time_s   float    Median lap time         (3 dp).
    lap_consistency_s   float    Std-dev of lap times    (3 dp, lower = better).
    total_valid_laps    int
    generated_at        timestamp
    season_year         int

    Args:
        laps:        Silver ``enriched_laps`` DataFrame.
        season_year: Integer season year — written as audit column.

    Returns:
        Dashboard-ready Gold DataFrame ordered by ``position_rank``.
    """
    log.info("  Building driver leaderboard …")

    agg = (
        _valid_laps(laps)
        .groupBy("driver_number", "full_name", "team_name")
        .agg(
            spark_round(min("lap_duration"),                   3).alias("fastest_lap_s"),
            spark_round(avg("lap_duration"),                   3).alias("avg_lap_time_s"),
            spark_round(percentile_approx("lap_duration", 0.5), 3).alias("median_lap_time_s"),
            spark_round(stddev("lap_duration"),                3).alias("lap_consistency_s"),
            count("lap_number").cast(IntegerType())             .alias("total_valid_laps"),
        )
    )

    rank_window = Window.orderBy(col("fastest_lap_s").asc())

    return (
        agg
        .withColumn("position_rank", dense_rank().over(rank_window))
        .select(
            "position_rank",
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
        .orderBy("position_rank")
    )


# ---------------------------------------------------------------------------
# Gold Table 2 — Constructor Standings
# ---------------------------------------------------------------------------

def build_constructor_standings(laps: DataFrame, season_year: int) -> DataFrame:
    """
    Team-level summary — aggregates across both drivers per constructor.

    Output schema
    -------------
    constructor_rank    int      Dense rank by best lap (1 = fastest).
    team_name           str
    best_lap_s          float    Fastest lap from either driver  (3 dp).
    avg_team_pace_s     float    Mean lap time across all drivers (3 dp).
    total_valid_laps    int      Combined lap count for the team.
    generated_at        timestamp
    season_year         int

    Args:
        laps:        Silver ``enriched_laps`` DataFrame.
        season_year: Integer season year — written as audit column.

    Returns:
        Dashboard-ready Gold DataFrame ordered by ``constructor_rank``.
    """
    log.info("  Building constructor standings …")

    agg = (
        _valid_laps(laps)
        .groupBy("team_name")
        .agg(
            spark_round(min("lap_duration"), 3).alias("best_lap_s"),
            spark_round(avg("lap_duration"), 3).alias("avg_team_pace_s"),
            count("lap_number").cast(IntegerType()).alias("total_valid_laps"),
        )
    )

    rank_window = Window.orderBy(col("best_lap_s").asc())

    return (
        agg
        .withColumn("constructor_rank", dense_rank().over(rank_window))
        .select(
            "constructor_rank",
            "team_name",
            "best_lap_s",
            "avg_team_pace_s",
            "total_valid_laps",
            current_timestamp().alias("generated_at"),
            lit(season_year).cast(IntegerType()).alias("season_year"),
        )
        .orderBy("constructor_rank")
    )


# ---------------------------------------------------------------------------
# Gold Table 3 — Lap-by-Lap Progression
# ---------------------------------------------------------------------------

def build_lap_progression(laps: DataFrame, season_year: int) -> DataFrame:
    """
    Lap-level detail for charting driver pace evolution over a race.
    Includes a rolling 5-lap average for smoothed trend lines.

    Output schema
    -------------
    full_name           str
    team_name           str
    driver_number       str
    lap_number          int
    lap_duration_s      float    Individual lap time (3 dp).
    rolling_avg_5lap_s  float    Rolling mean of current + 4 preceding laps (3 dp).
    generated_at        timestamp
    season_year         int

    Rolling window
    --------------
    Partitioned by ``driver_number``, ordered by ``lap_number``,
    rows between -4 and current (5-lap lookback).

    Args:
        laps:        Silver ``enriched_laps`` DataFrame.
        season_year: Integer season year — written as audit column.

    Returns:
        Dashboard-ready Gold DataFrame ordered by ``full_name``, ``lap_number``.
    """
    log.info("  Building lap progression (time-series) …")

    lap_window = (
        Window
        .partitionBy("driver_number")
        .orderBy("lap_number")
        .rowsBetween(-4, 0)         # current + 4 preceding laps
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