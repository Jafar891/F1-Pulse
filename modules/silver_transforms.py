# =============================================================================
# F1-Pulse | Silver Layer — Transformation Logic
# Module:   modules/silver_transforms.py
# Author:   Jafar891
# Updated:  2026
#
# Pure transformation functions: schema enforcement, type casting,
# filtering, enrichment joins.  No Spark I/O here — call read_bronze /
# write_silver from silver_helpers.py in the orchestrating notebook.
# =============================================================================

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, trim, upper, when,
    to_timestamp,
)
from pyspark.sql.types import BooleanType, FloatType, IntegerType

from modules.silver_helpers import assert_columns, log_quality_check

log = logging.getLogger("f1_pulse.silver")


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------

#: Columns that must exist in the Bronze sessions table.
SESSIONS_REQUIRED_COLS: list[str] = [
    "date_start", "date_end", "session_type", "session_key",
    "session_name", "location", "country_name", "year",
]


def transform_sessions(
    bronze_sessions: DataFrame,
    pipeline_year: int,
) -> DataFrame:
    """
    Clean and standardise the raw sessions table.

    Steps
    -----
    1. Assert required columns (schema-drift guard).
    2. Cast timestamps; normalise string columns.
    3. Filter to RACE + QUALIFYING session types only.
    4. Drop null session_key / start_time rows.
    5. Deduplicate on session_id.
    6. Select & rename to Silver contract columns.

    Args:
        bronze_sessions: Raw DataFrame from ``bronze.raw_sessions_<year>``.
        pipeline_year:   Integer year injected as ``pipeline_year`` audit col.

    Returns:
        Cleaned, typed DataFrame ready for ``silver.cleaned_sessions``.
    """
    assert_columns(bronze_sessions, SESSIONS_REQUIRED_COLS, "bronze.raw_sessions")

    total = bronze_sessions.count()

    df = (
        bronze_sessions
        .withColumn("start_time",   to_timestamp(col("date_start")))
        .withColumn("end_time",     to_timestamp(col("date_end")))
        .withColumn("country_name", trim(col("country_name")))
        .withColumn("session_type", trim(upper(col("session_type"))))
        .filter(col("session_type").isin("RACE", "QUALIFYING"))
        .filter(col("session_key").isNotNull())
        .filter(col("start_time").isNotNull())
        .select(
            col("session_key").cast(IntegerType()).alias("session_id"),
            trim(col("session_name")).alias("session_name"),
            col("session_type"),
            col("location"),
            col("country_name"),
            col("start_time"),
            col("end_time"),
            col("year").cast(IntegerType()).alias("year"),
            current_timestamp().alias("processed_at"),
            lit(pipeline_year).cast(IntegerType()).alias("pipeline_year"),
        )
        .dropDuplicates(["session_id"])
    )

    log_quality_check("Sessions", total, df.count())
    return df


# ---------------------------------------------------------------------------
# Laps
# ---------------------------------------------------------------------------

#: Columns that must exist in the Bronze laps table.
LAPS_REQUIRED_COLS: list[str] = [
    "driver_number", "lap_number", "lap_duration", "is_pit_out_lap",
]

#: Columns that must exist in the Bronze drivers table.
DRIVERS_REQUIRED_COLS: list[str] = [
    "driver_number", "full_name", "team_name",
]


def _clean_drivers(bronze_drivers: DataFrame) -> DataFrame:
    """
    Deduplicate and normalise the drivers lookup table.

    Internal helper — not intended to be called directly from notebooks.
    Keeps one record per ``driver_number``; trims whitespace introduced
    by the Bronze safe_cast_pdf step.

    Args:
        bronze_drivers: Raw DataFrame from ``bronze.raw_drivers_<year>``.

    Returns:
        Deduplicated drivers DataFrame with clean string columns.
    """
    df = (
        bronze_drivers
        .select(
            trim(col("driver_number")).alias("driver_number"),
            trim(col("full_name")).alias("full_name"),
            trim(col("team_name")).alias("team_name"),
            col("country_code"),
            col("headshot_url"),
        )
        .dropDuplicates(["driver_number"])
    )
    log.info(f"  👤 Unique drivers in Bronze: {df.count()}")
    return df


def transform_laps(
    bronze_laps: DataFrame,
    bronze_drivers: DataFrame,
    min_lap_duration_s: float,
) -> DataFrame:
    """
    Enrich laps with driver metadata and apply data-quality flags.

    Steps
    -----
    1. Assert required columns on both inputs.
    2. Deduplicate and normalise the drivers lookup.
    3. Cast lap columns to correct types (undoing Bronze object→str blanket cast).
    4. Inner-join laps ↔ drivers on ``driver_number``.
    5. Attach ``is_valid_lap`` flag — bad rows are kept for traceability.
    6. Select Silver contract columns.

    Quality flag logic (``is_valid_lap = False`` when)
    ---------------------------------------------------
    - ``is_pit_out_lap`` is True
    - ``lap_duration`` is NULL
    - ``lap_duration`` < ``min_lap_duration_s``

    Args:
        bronze_laps:        Raw DataFrame from ``bronze.raw_laps_<year>``.
        bronze_drivers:     Raw DataFrame from ``bronze.raw_drivers_<year>``.
        min_lap_duration_s: Minimum credible lap time in seconds (from config).

    Returns:
        Enriched, typed, quality-flagged DataFrame for ``silver.enriched_laps``.
    """
    assert_columns(bronze_laps,    LAPS_REQUIRED_COLS,    "bronze.raw_laps")
    assert_columns(bronze_drivers, DRIVERS_REQUIRED_COLS, "bronze.raw_drivers")

    total_laps = bronze_laps.count()

    drivers_clean = _clean_drivers(bronze_drivers)

    laps_typed = (
        bronze_laps
        .withColumn("lap_number",    col("lap_number").cast(IntegerType()))
        .withColumn("lap_duration",  col("lap_duration").cast(FloatType()))
        .withColumn("is_pit_out_lap", col("is_pit_out_lap").cast(BooleanType()))
        .withColumn("driver_number", trim(col("driver_number")))
    )

    enriched = laps_typed.join(drivers_clean, on="driver_number", how="inner")

    enriched = enriched.withColumn(
        "is_valid_lap",
        when(col("is_pit_out_lap") == True, False)
        .when(col("lap_duration").isNull(), False)
        .when(col("lap_duration") < min_lap_duration_s, False)
        .otherwise(True),
    )

    result = enriched.select(
        col("driver_number"),
        col("full_name"),
        col("team_name"),
        col("country_code"),
        col("headshot_url"),
        col("lap_number"),
        col("lap_duration"),
        col("is_pit_out_lap"),
        col("is_valid_lap"),
        current_timestamp().alias("processed_at"),
    )

    # Derive valid/flagged counts from a single cached result
    
    kept  = result.count()
    valid = result.filter(col("is_valid_lap") == True).count()

    log_quality_check("Laps (total)", total_laps, kept)
    log.info(f"  ✅ Valid laps   (is_valid_lap=True) : {valid:,}")
    log.info(f"  ⚠️  Flagged laps (is_valid_lap=False): {kept - valid:,}")

    return result