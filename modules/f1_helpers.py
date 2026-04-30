# =============================================================================
# F1-Pulse | Bronze Layer — F1 Helpers
# Module:   modules/f1_helpers.py
# Author:   Jafar891
# Updated:  2026
#
# Utility functions for Bronze ingestion:
#   - Circuit-aware session resolution from the OpenF1 sessions payload.
#   - Pandas schema normalisation before Spark conversion.
#   - JSON list → Spark DataFrame conversion with audit columns.
# =============================================================================

import logging
from typing import Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, when
from pyspark.sql.types import NullType, StringType

from config.config import SESSION_TYPE, RACE_ROUND

log = logging.getLogger("f1_pulse.helpers")


# ---------------------------------------------------------------------------
# Session helper
# ---------------------------------------------------------------------------

def get_latest_race_session(session_data: list) -> Optional[dict]:
    """
    Extract the race session matching SESSION_TYPE and RACE_ROUND from config.

    Filters the sessions payload by both session type and circuit name.
    Falls back to the latest session by type if no circuit match is found,
    so ingestion can still proceed on unexpected API responses.

    Args:
        session_data: Raw list of session dicts from the OpenF1 API.

    Returns:
        The matching session dict, or None if the list is empty.
    """
    matched = [
        s for s in session_data
        if s.get("session_type") == SESSION_TYPE
        and s.get("circuit_short_name") == RACE_ROUND
    ]

    if matched:
        log.info(f"  ✅ Matched session for circuit='{RACE_ROUND}'")
        return matched[-1]

    log.warning(
        f"No session found for circuit='{RACE_ROUND}' — "
        f"falling back to latest '{SESSION_TYPE}' session."
    )
    race_sessions = [
        s for s in session_data
        if s.get("session_type") == SESSION_TYPE
    ]
    return race_sessions[-1] if race_sessions else None


# ---------------------------------------------------------------------------
# Pandas casting helper
# ---------------------------------------------------------------------------

def safe_cast_pdf(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Stringify only object / mixed-type columns so Spark can infer schema
    cleanly — without converting numeric or boolean columns to strings.

    Args:
        pdf: Raw Pandas DataFrame from ``pd.DataFrame(json_list)``.

    Returns:
        Pandas DataFrame with object columns safely cast to str.
    """
    for c in pdf.columns:
        if pdf[c].dtype == object:
            # Do not replace with None here — Spark drops data during schema inference
            pdf[c] = pdf[c].astype(str)
    return pdf


# ---------------------------------------------------------------------------
# Pandas → Spark conversion
# ---------------------------------------------------------------------------

def pdf_to_spark(
    spark: SparkSession,
    data: list,
    race_location: str,
) -> Optional[DataFrame]:
    """
    Convert a JSON list → Pandas → Spark DataFrame.

    Applies ``safe_cast_pdf`` before Spark conversion to avoid schema
    inference errors on mixed-type columns.

    Audit columns added
    -------------------
    ingested_at     timestamp   Wall-clock time the row was written to Bronze.
    race_location   string      Circuit location tag written to every row.

    Args:
        spark:         Active SparkSession.
        data:          Raw JSON list from ``fetch_with_retry``.
        race_location: Circuit location written as an audit column.

    Returns:
        Spark DataFrame with audit columns, or None if ``data`` is empty
        or conversion fails.
    """
    if not data:
        log.warning("Empty dataset — skipping Spark conversion.")
        return None

    try:
        pdf = pd.DataFrame(data)
        pdf = safe_cast_pdf(pdf)

        df = spark.createDataFrame(pdf)

        # Safely convert stringified "None" back to true Spark nulls
        # — restricted to StringType columns to avoid BIGINT cast errors
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType):
                c = field.name
                df = df.withColumn(
                    c,
                    when(col(c).isin("None", "nan", "NaN"), lit(None)).otherwise(col(c))
                )

        # Cast any fully-null columns from NullType to StringType
        # — prevents Databricks UI errors on unresolvable schema
        for field in df.schema.fields:
            if isinstance(field.dataType, NullType):
                df = df.withColumn(field.name, col(field.name).cast(StringType()))

        # Add audit columns
        df = (
            df.withColumn("ingested_at", current_timestamp())
              .withColumn("race_location", lit(race_location))
        )
        return df

    except Exception as e:
        log.error(f"DataFrame conversion failed: {e}")
        return None