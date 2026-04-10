# =============================================================================
# F1-Pulse | Bronze Layer — F1 Helpers
# Module:   modules/f1_helpers.py
# Author:   Jafar891
# Updated:  2026
#
# Utility functions for Bronze ingestion:
#   - Session resolution from the OpenF1 sessions payload.
#   - Pandas schema normalisation before Spark conversion.
#   - JSON list → Spark DataFrame conversion with audit columns.
# =============================================================================

import logging
from typing import Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit

from config.config import SESSION_TYPE

log = logging.getLogger("f1_pulse.helpers")


# ---------------------------------------------------------------------------
# Session helper
# ---------------------------------------------------------------------------

def get_latest_race_session(session_data: list) -> Optional[dict]:
    """
    Safely extract the latest race session from the sessions payload.

    Filters by ``session_type == SESSION_TYPE`` (config).
    Falls back to the last record in the list if no match is found,
    so ingestion can still proceed on unexpected API responses.

    Args:
        session_data: Raw list of session dicts from the OpenF1 API.

    Returns:
        The most recent matching session dict, or None if the list is empty.
    """
    race_sessions = [
        s for s in session_data
        if s.get("session_type") == SESSION_TYPE
    ]

    if race_sessions:
        return race_sessions[-1]

    log.warning(
        f"No session_type='{SESSION_TYPE}' found — falling back to last record."
    )
    return session_data[-1] if session_data else None


# ---------------------------------------------------------------------------
# Pandas casting helper
# ---------------------------------------------------------------------------

def safe_cast_pdf(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Stringify only object / mixed-type columns so Spark can infer schema
    cleanly — without converting numeric or boolean columns to strings.

    ``None`` values in stringified columns are restored to Python ``None``
    rather than the literal string ``"None"``.

    Args:
        pdf: Raw Pandas DataFrame from ``pd.DataFrame(json_list)``.

    Returns:
        Pandas DataFrame with object columns safely cast to str.
    """
    for col in pdf.columns:
        if pdf[col].dtype == object:
            pdf[col] = pdf[col].astype(str).replace("None", None)
    return pdf


# ---------------------------------------------------------------------------
# Pandas → Spark conversion
# ---------------------------------------------------------------------------

def pdf_to_spark(
    spark: SparkSession,
    data: list,
    source_url: str,
) -> Optional[DataFrame]:
    """
    Convert a JSON list → Pandas → Spark DataFrame.

    Applies ``safe_cast_pdf`` before Spark conversion to avoid schema
    inference errors on mixed-type columns.

    Audit columns added
    -------------------
    ingested_at   timestamp   Wall-clock time the row was written to Bronze.
    source_url    string      API URL the data was fetched from.

    Args:
        spark:      Active SparkSession.
        data:       Raw JSON list from ``fetch_with_retry``.
        source_url: The URL that produced this data (written as audit column).

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
        df = (
            df.withColumn("ingested_at", current_timestamp())
              .withColumn("source_url",  lit(source_url))
        )
        return df

    except Exception as e:
        log.error(f"DataFrame conversion failed: {e}")
        return None