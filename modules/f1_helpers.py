import logging
import pandas as pd
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit

from config.config import SESSION_TYPE

log = logging.getLogger("f1_pulse.helpers")


# ---------------------------------------------------------------------------
# Session helper
# ---------------------------------------------------------------------------
def get_latest_race_session(session_data: list) -> Optional[dict]:
    """
    Safely extract the latest race session from the sessions list.
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
    Stringify only object/mixed-type columns so Spark can infer schema
    without converting numeric/boolean columns.
    """
    for col in pdf.columns:
        if pdf[col].dtype == object:
            pdf[col] = pdf[col].astype(str).replace("None", None)

    return pdf


# ---------------------------------------------------------------------------
# Pandas -> Spark helper
# ---------------------------------------------------------------------------
def pdf_to_spark(
    spark,
    data: list,
    source_url: str
) -> Optional[DataFrame]:
    """
    Convert JSON list -> Pandas -> Spark DataFrame
    Adds audit columns:
        - ingested_at
        - source_url
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
              .withColumn("source_url", lit(source_url))
        )

        return df

    except Exception as e:
        log.error(f"DataFrame conversion failed: {e}")
        return None