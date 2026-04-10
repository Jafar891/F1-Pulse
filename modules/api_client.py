# =============================================================================
# F1-Pulse | Bronze Layer — API Client
# Module:   modules/api_client.py
# Author:   Jafar891
# Updated:  2026
#
# HTTP client for the OpenF1 REST API.
# Provides retry logic with exponential backoff and client-error short-circuit.
# =============================================================================

import logging
import time
from typing import Optional

import requests

from config.config import MAX_RETRIES, REQUEST_TIMEOUT_S, RETRY_DELAY_S

log = logging.getLogger("f1_pulse.api")


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------

def fetch_with_retry(
    url: str,
    max_retries: int = MAX_RETRIES,
) -> Optional[list]:
    """
    GET a URL with retry and backoff. Expects a JSON list response.

    Retry behaviour
    ---------------
    - Retries on Timeout and generic RequestException.
    - Short-circuits immediately on 4xx client errors (no point retrying).
    - Waits ``RETRY_DELAY_S`` seconds between attempts.

    Args:
        url:         Full URL to fetch.
        max_retries: Maximum number of attempts (default from config).

    Returns:
        Parsed JSON list, or None if all attempts fail or response is invalid.
    """
    for attempt in range(1, max_retries + 1):

        try:
            log.info(f"GET {url}  (attempt {attempt}/{max_retries})")

            response = requests.get(url, timeout=REQUEST_TIMEOUT_S)
            response.raise_for_status()

            data = response.json()

            if not isinstance(data, list):
                log.error(f"Unexpected response format from {url}")
                return None

            log.info(f"  ✅ {len(data):,} records received")
            return data

        except requests.exceptions.Timeout:
            log.warning(f"  ⏱️  Timeout on attempt {attempt}")

        except requests.exceptions.HTTPError as e:
            log.error(f"  ❌ HTTP error: {e}")
            # Don't retry client errors — they won't resolve on their own
            if e.response is not None and e.response.status_code in (400, 401, 403, 404):
                break

        except requests.exceptions.RequestException as e:
            log.warning(f"  ⚠️  Request error: {e}")

        if attempt < max_retries:
            log.info(f"  Retrying in {RETRY_DELAY_S}s …")
            time.sleep(RETRY_DELAY_S)

    log.error(f"  ❌ All {max_retries} attempts failed for {url}")
    return None