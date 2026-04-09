import requests
import time
import logging
from typing import Optional

from config.config import (
    MAX_RETRIES,
    RETRY_DELAY_S,
    REQUEST_TIMEOUT_S
)

log = logging.getLogger("f1_pulse.api")


def fetch_with_retry(url: str, max_retries: int = MAX_RETRIES) -> Optional[list]:
    """
    GET request with retry and backoff.
    """
    for attempt in range(1, max_retries + 1):
        try:
            log.info(f"GET {url} (attempt {attempt}/{max_retries})")

            response = requests.get(url, timeout=REQUEST_TIMEOUT_S)
            response.raise_for_status()

            data = response.json()

            if not isinstance(data, list):
                log.error(f"Unexpected response format from {url}")
                return None

            log.info(f"✅ {len(data)} records received")
            return data

        except requests.exceptions.Timeout:
            log.warning(f"Timeout on attempt {attempt}")

        except requests.exceptions.HTTPError as e:
            log.error(f"HTTP error: {e}")

            # don't retry client errors
            if e.response is not None and e.response.status_code in (400, 401, 403, 404):
                break

        except requests.exceptions.RequestException as e:
            log.warning(f"Request error: {e}")

        if attempt < max_retries:
            log.info(f"Retrying in {RETRY_DELAY_S}s...")
            time.sleep(RETRY_DELAY_S)

    log.error(f"All {max_retries} attempts failed for {url}")
    return None