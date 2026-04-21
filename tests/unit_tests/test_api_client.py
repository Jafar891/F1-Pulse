# =============================================================================
# F1-Pulse | Unit Tests — api_client.py
# File:     tests/unit_tests/test_api_client.py
# Author:   Jafar891
# Updated:  2026
# =============================================================================

import path_setup  # noqa: F401  — inserts project root into sys.path

import pytest
import requests
from unittest import mock
from modules.api_client import fetch_with_retry


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def patch_config():
    """Patch config constants to keep tests fast and deterministic."""
    with mock.patch("modules.api_client.MAX_RETRIES", 3), \
         mock.patch("modules.api_client.RETRY_DELAY_S", 0), \
         mock.patch("modules.api_client.REQUEST_TIMEOUT_S", 5):
        yield


# ---------------------------------------------------------------------------
# Success path
# ---------------------------------------------------------------------------

def test_fetch_returns_list_on_success():
    mock_response = mock.MagicMock()
    mock_response.json.return_value = [{"session_key": 1}]
    mock_response.raise_for_status.return_value = None

    with mock.patch("requests.get", return_value=mock_response):
        result = fetch_with_retry("http://fake.url/sessions")

    assert result == [{"session_key": 1}]


def test_fetch_returns_empty_list():
    """An empty list is still a valid API response."""
    mock_response = mock.MagicMock()
    mock_response.json.return_value = []
    mock_response.raise_for_status.return_value = None

    with mock.patch("requests.get", return_value=mock_response):
        result = fetch_with_retry("http://fake.url/empty")

    assert result == []


# ---------------------------------------------------------------------------
# Retry behaviour
# ---------------------------------------------------------------------------

def test_fetch_retries_on_timeout_then_succeeds():
    mock_success = mock.MagicMock()
    mock_success.json.return_value = [{"key": "value"}]
    mock_success.raise_for_status.return_value = None

    with mock.patch(
        "requests.get",
        side_effect=[requests.exceptions.Timeout, mock_success]
    ):
        result = fetch_with_retry("http://fake.url")

    assert result == [{"key": "value"}]


def test_fetch_exhausts_all_retries_on_timeout(caplog):
    with mock.patch("requests.get", side_effect=requests.exceptions.Timeout):
        with caplog.at_level("ERROR"):
            result = fetch_with_retry("http://fake.url", max_retries=3)

    assert result is None
    assert "All 3 attempts failed" in caplog.text


def test_fetch_retries_on_request_exception():
    mock_success = mock.MagicMock()
    mock_success.json.return_value = [{"data": 1}]
    mock_success.raise_for_status.return_value = None

    with mock.patch(
        "requests.get",
        side_effect=[
            requests.exceptions.RequestException("Connection reset"),
            mock_success,
        ]
    ):
        result = fetch_with_retry("http://fake.url")

    assert result == [{"data": 1}]


# ---------------------------------------------------------------------------
# 4xx short-circuit — should NOT retry
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status_code", [400, 401, 403, 404])
def test_fetch_short_circuits_on_client_error(status_code, caplog):
    mock_response = mock.MagicMock()
    mock_response.status_code = status_code

    http_error = requests.exceptions.HTTPError(response=mock_response)

    with mock.patch("requests.get") as mock_get:
        mock_get.return_value.raise_for_status.side_effect = http_error

        with caplog.at_level("ERROR"):
            result = fetch_with_retry("http://fake.url", max_retries=3)

    assert result is None
    assert mock_get.call_count == 1   # short-circuit: attempted exactly once


# ---------------------------------------------------------------------------
# Invalid response format
# ---------------------------------------------------------------------------

def test_fetch_returns_none_on_non_list_response(caplog):
    mock_response = mock.MagicMock()
    mock_response.json.return_value = {"error": "unexpected dict"}
    mock_response.raise_for_status.return_value = None

    with mock.patch("requests.get", return_value=mock_response):
        with caplog.at_level("ERROR"):
            result = fetch_with_retry("http://fake.url")

    assert result is None
    assert "Unexpected response format" in caplog.text