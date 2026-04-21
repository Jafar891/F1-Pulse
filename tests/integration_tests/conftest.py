# =============================================================================
# F1-Pulse | Integration Test Configuration
# File:     tests/integration_tests/conftest.py
# Author:   Jafar891
# Updated:  2026
# =============================================================================

import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError(
            "No active SparkSession found. "
            "Run tests via pytest.main() in a notebook Python cell, "
            "not via %sh."
        )
    return session