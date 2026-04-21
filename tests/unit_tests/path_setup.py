# =============================================================================
# F1-Pulse | Test Path Bootstrap
# File:     tests/unit_tests/path_setup.py
# Author:   Jafar891
# Updated:  2026
#
# Resolves the project root from tests/unit_tests/ (two levels up) and
# inserts it into sys.path so that `from modules.xxx import yyy` works in
# every test file regardless of how pytest is invoked.
#
# Usage — first two lines of any test file:
#   import path_setup  # noqa: F401
#   from modules.xxx import yyy
# =============================================================================

import sys
import os

# tests/unit_tests/ -> tests/ -> project root
_project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)

if _project_root not in sys.path:
    sys.path.insert(0, _project_root)