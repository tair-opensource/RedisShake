"""
pybbt - Python Black Box Test

A simple and powerful black box testing framework for Python.
"""

from pybbt.assertion import (
    ASSERT_EQ,
    ASSERT_EQ_TIMEOUT,
    ASSERT_EXCEPTION,
    ASSERT_FALSE,
    ASSERT_GE,
    ASSERT_GT,
    ASSERT_LE,
    ASSERT_LT,
    ASSERT_MATCH,
    ASSERT_NE,
    ASSERT_NOT_MATCH,
    ASSERT_TRUE,
    ASSERT_TRUE_TIMEOUT,
)
from pybbt.case import case, get_case_context
from pybbt.context import get_global_flags
from pybbt.launcher import Launcher
from pybbt.logger import log, log_blue, log_gray, log_red, log_yellow
from pybbt.safe_thread import SafeThread

__version__ = "2.0.0"

__all__ = [
    # Assertions
    "ASSERT_TRUE",
    "ASSERT_FALSE",
    "ASSERT_EQ",
    "ASSERT_NE",
    "ASSERT_LT",
    "ASSERT_LE",
    "ASSERT_GT",
    "ASSERT_GE",
    "ASSERT_MATCH",
    "ASSERT_NOT_MATCH",
    "ASSERT_TRUE_TIMEOUT",
    "ASSERT_EQ_TIMEOUT",
    "ASSERT_EXCEPTION",
    # Test decorators
    "case",
    # Utilities
    "Launcher",
    "SafeThread",
    "get_case_context",
    "get_global_flags",
    # Logging
    "log",
    "log_blue",
    "log_gray",
    "log_red",
    "log_yellow",
]
