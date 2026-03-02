"""
Assertion functions for pybbt testing framework.
"""

import fnmatch
import time
import types
import typing

from pybbt.logger import log_red
from pybbt.utils.timer import Timer


def _fail(msg: str, fail_callback=None):
    """Internal helper for assertion failures."""
    log_red("----------------------------------------------")
    log_red(f"Assert Failed: {msg}")
    log_red("----------------------------------------------")
    if isinstance(fail_callback, types.LambdaType):
        fail_callback()


def _format_value(v) -> str:
    """Format a value for display."""
    if isinstance(v, bytes):
        return v.decode('utf-8', errors='replace')
    return str(v)


# ==================== Basic Assertions ====================

def ASSERT_TRUE(v, fail_callback=None):
    """Assert that v is True."""
    if v is not True:
        _fail(f"expect True, but is {_format_value(v)}", fail_callback)
        raise AssertionError(f"[{v}] != [True]")


def ASSERT_FALSE(v, fail_callback=None):
    """Assert that v is False."""
    if v:
        _fail(f"expect False, but is {_format_value(v)}", fail_callback)
        raise AssertionError(f"[{v}] != [False]")


def ASSERT_EQ(v0, v1, fail_callback=None):
    """Assert that v0 == v1."""
    s0, s1 = _format_value(v0), _format_value(v1)
    if s0 != s1 and v0 != v1:
        _fail(f"\nexpect: {s1}\nbut is: {s0}", fail_callback)
        raise AssertionError(f"[{s0}] != [{s1}]")


def ASSERT_NE(v0, v1, fail_callback=None):
    """Assert that v0 != v1."""
    s0, s1 = _format_value(v0), _format_value(v1)
    if s0 == s1 or v0 == v1:
        _fail(f"expect {s0} != {s1}", fail_callback)
        raise AssertionError(f"[{s0}] == [{s1}]")


# ==================== Comparison Assertions ====================

def ASSERT_LT(v0, v1, fail_callback=None):
    """Assert that v0 < v1."""
    if v0 >= v1:
        _fail(f"expect {v0} < {v1}", fail_callback)
        raise AssertionError(f"[{v0}] >= [{v1}]")


def ASSERT_LE(v0, v1, fail_callback=None):
    """Assert that v0 <= v1."""
    if v0 > v1:
        _fail(f"expect {v0} <= {v1}", fail_callback)
        raise AssertionError(f"[{v0}] > [{v1}]")


def ASSERT_GT(v0, v1, fail_callback=None):
    """Assert that v0 > v1."""
    if v0 <= v1:
        _fail(f"expect {v0} > {v1}", fail_callback)
        raise AssertionError(f"[{v0}] <= [{v1}]")


def ASSERT_GE(v0, v1, fail_callback=None):
    """Assert that v0 >= v1."""
    if v0 < v1:
        _fail(f"expect {v0} >= {v1}", fail_callback)
        raise AssertionError(f"[{v0}] < [{v1}]")


# ==================== Pattern Assertions ====================

def ASSERT_MATCH(v0, pattern, fail_callback=None):
    """Assert that v0 matches pattern (fnmatch style)."""
    s0 = _format_value(v0)
    if not fnmatch.fnmatch(s0, str(pattern)):
        _fail(f"'{s0}' does not match pattern '{pattern}'", fail_callback)
        raise AssertionError(f"[{s0}] not match [{pattern}]")


def ASSERT_NOT_MATCH(v0, pattern, fail_callback=None):
    """Assert that v0 does not match pattern."""
    s0 = _format_value(v0)
    if fnmatch.fnmatch(s0, str(pattern)):
        _fail(f"'{s0}' matches pattern '{pattern}'", fail_callback)
        raise AssertionError(f"[{s0}] match [{pattern}]")


# ==================== Timeout Assertions ====================

def ASSERT_TRUE_TIMEOUT(fn: typing.Callable, timeout=20, interval=0.5, fail_callback=None):
    """Assert that fn() returns True within timeout seconds."""
    ti = Timer()
    while True:
        try:
            if fn():
                return
        except Exception:
            pass
        if ti.elapsed() > timeout:
            _fail(f"timeout after {timeout}s", fail_callback)
            raise AssertionError(f"Assert timeout")
        time.sleep(interval)


def ASSERT_EQ_TIMEOUT(v0, v1, timeout=20, interval=0.5, fail_callback=None):
    """Assert that v0 == v1 within timeout seconds. v0/v1 can be callables."""
    if not callable(v0) and not callable(v1):
        raise ValueError("At least one of v0, v1 must be callable")

    ti = Timer()
    while True:
        val0 = v0() if callable(v0) else v0
        val1 = v1() if callable(v1) else v1
        s0, s1 = _format_value(val0), _format_value(val1)

        if s0 == s1 or val0 == val1:
            return

        if ti.elapsed() > timeout:
            _fail(f"timeout: [{s0}] != [{s1}]", fail_callback)
            raise AssertionError(f"Assert timeout, [{s0}] != [{s1}]")
        time.sleep(interval)


# ==================== Exception Assertions ====================

def ASSERT_EXCEPTION(fn: typing.Callable, expected_exception: Exception):
    """Assert that fn() raises an exception matching expected_exception."""
    try:
        fn()
    except Exception as e:
        if type(e) != type(expected_exception) or str(e) != str(expected_exception):
            _fail(f"exception mismatch:\n"
                  f"  got: {type(e).__name__}: {e}\n"
                  f"  expected: {type(expected_exception).__name__}: {expected_exception}")
            raise AssertionError("Exception not equal")
        return
    raise AssertionError(f"Expected exception {type(expected_exception).__name__}, but no exception was raised")
