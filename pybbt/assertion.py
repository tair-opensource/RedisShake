import fnmatch
import time
import types
import typing

from pybbt.logger import *
from pybbt.utils import Timer


def ASSERT_TRUE_TIMEOUT(v1: typing.Callable, timeout=10, interval=0.5):
    ti = Timer()
    while True:
        value = v1()
        if value:
            return
        if ti.elapsed() > timeout:
            raise AssertionError("Assert timeout")
        time.sleep(interval)


def ASSERT_EQ_TIMEOUT(v1, v2=None, timeout=10, interval=0.5):
    if v2 is None:
        time_start = time.time()
        if isinstance(v1, types.LambdaType):
            while True:
                value = v1()
                if value:
                    return
                if time.time() - time_start > timeout:
                    raise AssertionError("Assert timeout")
                time.sleep(interval)
        else:
            assert v1()
    else:
        time_start = time.time()
        while True:
            value1 = v1() if isinstance(v1, types.LambdaType) else v1
            value2 = v2() if isinstance(v2, types.LambdaType) else v2
            if str(value1) == str(value2):
                return
            else:
                if time.time() - time_start > timeout:
                    error = f"Assert timeout, [{value1}] != [{value2}]"
                    log_red(error)
                    raise AssertionError(error)
            time.sleep(interval)


def ASSERT_TRUE(v):
    if not (v is True):
        log_red("----------------------------------------------")
        log_red(f"Assert Failed: expect True, but is {v}")
        log_red("----------------------------------------------")
        raise AssertionError(f"[{v}] != [True]")


def ASSERT_FALSE(v):
    if v:
        log_red("----------------------------------------------")
        log_red(f"Assert Failed: expect False, but is {v}")
        log_red("----------------------------------------------")
        raise AssertionError(f"[{v}] != [False]")


def ASSERT_EQ(v0, v1):
    if str(v0) != str(v1):
        log_red("----------------------------------------------")
        log_red(f"Assert Failed: expect {v1}, but is {v0}")
        log_red("----------------------------------------------")
        raise AssertionError(f"[{v0}] != [{v1}]")


def ASSERT_NE(v0, v1):
    if str(v0) == str(v1):
        log_red("----------------------------------------------")
        log_red(f"Assert Failed: expect {v1} != {v0}")
        log_red("----------------------------------------------")
        raise AssertionError(f"[{v0}] == [{v1}]")


def ASSERT_MATCH(v0, v1):
    if not fnmatch.fnmatch(str(v0), str(v1)):
        log_red("----------------------------------------------")
        log_red(f"Assert Failed: expect {v1} match {v0}")
        log_red("----------------------------------------------")
        raise AssertionError(f"[{v0}] not match [{v1}]")


def ASSERT_NOT_MATCH(v0, v1):
    if fnmatch.fnmatch(str(v0), str(v1)):
        log_red("----------------------------------------------")
        log_red(f"Assert Failed: expect {v1} not match {v0}")
        log_red("----------------------------------------------")
        raise AssertionError(f"[{v0}] match [{v1}]")


def ASSERT_EXCEPTION(f0: typing.Callable, e1: Exception):
    try:
        f0()
    except Exception as e0:
        if type(e0) != type(e1) or str(e0) != str(e1):
            log_red("----------------------------------------------")
            log_red(f"Assert Failed.")
            log_red(f"e0 type: {type(e0)}, str: {str(e0)}")
            log_red(f"e1 type: {type(e1)}, str: {str(e1)}")
            log_red("----------------------------------------------")
            raise AssertionError(f"Exception not equal")
    else:
        raise Exception(f"Expect exception, but no exception raised")
