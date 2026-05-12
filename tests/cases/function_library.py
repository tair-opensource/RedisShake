import pybbt

import helpers as h
from helpers.constant import REDIS_SERVER_VERSION


SRC_LIB = """#!lua name=mylib
redis.register_function('myfunc', function() return 'src_version' end)
"""

DST_LIB = """#!lua name=mylib
redis.register_function('myfunc', function() return 'dst_version' end)
"""


@pybbt.case()
def test_function_library_rdb_replace_existing():
    """RDB sync must overwrite a function library that already exists on dst.

    Reproduces issue #1044: without REPLACE, FUNCTION LOAD fails with
    "ERR Library 'mylib' already exists" when dst already holds the library
    (e.g. after a redis-shake restart or retry).
    """
    if REDIS_SERVER_VERSION < 7.0:
        return

    src = h.Redis()
    dst = h.Redis()

    src.do("function", "load", SRC_LIB)
    pybbt.ASSERT_TRUE(src.do("save"))

    dst.do("function", "load", DST_LIB)
    pybbt.ASSERT_EQ(dst.do("fcall", "myfunc", 0), b"dst_version")

    opts = h.ShakeOpts.create_rdb_opts(f"{src.dir}/dump.rdb", dst)
    pybbt.log(f"opts: {opts}")
    h.Shake.run_once(opts)

    pybbt.ASSERT_EQ(dst.do("fcall", "myfunc", 0), b"src_version")


@pybbt.case()
def test_function_library_sync_replace_existing():
    """Same as the RDB case, but exercising the sync_reader path."""
    if REDIS_SERVER_VERSION < 7.0:
        return

    src = h.Redis()
    dst = h.Redis()

    src.do("function", "load", SRC_LIB)
    dst.do("function", "load", DST_LIB)

    opts = h.ShakeOpts.create_sync_opts(src, dst)
    pybbt.log(f"opts: {opts}")
    shake = h.Shake(opts)

    try:
        shake.wait_for_sync(timeout=60)
    except Exception as e:
        with open(f"{shake.dir}/data/shake.log") as f:
            pybbt.log(f.read())
        raise e

    pybbt.ASSERT_EQ(dst.do("fcall", "myfunc", 0), b"src_version")
