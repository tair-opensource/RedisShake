import os
import time

import pybbt

import helpers as h
from helpers.utils.timer import Timer


def _reader_aof_files(reader_dir):
    if not reader_dir or not os.path.isdir(reader_dir):
        return []
    return sorted(name for name in os.listdir(reader_dir) if name.endswith(".aof"))


def _wait_for_process_exit(shake, timeout):
    timer = Timer()
    while shake.server.is_running():
        if timer.elapsed() > timeout:
            with open(f"{shake.dir}/data/shake.log") as f:
                pybbt.log(f.read())
            raise TimeoutError(f"redis-shake did not exit within {timeout}s")
        time.sleep(0.1)


def _wait_for_reader_status(shake, expected_status, timeout):
    timer = Timer()
    last_status = None
    while True:
        status = shake.get_status()
        reader = status["reader"]
        if reader is not None:
            last_status = reader["status"]
            if last_status in expected_status:
                return reader

        if timer.elapsed() > timeout:
            with open(f"{shake.dir}/data/shake.log") as f:
                pybbt.log(f.read())
            raise TimeoutError(
                f"reader did not reach {expected_status} within {timeout}s, "
                f"last status={last_status}"
            )
        time.sleep(0.1)


@pybbt.case()
def sync_aof_false_does_not_buffer_incremental_stream():
    src = h.Redis()
    dst = h.Redis()

    pipe = src.pipeline()
    for i in range(500):
        pipe.set(f"sync_aof_false:rdb:{i}", "x" * 128)
    pipe.execute()
    src.do("save")

    opts = h.ShakeOpts.create_sync_opts(src, dst)
    opts["sync_reader"]["sync_aof"] = False
    opts["advanced"] = {
        "pipeline_count_limit": 1,
        "target_redis_max_qps": 20,
    }
    shake = h.Shake(opts)

    reader = _wait_for_reader_status(shake, {"syncing rdb"}, timeout=20)

    aof_keys = {f"sync_aof_false:aof:{i}": f"value-{i}" for i in range(20)}
    for key, value in aof_keys.items():
        src.do("set", key, value)

    observed_reader_dir = reader["dir"]
    timer = Timer()
    while timer.elapsed() < 5:
        status = shake.get_status()
        reader = status["reader"]
        if reader is None:
            time.sleep(0.1)
            continue
        observed_reader_dir = reader["dir"]
        aof_bytes = reader["aof_received_bytes"]
        aof_files = _reader_aof_files(observed_reader_dir)
        if aof_bytes != 0 or aof_files:
            raise AssertionError(
                f"sync_aof=false buffered AOF data: bytes={aof_bytes}, files={aof_files}"
            )
        time.sleep(0.1)

    pybbt.ASSERT_TRUE(observed_reader_dir is not None)
    pybbt.ASSERT_EQ(_reader_aof_files(observed_reader_dir), [])

    _wait_for_process_exit(shake, timeout=30)

    for i in range(0, 500, 100):
        pybbt.ASSERT_EQ(dst.do("get", f"sync_aof_false:rdb:{i}"), b"x" * 128)
    for key in aof_keys:
        pybbt.ASSERT_EQ(dst.do("get", key), None)
