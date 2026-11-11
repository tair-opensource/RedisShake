import os
import time

import pybbt

import helpers as h
from helpers.utils.timer import Timer


def _test_sync(src, dst):
    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)

    pybbt.ASSERT_TRUE(src.do("save"))

    opts = h.ShakeOpts.create_sync_opts(src, dst)
    pybbt.log(f"opts: {opts}")
    shake = h.Shake(opts)

    sync_timeout = 60

    # wait sync done
    try:
        shake.wait_for_sync(timeout=sync_timeout)
    except Exception as e:
        with open(f"{shake.dir}/data/shake.log") as f:
            pybbt.log(f.read())
        raise e

    # add data again
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)

    # wait sync done
    shake.wait_for_sync(timeout=sync_timeout)
    pybbt.log(shake.get_status())

    # check data
    inserter.check_data(src, cross_slots_cmd=cross_slots_cmd)
    inserter.check_data(dst, cross_slots_cmd=cross_slots_cmd)
    pybbt.ASSERT_EQ(src.dbsize(), dst.dbsize())


@pybbt.case()
def standalone_to_standalone():
    src = h.Redis()
    dst = h.Redis()
    _test_sync(src, dst)


@pybbt.case()
def standalone_to_cluster():
    if h.REDIS_SERVER_VERSION < 3.0:
        return
    src = h.Redis()
    dst = h.Cluster()
    _test_sync(src, dst)


@pybbt.case()
def cluster_to_standalone():
    if h.REDIS_SERVER_VERSION < 3.0:
        return
    src = h.Cluster()
    dst = h.Redis()
    _test_sync(src, dst)


@pybbt.case()
def cluster_to_cluster():
    if h.REDIS_SERVER_VERSION < 3.0:
        return
    src = h.Cluster()
    dst = h.Cluster()
    _test_sync(src, dst)


def _test_sync_rdb_false(diskless_sync):
    """Check the empty-RDB optimization and the fallback on older sources."""
    src = h.Redis(args=[
        "--repl-diskless-sync", diskless_sync,
        "--repl-diskless-sync-delay", "0",
        "--rdbcompression", "no",
    ])
    dst = h.Redis()

    # Insert data before sync — this will be part of the RDB snapshot
    rdb_keys = {
        "sync_rdb_false:rdb_key1": "value1",
        "sync_rdb_false:rdb_key2": "value2",
        "sync_rdb_false:large_key": "x" * (1024 * 1024),
    }
    for k, v in rdb_keys.items():
        src.do("set", k, v)
    src.do("save")
    pybbt.log(f"src dbsize after RDB data: {src.dbsize()}")

    # Start shake with sync_rdb=false
    opts = h.ShakeOpts.create_sync_opts(src, dst)
    opts["sync_reader"]["sync_rdb"] = False
    # Incremental-only sync must negotiate EOF even with the default setting.
    opts["sync_reader"]["try_diskless"] = False
    pybbt.log(f"opts: {opts}")
    output_bytes_before = src.client.info("stats")["total_net_output_bytes"]
    shake = h.Shake(opts)

    # Wait for shake to finish RDB phase and become consistent
    timer = Timer()
    while not shake.is_consistent():
        if timer.elapsed() > 60:
            with open(f"{shake.dir}/data/shake.log") as f:
                pybbt.log(f.read())
            raise TimeoutError("shake not consistent within 60s")
        time.sleep(0.1)
    pybbt.log("shake is consistent after RDB phase skipped")

    # Measure bytes sent by the source, not the reader's RDB counters: the
    # diskless reader currently does not count its final chunk. A full RDB
    # contains the uncompressed 1 MiB value, while an empty RDB plus handshake
    # traffic is much smaller. This source has no other replicas or writers.
    output_bytes = src.client.info("stats")["total_net_output_bytes"] - output_bytes_before
    if h.REDIS_SERVER_VERSION >= 7.0:
        pybbt.ASSERT_TRUE(output_bytes < 64 * 1024)
    elif diskless_sync == "no":
        pybbt.ASSERT_TRUE(output_bytes >= 1024 * 1024)
    # Older diskless implementations write directly from the forked child and
    # do not include the RDB transfer in the parent's network counters.
    pybbt.log(f"source sent {output_bytes} bytes with repl-diskless-sync={diskless_sync}")

    # Core assertion: no dump.rdb file should exist on disk.
    # Before the fix, receiveRDB() always wrote RDB to disk even when sync_rdb=false.
    status = shake.get_status()
    reader_dir = status["reader"]["dir"]
    rdb_file = os.path.join(reader_dir, "dump.rdb")
    pybbt.ASSERT_FALSE(os.path.exists(rdb_file))
    pybbt.log(f"verified: no dump.rdb on disk at {rdb_file}")

    # Verify RDB data is NOT in dst
    for k in rdb_keys:
        pybbt.ASSERT_EQ(dst.do("get", k), None)
    pybbt.ASSERT_EQ(dst.dbsize(), 0)
    pybbt.log("verified: RDB data not synced to dst")

    # Write new data to src — this is AOF (incremental) data
    aof_keys = {"sync_rdb_false:aof_key1": "aof_value1", "sync_rdb_false:aof_key2": "aof_value2"}
    for k, v in aof_keys.items():
        src.do("set", k, v)

    # Wait for AOF data to appear in dst
    for k, v in aof_keys.items():
        timer2 = Timer()
        while True:
            result = dst.do("get", k)
            if result == v.encode():
                break
            if timer2.elapsed() > 10:
                with open(f"{shake.dir}/data/shake.log") as f:
                    pybbt.log(f.read())
                raise TimeoutError(f"AOF key '{k}' not synced within timeout")
            time.sleep(0.1)
    pybbt.log("verified: AOF data synced to dst")

    # Final check: dst only has AOF keys, no RDB keys
    for k in rdb_keys:
        pybbt.ASSERT_EQ(dst.do("get", k), None)
    pybbt.ASSERT_EQ(dst.dbsize(), len(aof_keys))
    pybbt.log(f"test sync_rdb_false passed. src_dbsize={src.dbsize()}, dst_dbsize={dst.dbsize()}")


@pybbt.case()
def test_sync_rdb_false():
    _test_sync_rdb_false("no")


@pybbt.case()
def test_sync_rdb_false_diskless_source():
    _test_sync_rdb_false("yes")
