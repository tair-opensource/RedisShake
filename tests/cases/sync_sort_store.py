import pybbt

import helpers as h


@pybbt.case()
def cluster_sort_store():
    if h.REDIS_SERVER_VERSION < 3.0:
        return

    src = h.Cluster()
    dst = h.Cluster()
    source_key = "{sort-store}:source"
    destination_key = "{sort-store}:destination"
    pybbt.ASSERT_EQ(src.do("rpush", source_key, 3, 1, 2), 3)

    shake = h.Shake(h.ShakeOpts.create_sync_opts(src, dst))
    shake.wait_for_sync()

    # The destination list must be created by incremental replay.
    pybbt.ASSERT_EQ(src.do("sort", source_key, "store", destination_key), 3)
    shake.wait_for_sync()
    pybbt.ASSERT_EQ(dst.do("lrange", destination_key, 0, -1), [b"1", b"2", b"3"])
