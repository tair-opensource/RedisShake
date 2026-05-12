import pybbt

import helpers as h


@pybbt.case()
def test_filter_db():
    src = h.Redis()
    dst = h.Redis()

    opts = h.ShakeOpts.create_sync_opts(src, dst)
    opts["filter"] = {}
    opts["filter"]["function"] = """
        shake.log(DB)
        if DB == 0
        then
            return
        end
        shake.call(DB, ARGV)
    """
    pybbt.log(f"opts: {opts}")
    shake = h.Shake(opts)

    for db in range(16):
        src.do("select", db)
        src.do("set", "key", "value")

    # wait sync done (use db=1 because db=0 is filtered)
    shake.wait_for_sync(timeout=60, db=1)

    dst.do("select", 0)
    pybbt.ASSERT_EQ(dst.do("get", "key"), None)
    for db in range(1, 16):
        dst.do("select", db)
        pybbt.ASSERT_EQ(dst.do("get", "key"), b"value")


@pybbt.case()
def test_split_mset_to_set():
    src = h.Redis()
    dst = h.Redis()
    opts = h.ShakeOpts.create_sync_opts(src, dst)
    opts["filter"] = {}
    opts["filter"]["function"] = """
        shake.log(KEYS)
        if CMD == "MSET"
        then
            for i = 2, #ARGV, 2 -- MSET k1 v1 k2 v2 k3 v3 ...
            do
                shake.call(1, {"SET", ARGV[i], ARGV[i+1]}) -- move to db 1
            end
        end
    """
    pybbt.log(f"opts: {opts}")
    shake = h.Shake(opts)
    src.do("mset", "k1", "v1", "k2", "v2", "k3", "v3")

    # wait sync done - check both consistency AND data presence
    # is_consistent() can return true before MSET is captured by AOF streaming
    def data_synced():
        if not shake.is_consistent():
            return False
        dst.do("select", 1)
        return dst.do("get", "k1") == b"v1"

    try:
        pybbt.ASSERT_TRUE_TIMEOUT(data_synced, timeout=10, interval=0.01)
    except Exception as e:
        with open(f"{shake.dir}/data/shake.log") as f:
            pybbt.log(f.read())
        raise e
    pybbt.ASSERT_EQ(dst.do("get", "k2"), b"v2")
    pybbt.ASSERT_EQ(dst.do("get", "k3"), b"v3")
