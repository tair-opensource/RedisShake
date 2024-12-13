import helpers as h
import pybbt as p


@p.subcase()
def filter_db():
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
    p.log(f"opts: {opts}")
    shake = h.Shake(opts)

    for db in range(16):
        src.do("select", db)
        src.do("set", "key", "value")

    # wait sync done
    p.ASSERT_TRUE_TIMEOUT(lambda: shake.is_consistent(), timeout=10, interval=0.01)

    dst.do("select", 0)
    p.ASSERT_EQ(dst.do("get", "key"), None)
    for db in range(1, 16):
        dst.do("select", db)
        p.ASSERT_EQ(dst.do("get", "key"), b"value")


@p.subcase()
def split_mset_to_set():
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
    p.log(f"opts: {opts}")
    shake = h.Shake(opts)
    src.do("mset", "k1", "v1", "k2", "v2", "k3", "v3")
    # wait sync done
    p.ASSERT_TRUE_TIMEOUT(lambda: shake.is_consistent(), timeout=10, interval=0.01)
    dst.do("select", 1)
    p.ASSERT_EQ(dst.do("get", "k1"), b"v1")
    p.ASSERT_EQ(dst.do("get", "k2"), b"v2")
    p.ASSERT_EQ(dst.do("get", "k3"), b"v3")


@p.case(tags=["function"])
def main():
    filter_db()
    split_mset_to_set()


if __name__ == '__main__':
    main()
