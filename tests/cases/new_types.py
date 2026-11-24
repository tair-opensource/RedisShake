import pybbt

import helpers as h


def _supported(ret):
    return not (isinstance(ret, str) and ret.startswith("-"))


def _sync(src, dst, max_bulk_len=None):
    pybbt.ASSERT_TRUE(src.do("save"))
    opts = h.ShakeOpts.create_sync_opts(src, dst)
    if max_bulk_len is not None:
        opts.setdefault("advanced", {})["target_redis_proto_max_bulk_len"] = max_bulk_len
    shake = h.Shake(opts)
    shake.wait_for_sync(timeout=60)


@pybbt.case()
def redis_8_12_new_types():
    src = h.Redis()
    dst = h.Redis()

    has_set_ex = _supported(src.do("saddex", "setex:lp", "EX", "300", "m1", "m2"))
    if has_set_ex:
        src.do("sadd", "setex:lp", "m3")
    has_array = _supported(src.do("arset", "arr:k", "0", "hello", "42", "3.14"))
    has_bitmap = False
    if _supported(src.do("setbit", "bmp:k", "10", "1")):
        if _supported(src.do("bitconvert", "bmp:k", "ROARING")):
            has_bitmap = True
        else:
            src.do("del", "bmp:k")

    if not (has_set_ex or has_array or has_bitmap):
        return

    _sync(src, dst)

    if has_set_ex:
        pybbt.ASSERT_EQ(set(src.do("smembers", "setex:lp")), set(dst.do("smembers", "setex:lp")))
        pybbt.ASSERT_TRUE(dst.do("spttl", "setex:lp", "MEMBERS", "1", "m1")[0] > 0)
        pybbt.ASSERT_EQ(dst.do("spttl", "setex:lp", "MEMBERS", "1", "m3"), [-1])
    if has_array:
        pybbt.ASSERT_EQ(src.do("arlen", "arr:k"), dst.do("arlen", "arr:k"))
        pybbt.ASSERT_EQ(src.do("argetrange", "arr:k", "0", "2"), dst.do("argetrange", "arr:k", "0", "2"))
    if has_bitmap:
        pybbt.ASSERT_EQ(src.do("getbit", "bmp:k", "10"), dst.do("getbit", "bmp:k", "10"))
        pybbt.ASSERT_EQ(src.do("bitcount", "bmp:k"), dst.do("bitcount", "bmp:k"))


@pybbt.case()
def redis_8_12_new_types_rewrite():
    src = h.Redis()
    dst = h.Redis()

    has_set_ex = _supported(src.do("saddex", "setex:rw", "EX", "300", "m1", "m2"))
    if has_set_ex:
        src.do("sadd", "setex:rw", "m3")
    has_array = _supported(src.do("arset", "arr:rw", "0", "hello", "42"))

    if not (has_set_ex or has_array):
        return

    _sync(src, dst, max_bulk_len=0)

    if has_set_ex:
        pybbt.ASSERT_EQ(set(src.do("smembers", "setex:rw")), set(dst.do("smembers", "setex:rw")))
        pybbt.ASSERT_TRUE(dst.do("spttl", "setex:rw", "MEMBERS", "1", "m1")[0] > 0)
        pybbt.ASSERT_EQ(dst.do("spttl", "setex:rw", "MEMBERS", "1", "m3"), [-1])
    if has_array:
        pybbt.ASSERT_EQ(src.do("arlen", "arr:rw"), dst.do("arlen", "arr:rw"))
        pybbt.ASSERT_EQ(src.do("argetrange", "arr:rw", "0", "1"), dst.do("argetrange", "arr:rw", "0", "1"))


@pybbt.case()
def valkey_9_2_path_hash():
    src = h.Redis()
    if not _supported(src.do("phset", "ph:k", "/a/b", "FIELDS", "2", "f1", "v1", "f2", "v2")):
        return
    src.do("phset", "ph:k", "/a/c", "FIELDS", "1", "f3", "v3")

    # 1. Default RESTORE path
    dst1 = h.Redis()
    _sync(src, dst1)
    pybbt.ASSERT_EQ(src.do("phgetall", "ph:k", "/a/b"), dst1.do("phgetall", "ph:k", "/a/b"))
    pybbt.ASSERT_EQ(src.do("phgetall", "ph:k", "/a/c"), dst1.do("phgetall", "ph:k", "/a/c"))

    # 2. Command rewrite path (target_redis_proto_max_bulk_len = 0)
    dst2 = h.Redis()
    _sync(src, dst2, max_bulk_len=0)
    pybbt.ASSERT_EQ(src.do("phgetall", "ph:k", "/a/b"), dst2.do("phgetall", "ph:k", "/a/b"))
    pybbt.ASSERT_EQ(src.do("phgetall", "ph:k", "/a/c"), dst2.do("phgetall", "ph:k", "/a/c"))
