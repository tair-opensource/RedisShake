import pybbt

import helpers as h


def _supported(ret):
    return not (isinstance(ret, str) and ret.startswith("-"))


def _group_fields(g):
    # XINFO GROUPS is a dict on some redis-py versions and a flat field/value
    # list on others; normalize to a dict with decoded keys.
    items = g.items() if isinstance(g, dict) else zip(g[0::2], g[1::2])
    return {(k.decode() if isinstance(k, bytes) else k): v for k, v in items}


def _test_stream_sync(src, dst):
    key = "stream:test"
    id1 = src.do("xadd", key, "*", "f1", "v1")
    src.do("xadd", key, "*", "f2", "v2")
    src.do("xadd", key, "*", "f3", "v3")
    src.do("xgroup", "create", key, "g1", "0")
    src.do("xreadgroup", "group", "g1", "c1", "count", "2", "streams", key, ">")

    # IDMP (Redis >= 8.6) and NACK (Redis >= 8.12) populate the trailing sections
    # of RDB types 26/27. Feature-detect: older servers and Valkey lack them.
    if _supported(src.do("xadd", key, "IDMP", "p1", "iid1", "*", "f", "v")):
        src.do("xadd", key, "IDMP", "p1", "iid2", "*", "f", "v")
        src.do("xadd", key, "IDMP", "p2", "iid3", "*", "f", "v")
    src.do("xnack", key, "g1", "FAIL", "IDS", "1", id1)

    pybbt.ASSERT_TRUE(src.do("save"))
    opts = h.ShakeOpts.create_sync_opts(src, dst)
    shake = h.Shake(opts)
    shake.wait_for_sync(timeout=60)

    pybbt.ASSERT_EQ(src.do("xlen", key), dst.do("xlen", key))
    pybbt.ASSERT_EQ(src.do("xrange", key, "-", "+"), dst.do("xrange", key, "-", "+"))
    sg = [_group_fields(g) for g in src.do("xinfo", "groups", key)]
    dg = [_group_fields(g) for g in dst.do("xinfo", "groups", key)]
    pybbt.ASSERT_EQ(len(sg), len(dg))
    for s, d in zip(sg, dg):
        for field in ("name", "consumers", "pending", "last-delivered-id"):
            pybbt.ASSERT_EQ(s.get(field), d.get(field))


@pybbt.case()
def standalone_to_standalone():
    if h.REDIS_SERVER_VERSION < 5.0:
        return
    src = h.Redis()
    dst = h.Redis()
    _test_stream_sync(src, dst)