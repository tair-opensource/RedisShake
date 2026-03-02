import pybbt

import helpers as h


def _test_scan(src, dst):
    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    pybbt.ASSERT_TRUE(src.do("save"))
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)  # add data again

    opts = h.ShakeOpts.create_scan_opts(src, dst)
    pybbt.log(f"opts: {opts}")

    # run shake
    h.Shake.run_once(opts)

    # check data
    inserter.check_data(src, cross_slots_cmd=cross_slots_cmd)
    inserter.check_data(dst, cross_slots_cmd=cross_slots_cmd)
    pybbt.ASSERT_EQ(src.dbsize(), dst.dbsize())


@pybbt.case()
def standalone_to_standalone():
    src = h.Redis()
    dst = h.Redis()
    _test_scan(src, dst)


@pybbt.case()
def standalone_to_cluster():
    if h.REDIS_SERVER_VERSION < 3.0:
        return
    src = h.Redis()
    dst = h.Cluster()
    _test_scan(src, dst)


@pybbt.case()
def cluster_to_standalone():
    if h.REDIS_SERVER_VERSION < 3.0:
        return
    src = h.Cluster()
    dst = h.Redis()
    _test_scan(src, dst)


@pybbt.case()
def cluster_to_cluster():
    if h.REDIS_SERVER_VERSION < 3.0:
        return
    src = h.Cluster()
    dst = h.Cluster()
    _test_scan(src, dst)
