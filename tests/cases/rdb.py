import pybbt

import helpers as h


def _test_rdb(src, dst):
    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    pybbt.ASSERT_TRUE(src.do("save"))

    opts = h.ShakeOpts.create_rdb_opts(f"{src.dir}/dump.rdb", dst)
    pybbt.log(f"opts: {opts}")
    h.Shake.run_once(opts)

    # check data
    inserter.check_data(src, cross_slots_cmd=cross_slots_cmd)
    inserter.check_data(dst, cross_slots_cmd=cross_slots_cmd)
    pybbt.ASSERT_EQ(src.dbsize(), dst.dbsize())


@pybbt.case()
def rdb_to_standalone():
    src = h.Redis()
    dst = h.Redis()
    _test_rdb(src, dst)


@pybbt.case()
def rdb_to_cluster():
    if h.REDIS_SERVER_VERSION < 3.0:
        return
    src = h.Redis()
    dst = h.Cluster()
    _test_rdb(src, dst)
