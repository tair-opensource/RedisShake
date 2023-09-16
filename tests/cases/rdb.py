import pybbt as p

import helpers as h


def test(src, dst):
    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    p.ASSERT_TRUE(src.do("save"))

    opts = h.ShakeOpts.create_rdb_opts(f"{src.dir}/dump.rdb", dst)
    p.log(f"opts: {opts}")
    h.Shake.run_once(opts)

    # check data
    inserter.check_data(src, cross_slots_cmd=cross_slots_cmd)
    inserter.check_data(dst, cross_slots_cmd=cross_slots_cmd)
    p.ASSERT_EQ(src.dbsize(), dst.dbsize())


@p.subcase()
def rdb_to_standalone():
    src = h.Redis()
    dst = h.Redis()
    test(src, dst)


@p.subcase()
def rdb_to_cluster():
    if h.REDIS_SERVER_VERSION < 3.0:
        return
    src = h.Redis()
    dst = h.Cluster()
    test(src, dst)


@p.case(tags=["sync"])
def main():
    rdb_to_standalone()
    rdb_to_cluster()


if __name__ == '__main__':
    main()
