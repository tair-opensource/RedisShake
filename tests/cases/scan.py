import pybbt as p

import helpers as h


def test(src, dst):
    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    p.ASSERT_TRUE(src.do("save"))
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)  # add data again

    opts = h.ShakeOpts.create_scan_opts(src, dst)
    p.log(f"opts: {opts}")

    # run shake
    h.Shake.run_once(opts)

    # check data
    inserter.check_data(src, cross_slots_cmd=cross_slots_cmd)
    inserter.check_data(dst, cross_slots_cmd=cross_slots_cmd)
    p.ASSERT_EQ(src.dbsize(), dst.dbsize())


@p.subcase()
def standalone_to_standalone():
    src = h.Redis()
    dst = h.Redis()
    test(src, dst)


@p.subcase()
def standalone_to_cluster():
    if h.REDIS_SERVER_VERSION < 3.0:
        return
    src = h.Redis()
    dst = h.Cluster()
    test(src, dst)


@p.subcase()
def cluster_to_standalone():
    if h.REDIS_SERVER_VERSION < 3.0:
        return
    src = h.Cluster()
    dst = h.Redis()
    test(src, dst)


@p.subcase()
def cluster_to_cluster():
    if h.REDIS_SERVER_VERSION < 3.0:
        return
    src = h.Cluster()
    dst = h.Cluster()
    test(src, dst)


@p.case(tags=["scan"])
def main():
    standalone_to_standalone()
    standalone_to_cluster()
    cluster_to_standalone()
    cluster_to_cluster()


if __name__ == '__main__':
    main()
