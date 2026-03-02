import pybbt as p

import helpers as h


def test(src, dst):
    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)

    p.ASSERT_TRUE(src.do("save"))

    opts = h.ShakeOpts.create_sync_opts(src, dst)
    p.log(f"opts: {opts}")
    shake = h.Shake(opts)

    # Use longer timeout for cluster sync (cluster gossip and cross-node sync takes more time)
    sync_timeout = 30 if (src.is_cluster() or dst.is_cluster()) else 10

    # wait sync done
    try:
        shake.wait_for_sync(timeout=sync_timeout)
    except Exception as e:
        with open(f"{shake.dir}/data/shake.log") as f:
            p.log(f.read())
        raise e

    # add data again
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)

    # wait sync done
    shake.wait_for_sync(timeout=sync_timeout)
    p.log(shake.get_status())

    # check data
    inserter.check_data(src, cross_slots_cmd=cross_slots_cmd)
    inserter.check_data(dst, cross_slots_cmd=cross_slots_cmd)
    p.ASSERT_EQ(src.dbsize(), dst.dbsize())


def standalone_to_standalone():
    src = h.Redis()
    dst = h.Redis()
    test(src, dst)


def standalone_to_cluster():
    if h.REDIS_SERVER_VERSION < 3.0:
        return
    src = h.Redis()
    dst = h.Cluster()
    test(src, dst)


def cluster_to_standalone():
    if h.REDIS_SERVER_VERSION < 3.0:
        return
    src = h.Cluster()
    dst = h.Redis()
    test(src, dst)


def cluster_to_cluster():
    if h.REDIS_SERVER_VERSION < 3.0:
        return
    src = h.Cluster()
    dst = h.Cluster()
    test(src, dst)


@p.case()
def main():
    standalone_to_standalone()
    standalone_to_cluster()
    cluster_to_standalone()
    cluster_to_cluster()


if __name__ == '__main__':
    main()
