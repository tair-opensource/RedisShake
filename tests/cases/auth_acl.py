import pybbt as p

import helpers as h


@p.subcase()
def acl():
    src = h.Redis()
    dst = h.Redis()

    src.client.execute_command("acl", "setuser", "user0", ">password0", "~*", "+@all")
    src.client.execute_command("acl", "setuser", "user0", "on")
    src.client.execute_command("auth", "user0", "password0")  # for Redis 4.0

    dst.client.execute_command("acl", "setuser", "user1", ">password1", "~*", "+@all")
    dst.client.execute_command("acl", "setuser", "user1", "on")
    dst.client.execute_command("auth", "user1", "password1")  # for Redis 4.0

    inserter = h.DataInserter()
    inserter.add_data(src, cross_slots_cmd=True)

    opts = h.ShakeOpts.create_sync_opts(src, dst)
    opts["sync_reader"]["username"] = "user0"
    opts["sync_reader"]["password"] = "password0"
    opts["redis_writer"]["username"] = "user1"
    opts["redis_writer"]["password"] = "password1"
    p.log(f"opts: {opts}")
    shake = h.Shake(opts)

    # wait sync done
    p.ASSERT_TRUE_TIMEOUT(lambda: shake.is_consistent(), interval=0.01)
    p.log(shake.get_status())

    # check data
    inserter.check_data(src, cross_slots_cmd=True)
    inserter.check_data(dst, cross_slots_cmd=True)
    p.ASSERT_EQ(src.dbsize(), dst.dbsize())


@p.case(tags=["acl"])
def main():
    if h.REDIS_SERVER_VERSION < 6.0:
        return

    acl()


if __name__ == '__main__':
    main()
