import pybbt

import helpers as h


@pybbt.case()
def test_acl():
    if h.REDIS_SERVER_VERSION < 6.0:
        return

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
    pybbt.log(f"opts: {opts}")
    shake = h.Shake(opts)

    # wait sync done
    shake.wait_for_sync(timeout=10)
    pybbt.log(shake.get_status())

    # check data
    inserter.check_data(src, cross_slots_cmd=True)
    inserter.check_data(dst, cross_slots_cmd=True)
    pybbt.ASSERT_EQ(src.dbsize(), dst.dbsize())
