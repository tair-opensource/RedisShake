import os
import time

import pybbt

import helpers as h


def format_command(*args):
    cmd = f"*{len(args)}\r\n"
    for a in args:
        cmd += f"${len(a)}\r\n{a}\r\n"
    return cmd


def append_to_file(write_file, strings):
    with open(write_file, "w+") as fp:
        for string in strings:
            fp.write(string)


def create_aof_dir(dir_path):
    os.makedirs(dir_path, exist_ok=True)


def get_aof_file_relative_path():
    if h.REDIS_SERVER_VERSION >= 7.0:
        aof_file = "/appendonlydir/appendonly.aof.manifest"
    else:
        aof_file = "/appendonly.aof"
    return aof_file


def get_aof_file_path(src):
    return os.path.join(src.dir, get_aof_file_relative_path().lstrip("/"))


def wait_for_aof_ready(src, timeout=15):
    """
    Wait until source AOF file is readable and AOF rewrite is not in progress.
    This avoids races on old Redis versions (e.g. 2.8) where AOF rewrite may
    still be running right after enabling appendonly.
    """
    aof_file_path = get_aof_file_path(src)
    begin = time.time()

    while True:
        info = src.do("INFO", "persistence")
        if isinstance(info, bytes):
            info = info.decode("utf-8", errors="ignore")
        else:
            info = str(info)

        rewrite_in_progress = "aof_rewrite_in_progress:1" in info
        if os.path.exists(aof_file_path):
            file_size = os.path.getsize(aof_file_path)
            if file_size > 0 and not rewrite_in_progress:
                pybbt.log(f"aof ready: {aof_file_path}, size={file_size}")
                return aof_file_path

        if time.time() - begin > timeout:
            size = os.path.getsize(aof_file_path) if os.path.exists(aof_file_path) else -1
            raise TimeoutError(f"aof not ready in {timeout}s, path={aof_file_path}, size={size}, rewrite={rewrite_in_progress}")

        time.sleep(0.1)


def get_base_file_from_manifest(manifest_path):
    with open(manifest_path, "r", encoding="utf-8") as manifest:
        for line in manifest:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            # Expected format:
            # file <filename> seq <n> type b
            if len(parts) >= 6 and parts[0] == "file" and parts[4] == "type" and parts[5] == "b":
                return os.path.join(os.path.dirname(manifest_path), parts[1])
    return None


def _test_aof(src, dst):
    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    pybbt.ASSERT_TRUE(src.do("save"))
    wait_for_aof_ready(src)

    opts = h.ShakeOpts.create_aof_opts(f"{src.dir}{get_aof_file_relative_path()}", dst)
    h.Shake.run_once(opts)
    # check data
    inserter.check_data(dst, cross_slots_cmd=cross_slots_cmd)
    pybbt.ASSERT_EQ(src.dbsize(), dst.dbsize())


def _test_base_file(dst):
    current_directory = pybbt.get_case_context().dir + "_own"
    create_aof_dir(current_directory + "/appendonlydir")
    manifest_filepath = current_directory + "/appendonlydir/appendonly.aof.manifest"
    commands = []
    commands += "file appendonly.aof.1.base.aof seq 1 type b\n"
    append_to_file(manifest_filepath, commands)

    base_file_path = current_directory + "/appendonlydir/appendonly.aof.1.base.aof"
    commands = []
    commands += format_command("set", "k1", "v1")
    commands += format_command("set", "k2", "v2")
    append_to_file(base_file_path, commands)

    opts = h.ShakeOpts.create_aof_opts(f"{current_directory}{get_aof_file_relative_path()}", dst)
    pybbt.log(f"opts: {opts}")
    h.Shake.run_once(opts)

    pip = dst.pipeline()
    pip.get("k1")
    pip.get("k2")
    ret = pip.execute()
    pybbt.ASSERT_EQ(ret, [b"v1", b"v2"])
    pybbt.ASSERT_EQ(dst.dbsize(), 2)


def _test_error(src, dst):
    ret = src.do("CONFIG SET", "appendonly", "yes")
    pybbt.log(f"aof_ret: {ret}")
    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    pybbt.ASSERT_TRUE(src.do("save"))
    wait_for_aof_ready(src)
    # destroy file
    file_path = get_aof_file_path(src)
    with open(file_path, "r+") as file:
        destroy_data = "xxxxs"
        file.seek(0, 0)
        file.write(destroy_data)

    opts = h.ShakeOpts.create_aof_opts(f"{src.dir}/appendonlydir/appendonly.aof.manifest", dst)
    pybbt.log(f"opts: {opts}")
    h.Shake.run_once(opts)

    # cant restore
    pybbt.ASSERT_EQ(dst.dbsize(), 0)


def _test_rm_file(src, dst):
    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    pybbt.ASSERT_TRUE(src.do("save"))
    manifest_path = wait_for_aof_ready(src)
    # rm file
    file_path = get_base_file_from_manifest(manifest_path)
    pybbt.ASSERT_TRUE(file_path is not None)
    pybbt.log(f"remove aof base file: {file_path}")
    os.remove(file_path)
    opts = h.ShakeOpts.create_aof_opts(f"{src.dir}{get_aof_file_relative_path()}", dst)
    h.Shake.run_once(opts)
    # cant restore
    pybbt.ASSERT_EQ(dst.dbsize(), 0)


def _test_history_file(src, dst):
    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    for i in range(1000):
        inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    pybbt.ASSERT_TRUE(src.do("BGREWRITEAOF"))

    opts = h.ShakeOpts.create_aof_opts(f"{src.dir}{get_aof_file_relative_path()}", dst)
    h.Shake.run_once(opts)
    # check data
    inserter.check_data(dst, cross_slots_cmd=cross_slots_cmd)
    pybbt.ASSERT_EQ(src.dbsize(), dst.dbsize())


def _test_base_file_timestamp(dst):
    current_directory = pybbt.get_case_context().dir + "_own"
    create_aof_dir(current_directory)
    manifest_filepath = current_directory + "/appendonlydir/appendonly.aof.manifest"
    commands = []
    commands += "file appendonly.aof.1.base.aof seq 1 type b\n"
    append_to_file(manifest_filepath, commands)

    base_file_path = current_directory + "/appendonlydir/appendonly.aof.1.base.aof"
    commands = []
    commands += "#TS1233\r\n"
    commands += format_command("set", "k1", "v1")
    commands += "#TS1234\r\n"
    commands += format_command("set", "k2", "v2")
    commands += "#TS1235\r\n"
    commands += format_command("set", "k3", "v3")
    append_to_file(base_file_path, commands)

    opts = h.ShakeOpts.create_aof_opts(f"{current_directory}{get_aof_file_relative_path()}", dst, 1234)
    pybbt.log(f"opts: {opts}")
    h.Shake.run_once(opts)

    pip = dst.pipeline()
    pip.get("k1")
    pip.get("k2")
    pip.get("k3")
    ret = pip.execute()
    pybbt.ASSERT_EQ(ret, [b"v1", b"v2", b"v3"])
    pybbt.ASSERT_EQ(dst.dbsize(), 3)


def _test_base_and_incr_timestamp(dst):
    current_directory = pybbt.get_case_context().dir + "_own"
    create_aof_dir(current_directory + "/appendonlydir")
    manifest_filepath = current_directory + "/appendonlydir/appendonly.aof.manifest"
    commands = []
    commands += "file appendonly.aof.1.base.aof seq 1 type b\n"
    commands += "file appendonly.aof.1.incr.aof seq 1 type i\n"
    commands += "file appendonly.aof.2.incr.aof seq 2 type i\n"
    append_to_file(manifest_filepath, commands)

    base_file_path = current_directory + "/appendonlydir/appendonly.aof.1.base.aof"
    commands = []
    commands += format_command("set", "k1", "v1")
    append_to_file(base_file_path, commands)

    commands = []
    incr1_file_path = current_directory + "/appendonlydir/appendonly.aof.1.incr.aof"
    commands += "#TS1233\r\n"
    commands += format_command("set", "k2", "v2")
    append_to_file(incr1_file_path, commands)

    commands = []
    incr2_file_path = current_directory + "/appendonlydir/appendonly.aof.2.incr.aof"
    commands += "#TS1235\r\n"
    commands += format_command("set", "k3", "v3")
    append_to_file(incr2_file_path, commands)

    opts = h.ShakeOpts.create_aof_opts(f"{current_directory}{get_aof_file_relative_path()}", dst, 1234)
    pybbt.log(f"opts: {opts}")
    h.Shake.run_once(opts)

    pip = dst.pipeline()
    pip.get("k1")
    pip.get("k2")
    ret = pip.execute()
    pybbt.ASSERT_EQ(ret, [b"v1", b"v2"])
    pybbt.ASSERT_EQ(dst.dbsize(), 2)


# Temporarily disabled: AOF reader tests.

@pybbt.case(skip=True)
def aof_to_standalone():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    src = h.Redis()
    ret = src.do("CONFIG SET", "appendonly", "yes")
    pybbt.log(f"aof_ret: {ret}")
    ret = src.do("CONFIG SET", "aof-timestamp-enabled", "yes")
    pybbt.log(f"aof_ret: {ret}")
    dst = h.Redis()
    _test_aof(src, dst)


@pybbt.case(skip=True)
def aof_to_standalone_base_file():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    dst = h.Redis()
    _test_base_file(dst)


@pybbt.case(skip=True)
def aof_to_standalone_single():
    if h.REDIS_SERVER_VERSION >= 7.0:
        return
    src = h.Redis()
    ret = src.do("CONFIG SET", "aof-use-rdb-preamble", "no")
    pybbt.log(f"aof_ret: {ret}")
    ret = src.do("CONFIG SET", "appendonly", "yes")
    pybbt.log(f"aof_ret: {ret}")
    dst = h.Redis()
    _test_aof(src, dst)


@pybbt.case(skip=True)
def aof_to_standalone_error():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    src = h.Redis()
    ret = src.do("CONFIG SET", "appendonly", "yes")
    dst = h.Redis()
    _test_error(src, dst)


@pybbt.case(skip=True)
def aof_to_standalone_rm_file():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    src = h.Redis()
    ret = src.do("CONFIG SET", "appendonly", "yes")
    dst = h.Redis()
    _test_rm_file(src, dst)


@pybbt.case(skip=True)
def aof_to_cluster():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    src = h.Redis()
    ret = src.do("CONFIG SET", "appendonly", "yes")
    pybbt.log(f"aof_ret: {ret}")
    dst = h.Cluster()
    _test_aof(src, dst)


@pybbt.case(skip=True)
def aof_to_standalone_timestamp():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    dst = h.Redis()
    ret = dst.do("FLUSHALL")
    _test_base_file_timestamp(dst)
    ret = dst.do("FLUSHALL")
    _test_base_and_incr_timestamp(dst)
    ret = dst.do("FLUSHALL")


# @pybbt.case(skip=True)
# def aof_to_standalone_history_file():
#     if h.REDIS_SERVER_VERSION < 7.0:
#         return
#     src = h.Redis()
#     ret = src.do("CONFIG SET", "aof-disable-auto-gc", "yes")
#     pybbt.log(f"aof_ret: {ret}")
#     ret = src.do("CONFIG SET", "appendonly", "yes")
#     pybbt.log(f"aof_ret: {ret}")
#     ret = src.do("CONFIG SET", "aof-timestamp-enabled", "yes")
#     pybbt.log(f"aof_ret: {ret}")
#     dst = h.Redis()
#     _test_history_file(src, dst)
