import pybbt as p

import helpers as h
import os
import time
#format aof command
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
    if h.REDIS_SERVER_VERSION  >= 7.0:
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
                p.log(f"aof ready: {aof_file_path}, size={file_size}")
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

def test(src, dst):
    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    p.ASSERT_TRUE(src.do("save"))
    wait_for_aof_ready(src)

    opts = h.ShakeOpts.create_aof_opts(f"{src.dir}{get_aof_file_relative_path()}", dst)
    h.Shake.run_once(opts)
    # check data
    inserter.check_data(dst, cross_slots_cmd=cross_slots_cmd)
    p.ASSERT_EQ(src.dbsize(), dst.dbsize())

def test_base_file(dst):
    #creat manifest file
    current_directory = p.get_case_context().dir  + "_own"
    create_aof_dir(current_directory + "/appendonlydir")
    manifest_filepath = current_directory + "/appendonlydir/appendonly.aof.manifest"
    commands = []
    commands += "file appendonly.aof.1.base.aof seq 1 type b\n"
    append_to_file(manifest_filepath, commands)

    #creat aof file
    base_file_path = current_directory + "/appendonlydir/appendonly.aof.1.base.aof"
    commands = []
    commands  += format_command("set", "k1", "v1")
    commands  += format_command("set", "k2", "v2")
    append_to_file(base_file_path, commands)

    #start shake redis
    opts = h.ShakeOpts.create_aof_opts(f"{current_directory}{get_aof_file_relative_path()}", dst)
    p.log(f"opts: {opts}")
    h.Shake.run_once(opts)

    #check data
    pip = dst.pipeline()
    pip.get("k1")
    pip.get("k2")
    ret = pip.execute()
    p.ASSERT_EQ(ret, [b"v1", b"v2"])
    p.ASSERT_EQ(dst.dbsize(), 2)


def test_error(src, dst):
    #set aof
    ret = src.do("CONFIG SET", "appendonly", "yes")
    p.log(f"aof_ret: {ret}")
    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    p.ASSERT_TRUE(src.do("save"))
    wait_for_aof_ready(src)
    #destroy file
    file_path = get_aof_file_path(src)
    with open(file_path, "r+") as file:
        destroy_data = "xxxxs"
        file.seek(0, 0)
        file.write(destroy_data)


    opts = h.ShakeOpts.create_aof_opts(f"{src.dir}/appendonlydir/appendonly.aof.manifest", dst)
    p.log(f"opts: {opts}")
    h.Shake.run_once(opts)

    #cant restore
    p.ASSERT_EQ( dst.dbsize(), 0)



def test_rm_file(src, dst):
    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    p.ASSERT_TRUE(src.do("save"))
    manifest_path = wait_for_aof_ready(src)
    #rm file
    file_path = get_base_file_from_manifest(manifest_path)
    p.ASSERT_TRUE(file_path is not None)
    p.log(f"remove aof base file: {file_path}")
    os.remove(file_path)
    opts = h.ShakeOpts.create_aof_opts(f"{src.dir}{get_aof_file_relative_path()}", dst)
    h.Shake.run_once(opts)
    #cant restore
    p.ASSERT_EQ(dst.dbsize(), 0)

def test_history_file(src, dst):

    cross_slots_cmd = not (src.is_cluster() or dst.is_cluster())
    inserter = h.DataInserter()
    for i in  range(1000):
        inserter.add_data(src, cross_slots_cmd=cross_slots_cmd)
    p.ASSERT_TRUE(src.do("BGREWRITEAOF"))

    opts = h.ShakeOpts.create_aof_opts(f"{src.dir}{get_aof_file_relative_path()}", dst)
    h.Shake.run_once(opts)
    # check data
    inserter.check_data(dst, cross_slots_cmd=cross_slots_cmd)
    p.ASSERT_EQ(src.dbsize(), dst.dbsize())


def test_base_file_timestamp(dst): # base file play back all
    #creat manifest file
    current_directory = p.get_case_context().dir + "_own"
    create_aof_dir(current_directory)
    manifest_filepath = current_directory + "/appendonlydir/appendonly.aof.manifest"
    commands = []
    commands += "file appendonly.aof.1.base.aof seq 1 type b\n"
    append_to_file(manifest_filepath, commands)

    #creat aof file
    base_file_path = current_directory + "/appendonlydir/appendonly.aof.1.base.aof"
    commands = []
    commands  += "#TS1233\r\n"
    commands  += format_command("set", "k1", "v1")
    commands  += "#TS1234\r\n"
    commands  += format_command("set", "k2", "v2")
    commands  += "#TS1235\r\n"
    commands  += format_command("set", "k3", "v3")
    append_to_file(base_file_path, commands)

    #start shake redis
    opts = h.ShakeOpts.create_aof_opts(f"{current_directory}{get_aof_file_relative_path()}", dst, 1234)
    p.log(f"opts: {opts}")
    h.Shake.run_once(opts)

    #check data
    pip = dst.pipeline()
    pip.get("k1")
    pip.get("k2")
    pip.get("k3")
    ret = pip.execute()
    p.ASSERT_EQ(ret, [b"v1",b"v2",b"v3",])
    p.ASSERT_EQ(dst.dbsize(), 3)

def test_base_and_incr_timestamp(dst):

    #creat manifest file
    current_directory = p.get_case_context().dir + "_own"
    create_aof_dir(current_directory + "/appendonlydir")
    manifest_filepath = current_directory + "/appendonlydir/appendonly.aof.manifest"
    commands = []
    commands += "file appendonly.aof.1.base.aof seq 1 type b\n"
    commands += "file appendonly.aof.1.incr.aof seq 1 type i\n"
    commands += "file appendonly.aof.2.incr.aof seq 2 type i\n"
    append_to_file(manifest_filepath, commands)

    #creat aof base file
    base_file_path = current_directory + "/appendonlydir/appendonly.aof.1.base.aof"
    commands = []
    commands  += format_command("set", "k1", "v1")
    append_to_file(base_file_path, commands)

    commands = []
    #create aof incr file
    incr1_file_path = current_directory + "/appendonlydir/appendonly.aof.1.incr.aof"
    commands  += "#TS1233\r\n"
    commands  += format_command("set", "k2", "v2")
    append_to_file(incr1_file_path , commands)

    commands = []
    incr2_file_path = current_directory + "/appendonlydir/appendonly.aof.2.incr.aof"
    commands  += "#TS1235\r\n"
    commands  += format_command("set", "k3", "v3")
    append_to_file(incr2_file_path , commands)
    #start shake redis
    opts = h.ShakeOpts.create_aof_opts(f"{current_directory}{get_aof_file_relative_path()}", dst, 1234)
    p.log(f"opts: {opts}")
    h.Shake.run_once(opts)

    #check data
    pip = dst.pipeline()
    pip.get("k1")
    pip.get("k2")
    ret = pip.execute()
    p.ASSERT_EQ(ret, [b"v1",b"v2"])
    p.ASSERT_EQ(dst.dbsize(), 2)


def aof_to_standalone():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    src = h.Redis()
    #set aof
    ret = src.do("CONFIG SET", "appendonly", "yes")
    p.log(f"aof_ret: {ret}")

    ret = src.do("CONFIG SET", "aof-timestamp-enabled", "yes")
    p.log(f"aof_ret: {ret}")
    dst = h.Redis()
    test(src, dst)

def aof_to_standalone_base_file():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    dst = h.Redis()
    test_base_file(dst)


def aof_to_standalone_rm_file():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    src = h.Redis()
    #set aof
    ret = src.do("CONFIG SET", "appendonly", "yes")
    dst = h.Redis()
    test_rm_file(src, dst)

def aof_to_standalone_error():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    src = h.Redis()
    #set aof
    ret = src.do("CONFIG SET", "appendonly", "yes")
    dst = h.Redis()
    test_error(src, dst)

def aof_to_cluster():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    src = h.Redis()
    #set aof
    ret = src.do("CONFIG SET", "appendonly", "yes")
    p.log(f"aof_ret: {ret}")
    dst = h.Cluster()
    test(src, dst)

def aof_to_standalone_single():
    if h.REDIS_SERVER_VERSION >= 7.0:
        return
    src = h.Redis()
    #set preamble no
    ret = src.do("CONFIG SET", "aof-use-rdb-preamble", "no")
    p.log(f"aof_ret: {ret}")
    #set aof
    ret = src.do("CONFIG SET", "appendonly", "yes")
    p.log(f"aof_ret: {ret}")
    dst = h.Redis()
    test(src, dst)

def aof_to_standalone_timestamp():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    dst = h.Redis()

    ret = dst.do("FLUSHALL")
    test_base_file_timestamp(dst)
    ret = dst.do("FLUSHALL")
    test_base_and_incr_timestamp(dst)
    ret = dst.do("FLUSHALL")

def aof_to_standalone_history_file():
    if h.REDIS_SERVER_VERSION < 7.0:
        return
    src = h.Redis()
    #set aof
    #set hist
    ret = src.do("CONFIG SET", "aof-disable-auto-gc", "yes")
    p.log(f"aof_ret: {ret}")

    ret = src.do("CONFIG SET", "appendonly", "yes")
    p.log(f"aof_ret: {ret}")

    ret = src.do("CONFIG SET", "aof-timestamp-enabled", "yes")
    p.log(f"aof_ret: {ret}")

    dst = h.Redis()
    test_history_file(src, dst)

# Temporarily disabled: AOF reader test entry.
@p.case(skip=True)
def main():
    aof_to_standalone() # base + incr aof-multi
    aof_to_standalone_base_file() # base file aof-multi
    aof_to_standalone_single() #single aof
    aof_to_standalone_error() # error aof file
    aof_to_standalone_rm_file() # rm aof file
    # aof_to_standalone_history_file() # history + incr aof-multi
    aof_to_cluster() #test cluster
    aof_to_standalone_timestamp() #set timestamp aof-multi


if __name__ == '__main__':
    main()
