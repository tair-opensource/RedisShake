import os
import random
import shutil
import time

import redis
import redistrib.command

import launcher

DIR = "."  # RedisShake/test
BASE_CONF_PATH = "../conf/redis-shake.conf"
SHAKE_EXE = "../bin/redis-shake.darwin"


def get_port():
    cmd = "netstat -ntl |grep -v Active| grep -v Proto|awk '{print $4}'|awk -F: '{print $NF}'"
    proc = os.popen(cmd).read()
    proc_ports = proc.split("\n")
    port = random.randint(15000, 20000)
    while port in proc_ports:
        port = random.randint(15000, 20000)
    return port


def get_work_dir(port):
    os.makedirs(f"{DIR}/tmp", exist_ok=True)

    work_dir = f"{DIR}/tmp/{port}"
    if os.path.exists(work_dir):
        shutil.rmtree(work_dir)
    os.makedirs(work_dir)

    return work_dir


def test_work_dir():
    print(get_work_dir(1234))


def load_conf(file_path):
    conf = {}
    with open(file_path, "r") as fp:
        for line in fp:
            line = line.strip()
            if line.startswith('#') or line == "":
                continue
            key, val = line.split('=')
            conf[key.strip()] = val.strip()
    return conf


def save_conf(conf, file_path):
    with open(file_path, "w") as fp:
        for k, v in conf.items():
            fp.write(f"{k}={v}\n")


def get_redis():
    port = get_port()
    work_dir = get_work_dir(f"redis_{port}")
    redis_server = launcher.Launcher(["redis-server", f"--port {port}"], work_dir)
    redis_server.fire()
    time.sleep(1)
    r = redis.Redis(port=port, socket_connect_timeout=5)
    return r, port


def get_cluster_redis(num):
    port_list = []
    r_list = []
    for _ in range(num):
        port = get_port()
        print(f"get port {port}")
        work_dir = get_work_dir(f"redis_cluster_{port}")
        redis_server = launcher.Launcher(["redis-server", f"--port {port}", "--cluster-enabled yes"], work_dir)
        redis_server.fire()
        r = redis.Redis(port=port, socket_connect_timeout=5)
        port_list.append(port)
        r_list.append(r)
    time.sleep(1)
    return port_list, r_list


def test_sync_standalone2standalone():
    r1, port1 = get_redis()
    r2, port2 = get_redis()
    conf = load_conf(BASE_CONF_PATH)
    conf["source.address"] = f"127.0.0.1:{port1}"
    conf["target.address"] = f"127.0.0.1:{port2}"
    conf["source.password_raw"] = ""
    conf["target.password_raw"] = ""
    work_dir = get_work_dir("sync_standalone2standalone")
    conf_path = f"{work_dir}/redis-shake.conf"
    save_conf(conf, conf_path)
    redis_shake = launcher.Launcher([SHAKE_EXE, "-conf", "./redis-shake.conf", "-type", "sync"], work_dir)
    redis_shake.fire()
    redis_shake.p()


# DEBUG POPULATE count [prefix] [size]
def test_sync_cluster2cluster():
    # redis start
    port_list, r_list = get_cluster_redis(6)
    print(f"redis cluster nodes:", port_list)
    # populate data
    for r, port in zip(r_list, port_list):
        r.execute_command(f"DEBUG POPULATE 100000 prefix_{port} 100000")

    redistrib.command.create([('127.0.0.1', port_list[0]),
                              ('127.0.0.1', port_list[1]),
                              ('127.0.0.1', port_list[2])], max_slots=16384)
    print(f"redis cluster source:", port_list[:3])

    redistrib.command.create([('127.0.0.1', port_list[3]),
                              ('127.0.0.1', port_list[4]),
                              ('127.0.0.1', port_list[5])], max_slots=16384)
    print(f"redis cluster target:", port_list[3:])

    conf = load_conf(BASE_CONF_PATH)
    conf["source.type"] = f"cluster"
    conf["source.address"] = f"127.0.0.1:{port_list[0]};127.0.0.1:{port_list[1]};127.0.0.1:{port_list[2]}"
    conf["source.password_raw"] = ""
    conf["target.type"] = f"cluster"
    conf["target.address"] = f"127.0.0.1:{port_list[3]};127.0.0.1:{port_list[4]};127.0.0.1:{port_list[5]}"
    conf["target.password_raw"] = ""
    conf["target.dbmap"] = ""
    conf["key_exists"] = "rewrite"
    work_dir = get_work_dir("sync_cluster2cluster")
    conf_path = f"{work_dir}/redis-shake.conf"
    save_conf(conf, conf_path)

    while 1:
        time.sleep(100000)


def test_sync_standalone2cluster():
    r, port = get_redis()
    print(f"redis source:", port)
    port_list, r_list = get_cluster_redis(3)
    redistrib.command.create([('127.0.0.1', port_list[0]),
                              ('127.0.0.1', port_list[1]),
                              ('127.0.0.1', port_list[2])], max_slots=16384)
    print(f"redis cluster target:", port_list[:3])

    conf = load_conf(BASE_CONF_PATH)
    conf["source.type"] = f"standalone"
    conf["source.address"] = f"127.0.0.1:{port}"
    conf["source.password_raw"] = ""
    conf["target.type"] = f"cluster"
    conf["target.address"] = f"127.0.0.1:{port_list[0]};127.0.0.1:{port_list[1]};127.0.0.1:{port_list[2]}"
    conf["target.password_raw"] = ""
    conf["target.dbmap"] = ""
    conf["key_exists"] = "rewrite"
    work_dir = get_work_dir("sync_standalone2cluster")
    conf_path = f"{work_dir}/redis-shake.conf"
    save_conf(conf, conf_path)

    while 1:
        time.sleep(100000)


if __name__ == '__main__':
    SHAKE_EXE = os.path.abspath(SHAKE_EXE)
    shutil.rmtree(f"{DIR}/tmp")
    # test_sync_standalone2standalone()
    test_sync_cluster2cluster()
    # test_sync_standalone2cluster()
