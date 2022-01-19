import os
import shutil
import time
import sys

import redis
import redistrib.command
import requests
from colorama import Fore, Style

import launcher


def green_print(string):
    print(Fore.GREEN + str(string) + Style.RESET_ALL)


def wait():
    while True:
        time.sleep(1024)


DIR = "."  # RedisShake/test
BASE_CONF_PATH = "../conf/redis-shake.conf"
SHAKE_PATH_LINUX = "../bin/redis-shake.linux"
SHAKE_PATH_MACOS = "../bin/redis-shake.darwin"
REDIS_PATH = "../bin/redis-server"
SHAKE_EXE = ""
REDIS_EXE = ""
USED_PORT = []
METRIC_URL = "http://127.0.0.1:9320/metric"


def get_port():
    cmd = "netstat -ntl |grep -v Active| grep -v Proto|awk '{print $4}'|awk -F: '{print $NF}'"
    proc = os.popen(cmd).read()
    proc_ports = set(proc.split("\n"))
    port = 20000
    while port in proc_ports or port in USED_PORT:
        port += 1
    USED_PORT.append(port)
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


class Redis:
    def __init__(self, port, work_dir, cluster_enable=False):
        if cluster_enable:
            self.server = launcher.Launcher(
                [REDIS_EXE, "--logfile", "redis.log", "--port", str(port), "--cluster-enabled yes"], work_dir)
        else:
            self.server = launcher.Launcher([REDIS_EXE, "--logfile", "redis.log", "--port", str(port)], work_dir)
        self.server.fire()
        self.client = None
        self.port = port
        self.work_dir = work_dir

    def wait_start(self):
        log_file = f"{self.work_dir}/redis.log"
        while not os.path.exists(log_file):
            time.sleep(0.3)
        with open(log_file, "r") as f:
            while "Ready to accept connections" not in f.readline():
                time.sleep(0.1)
        self.client = redis.Redis(port=self.port)
        print(f"Redis start at {self.port}.")

    def stop(self):
        self.server.stop()


def get_redis():
    port = get_port()
    work_dir = get_work_dir(f"redis_{port}")
    r = Redis(port, work_dir)
    r.wait_start()
    return r


def get_cluster_redis(num):
    port_list = []
    r_list = []
    for _ in range(num):
        port = get_port()
        work_dir = get_work_dir(f"redis_cluster_{port}")
        r = Redis(port, work_dir, cluster_enable=True)
        r_list.append(r)
        port_list.append(port)
    for r in r_list:
        r.wait_start()
    return port_list, r_list


def test_sync_standalone2standalone():
    r1 = get_redis()
    r2 = get_redis()
    r1.client.execute_command(f"DEBUG POPULATE 1024 prefix_{r1.port} 1024")
    r2.client.execute_command(f"DEBUG POPULATE 1024 prefix_{r2.port} 1024")
    conf = load_conf(BASE_CONF_PATH)
    conf["source.address"] = f"127.0.0.1:{r1.port}"
    conf["target.address"] = f"127.0.0.1:{r2.port}"
    conf["source.password_raw"] = ""
    conf["target.password_raw"] = ""
    work_dir = get_work_dir("sync_standalone2standalone")
    conf_path = f"{work_dir}/redis-shake.conf"
    save_conf(conf, conf_path)
    shake = launcher.Launcher([SHAKE_EXE, "-conf", "redis-shake.conf", "-type", "sync"], work_dir)
    shake.fire()
    time.sleep(3)
    ret = requests.get(METRIC_URL)
    assert ret.json()[0]["FullSyncProgress"] == 100
    print("sync successful!")

    source_cnt = int(r1.client.execute_command("dbsize"))
    target_cnt = int(r2.client.execute_command("dbsize"))
    print(f"source_cnt: {source_cnt}, target_cnt: {target_cnt}")
    assert source_cnt == target_cnt / 2 == 1024

    r1.stop()
    r2.stop()
    shake.stop()


# DEBUG POPULATE count [prefix] [size]
def test_sync_cluster2cluster():
    # redis start
    port_list, r_list = get_cluster_redis(6)
    print(f"redis cluster nodes:", port_list)

    # populate data
    for r in r_list:
        r.client.execute_command(f"DEBUG POPULATE 1024 prefix_{r.port} 1024")

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

    shake = launcher.Launcher([SHAKE_EXE, "-conf", "redis-shake.conf", "-type", "sync"], work_dir)
    shake.fire()
    time.sleep(3)
    ret = requests.get(METRIC_URL)
    assert ret.json()[0]["FullSyncProgress"] == 100
    print("sync successful!")

    source_cnt = 0
    for r in r_list[:3]:
        source_cnt += int(r.client.execute_command("dbsize"))
    target_cnt = 0
    for r in r_list[3:]:
        target_cnt += int(r.client.execute_command("dbsize"))
    print(f"source_cnt: {source_cnt}, target_cnt: {target_cnt}")
    assert source_cnt == target_cnt / 2 == 1024 * 3

    for r in r_list:
        r.stop()
    shake.stop()


def test_sync_standalone2cluster():
    r = get_redis()
    r.client.execute_command(f"DEBUG POPULATE 1024 prefix_{r.port} 1024")
    port_list, r_list = get_cluster_redis(3)
    for r_ in r_list:
        r_.client.execute_command(f"DEBUG POPULATE 1024 prefix_{r_.port} 1024")
    print(f"redis source:", r.port)
    redistrib.command.create([('127.0.0.1', port_list[0]),
                              ('127.0.0.1', port_list[1]),
                              ('127.0.0.1', port_list[2])], max_slots=16384)
    print(f"redis cluster target:", port_list)

    conf = load_conf(BASE_CONF_PATH)
    conf["source.type"] = f"standalone"
    conf["source.address"] = f"127.0.0.1:{r.port}"
    conf["source.password_raw"] = ""
    conf["target.type"] = f"cluster"
    conf["target.address"] = f"127.0.0.1:{port_list[0]};127.0.0.1:{port_list[1]};127.0.0.1:{port_list[2]}"
    conf["target.password_raw"] = ""
    conf["target.dbmap"] = ""
    conf["key_exists"] = "rewrite"
    work_dir = get_work_dir("sync_standalone2cluster")
    conf_path = f"{work_dir}/redis-shake.conf"
    save_conf(conf, conf_path)

    shake = launcher.Launcher([SHAKE_EXE, "-conf", "redis-shake.conf", "-type", "sync"], work_dir)
    shake.fire()
    time.sleep(3)
    ret = requests.get(METRIC_URL)
    assert ret.json()[0]["FullSyncProgress"] == 100
    print("sync successful!")

    source_cnt = int(r.client.execute_command("dbsize"))
    target_cnt = 0
    for r_ in r_list:
        target_cnt += int(r_.client.execute_command("dbsize"))
    print(f"source_cnt: {source_cnt}, target_cnt: {target_cnt}")
    assert source_cnt == target_cnt / 4 == 1024

    r.stop()
    for r_ in r_list:
        r_.stop()
    shake.stop()


def action_sync_standalone2standalone_bigdata():
    r1 = get_redis()
    r2 = get_redis()
    r1.client.execute_command(f"DEBUG POPULATE 1000000 prefix_{r1.port} 10")  # 4GB RAM
    conf = load_conf(BASE_CONF_PATH)
    conf["source.address"] = f"127.0.0.1:{r1.port}"
    conf["target.address"] = f"127.0.0.1:{r2.port}"
    conf["source.password_raw"] = ""
    conf["target.password_raw"] = ""
    conf["key_exists"] = "rewrite"
    work_dir = get_work_dir("action_sync_standalone2standalone_bigdata")
    conf_path = f"{work_dir}/redis-shake.conf"
    save_conf(conf, conf_path)

    print("need run redis-shake manually, and command+c to shutdown main.py")
    wait()


def action_sync_standalone2cluster():
    r = get_redis()
    port_list, r_list = get_cluster_redis(3)
    print(f"redis source:", r.port)
    redistrib.command.create([('127.0.0.1', port_list[0]),
                              ('127.0.0.1', port_list[1]),
                              ('127.0.0.1', port_list[2])], max_slots=16384)
    print(f"redis cluster target:", port_list)

    conf = load_conf(BASE_CONF_PATH)
    conf["source.type"] = f"standalone"
    conf["source.address"] = f"127.0.0.1:{r.port}"
    conf["source.password_raw"] = ""
    conf["target.type"] = f"cluster"
    conf["target.address"] = f"127.0.0.1:{port_list[0]};127.0.0.1:{port_list[1]};127.0.0.1:{port_list[2]}"
    conf["target.password_raw"] = ""
    conf["target.dbmap"] = ""
    conf["key_exists"] = "rewrite"
    work_dir = get_work_dir("action_sync_standalone2cluster")
    conf_path = f"{work_dir}/redis-shake.conf"
    save_conf(conf, conf_path)

    print("need run redis-shake manually, and command+c to shutdown main.py")
    wait()


def test_sync_select_db(target_db=-1):
    r1 = get_redis()
    r2 = get_redis()
    r1.client.execute_command("select", "1")
    for i in range(10):
        r1.client.set(str(i), "v")

    conf = load_conf(BASE_CONF_PATH)
    conf["source.address"] = f"127.0.0.1:{r1.port}"
    conf["target.address"] = f"127.0.0.1:{r2.port}"
    conf["source.password_raw"] = ""
    conf["target.password_raw"] = ""
    conf["target.db"] = target_db

    work_dir = get_work_dir("test_sync_select_db_with_target_db")
    conf_path = f"{work_dir}/redis-shake.conf"
    save_conf(conf, conf_path)
    shake = launcher.Launcher([SHAKE_EXE, "-conf", "redis-shake.conf", "-type", "sync"], work_dir)
    shake.fire()
    time.sleep(3)
    ret = requests.get(METRIC_URL)
    assert ret.json()[0]["FullSyncProgress"] == 100

    r1.client.execute_command("select", "2" if target_db == -1 else target_db)
    for i in range(10, 20):
        r1.client.set(str(i), "v20")
    time.sleep(1)

    r2.client.execute_command("select", "1" if target_db == -1 else target_db)
    for i in range(10):
        assert r2.client.get(str(i)) == b'v'

    r2.client.execute_command("select", "2" if target_db == -1 else target_db)
    for i in range(10, 20):
        assert r2.client.get(str(i)) == b'v20'

    print("sync successful!")

    r1.stop()
    r2.stop()
    shake.stop()


if __name__ == '__main__':
    if sys.platform.startswith('linux'):
        SHAKE_EXE = os.path.abspath(SHAKE_PATH_LINUX)
    elif sys.platform.startswith('darwin'):
        SHAKE_EXE = os.path.abspath(SHAKE_PATH_MACOS)
    REDIS_EXE = os.path.abspath(REDIS_PATH)
    os.system("killall -9 redis-server")
    shutil.rmtree("{DIR}/tmp", ignore_errors=True, onerror=None)
    green_print("----------- test_sync_select_db --------")
    test_sync_select_db()
    green_print("----------- test_sync_select_db with target db--------")
    test_sync_select_db(3)
    green_print("----------- test_sync_standalone2standalone --------")
    test_sync_standalone2standalone()
    green_print("----------- test_sync_cluster2cluster --------")
    test_sync_cluster2cluster()
    green_print("----------- test_sync_standalone2cluster --------")
    test_sync_standalone2cluster()

    # action_sync_standalone2standalone_bigdata()
    # action_sync_standalone2cluster()
