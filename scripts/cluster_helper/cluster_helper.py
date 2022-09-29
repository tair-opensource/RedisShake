#!/usr/bin/env python3
# encoding: utf-8
import datetime
import os
import shutil
import signal
import sys
import time
import requests
import toml
from pathlib import Path
import redis
from launcher import Launcher

USAGE = """
cluster_helper is a helper script to start many redis-shake for syncing from cluster.

Usage:
   $ python3 cluster_helper.py ./bin/redis-shake sync.toml
   or
   $ python3 cluster_helper.py ./bin/redis-shake sync.toml ./bin/filters/key_prefix.lua
"""

REDIS_SHAKE_PATH = ""
LUA_FILTER_PATH = ""
SLEEP_SECONDS = 5
stopped = False
toml_template = {}


class Shake:
    def __init__(self):
        self.metrics_port = 0
        self.launcher = None


nodes = {}


def parse_args():
    if len(sys.argv) != 3 and len(sys.argv) != 4:
        print(USAGE)
        exit(1)
    global REDIS_SHAKE_PATH, LUA_FILTER_PATH, toml_template

    # 1. check redis-shake path
    REDIS_SHAKE_PATH = sys.argv[1]
    if not Path(REDIS_SHAKE_PATH).is_file():
        print(f"redis-shake path [{REDIS_SHAKE_PATH}] is not a file")
        print(USAGE)
        exit(1)
    print(f"redis-shake path: {REDIS_SHAKE_PATH}")
    REDIS_SHAKE_PATH = os.path.abspath(REDIS_SHAKE_PATH)
    print(f"redis-shake abs path: {REDIS_SHAKE_PATH}")

    # 2. check and load toml file
    toml_template = toml.load(sys.argv[2])
    print(toml_template)
    if "username" not in toml_template["source"]:
        toml_template["source"]["username"] = ""
    if "password" not in toml_template["source"]:
        toml_template["source"]["password"] = ""
    if "tls" not in toml_template["source"]:
        toml_template["source"]["tls"] = False
    if "advanced" not in toml_template:
        toml_template["advanced"] = {}

    # 3. check filter
    if len(sys.argv) == 4:
        LUA_FILTER_PATH = sys.argv[3]
        if not Path(LUA_FILTER_PATH).is_file():
            print(f"filter path [{LUA_FILTER_PATH}] is not a file")
            print(USAGE)
            exit(1)
        print(f"filter path: {LUA_FILTER_PATH}")
        LUA_FILTER_PATH = os.path.abspath(LUA_FILTER_PATH)
        print(f"filter abs path: {LUA_FILTER_PATH}")


def stop():
    for shake in nodes.values():
        shake.launcher.stop()
    exit(0)


def loop():
    last_allow_entries_count = {address: 0 for address in nodes.keys()}
    last_disallow_entries_count = {address: 0 for address in nodes.keys()}
    while True:
        if stopped:
            stop()
        print(f"================ {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ================")

        metrics = []
        for address, shake in nodes.items():
            try:
                ret = requests.get(f"http://localhost:{shake.metrics_port}").json()
                metrics.append(ret)
            except requests.exceptions.RequestException as e:
                print(f"get metrics from [{address}] failed: {e}")

        for metric in sorted(metrics, key=lambda x: x["address"]):
            address = metric['address']
            if metric['rdb_file_size'] == 0:
                if metric['is_doing_bgsave']:
                    print(f"{address} is doing bgsave...")
                else:
                    print(f"{metric['address']} handshaking...")
            elif metric['rdb_received_size'] < metric['rdb_file_size']:
                print(f"{metric['address']} receiving rdb. "
                      f"percent=[{metric['rdb_received_size'] / metric['rdb_file_size'] * 100:.2f}]%, "
                      f"rdbFileSize=[{metric['rdb_file_size'] / 1024 / 1024 / 1024:.3f}]G, "
                      f"rdbReceivedSize=[{metric['rdb_received_size'] / 1024 / 1024 / 1024:.3f}]G")
            elif metric['rdb_send_size'] < metric['rdb_file_size']:
                print(f"{metric['address']} syncing rdb. "
                      f"percent=[{metric['rdb_send_size'] / metric['rdb_file_size'] * 100:.2f}]%, "
                      f"allowOps=[{(metric['allow_entries_count'] - last_allow_entries_count[address]) / SLEEP_SECONDS:.2f}], "
                      f"disallowOps=[{(metric['disallow_entries_count'] - last_disallow_entries_count[address]) / SLEEP_SECONDS:.2f}], "
                      f"entryId=[{metric['entry_id']}], "
                      f"InQueueEntriesCount=[{metric['in_queue_entries_count']}], "
                      f"unansweredBytesCount=[{metric['unanswered_bytes_count']}]bytes, "
                      f"rdbFileSize=[{metric['rdb_file_size'] / 1024 / 1024 / 1024:.3f}]G, "
                      f"rdbSendSize=[{metric['rdb_send_size'] / 1024 / 1024 / 1024:.3f}]G")
            else:
                print(f"{metric['address']} syncing aof. "
                      f"allowOps=[{(metric['allow_entries_count'] - last_allow_entries_count[address]) / SLEEP_SECONDS:.2f}], "
                      f"disallowOps=[{(metric['disallow_entries_count'] - last_disallow_entries_count[address]) / SLEEP_SECONDS:.2f}], "
                      f"entryId=[{metric['entry_id']}], "
                      f"InQueueEntriesCount=[{metric['in_queue_entries_count']}], "
                      f"unansweredBytesCount=[{metric['unanswered_bytes_count']}]bytes, "
                      f"diff=[{metric['aof_received_offset'] - metric['aof_applied_offset']}], "
                      f"aofReceivedOffset=[{metric['aof_received_offset']}], "
                      f"aofAppliedOffset=[{metric['aof_applied_offset']}]")
            last_allow_entries_count[address] = metric['allow_entries_count']
            last_disallow_entries_count[address] = metric['disallow_entries_count']

        time.sleep(SLEEP_SECONDS)


def main():
    parse_args()

    # parse args
    address = toml_template["source"]["address"]
    host, port = address.split(":")
    username = toml_template["source"]["username"]
    password = toml_template["source"]["password"]
    tls = toml_template["source"]["tls"]
    print(f"host: {host}, port: {port}, username: {username}, password: {password}, tls: {tls}")
    cluster = redis.RedisCluster(host=host, port=port, username=username, password=password, ssl=tls)
    print("cluster nodes:", cluster.cluster_nodes())

    # parse cluster nodes
    for address, node in cluster.cluster_nodes().items():
        if "master" in node["flags"]:
            nodes[address] = Shake()
    print(f"addresses:")
    for k in nodes.keys():
        print(k)

    # create workdir and start redis-shake
    if os.path.exists("data"):
        shutil.rmtree("data")
    os.mkdir("data")
    os.chdir("data")
    start_port = 11007
    for address in nodes.keys():
        workdir = address.replace(".", "_").replace(":", "_")

        os.mkdir(workdir)
        tmp_toml = toml_template
        tmp_toml["source"]["address"] = address
        start_port += 1
        tmp_toml["advanced"]["metrics_port"] = start_port

        with open(f"{workdir}/sync.toml", "w") as f:
            toml.dump(tmp_toml, f)

        # start redis-shake
        args = [REDIS_SHAKE_PATH, f"sync.toml"]
        if LUA_FILTER_PATH != "":
            args.append(LUA_FILTER_PATH)
        launcher = Launcher(args=args, work_dir=workdir)
        nodes[address].launcher = launcher
        nodes[address].metrics_port = start_port

    signal.signal(signal.SIGINT, signal_handler)
    print("start syncing...")
    loop()


def signal_handler(sig, frame):
    global stopped
    print("\nYou pressed Ctrl+C!")
    stopped = True


if __name__ == '__main__':
    main()
