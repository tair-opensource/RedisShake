import time

import pybbt

import helpers as h
from helpers.utils.timer import Timer

# Redis closes normal clients idle for longer than this, but never subscribed ones.
SRC_TIMEOUT = 15
# redis-shake pings both connections every 10 seconds.
MAX_SUBSCRIPTION_IDLE = 12


def _subscription_idle(src):
    # CLIENT LIST TYPE needs Redis 5.0, so filter on psub instead.
    clients = [c for c in src.client.client_list() if int(c["psub"]) > 0]
    pybbt.ASSERT_EQ(len(clients), 1)
    return int(clients[0]["idle"])


@pybbt.case()
def ksn_keeps_idle_connections_alive():
    src = h.Redis(["--timeout", str(SRC_TIMEOUT), "--notify-keyspace-events", "EA"])
    dst = h.Redis()
    src.do("set", "key", "value")

    opts = h.ShakeOpts.create_scan_opts(src, dst)
    opts["scan_reader"]["ksn"] = True
    shake = h.Shake(opts)
    shake.wait_for_sync()

    # Keep the source quiet past its timeout: without heartbeats the DUMP
    # connection is closed and the subscription idle time keeps growing.
    timer = Timer()
    while timer.elapsed() < SRC_TIMEOUT + 5:
        pybbt.ASSERT_LT(_subscription_idle(src), MAX_SUBSCRIPTION_IDLE)
        time.sleep(1)

    # Writes and deletes a marker key, which needs the DUMP connection.
    shake.wait_for_sync()
    pybbt.ASSERT_EQ(dst.do("get", "key"), b"value")
