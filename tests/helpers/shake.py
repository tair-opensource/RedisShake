import time
import typing

import pybbt
import requests
import toml

from helpers.constant import PATH_REDIS_SHAKE
from helpers.redis import Redis
from helpers.utils.filesystem import create_empty_dir
from helpers.utils.network import get_free_port
from helpers.utils.timer import Timer


# [SyncClusterReader]
# address = "127.0.0.1:6379"
# username = "" # keep empty if not using ACL
# password = "" # keep empty if no authentication is required
# tls = false
#
# [RedisClusterWriter]
# address = "127.0.0.1:6380"
# username = "" # keep empty if not using ACL
# password = "" # keep empty if no authentication is required
# tls = false

class ShakeOpts:
    @staticmethod
    def create_sync_opts(src: Redis, dst: Redis) -> typing.Dict:
        d = {}
        if src.is_cluster():
            d["SyncClusterReader"] = {"address": src.get_address()}
        else:
            d["SyncStandaloneReader"] = {"address": src.get_address()}
        if dst.is_cluster():
            d["RedisClusterWriter"] = {"address": dst.get_address()}
        else:
            d["RedisStandaloneWriter"] = {"address": dst.get_address()}
        return d

    @staticmethod
    def create_scan_opts(src: Redis, dst: Redis) -> typing.Dict:
        d = {}
        if src.is_cluster():
            d["ScanClusterReader"] = {"address": src.get_address()}
        else:
            d["ScanStandaloneReader"] = {"address": src.get_address()}
        if dst.is_cluster():
            d["RedisClusterWriter"] = {"address": dst.get_address()}
        else:
            d["RedisStandaloneWriter"] = {"address": dst.get_address()}
        return d

    @staticmethod
    def create_rdb_opts(rdb_path: str, dts: Redis) -> typing.Dict:
        d = {"RdbReader": {"filepath": rdb_path}}
        if dts.is_cluster():
            d["RedisClusterWriter"] = {"address": dts.get_address()}
        else:
            d["RedisStandaloneWriter"] = {"address": dts.get_address()}
        return d


class Shake:
    def __init__(self, opts: typing.Dict):
        self.case_ctx = pybbt.get_case_context()
        self.status_port = get_free_port()
        self.status_url = f"http://localhost:{self.status_port}"
        opts["advanced"] = {"status_port": self.status_port, "log_level": "debug"}

        self.dir = f"{self.case_ctx.dir}/shake{self.status_port}"
        create_empty_dir(self.dir)
        with open(f"{self.dir}/shake.toml", "w") as f:
            toml.dump(opts, f)
        self.server = pybbt.Launcher(args=[PATH_REDIS_SHAKE, "shake.toml"], work_dir=self.dir)
        self.case_ctx.add_exit_hook(lambda: self.server.stop())
        self._wait_start()

    @staticmethod
    def run_once(opts: typing.Dict):
        status_port = get_free_port()
        run_dir = f"{pybbt.get_case_context().dir}/shake{status_port}"
        create_empty_dir(run_dir)
        with open(f"{run_dir}/shake.toml", "w") as f:
            toml.dump(opts, f)
        server = pybbt.Launcher(args=[PATH_REDIS_SHAKE, "shake.toml"], work_dir=run_dir)
        server.wait_stop()

    def get_status(self):
        ret = requests.get(self.status_url)
        return ret.json()

    def _wait_start(self, timeout=5):
        timer = Timer()
        while True:
            try:
                self.get_status()
                return
            except requests.exceptions.ConnectionError:
                pass
            if timer.elapsed() > timeout:
                raise Exception(f"Shake server not started in {timeout} seconds")

    def is_consistent(self):
        return self.get_status()["consistent"]
