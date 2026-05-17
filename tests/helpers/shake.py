import os
import typing
import time
import uuid

import pybbt
import requests
import toml
from helpers.constant import PATH_REDIS_SHAKE
from helpers.redis import Redis
from helpers.utils.filesystem import create_empty_dir
from helpers.utils.network import get_free_port
from helpers.utils.timer import Timer

# Get the tests directory as base path for resolving relative paths
_TESTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _abs_path(path: str) -> str:
    """Convert relative path to absolute path based on tests directory."""
    if os.path.isabs(path):
        return path
    return os.path.join(_TESTS_DIR, path)


class ShakeOpts:
    @staticmethod
    def create_sync_opts(src: Redis, dst: Redis) -> typing.Dict:
        d = {
            "sync_reader": {
                "cluster": src.is_cluster(),
                "address": src.get_address()
            },
            "redis_writer": {
                "cluster": dst.is_cluster(),
                "address": dst.get_address()
            },
            "__src": src,
            "__dst": dst,
        }
        return d

    @staticmethod
    def create_scan_opts(src: Redis, dst: Redis) -> typing.Dict:
        d = {
            "scan_reader": {
                "cluster": src.is_cluster(),
                "address": src.get_address()
            },
            "redis_writer": {
                "cluster": dst.is_cluster(),
                "address": dst.get_address()
            },
            "__src": src,
            "__dst": dst,
        }
        return d

    @staticmethod
    def create_rdb_opts(rdb_path: str, dst: Redis) -> typing.Dict:
        d = {
            "rdb_reader": {"filepath": _abs_path(rdb_path)},
            "redis_writer": {
                "cluster": dst.is_cluster(),
                "address": dst.get_address()
            },
            "__dst": dst,
        }
        return d

    @staticmethod
    def create_aof_opts(aof_path: str, dst: Redis, timestamp: int = 0) -> typing.Dict:
        d = {
            "aof_reader": {"filepath": _abs_path(aof_path), "timestamp": timestamp},
            "redis_writer": {
                "cluster": dst.is_cluster(),
                "address": dst.get_address()
            },
            "advanced": {
                "log_level": "debug"
            },
            "__dst": dst,
        }
        return d


class Shake:
    def __init__(self, opts: typing.Dict):
        self.case_ctx = pybbt.get_case_context()
        self.status_port = get_free_port()
        self.status_url = f"http://localhost:{self.status_port}"

        # 提取并保存 src/dst 引用
        self.src = opts.pop("__src", None)
        self.dst = opts.pop("__dst", None)

        advanced = opts.setdefault("advanced", {})
        advanced["status_port"] = self.status_port
        advanced.setdefault("log_level", "debug")

        self.dir = f"{self.case_ctx.dir}/shake{self.status_port}"
        create_empty_dir(self.dir)
        with open(f"{self.dir}/shake.toml", "w") as f:
            toml.dump(opts, f)
        self.server = pybbt.Launcher(args=[PATH_REDIS_SHAKE, "shake.toml"], work_dir=self.dir)
        self.case_ctx.add_exit_hook(lambda: self.server.stop())
        self._wait_start()

    @staticmethod
    def run_once(opts: typing.Dict):
        # 移除内部字段
        opts.pop("__src", None)
        opts.pop("__dst", None)
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
            except (requests.exceptions.ConnectionError, requests.exceptions.JSONDecodeError):
                pass
            if timer.elapsed() > timeout:
                raise Exception(f"Shake server not started in {timeout} seconds")

    def is_consistent(self):
        return self.get_status()["consistent"]

    def wait_for_sync(self, timeout: float = 60, db: int = 0):
        """
        Wait for data to be fully synced from src to dst.

        1. Wait for is_consistent() - RDB sync done
        2. Check dbsize matches
        3. Write marker key to src
        4. Wait for marker to appear in dst
        5. Delete marker key
        6. Wait for consistent again
        """
        if self.src is None or self.dst is None:
            raise ValueError("src and dst not available, use ShakeOpts.create_sync_opts() or create_scan_opts()")

        timer = Timer()

        # 1. Wait for is_consistent()
        while not self.is_consistent():
            if timer.elapsed() > timeout:
                raise TimeoutError(f"is_consistent() not reached within {timeout}s")
            time.sleep(0.01)

        # 2. Check dbsize (cluster doesn't support SELECT, use db 0)
        if not self.src.is_cluster():
            self.src.do("select", db)
        if not self.dst.is_cluster():
            self.dst.do("select", db)
        src_dbsize = self.src.dbsize()
        while self.dst.dbsize() != src_dbsize:
            if timer.elapsed() > timeout:
                raise TimeoutError(f"dbsize mismatch within {timeout}s: src={src_dbsize}, dst={self.dst.dbsize()}")
            time.sleep(0.01)

        # 3. Write marker key
        marker_key = f"__shake_sync_marker_{uuid.uuid4()}"
        marker_value = str(time.time())
        pybbt.log(f"Writing marker key: {marker_key}")
        self.src.do("set", marker_key, marker_value)

        # 4. Wait for marker in dst
        while True:
            result = self.dst.do("get", marker_key)
            if result == marker_value.encode():
                break
            if timer.elapsed() > timeout:
                # Log current status for debugging
                status = self.get_status()
                pybbt.log(f"Sync status: consistent={status.get('consistent')}, "
                         f"src_dbsize={src_dbsize}, dst_dbsize={self.dst.dbsize()}")
                raise TimeoutError(f"marker key '{marker_key}' not synced within {timeout}s")
            time.sleep(0.1)  # Increased from 0.01 to 0.1 for cluster stability

        # 5. Delete marker key
        self.src.do("del", marker_key)
        pybbt.log(f"Deleted marker key from src: {marker_key}")

        # 6. Wait for marker to be deleted in dst
        while True:
            result = self.dst.do("get", marker_key)
            if result is None:
                pybbt.log(f"Marker key deleted from dst: {marker_key}")
                break
            if timer.elapsed() > timeout:
                # Log detailed status for debugging
                status = self.get_status()
                pybbt.log(f"Timeout waiting for marker deletion. Status: consistent={status.get('consistent')}")
                pybbt.log(f"Marker key still exists in dst with value: {result}")
                raise TimeoutError(f"marker key not deleted within {timeout}s")
            time.sleep(0.1)  # Increased from 0.01 to 0.1 for cluster stability

        # 7. Wait for consistent again
        while not self.is_consistent():
            if timer.elapsed() > timeout:
                raise TimeoutError(f"final is_consistent() not reached within {timeout}s")
            time.sleep(0.01)
