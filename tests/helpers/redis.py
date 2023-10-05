import time

import pybbt
import redis

from helpers.constant import PATH_REDIS_SERVER, REDIS_SERVER_MODULES_ENABLED, REDIS_SERVER_VERSION
from helpers.utils.network import get_free_port
from helpers.utils.timer import Timer


class Redis:
    def __init__(self, args=None):
        self.case_ctx = pybbt.get_case_context()
        if args is None:
            args = []
        self.host = "127.0.0.1"
        self.port = get_free_port()
        self.dir = f"{self.case_ctx.dir}/redis_{self.port}"
        args.extend(["--port", str(self.port)])
        if REDIS_SERVER_MODULES_ENABLED:
            args.extend(["--loadmodule", "tairstring_module.so"])
            args.extend(["--loadmodule", "tairhash_module.so"])
            args.extend(["--loadmodule", "tairzset_module.so"])
        self.server = pybbt.Launcher(args=[PATH_REDIS_SERVER] + args, work_dir=self.dir)

        self._wait_start()
        self.client = redis.Redis(host=self.host, port=self.port)
        self.case_ctx.add_exit_hook(lambda: self.server.stop())
        pybbt.log_yellow(f"redis server started at {self.host}:{self.port}, redis-cli -p {self.port}")

    def _wait_start(self, timeout=5):
        timer = Timer()
        while True:
            try:
                r = redis.Redis(host=self.host, port=self.port)
                r.ping()
                return
            except redis.exceptions.ConnectionError:
                time.sleep(0.1)
            if timer.elapsed() > timeout:
                stdout = f"{self.dir}/stdout"
                with open(stdout, "r") as f:
                    for line in f.readlines():
                        pybbt.log_red(line.strip())
                raise TimeoutError("redis server not started")

    def do(self, *args):
        try:
            ret = self.client.execute_command(*args)
        except redis.exceptions.ResponseError as e:
            return f"-{str(e)}"
        return ret

    def pipeline(self):
        return self.client.pipeline(transaction=False)

    def get_address(self):
        return f"{self.host}:{self.port}"

    def is_cluster(self):
        if REDIS_SERVER_VERSION < 3.0:
            return False
        return self.client.info()["cluster_enabled"]

    def dbsize(self):
        return self.client.dbsize()
