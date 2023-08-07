import time
import os

import pybbt
import redis

from helpers.constant import PATH_REDIS_SERVER, REDIS_SERVER_VERSION
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
        

        # v1
        # print(os.environ.get("moduleSupported"))
        # print(os.getenv("moduleSupported"))
        # env_file = os.getenv('GITHUB_ENV')
        # with open(env_file, "a") as myfile:
        #     print(myfile.read())
        # if os.getenv("modulesupported"):
        #     args.extend(["--loadmodule", "tairstring_module.so"])
        #     args.extend(["--loadmodule", "tairhash_module.so"])
        #     args.extend(["--loadmodule", "tairzset_module.so"])
        #     self.server = pybbt.Launcher(args=[PATH_REDIS_SERVER] + args, work_dir=self.dir)
        # else:
        #     self.server = pybbt.Launcher(args=[PATH_REDIS_SERVER] + args, work_dir=self.dir)


        # v2
        # print(os.environ.get("GITHUB_ENV"))
        # github_env_path = os.environ.get("GITHUB_ENV")
        # github_env_vars = read_github_env(github_env_path)
        # my_variable_value = github_env_vars.get('moduleSupported')
        # print(my_variable_value)


        # v3
        # print(os.getenv('GITHUB_ENV'))
        # env_file = os.getenv('GITHUB_ENV')
        # with open(env_file, 'r') as myfile:
        #     print(myfile.read())


        # v4 完善
        print(os.getenv('GITHUB_ENV'))
        env_file = os.getenv('GITHUB_ENV')
        envs = read_github_env(env_file)
        moduleSupported = envs['moduleSupported']
        print(moduleSupported)
        

        if (moduleSupported == 'true'):
            args.extend(["--loadmodule", "tairstring_module.so"])
            args.extend(["--loadmodule", "tairhash_module.so"])
            args.extend(["--loadmodule", "tairzset_module.so"])
            self.server = pybbt.Launcher(args=[PATH_REDIS_SERVER] + args, work_dir=self.dir)
        else:
            self.server = pybbt.Launcher(args=[PATH_REDIS_SERVER] + args, work_dir=self.dir)



        args.extend(["--loadmodule", "tairstring_module.so"])
        args.extend(["--loadmodule", "tairhash_module.so"])
        args.extend(["--loadmodule", "tairzset_module.so"])
        self.server = pybbt.Launcher(args=[PATH_REDIS_SERVER] + args, work_dir=self.dir)

        self._wait_start()
        self.client = redis.Redis(host=self.host, port=self.port)
        self.case_ctx.add_exit_hook(lambda: self.server.stop())
        pybbt.log_yellow(f"redis server started at {self.host}:{self.port}, redis-cli -p {self.port}")

    def _wait_start(self, timeout=10):
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





def read_github_env(env_path):
    # env_vars = {}
    # with open(env_path, 'r') as env_file:
    #     for line in env_file:
    #         key, value = line.strip().split('=')
    #         env_vars[key] = value
    # return env_vars
    with open(env_path, 'r') as file:
        lines = file.readlines()
        key_value_pairs = {}
        for line in lines:
            key, value = line.strip().split('=')
            key_value_pairs[key.strip()] = value.strip()
        return key_value_pairs