import shutil
import subprocess
from pathlib import Path

import pybbt

BASE_PATH = f"{Path(__file__).parent.parent.parent.absolute()}"  # project path
PATH_REDIS_SHAKE = f"{BASE_PATH}/bin/redis-shake"
PATH_REDIS_SERVER = shutil.which('redis-server')
output = subprocess.check_output(f"{PATH_REDIS_SERVER} --version", shell=True)
output_str = output.decode("utf-8")
REDIS_SERVER_VERSION = float(output_str.split("=")[1].split(" ")[0][:3])

# REDIS_SERVER_MODULES_ENABLED
REDIS_SERVER_MODULES_ENABLED = REDIS_SERVER_VERSION >= 5.0 and "modules" in pybbt.get_global_flags()

if __name__ == '__main__':
    print(BASE_PATH)
    print(PATH_REDIS_SHAKE)
    print(PATH_REDIS_SERVER)
    print(REDIS_SERVER_VERSION)
