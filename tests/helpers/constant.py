import shutil
import subprocess
from pathlib import Path

import pybbt

BASE_PATH = f"{Path(__file__).parent.parent.parent.absolute()}"  # project path
PATH_REDIS_SHAKE = f"{BASE_PATH}/bin/redis-shake"
PATH_REDIS_SERVER = shutil.which('redis-server')
output = subprocess.check_output(f"{PATH_REDIS_SERVER} --version", shell=True)
output_str = output.decode("utf-8")
_ver_parts = output_str.split("=")[1].split(" ")[0].split(".")
REDIS_SERVER_VERSION_TUPLE = (int(_ver_parts[0]), int(_ver_parts[1]))
REDIS_SERVER_VERSION = float(f"{_ver_parts[0]}.{_ver_parts[1]}")

# Detect if running Valkey or Redis
IS_VALKEY = "Valkey" in output_str or "valkey" in output_str

# Hash field expiration support: Redis >= 8.0 or Valkey >= 9.0
# Exclude unstable/dev versions (255.x.x) which may not have all features
HASH_FIELD_EXPIRATION_SUPPORTED = (
    (not IS_VALKEY and (8, 0) <= REDIS_SERVER_VERSION_TUPLE < (255, 0)) or
    (IS_VALKEY and (9, 0) <= REDIS_SERVER_VERSION_TUPLE < (255, 0))
)

# Stream XACKDEL/XDELEX support: Redis >= 8.2 only (not in Valkey yet)
STREAM_XACKDEL_SUPPORTED = not IS_VALKEY and (8, 2) <= REDIS_SERVER_VERSION_TUPLE < (255, 0)

# Redis >= 8.12 enables cluster-bus-port-protected-mode by default when cluster-enabled yes
CLUSTER_BUS_PORT_PROTECTED_MODE_DEFAULT = not IS_VALKEY and (8, 12) <= REDIS_SERVER_VERSION_TUPLE < (255, 0)

# REDIS_SERVER_MODULES_ENABLED
REDIS_SERVER_MODULES_ENABLED = REDIS_SERVER_VERSION >= 5.0 and "modules" in pybbt.get_global_flags()

if __name__ == '__main__':
    print(BASE_PATH)
    print(PATH_REDIS_SHAKE)
    print(PATH_REDIS_SERVER)
    print(REDIS_SERVER_VERSION)
