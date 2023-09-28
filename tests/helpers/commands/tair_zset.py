import pybbt

from helpers.commands.checker import Checker
from helpers.constant import REDIS_SERVER_MODULES_ENABLED
from helpers.redis import Redis


class TairZsetChecker(Checker):
    PREFIX = "tairZset"

    def __init__(self):
        self.cnt = 0

    def add_data(self, r: Redis, cross_slots_cmd: bool):
        if not REDIS_SERVER_MODULES_ENABLED:
            return

        p = r.pipeline()

        # different key
        # int or float
        p.execute_command("EXZADD", f"{self.PREFIX}_{self.cnt}_key01", "1.1#1.2", "mem01", "2.2#2.3", "mem02")
        p.execute_command("EXZADD", f"{self.PREFIX}_{self.cnt}_key01", "3.3#3.4", "mem03", "4.4#4.5", "mem04")
        p.execute_command("EXZADD", f"{self.PREFIX}_{self.cnt}_key02", "1.1#1.2", "mem01")
        p.execute_command("EXZADD", f"{self.PREFIX}_{self.cnt}_key02", "2.2#2.3", "mem02")
        ret = p.execute()
        pybbt.ASSERT_EQ(ret, [2, 2, 1, 1])

        self.cnt += 1

    def check_data(self, r: Redis, cross_slots_cmd: bool):
        if not REDIS_SERVER_MODULES_ENABLED:
            return

        for i in range(self.cnt):
            p = r.pipeline()
            p.execute_command("EXZSCORE", f"{self.PREFIX}_{i}_key01", "mem01")
            p.execute_command("EXZSCORE", f"{self.PREFIX}_{i}_key01", "mem02")
            p.execute_command("EXZSCORE", f"{self.PREFIX}_{i}_key01", "mem03")
            p.execute_command("EXZSCORE", f"{self.PREFIX}_{i}_key01", "mem04")
            p.execute_command("EXZSCORE", f"{self.PREFIX}_{i}_key02", "mem01")
            p.execute_command("EXZSCORE", f"{self.PREFIX}_{i}_key02", "mem02")

            ret = p.execute()
            pybbt.ASSERT_EQ(ret,
                            [b'1.1000000000000001#1.2',
                             b'2.2000000000000002#2.2999999999999998', b'3.2999999999999998#3.3999999999999999',
                             b'4.4000000000000004#4.5', b'1.1000000000000001#1.2',
                             b'2.2000000000000002#2.2999999999999998'])
