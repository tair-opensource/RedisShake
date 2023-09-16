import pybbt

from helpers.commands.checker import Checker
from helpers.redis import Redis


class SelectChecker(Checker):
    PREFIX = "select"

    def __init__(self):
        self.cnt = 0

    def add_data(self, r: Redis, cross_slots_cmd: bool):
        if not cross_slots_cmd:
            return
        p = r.pipeline()
        p.select(1)
        p.set(f"{self.PREFIX}_{self.cnt}_db1", "db1")
        p.select(2)
        p.set(f"{self.PREFIX}_{self.cnt}_db2", "db2")
        p.select(3)
        p.set(f"{self.PREFIX}_{self.cnt}_db3", "db3")
        p.select(0)
        ret = p.execute()
        pybbt.ASSERT_EQ(ret, [True, True, True, True, True, True, True])
        self.cnt += 1

    def check_data(self, r: Redis, cross_slots_cmd: bool):
        if not cross_slots_cmd:
            return
        for i in range(self.cnt):
            p = r.pipeline()
            p.select(1)
            p.get(f"{self.PREFIX}_{i}_db1")
            p.select(2)
            p.get(f"{self.PREFIX}_{i}_db2")
            p.select(3)
            p.get(f"{self.PREFIX}_{i}_db3")
            p.select(0)
            ret = p.execute()
            pybbt.ASSERT_EQ(ret, [True, b'db1', True, b'db2', True, b'db3', True])
