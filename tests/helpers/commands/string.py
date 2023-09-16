import pybbt

from helpers.commands.checker import Checker
from helpers.redis import Redis


class StringChecker(Checker):
    PREFIX = "string"

    def __init__(self):
        self.cnt = 0

    def add_data(self, r: Redis, cross_slots_cmd: bool):
        p = r.pipeline()
        p.set(f"{self.PREFIX}_{self.cnt}_str", "string")
        p.set(f"{self.PREFIX}_{self.cnt}_int", 0)
        p.set(f"{self.PREFIX}_{self.cnt}_int0", -1)
        p.set(f"{self.PREFIX}_{self.cnt}_int1", 123456789)
        ret = p.execute()
        pybbt.ASSERT_EQ(ret, [True, True, True, True])
        self.cnt += 1

    def check_data(self, r: Redis, cross_slots_cmd: bool):
        for i in range(self.cnt):
            p = r.pipeline()
            p.get(f"{self.PREFIX}_{i}_str")
            p.get(f"{self.PREFIX}_{i}_int")
            p.get(f"{self.PREFIX}_{i}_int0")
            p.get(f"{self.PREFIX}_{i}_int1")
            ret = p.execute()
            pybbt.ASSERT_EQ(ret, [b"string", b"0", b"-1", b"123456789"])
