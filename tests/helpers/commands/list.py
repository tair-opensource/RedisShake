import pybbt

from helpers.commands.checker import Checker
from helpers.redis import Redis


class ListChecker(Checker):
    PREFIX = "list"

    def __init__(self):
        self.cnt = 0

    def add_data(self, r: Redis, cross_slots_cmd: bool):
        p = r.pipeline()
        p.lpush(f"{self.PREFIX}_{self.cnt}_list", 0)
        p.lpush(f"{self.PREFIX}_{self.cnt}_list", 1)
        p.lpush(f"{self.PREFIX}_{self.cnt}_list", 2)
        p.lpush(f"{self.PREFIX}_{self.cnt}_list_str", "string0")
        p.lpush(f"{self.PREFIX}_{self.cnt}_list_str", "string1")
        p.lpush(f"{self.PREFIX}_{self.cnt}_list_str", "string2")
        ret = p.execute()
        pybbt.ASSERT_EQ(ret, [1, 2, 3, 1, 2, 3])
        self.cnt += 1

    def check_data(self, r: Redis, cross_slots_cmd: bool):
        for i in range(self.cnt):
            p = r.pipeline()
            p.lrange(f"{self.PREFIX}_{i}_list", 0, -1)
            p.lrange(f"{self.PREFIX}_{i}_list_str", 0, -1)
            ret = p.execute()
            pybbt.ASSERT_EQ(ret, [[b"2", b"1", b"0"], [b"string2", b"string1", b"string0"]])
