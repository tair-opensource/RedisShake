import pybbt

from helpers.commands.checker import Checker
from helpers.constant import REDIS_SERVER_MODULES_ENABLED
from helpers.redis import Redis


class TairHashChecker(Checker):
    PREFIX = "tairHash"

    def __init__(self):
        self.cnt = 0

    def add_data(self, r: Redis, cross_slots_cmd: bool):
        if not REDIS_SERVER_MODULES_ENABLED:
            return

        p = r.pipeline()
        # different parameters type
        p.execute_command("EXHSET", f"{self.PREFIX}_{self.cnt}", "field", "value")
        p.execute_command("EXHSET", f"{self.PREFIX}_{self.cnt}_ABS", "field_abs", "value_abs", "ABS", 2)
        p.execute_command("EXHSET", f"{self.PREFIX}_{self.cnt}_EX", "field_ex", "value_ex", "EX", 20000)

        # different key
        # different field
        p.execute_command("EXHSET", f"{self.PREFIX}_{self.cnt}_ALL_01", "field_all_01", "value_all_01", "EX", 20000, "ABS", 2)
        p.execute_command("EXHSET", f"{self.PREFIX}_{self.cnt}_ALL_01", "field_all_02", "value_all_02", "EX", 20000, "ABS", 3)

        p.execute_command("EXHSET", f"{self.PREFIX}_{self.cnt}_ALL_02", "field_all_01", "value_all_01", "EX", 20000, "ABS", 2)
        p.execute_command("EXHSET", f"{self.PREFIX}_{self.cnt}_ALL_02", "field_all_02", "value_all_02", "EX", 20000, "ABS", 3)

        ret = p.execute()
        # pybbt.ASSERT_EQ(ret, [b"1", b"1", b"1", b"1",b"1", b"1",b"1"])
        pybbt.ASSERT_EQ(ret, [1, 1, 1, 1, 1, 1, 1])

        self.cnt += 1

    def check_data(self, r: Redis, cross_slots_cmd: bool):
        if not REDIS_SERVER_MODULES_ENABLED:
            return

        for i in range(self.cnt):
            p = r.pipeline()

            p.execute_command("EXHGET", f"{self.PREFIX}_{i}", "field")
            p.execute_command("EXHGET", f"{self.PREFIX}_{i}_ABS", "field_abs")
            p.execute_command("EXHGET", f"{self.PREFIX}_{i}_EX", "field_ex")

            p.execute_command("EXHGET", f"{self.PREFIX}_{i}_ALL_01", "field_all_01")
            p.execute_command("EXHGET", f"{self.PREFIX}_{i}_ALL_01", "field_all_02")

            p.execute_command("EXHGET", f"{self.PREFIX}_{i}_ALL_02", "field_all_01")
            p.execute_command("EXHGET", f"{self.PREFIX}_{i}_ALL_02", "field_all_02")

            ret = p.execute()
            # 需要确定一下如果一个命令返回多个值是如何封装的
            pybbt.ASSERT_EQ(ret, [b"value", b"value_abs", b"value_ex", b"value_all_01", b"value_all_02", b"value_all_01", b"value_all_02", ])
