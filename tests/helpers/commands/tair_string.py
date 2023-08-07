import pybbt

from helpers.commands.checker import Checker
from helpers.redis import Redis


class TairStringChecker(Checker):
    PREFIX = "tairString"

    def __init__(self):
        self.cnt = 0

    def add_data(self, r: Redis, cross_slots_cmd: bool):
        p = r.pipeline()

        # 时间有不确定性
        # p.execute_command("EXSET",f"{self.PREFIX}_{self.cnt}_ALL", "all_value", "ABS", 2, "FLAGS", 3)

        # different parameters type
        p.execute_command("EXSET",f"{self.PREFIX}_{self.cnt}_ABS", "abs_value", "VER",2)
        p.execute_command("EXSET",f"{self.PREFIX}_{self.cnt}_FLAGS", "flags_value", "FLAGS", 2)
        p.execute_command("Exset",f"{self.PREFIX}_{self.cnt}_EX", "ex_value", "EX", 20000)

        # different key
        p.execute_command("Exset",f"{self.PREFIX}_{self.cnt}_ALL_01", "all_value_01", "EX", 20000, "ABS", 3, "FLAGS", 4)
        p.execute_command("Exset",f"{self.PREFIX}_{self.cnt}_ALL_02", "all_value_02", "EX", 20000, "ABS", 4, "FLAGS", 5)

        ret = p.execute()
        pybbt.ASSERT_EQ(ret, [b"OK", b"OK", b"OK", b"OK", b"OK"])
        self.cnt += 1

    def check_data(self, r: Redis, cross_slots_cmd: bool):
        for i in range(self.cnt):
            p = r.pipeline()

            p.execute_command("EXGET",f"{self.PREFIX}_{i}_ABS")
            p.execute_command("EXGET",f"{self.PREFIX}_{i}_FLAGS", "WITHFLAGS")
            p.execute_command("EXGET",f"{self.PREFIX}_{i}_EX")
            p.execute_command("EXGET",f"{self.PREFIX}_{i}_ALL_01", "WITHFLAGS")
            p.execute_command("EXGET",f"{self.PREFIX}_{i}_ALL_02", "WITHFLAGS")


            ret = p.execute()
            # 需要确定一下如果一个命令返回多个值是如何封装的
            pybbt.ASSERT_EQ(ret, [[b"abs_value", 1], [b"flags_value", 1, 2], [b"ex_value", 1], [b"all_value_01", 3, 4], [b"all_value_02", 4, 5]])
