import pybbt

from helpers.commands.checker import Checker
from helpers.redis import Redis


class ListChecker(Checker):
    PREFIX = "tairString"

    def __init__(self):
        self.cnt = 0

    def add_data(self, r: Redis, cross_slots_cmd: bool):
        p = r.pipeline()
        # p.set(f"{self.PREFIX}_{self.cnt}_str", "string")
        # p.set(f"{self.PREFIX}_{self.cnt}_int", 0)
        # p.set(f"{self.PREFIX}_{self.cnt}_int0", -1)
        # p.set(f"{self.PREFIX}_{self.cnt}_int1", 123456789)

        # 时间有不确定性
        # p.execute_command("Exset",f"{self.PREFIX}_{self.cnt}_EX", "value01", "EX" 20000)
        # p.execute_command("Exset",f"{self.PREFIX}_{self.cnt}_ALL", "all_value", "EX" 20000, "ABS", 2, "FLAGS", 3)

        p.execute_command("EXSET",f"{self.PREFIX}_{self.cnt}_ABS", "abs_value", "EX",2)
        p.execute_command("EXSET",f"{self.PREFIX}_{self.cnt}_FLAGS", "flags_value", "FLAGS", 2)
        p.execute_command("EXSET",f"{self.PREFIX}_{self.cnt}_ALL", "all_value", "ABS", 2, "FLAGS", 3)
        

        ret = p.execute()
        pybbt.ASSERT_EQ(ret, ["OK", "OK", "OK"])
        self.cnt += 1

    def check_data(self, r: Redis, cross_slots_cmd: bool):
        for i in range(self.cnt):
            p = r.pipeline()
            # p.get(f"{self.PREFIX}_{i}_str")
            # p.get(f"{self.PREFIX}_{i}_int")
            # p.get(f"{self.PREFIX}_{i}_int0")
            # p.get(f"{self.PREFIX}_{i}_int1")

            p.execute_command("EXGET",f"{self.PREFIX}_{self.cnt}_ABS")
            p.execute_command("EXGET",f"{self.PREFIX}_{self.cnt}_FLAGS", "WITHFLAGS")
            p.execute_command("EXGET",f"{self.PREFIX}_{self.cnt}_ALL")

            ret = p.execute()
            # 需要确定一下如果一个命令返回多个值是如何封装的
            pybbt.ASSERT_EQ(ret, [b"abs_value", b"2", b"flags_value", b"1", b"2",b"all_value", b"1", b"3"])
