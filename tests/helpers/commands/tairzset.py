import pybbt

from helpers.commands.checker import Checker
from helpers.redis import Redis


class ListChecker(Checker):
    PREFIX = "tairZset"

    def __init__(self):
        self.cnt = 0

    def add_data(self, r: Redis, cross_slots_cmd: bool):
        p = r.pipeline()

        p.execute_command("EXZADD",f"{self.PREFIX}_{self.cnt}_key01", "1#2", "mem01","3#4", "mem02")
        p.execute_command("EXZADD",f"{self.PREFIX}_{self.cnt}_key01", "1.1#2.2", "mem03","3.3#4.4", "mem04")

        p.execute_command("EXZADD",f"{self.PREFIX}_{self.cnt}_key02", "1#2", "mem01","3#4", "mem02")
        p.execute_command("EXZADD",f"{self.PREFIX}_{self.cnt}_key02", "1.1#2.2", "mem03","3.3#4.4", "mem04")
        
        ret = p.execute()
        pybbt.ASSERT_EQ(ret, ["OK", "OK", "OK"])
        self.cnt += 1

    def check_data(self, r: Redis, cross_slots_cmd: bool):
        for i in range(self.cnt):
            p = r.pipeline()

            p.execute_command("EXZSCORE",f"{self.PREFIX}_{self.cnt}_key01", "1#2", "mem01","3#4", "mem02")
            p.execute_command("EXZSCORE",f"{self.PREFIX}_{self.cnt}_key01", "1.1#2.2", "mem03","3.3#4.4", "mem02")

            p.execute_command("EXZSCORE",f"{self.PREFIX}_{self.cnt}_key02", "1#2", "mem01","3#4", "mem02")
            p.execute_command("EXZSCORE",f"{self.PREFIX}_{self.cnt}_key02", "1.1#2.2", "mem03","3.3#4.4", "mem02")
            

            ret = p.execute()
            pybbt.ASSERT_EQ(ret, [b"1#2", b"3#4", b"1.1#2.2", b"3.3#4.4", b"1#2", b"3#4", b"1.1#2.2", b"3.3#4.4"])
